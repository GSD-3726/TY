#!/usr/bin/env python3
"""
IPTV 组播提取工具（配置版：酒店/组播 二选一）
- 配置区直接选择 酒店提取 或 组播提取
- 点击开始提取后等待30秒再提取数据
- 支持流式测速（基于下载速度）、适配GitHub Actions
- 【日志精简】仅显示进度条和最终统计，不输出每个链接的详细日志
- 【取消预检】直接测速，不进行连通性预检
- 【实时进度】进度条每5%立即更新
"""

import asyncio
import json
import logging
import os
import re
import sys
import time
import shutil
import datetime
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any
import functools
from urllib.parse import urlparse

import aiohttp
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError


# ============================================================================
# ======================== 【中文配置区】=====================================
# ============================================================================

# -------------------------- 1. 网站与爬取设置 -------------------------------
TARGET_URL            = "https://iptv.809899.xyz"       # 目标网站地址
HEADLESS              = True                            # 无头模式（GitHub运行必须True）
BROWSER_TYPE          = "chromium"                      # 浏览器内核
MAX_IPS               = 5                              # 最多处理多少个IP
MAX_TOTAL_CHANNELS     = 0                               # 总频道上限（0=不限制）
PAGE_LOAD_TIMEOUT      = 120000                          # 页面加载超时（毫秒）

# -------------------------- 2. 提取模式选择【核心：二选一】------------------
# 请在这里直接选择，只能填一个：
# "酒店提取"  或者  "组播提取"
EXTRACT_MODE          = "酒店提取"

# -------------------------- 3. 输出文件设置 --------------------------------
OUTPUT_DIR            = Path(__file__).parent           # 输出目录（当前脚本目录）
OUTPUT_M3U_FILENAME   = OUTPUT_DIR / "iptv_channels.m3u"
OUTPUT_TXT_FILENAME   = OUTPUT_DIR / "iptv_channels.txt"
MAX_LINKS_PER_CHANNEL = 5                               # 每个频道最多保留几条链接

# -------------------------- 4. 测速设置（流式测速）--------------------------
ENABLE_SPEED_TEST     = True                            # 是否启用测速
REQUEST_TIMEOUT       = 8                               # 请求超时时间（秒）
STREAM_TEST_BYTES     = 512 * 1024                      # 测速下载字节数（512KB）
SPEED_CONCURRENCY     = 10                              # 并发测速数量
MIN_SPEED_KB          = 100.0                           # 最低有效速度（KB/s）

# -------------------------- 5. GitHub 源订阅设置 ---------------------------
ENABLE_GITHUB_SOURCES = True                            # 是否启用GitHub源
GITHUB_M3U_LINKS = [
    "https://gh-proxy.com/https://raw.githubusercontent.com/hujingguang/ChinaIPTV/main/cnTV_AutoUpdate.m3u8",
    "https://gh-proxy.com/https://raw.githubusercontent.com/ioptu/IPTV.txt2m3u.player/main/migu.m3u",
    "https://gh-proxy.com/https://raw.githubusercontent.com/ioptu/IPTV.txt2m3u.player/main/httop_merged.m3u",
    "https://gh-proxy.com/https://raw.githubusercontent.com/ioptu/IPTV.txt2m3u.player/main/httop.m3u",
    "https://gh-proxy.com/https://raw.githubusercontent.com/ioptu/IPTV.txt2m3u.player/main/htldx_merged.m3u",
    "https://gh-proxy.com/https://raw.githubusercontent.com/3377/IPTV/master/output/result.txt",
]

# -------------------------- 6. 延时与等待设置 ------------------------------
DELAY_BETWEEN_IPS      = 0.5                             # 切换IP间隔（秒）
DELAY_AFTER_CLICK      = 1.0                             # 点击弹窗等待（秒）
MAX_CHANNELS_PER_IP    = 0                               # 单个IP最多提取频道数
DATA_LOAD_TIMEOUT      = 120                             # 数据加载超时（秒）
AFTER_START_WAIT       = 30                              # 点击【开始提取】后等待秒数

# -------------------------- 7. 数据清洗设置 --------------------------------
ENABLE_CHINESE_CLEAN   = True                            # 清理非中文字符
ENABLE_DEDUPLICATION   = True                            # 全局去重
ENABLE_SCREENSHOTS     = False                           # 调试截图
CCTV_USE_MAPPING       = True                            # CCTV映射中文名称

# -------------------------- 8. 网络协议 ------------------------------------
DEFAULT_PROTOCOL       = "http://"                       # 默认协议

# -------------------------- 9. 测速缓存设置 --------------------------------
ENABLE_CACHE           = True                            # 启用测速缓存
CACHE_FILE             = OUTPUT_DIR / "iptv_speed_cache.json"
CACHE_EXPIRE_HOURS     = 48                              # 缓存过期小时

# -------------------------- 10. 更新信息显示 --------------------------------
TIME_DISPLAY_AT_TOP    = False                           # 更新时间是否放顶部
UPDATE_STREAM_URL      = "https://gitee.com/bmg369/tvtest/raw/master/cg/index.m3u8"

# -------------------------- 11. 页面按钮文字匹配 ----------------------------
PAGE_CONFIG = {
    "engine_search": ["引索搜索", "引擎搜索", "关键词搜索"],
    "hotel":        ["酒店提取"],
    "multicast":    ["组播提取"],
    "start_button": ["开始播放", "开始搜索", "开始提取"],
}

# -------------------------- 12. 其他配置（预检已禁用）-----------------------
ENABLE_URL_PRE_CHECK   = False   # 已禁用，直接测速
SKIP_INTERNAL_IP       = True    # 是否跳过内网IP
ENABLE_VERBOSE_LOGGING = False   # 详细日志已关闭


# ============================================================================
# ============================ 频道分类规则 ==================================
# ============================================================================
CATEGORY_RULES = [
    {"name": "4K专区",      "keywords": ["4k"]},
    {"name": "央视频道",    "keywords": ["cctv", "cetv", "中央"]},
    {"name": "卫视频道",    "keywords": ["卫视", "凤凰", "tvb", "湖南", "浙江", "江苏", "东方"]},
    {"name": "电影频道",    "keywords": ["电影", "影院", "chc"]},
    {"name": "轮播频道",    "keywords": ["轮播"]},
    {"name": "儿童频道",    "keywords": ["少儿", "动画", "卡通"]},
]

GROUP_ORDER = ["央视频道", "卫视频道", "电影频道", "4K专区", "儿童频道", "轮播频道"]

CCTV_NAME_MAPPING = {
    "1": "综合", "2": "财经", "3": "综艺", "4": "国际", "5": "体育",
    "5+": "体育赛事", "6": "电影", "7": "国防军事", "8": "电视剧",
    "9": "纪录", "10": "科教", "11": "戏曲", "12": "社会与法",
    "13": "新闻", "14": "少儿", "15": "音乐", "16": "奥林匹克",
    "17": "农业农村",
}

CCTV_ORDER = [
    "CCTV-1综合", "CCTV-2财经", "CCTV-3综艺", "CCTV-4国际",
    "CCTV-5体育", "CCTV-5+体育赛事", "CCTV-6电影", "CCTV-7国防军事",
    "CCTV-8电视剧", "CCTV-9纪录", "CCTV-10科教", "CCTV-11戏曲",
    "CCTV-12社会与法", "CCTV-13新闻", "CCTV-14少儿", "CCTV-15音乐",
    "CCTV-16奥林匹克", "CCTV-17农业农村", "CETV1", "CETV2", "CETV4", "CETV5"
]

# ============================================================================
# ============================= 日志配置（北京时间） ===========================
# ============================================================================
log_level = logging.INFO  # 固定INFO级别，不输出DEBUG

class BeijingFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.datetime.fromtimestamp(
            record.created,
            datetime.timezone(datetime.timedelta(hours=8))
        )
        s = dt.strftime("%Y-%m-%d %H:%M:%S")
        return f"{s},{int(record.msecs):03d}"

logger = logging.getLogger('IPTV-Extractor')
logger.setLevel(log_level)
logger.handlers.clear()

stdout_h = logging.StreamHandler(sys.stdout)
file_h = logging.FileHandler(OUTPUT_DIR / 'iptv_extractor.log', encoding='utf-8')
formatter = BeijingFormatter("%(asctime)s - %(levelname)s - %(message)s")
stdout_h.setFormatter(formatter)
file_h.setFormatter(formatter)
logger.addHandler(stdout_h)
logger.addHandler(file_h)

# ============================================================================
# ========================= 工具函数 =========================================
# ============================================================================
CCTV_PATTERN = re.compile(r'(cctv)[-\s]?(\d{1,3}|5\+)', re.IGNORECASE)
CHINESE_ONLY_PATTERN = re.compile(r'[^\u4e00-\u9fff]')
INTERNAL_IP_PATTERN = re.compile(r'^(192\.168\.|10\.|172\.(1[6-9]|2[0-9]|3[0-1])\.|127\.0\.0\.1)')

def build_classifier():
    compiled = []
    for rule in CATEGORY_RULES:
        if not rule["keywords"]: continue
        pattern = re.compile("|".join(re.escape(kw.lower()) for kw in rule["keywords"]))
        compiled.append((rule["name"], pattern))
    return lambda name: next((group for group, pat in compiled if pat.search(name.lower())), None)

classify_channel = build_classifier()

def normalize_cctv(name: str) -> str:
    name_lower = name.lower()
    if "cctv5+" in name_lower:
        return "CCTV-5+体育赛事" if CCTV_USE_MAPPING else "CCTV5+"
    cctv_match = CCTV_PATTERN.search(name_lower)
    if cctv_match:
        num = cctv_match.group(2)
        if CCTV_USE_MAPPING and num in CCTV_NAME_MAPPING:
            return f"CCTV-{num}{CCTV_NAME_MAPPING[num]}"
        return f"CCTV-{num}"
    return name

def clean_chinese_only(name: str) -> str:
    return CHINESE_ONLY_PATTERN.sub('', name)

def build_selector(text_list, element_type="button"):
    if not text_list: return ""
    if len(text_list) == 1:
        return f"{element_type}:has-text('{text_list[0]}')"
    pattern = "|".join(re.escape(t) for t in text_list)
    return f"{element_type}:text-matches('{pattern}')"

def is_internal_ip(url: str) -> bool:
    """判断URL是否指向内网IP"""
    try:
        host = urlparse(url).hostname
        if not host:
            return False
        return bool(INTERNAL_IP_PATTERN.match(host))
    except:
        return False

# ============================================================================
# ========================= 缓存管理（兼容旧格式）==============================
# ============================================================================
CACHE_EXPIRE_SECONDS = CACHE_EXPIRE_HOURS * 3600

def load_cache():
    if not ENABLE_CACHE or not CACHE_FILE.exists():
        return {}
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            raw_cache = json.load(f)
        converted = {}
        now = time.time()
        for url, value in raw_cache.items():
            if isinstance(value, (int, float)):
                converted[url] = {"speed": value, "timestamp": now}
            elif isinstance(value, dict) and "speed" in value:
                if now - value.get("timestamp", 0) < CACHE_EXPIRE_SECONDS:
                    converted[url] = value
            else:
                logger.debug(f"忽略无法识别的缓存项: {url}")
        logger.info(f"缓存加载完成，有效条目数: {len(converted)}")
        return converted
    except json.JSONDecodeError:
        logger.debug("缓存文件损坏或为空，将重新创建")
        return {}
    except Exception as e:
        logger.debug(f"加载缓存异常: {e}，将重新创建")
        return {}

def save_cache(cache):
    if not ENABLE_CACHE:
        return
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning(f"保存缓存失败: {e}")

def is_cache_valid(timestamp):
    return time.time() - timestamp < CACHE_EXPIRE_SECONDS

# ============================================================================
# ========================= 重试、进度、测速 ==================================
# ============================================================================
def retry_async(max_retries=2, delay=1.0, exceptions=(Exception,)):
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            for attempt in range(1, max_retries+1):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_retries: raise
                    logger.warning(f"尝试 {attempt}/{max_retries} 失败: {e}，重试")
                    await asyncio.sleep(delay)
            return None
        return wrapper
    return decorator

def print_progress_bar(current: int, total: int, success: int, failed: int, last_percent: int) -> int:
    if total == 0:
        return 0
    percent_int = int((current / total) * 100)
    if not ((percent_int % 5 == 0 and percent_int > last_percent) or current == total or current == 0):
        return last_percent
    if percent_int == last_percent and current != total:
        return last_percent
    bar = '█' * int(20 * current / total) + '░' * (20 - int(20 * current / total))
    logger.info(f"[{percent_int:3d}%] {bar} ({current}/{total}) | 成功:{success} | 失败:{failed}")
    sys.stdout.flush()
    return percent_int

async def test_stream_speed(url: str) -> float:
    """异步测速：下载指定字节数，计算速度（KB/s），失败返回0"""
    try:
        start = time.time()
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as resp:
                if resp.status != 200:
                    return 0.0
                size = 0
                async for chunk in resp.content.iter_chunked(64 * 1024):
                    size += len(chunk)
                    if size >= STREAM_TEST_BYTES:
                        break
                elapsed = time.time() - start
                if elapsed == 0:
                    return 0.0
                speed = size / elapsed / 1024
                return round(speed, 2)
    except Exception:
        return 0.0

# ============================================================================
# ========================= 测速主流程（简化版）===============================
# ============================================================================
async def run_speed_test(channel_map: Dict[Tuple[str, str], List[str]]) -> Dict[Tuple[str, str], List[str]]:
    """
    对频道映射中的链接进行测速筛选，返回每个频道有效链接列表。
    取消预检，直接测速；日志仅输出进度条。
    """
    if not channel_map:
        return {}
    cache = load_cache() if ENABLE_CACHE else {}
    new_cache = {}
    result_map = defaultdict(list)
    total = sum(len(us) for us in channel_map.values())
    cached_ok = 0
    pending = []

    # 1. 检查缓存，跳过内网IP
    for (g, n), us in channel_map.items():
        for u in us:
            cache_item = cache.get(u)
            if cache_item and isinstance(cache_item, dict) and "speed" in cache_item:
                if is_cache_valid(cache_item.get("timestamp", 0)):
                    speed = cache_item["speed"]
                    if speed > MIN_SPEED_KB:
                        result_map[(g, n)].append((u, speed))
                    cached_ok += 1
                    continue
            if SKIP_INTERNAL_IP and is_internal_ip(u):
                continue
            pending.append((g, n, u))

    logger.info(f"总链接:{total} 缓存有效:{cached_ok} 需测速:{len(pending)}")
    if not pending:
        final = {}
        for k, vs in result_map.items():
            vs.sort(key=lambda x: -x[1])
            final[k] = [u for u, _ in vs[:MAX_LINKS_PER_CHANNEL]]
        return final

    # 2. 并发测速（无预检）
    sem = asyncio.Semaphore(SPEED_CONCURRENCY)

    async def test_one(item):
        g, n, u = item
        async with sem:
            speed = await test_stream_speed(u)
            return g, n, u, speed

    tasks = [test_one(i) for i in pending]
    c, ok, ng, lp = 0, 0, 0, -100
    print_progress_bar(0, len(tasks), ok, ng, lp)

    for coro in asyncio.as_completed(tasks):
        g, n, u, speed = await coro
        c += 1
        if speed > MIN_SPEED_KB:
            ok += 1
            result_map[(g, n)].append((u, speed))
            if ENABLE_CACHE:
                new_cache[u] = {"speed": speed, "timestamp": time.time()}
        else:
            ng += 1
            if ENABLE_CACHE:
                new_cache[u] = {"speed": 0, "timestamp": time.time()}
        lp = print_progress_bar(c, len(tasks), ok, ng, lp)

    # 3. 更新缓存
    if ENABLE_CACHE and new_cache:
        cache.update(new_cache)
        save_cache(cache)

    # 4. 排序并截取
    final = {}
    for k, vs in result_map.items():
        vs.sort(key=lambda x: -x[1])
        final[k] = [u for u, _ in vs[:MAX_LINKS_PER_CHANNEL]]

    logger.info(f"测速完成，共 {len(final)} 个频道通过筛选")
    return final

# ============================================================================
# ========================= GitHub M3U 解析 ==================================
# ============================================================================
@retry_async(max_retries=3, delay=2)
async def download_github_m3u(url):
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as s:
            async with s.get(url, headers={'User-Agent': 'Mozilla/5.0'}) as r:
                if r.status == 200:
                    t = await r.text()
                    logger.info(f"下载成功 {url}")
                    return t
    except Exception as e:
        logger.debug(f"下载失败 {url}: {e}")
    return ""

def parse_m3u_file(content):
    ch = []
    g = n = u = ""
    for l in content.splitlines():
        l = l.strip()
        if l.startswith("#EXTINF"):
            m = re.search(r'#EXTINF:-1.*?group-title="([^"]*)",(.+)', l)
            if m:
                g = m.group(1).strip()
                n = m.group(2).strip()
            else:
                m = re.search(r'#EXTINF:-1.*?,(.+)', l)
                if m:
                    n = m.group(1).strip()
        elif l.startswith("http"):
            u = l.split("?")[0]
            if n and u:
                nn = normalize_cctv(n)
                gr = classify_channel(nn) or g
                fn = nn if gr == "央视频道" else (clean_chinese_only(n) if ENABLE_CHINESE_CLEAN else n)
                ch.append((gr, fn, u))
            g = n = u = ""
    return ch

# ============================================================================
# ========================= 页面点击与提取 ===================================
# ============================================================================
async def robust_click(loc, timeout=10000):
    try:
        await loc.scroll_into_view_if_needed(timeout=3000)
        await asyncio.sleep(0.2)
        await loc.click(force=True, timeout=timeout)
        return True
    except:
        try:
            await loc.evaluate("el=>el.click()")
            return True
        except:
            return False

async def wait_for_element(page, sel, timeout=30000):
    try:
        await page.wait_for_selector(sel, timeout=timeout)
        return True
    except:
        return False

@retry_async(max_retries=2, delay=1)
async def extract_one_ip(page, row, idx):
    e = []
    try:
        addr = await row.locator("div.item-title").first.inner_text(timeout=3000)
        addr = addr.strip()
        if not addr:
            return []
        logger.info(f"处理IP [{idx}]: {addr}")
    except:
        return []
    try:
        btn = row.locator("button:has(i.fa-list)").first
        if await btn.count() > 0:
            if not await robust_click(btn):
                await row.click()
        else:
            await row.click()
        await asyncio.sleep(DELAY_AFTER_CLICK)
        if not await wait_for_element(page, ".modal-dialog", 5000):
            return []
        items = page.locator(".modal-dialog .item-content")
        total = await items.count()
        if total == 0:
            return []
        if MAX_CHANNELS_PER_IP > 0:
            total = min(total, MAX_CHANNELS_PER_IP)
        for i in range(total):
            try:
                n = await items.nth(i).locator(".item-title").inner_text(timeout=2000)
                u = await items.nth(i).locator(".item-subtitle").inner_text(timeout=2000)
                n, u = n.strip(), u.strip()
                if not n or not u:
                    continue
                if not u.startswith(('http://', 'https://', 'rtsp://', 'rtmp://')):
                    u = DEFAULT_PROTOCOL + u
                nn = normalize_cctv(n)
                g = classify_channel(nn)
                if not g:
                    continue
                fn = nn if g == "央视频道" else (clean_chinese_only(n) if ENABLE_CHINESE_CLEAN else n)
                e.append((g, fn, u))
            except:
                continue
    except:
        pass
    return e

async def wait_data(page):
    logger.info("等待数据加载...")
    for _ in range(DATA_LOAD_TIMEOUT // 30 + 1):
        await asyncio.sleep(30)
        ok = await page.evaluate('''()=>{
            for(let i of document.querySelectorAll('div.ios-list-item')){
                let s=i.querySelector('.item-subtitle')?.innerText||'';
                if(s.includes('频道:'))return true;
            }return false;
        }''')
        if ok:
            logger.info("数据加载完成")
            return True
    logger.error("数据加载超时，爬取失败")
    return False

# ============================================================================
# ========================= 结果导出 =========================================
# ============================================================================
def export_results_with_timestamp(channel_map):
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    gu = UPDATE_STREAM_URL
    g = defaultdict(list)
    for (gr, n), us in channel_map.items():
        for u in us:
            g[gr].append((n, u))
    with open(OUTPUT_M3U_FILENAME, 'w', encoding='utf-8') as f:
        f.write("#EXTM3U\n")
        if TIME_DISPLAY_AT_TOP:
            f.write(f'#EXTINF:-1 tvg-name="更新" group-title="更新时间",{now}\n{gu}\n\n')
        for gro in GROUP_ORDER:
            if gro not in g:
                continue
            chs = g[gro]
            if gro == "央视频道":
                d = {n: u for n, u in chs}
                chs = [(n, d[n]) for n in CCTV_ORDER if n in d]
            else:
                chs = sorted(chs, key=lambda x: x[0])
            for n, u in chs:
                f.write(f'#EXTINF:-1 group-title="{gro}",{n}\n{u}\n')
            f.write("\n")
        if not TIME_DISPLAY_AT_TOP:
            f.write(f'#EXTINF:-1 group-title="更新时间",{now}\n{gu}\n')
    with open(OUTPUT_TXT_FILENAME, 'w', encoding='utf-8') as f:
        if TIME_DISPLAY_AT_TOP:
            f.write("更新时间,#genre#\n")
            f.write(f"{now},{gu}\n\n")
        for gro in GROUP_ORDER:
            if gro not in g:
                continue
            f.write(f"{gro},#genre#\n")
            chs = g[gro]
            if gro == "央视频道":
                d = {n: u for n, u in chs}
                chs = [(n, d[n]) for n in CCTV_ORDER if n in d]
            else:
                chs = sorted(chs, key=lambda x: x[0])
            for n, u in chs:
                f.write(f"{n},{u}\n")
            f.write("\n")
        if not TIME_DISPLAY_AT_TOP:
            f.write("更新时间,#genre#\n")
            f.write(f"{now},{gu}\n")
    logger.info(f"导出完成：{len(channel_map)} 个频道")

# ============================================================================
# ========================= 主流程 ===========================================
# ============================================================================
async def main():
    if EXTRACT_MODE not in ["酒店提取", "组播提取"]:
        logger.error("配置错误！EXTRACT_MODE 只能填写：酒店提取 或 组播提取")
        return

    logger.info(f"✅ 当前运行模式：【{EXTRACT_MODE}】")
    all_channels = []

    if ENABLE_GITHUB_SOURCES:
        for url in GITHUB_M3U_LINKS:
            txt = await download_github_m3u(url)
            if txt:
                channels = parse_m3u_file(txt)
                all_channels.extend(channels)

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=HEADLESS,
            args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage", "--disable-gpu", "--single-process"]
        )
        ctx = await browser.new_context(viewport={"width": 1920, "height": 1080})
        page = await ctx.new_page()

        try:
            logger.info(f"正在访问：{TARGET_URL}")
            await page.goto(TARGET_URL, timeout=PAGE_LOAD_TIMEOUT, wait_until="networkidle")

            eng_sel = build_selector(PAGE_CONFIG["engine_search"], "a.sidebar-link,button,div.segment-item")
            if eng_sel:
                eng = page.locator(eng_sel).first
                if await eng.count() > 0:
                    logger.info("点击引擎搜索")
                    await robust_click(eng)

            if EXTRACT_MODE == "酒店提取":
                tab_sel = build_selector(PAGE_CONFIG["hotel"], "div.segment-item")
                logger.info("点击网页按钮：【酒店提取】")
            else:
                tab_sel = build_selector(PAGE_CONFIG["multicast"], "div.segment-item")
                logger.info("点击网页按钮：【组播提取】")

            tab = page.locator(tab_sel).first
            await robust_click(tab)

            start_sel = build_selector(PAGE_CONFIG["start_button"], "button")
            start_btn = page.locator(start_sel).first
            logger.info("点击【开始提取】")
            await robust_click(start_btn)

            logger.info(f"⏳ 等待 {AFTER_START_WAIT} 秒后开始提取数据...")
            await asyncio.sleep(AFTER_START_WAIT)

            if not await wait_data(page):
                logger.error("❌ 网站爬取失败")
            else:
                rows = page.locator("div.ios-list-item").filter(
                    has=page.locator("div.item-subtitle:has-text('频道:')")
                )
                total_rows = await rows.count()
                process_count = min(total_rows, MAX_IPS) if MAX_IPS > 0 else total_rows
                logger.info(f"准备处理前 {process_count} 个IP")

                web_channels = []
                for i in range(process_count):
                    entries = await extract_one_ip(page, rows.nth(i), i + 1)
                    if entries:
                        web_channels.extend(entries)
                    if MAX_TOTAL_CHANNELS > 0 and len(web_channels) >= MAX_TOTAL_CHANNELS:
                        web_channels = web_channels[:MAX_TOTAL_CHANNELS]
                        logger.info("已达频道上限，停止提取")
                        break
                    await asyncio.sleep(DELAY_BETWEEN_IPS)

                all_channels.extend(web_channels)
                logger.info(f"网站提取完成：{len(web_channels)} 条")

        except Exception as e:
            logger.exception("❌ 爬取过程异常")
        finally:
            await page.close()
            await ctx.close()
            await browser.close()

    if not all_channels:
        logger.error("❌ 未获取到任何频道")
        return

    # 去重
    channel_map = defaultdict(list)
    seen = set()
    for g, n, u in all_channels:
        key = (g, n, u)
        if key in seen:
            continue
        seen.add(key)
        channel_map[(g, n)].append(u)

    if ENABLE_SPEED_TEST:
        logger.info("开始流式测速筛选")
        channel_map = await run_speed_test(channel_map)

    export_results_with_timestamp(channel_map)
    logger.info("🎉 任务全部完成！")

if __name__ == "__main__":
    if sys.platform == 'linux':
        asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())
    elif sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
