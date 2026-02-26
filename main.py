#!/usr/bin/env python3
"""
IPTV 组播提取工具 - 流畅不卡顿版（优化版 + 实时日志 + 测速缓存）
解决：TS片段切换卡顿、加载慢、播放断断续续、日志不实时显示、重复测速
"""

import asyncio
import json
import logging
import os
import re
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any
from urllib.parse import urljoin
import functools

import aiohttp
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# ============================================================================
# ======================== 【配置区 · 全中文说明】 =============================
# ============================================================================

# 1. 网站与浏览器设置 --------------------------------------------------------
TARGET_URL = "https://iptv.809899.xyz"          # 目标网站地址（支持HTTP/HTTPS）
HEADLESS = True                                  # 是否无头模式（True不显示浏览器窗口）
BROWSER_TYPE = "chromium"                        # 浏览器类型：chromium/firefox/webkit
MAX_IPS = 15                                      # 最多提取前N个IP地址的频道（若为域名也会计入）
PAGE_LOAD_TIMEOUT = 120000                       # 页面加载超时（毫秒）

# 2. 输出文件设置 ------------------------------------------------------------
OUTPUT_M3U_FILENAME = "iptv_channels.m3u"        # 生成的M3U播放列表文件名
OUTPUT_TXT_FILENAME = "iptv_channels.txt"        # 生成的TXT格式文件名
MAX_LINKS_PER_CHANNEL = 10                        # 每个频道保留的最优链接数量

# 3. 测速总开关 --------------------------------------------------------------
ENABLE_SPEED_TEST = True                         # 是否进行速度测试（False则直接保存所有提取到的链接）

# 4. 测速并发控制 ------------------------------------------------------------
SPEED_TEST_CONCURRENCY = 5                       # 同时测速的协程数（网络带宽大可提高，否则易超时）
SPEED_TEST_TIMEOUT = 10                          # 每个链接测速总超时（秒）
SPEED_TEST_VERBOSE = False                        # 是否打印详细测速日志

# 5. 测速采样参数 ------------------------------------------------------------
TS_SAMPLE_COUNT = 5                               # 每个HLS流下载的TS片段数量（越多越准，但耗时）
TS_DOWNLOAD_TIMEOUT = 5                           # 单个TS片段下载超时（秒）
GENERIC_SAMPLE_SIZE = 1024 * 1024                 # 通用测速时下载的样本大小（字节，默认1MB）
GENERIC_DOWNLOAD_TIMEOUT = 10                     # 通用测速下载超时（秒）

# 6. 流畅度判定标准 ----------------------------------------------------------
MIN_STABLE_SPEED = 1.0                            # 稳定播放所需最低速度（Mbps）

# 7. 分辨率过滤 --------------------------------------------------------------
ENABLE_RESOLUTION_FILTER = False                    # 是否根据分辨率过滤低清频道
MIN_RESOLUTION_WIDTH = 1280                         # 最小宽度（像素）
MIN_RESOLUTION_HEIGHT = 720                         # 最小高度（像素）
FALLBACK_TO_SPEED_WHEN_NO_RESOLUTION = False        # 若无分辨率信息，是否仅根据速度保留（True则保留）

# 8. 页面操作延迟 ------------------------------------------------------------
DELAY_BETWEEN_IPS = 1.0                             # 处理完一个IP后等待秒数
DELAY_AFTER_CLICK = 0.5                             # 点击后等待秒数（等待弹窗加载）
MAX_CHANNELS_PER_IP = 0                              # 每个IP最多提取频道数（0为不限制）

# 9. 数据清洗与去重 ----------------------------------------------------------
ENABLE_CHINESE_CLEAN = True                         # 是否移除频道名中的非中文字符（仅对非央视频道）
ENABLE_DEDUPLICATION = True                          # 是否去重（相同频道名+链接只保留一个）
ENABLE_SCREENSHOTS = False                           # 是否在关键步骤截图（用于调试）
CCTV_USE_MAPPING = True                              # 是否将CCTV数字映射为中文名称（如CCTV-1综合）

# 10. 网络与安全设置 ---------------------------------------------------------
ENABLE_SSL_VERIFY = False                            # 是否验证SSL证书（False可避免某些证书错误）

# 11. 协议补全设置 -----------------------------------------------------------
DEFAULT_PROTOCOL = "http://"                         # 当链接缺少协议时自动添加的默认协议（可改为 rtsp:// 等）

# 12. 缓存设置 ---------------------------------------------------------------
ENABLE_CACHE = True                                  # 是否启用缓存（相同链接跳过测速）
CACHE_FILE = "iptv_speed_cache.json"                # 缓存文件路径
CACHE_EXPIRE_HOURS = 24                              # 缓存过期时间（小时），0表示永不过期

# ============================================================================
# ============================ 频道分类规则 ==================================
# ============================================================================

OUTPUT_DIR = Path(__file__).parent

# 页面元素选择器配置（基于文本，提高鲁棒性）
PAGE_CONFIG = {
    "engine_search": ["引索搜索", "引擎搜索", "关键词搜索"],
    "multicast_tab": ["酒店提取"],
    "start_button": ["开始播放", "开始搜索", "开始提取"],
}

# 频道分类规则（关键词匹配）
CATEGORY_RULES = [
    {"name": "4K专区",      "keywords": ["4k"]},
    {"name": "央视频道",    "keywords": ["cctv", "cetv", "中央"]},
    {"name": "卫视频道",    "keywords": ["卫视", "凤凰", "tvb", "湖南", "浙江", "江苏", "东方"]},
    {"name": "电影频道",    "keywords": ["电影", "影院", "chc"]},
    {"name": "轮播频道",    "keywords": ["轮播"]},
    {"name": "儿童频道",    "keywords": ["少儿", "动画", "卡通"]},
]

# 输出分组顺序
GROUP_ORDER = [
    "央视频道", "卫视频道", "电影频道", "4K专区", "儿童频道", "轮播频道"
]

# CCTV频道号映射
CCTV_NAME_MAPPING = {
    "1": "综合", "2": "财经", "3": "综艺", "4": "国际", "5": "体育",
    "5+": "体育赛事", "6": "电影", "7": "国防军事", "8": "电视剧",
    "9": "纪录", "10": "科教", "11": "戏曲", "12": "社会与法",
    "13": "新闻", "14": "少儿", "15": "音乐", "16": "奥林匹克",
    "17": "农业农村",
}

# ============================================================================
# ============================= 日志配置 (修改版) =============================
# ============================================================================

class UnbufferedStreamHandler(logging.StreamHandler):
    """自定义Handler：强制每次输出后立即刷新缓冲区，解决日志延迟显示问题"""
    def emit(self, record):
        super().emit(record)
        self.flush()

# 配置日志系统
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        UnbufferedStreamHandler(sys.stdout),  # 使用自定义的无缓冲Handler输出到屏幕
        logging.FileHandler('iptv_extractor.log', encoding='utf-8') # 正常输出到文件
    ]
)
logger = logging.getLogger('IPTV-Extractor')

# ============================================================================
# ============================= 缓存工具函数 ==================================
# ============================================================================

def load_cache() -> Dict[str, Dict[str, Any]]:
    """加载测速缓存，自动清理过期数据"""
    if not ENABLE_CACHE:
        return {}
    
    cache_path = Path(CACHE_FILE)
    if not cache_path.exists():
        return {}

    try:
        with open(cache_path, "r", encoding="utf-8") as f:
            cache = json.load(f)
        
        now = time.time()
        expire_seconds = CACHE_EXPIRE_HOURS * 3600
        valid_cache = {}
        
        for url, data in cache.items():
            # 检查是否过期
            if expire_seconds == 0 or (now - data.get("timestamp", 0)) < expire_seconds:
                valid_cache[url] = data
        
        # 如果清理了过期数据，立即回写
        if len(valid_cache) != len(cache):
            save_cache(valid_cache)
            logger.info(f"缓存清理：移除 {len(cache) - len(valid_cache)} 条过期记录")
            
        return valid_cache
    except Exception as e:
        logger.warning(f"加载缓存失败，将忽略缓存: {e}")
        return {}

def save_cache(cache: Dict[str, Dict[str, Any]]):
    """保存测速缓存到文件"""
    if not ENABLE_CACHE:
        return
    
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning(f"保存缓存失败: {e}")

# ============================================================================
# ============================= 工具函数 =====================================
# ============================================================================

IP_PATTERN = re.compile(r'^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$')
CCTV_PATTERN = re.compile(r'(cctv)[-\s]?(\d{1,3})', re.IGNORECASE)
CETV_PATTERN = re.compile(r'(cetv)[-\s]?(\d)', re.IGNORECASE)
RESOLUTION_PATTERN = re.compile(r'(\d+)x(\d+)')
CHINESE_ONLY_PATTERN = re.compile(r'[^\u4e00-\u9fff]')

def build_classifier():
    """构建频道分类器"""
    compiled = []
    for rule in CATEGORY_RULES:
        if not rule["keywords"]:
            continue
        pattern = re.compile("|".join(re.escape(kw.lower()) for kw in rule["keywords"]))
        compiled.append((rule["name"], pattern))
    return lambda name: next((group for group, pat in compiled if pat.search(name.lower())), None)

classify_channel = build_classifier()

def normalize_cctv(name: str) -> str:
    """标准化CCTV/CETV频道名称"""
    name_lower = name.lower()
    # 特殊处理CCTV5+
    if "cctv5+" in name_lower or "cctv-5+" in name_lower:
        return "CCTV-5+体育赛事" if CCTV_USE_MAPPING else "CCTV5+"
    cctv_match = CCTV_PATTERN.search(name_lower)
    if cctv_match:
        num = cctv_match.group(2)
        if CCTV_USE_MAPPING and num in CCTV_NAME_MAPPING:
            return f"CCTV-{num}{CCTV_NAME_MAPPING[num]}"
        return f"CCTV-{num}"
    cetv_match = CETV_PATTERN.search(name_lower)
    if cetv_match:
        return f"CETV-{cetv_match.group(2)}"
    return name

def clean_chinese_only(name: str) -> str:
    """移除频道名中的非中文字符（保留中文）"""
    return CHINESE_ONLY_PATTERN.sub('', name)

def build_selector(text_list, element_type="button"):
    """根据文本列表生成组合选择器"""
    if not text_list:
        return ""
    if len(text_list) == 1:
        return f"{element_type}:has-text('{text_list[0]}')"
    pattern = "|".join(re.escape(t) for t in text_list)
    return f"{element_type}:text-matches('{pattern}')"

# 页面元素选择器
ENGINE_SELECTOR = build_selector(PAGE_CONFIG["engine_search"], "a.sidebar-link,button,div.segment-item")
MCAST_SELECTOR = build_selector(PAGE_CONFIG["multicast_tab"], "div.segment-item")
START_SELECTOR = build_selector(PAGE_CONFIG["start_button"], "button")

# ============================================================================
# ========================= 重试装饰器 =======================================
# ============================================================================

def retry_async(max_retries=3, delay=1.0, exceptions=(Exception,)):
    """异步函数重试装饰器"""
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            for attempt in range(1, max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_retries:
                        raise
                    logger.warning(f"尝试 {attempt}/{max_retries} 失败: {e}，{delay}秒后重试...")
                    await asyncio.sleep(delay)
            return None
        return wrapper
    return decorator

# ============================================================================
# ========================= 流畅度检测核心代码 ================================
# ============================================================================

async def fetch_url(session: aiohttp.ClientSession, url: str, timeout: int, headers: dict = None) -> Optional[bytes]:
    """通用下载函数，返回二进制内容或None"""
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout), headers=headers or {}) as resp:
            if resp.status == 200:
                return await resp.read()
    except asyncio.TimeoutError:
        logger.debug(f"下载超时: {url}")
    except Exception as e:
        logger.debug(f"下载失败 {url}: {e}")
    return None

async def test_stream(session: aiohttp.ClientSession, url: str) -> Tuple[bool, float, Optional[Tuple[int, int]]]:
    """
    测试流媒体速度，返回 (是否达标, 平均速度Mbps, 分辨率(如果有))
    支持HLS(m3u8)和普通HTTP流
    """
    # 首先尝试作为HLS处理
    is_hls = False
    resolution = None
    content = await fetch_url(session, url, 5)  # 先快速获取m3u8内容
    if content:
        txt = content.decode("utf-8", "ignore")
        lines = txt.splitlines()
        # 检测是否为m3u8
        if any(line.startswith("#EXTM3U") for line in lines):
            is_hls = True
            # 解析分辨率（从EXT-X-STREAM-INF或EXTINF中）
            base = url[:url.rfind('/')+1] if '/' in url else url
            ts_list = []
            for line in lines:
                if line.startswith("#EXT-X-STREAM-INF:"):
                    m = RESOLUTION_PATTERN.search(line)
                    if m:
                        resolution = (int(m.group(1)), int(m.group(2)))
                elif not line.startswith("#") and line.strip():
                    ts_list.append(urljoin(base, line.strip()))
            if not ts_list:
                # 可能是单一的m3u8，尝试使用通用测速
                pass
            else:
                # 采样下载TS片段测速
                sample = ts_list[:min(TS_SAMPLE_COUNT, len(ts_list))]
                speeds = []
                for ts_url in sample:
                    t0 = time.monotonic()
                    data = await fetch_url(session, ts_url, TS_DOWNLOAD_TIMEOUT)
                    cost = time.monotonic() - t0
                    if data and cost > 0:
                        speed = (len(data) * 8) / cost / 1e6  # Mbps
                        speeds.append(speed)
                if speeds:
                    avg_speed = sum(speeds) / len(speeds)
                    return avg_speed >= MIN_STABLE_SPEED, avg_speed, resolution

    # 如果不是HLS或HLS测速失败，尝试通用测速（下载前N字节）
    logger.debug(f"使用通用测速: {url}")
    headers = {"Range": f"bytes=0-{GENERIC_SAMPLE_SIZE-1}"}  # 尝试范围请求
    t0 = time.monotonic()
    data = await fetch_url(session, url, GENERIC_DOWNLOAD_TIMEOUT, headers=headers)
    cost = time.monotonic() - t0
    if data and cost > 0:
        speed = (len(data) * 8) / cost / 1e6
        # 如果数据量小于请求范围，可能已经是完整文件，但速度达标仍视为有效
        return speed >= MIN_STABLE_SPEED, speed, resolution
    return False, 0.0, resolution

async def test_speed_task(url: str, sem: asyncio.Semaphore, session: aiohttp.ClientSession):
    """单个测速任务，受信号量控制，返回 (url, 是否达标, 速度, 分辨率)"""
    async with sem:
        try:
            ok, speed, resolution = await test_stream(session, url)
            
            # 即使不达标，也返回结果以便存入缓存（避免下次重复测无效链接）
            if not ok:
                return (url, False, speed, resolution)
                
            # 分辨率过滤
            if ENABLE_RESOLUTION_FILTER and resolution:
                w, h = resolution
                if w < MIN_RESOLUTION_WIDTH or h < MIN_RESOLUTION_HEIGHT:
                    return (url, False, speed, resolution)
            elif ENABLE_RESOLUTION_FILTER and not resolution and not FALLBACK_TO_SPEED_WHEN_NO_RESOLUTION:
                return (url, False, speed, resolution)
                
            return (url, True, speed, resolution)
        except Exception as e:
            logger.debug(f"测速异常 {url}: {e}")
            return (url, False, 0.0, None)

async def run_speed_test(channel_map: Dict[Tuple[str, str], List[str]]) -> Dict[Tuple[str, str], List[str]]:
    """
    对频道映射进行测速，返回每个频道过滤后的链接列表
    加入了缓存逻辑：相同链接直接读取历史结果
    """
    if not channel_map:
        return {}

    # 1. 加载缓存
    cache = load_cache()
    new_cache_entries = {}  # 本次新产生的缓存数据

    # 2. 准备数据结构
    result_map = defaultdict(list)  # 最终结果 {(group, name): [(url, speed), ...]}
    pending_tasks = []              # 需要进行测速的元数据列表 (group, name, url)

    # 3. 分流：缓存命中 vs 需要测速
    for (group, name), urls in channel_map.items():
        for url in urls:
            if url in cache:
                # --- 缓存命中 ---
                data = cache[url]
                ok = data["ok"]
                speed = data["speed"]
                resolution = tuple(data["resolution"]) if data["resolution"] else None
                
                if ok:
                    # 重新应用分辨率过滤（防止过滤规则变更）
                    passed_filter = True
                    if ENABLE_RESOLUTION_FILTER and resolution:
                        w, h = resolution
                        if w < MIN_RESOLUTION_WIDTH or h < MIN_RESOLUTION_HEIGHT:
                            passed_filter = False
                    elif ENABLE_RESOLUTION_FILTER and not resolution and not FALLBACK_TO_SPEED_WHEN_NO_RESOLUTION:
                        passed_filter = False
                    
                    if passed_filter:
                        result_map[(group, name)].append((url, speed))
            else:
                # --- 缓存未命中，加入待测试队列 ---
                pending_tasks.append((group, name, url))

    logger.info(f"缓存命中 {len([item for sublist in result_map.values() for item in sublist])} 条，待测速 {len(pending_tasks)} 条")

    # 4. 执行并发测速（仅针对未命中缓存的链接）
    if pending_tasks:
        conn = aiohttp.TCPConnector(ssl=ENABLE_SSL_VERIFY)
        async with aiohttp.ClientSession(connector=conn) as session:
            sem = asyncio.Semaphore(SPEED_TEST_CONCURRENCY)
            
            # 构建协程任务列表
            tasks = []
            for (group, name, url) in pending_tasks:
                tasks.append(test_speed_task(url, sem, session))
            
            # 等待所有测速完成
            speed_test_results = await asyncio.gather(*tasks)

            # 5. 处理测速结果并更新缓存
            for i, res in enumerate(speed_test_results):
                url, ok, speed, resolution = res
                group, name, _ = pending_tasks[i]
                
                # 记录到新缓存（无论是否达标，都记录以避免下次重测）
                new_cache_entries[url] = {
                    "ok": ok,
                    "speed": speed,
                    "resolution": list(resolution) if resolution else None,
                    "timestamp": time.time()
                }
                
                # 如果达标，加入结果集
                if ok:
                    result_map[(group, name)].append((url, speed))

    # 6. 合并并保存缓存
    if new_cache_entries:
        cache.update(new_cache_entries)
        save_cache(cache)
        logger.info(f"缓存更新：新增 {len(new_cache_entries)} 条记录")

    # 7. 排序并截取前N个链接
    final_map = {}
    for key, items in result_map.items():
        items.sort(key=lambda x: -x[1])  # 按速度降序
        final_map[key] = [url for url, _ in items[:MAX_LINKS_PER_CHANNEL]]

    logger.info(f"处理完成，保留 {sum(len(v) for v in final_map.values())} 条优质链接")
    return final_map

# ============================================================================
# ============================ 页面交互函数 ==================================
# ============================================================================

async def robust_click(locator, timeout=10000):
    """稳健点击，支持多种方式"""
    try:
        await locator.scroll_into_view_if_needed(timeout=3000)
        await asyncio.sleep(0.2)
        await locator.click(force=True, timeout=timeout)
        return True
    except Exception as e:
        logger.debug(f"常规点击失败，尝试JS点击: {e}")
        try:
            await locator.evaluate("el => el.click()")
            return True
        except Exception as e2:
            logger.debug(f"JS点击也失败: {e2}")
            return False

async def wait_for_element(page, selector, state="visible", timeout=30000):
    """等待元素出现，带超时"""
    try:
        await page.wait_for_selector(selector, state=state, timeout=timeout)
        return True
    except PlaywrightTimeoutError:
        return False

@retry_async(max_retries=2, delay=1.0, exceptions=(Exception,))
async def extract_one_ip(page, row, ip_index):
    """从单个地址行提取频道信息，支持重试和截图"""
    entries = []
    addr = None
    try:
        addr_elem = row.locator("div.item-title").first
        addr = await addr_elem.inner_text(timeout=3000)
        addr = addr.strip()
        if not addr:  # 只检查非空，不再验证IP格式
            return []
        logger.info(f"处理地址 [{ip_index}]: {addr}")
    except Exception as e:
        logger.warning(f"提取地址失败: {e}")
        if ENABLE_SCREENSHOTS:
            await page.screenshot(path=f"screenshots/addr_fail_{ip_index}.png")
        return []

    # 点击展开按钮
    try:
        list_btn = row.locator("button:has(i.fa-list)").first
        if await list_btn.count() > 0:
            if not await robust_click(list_btn):
                # 如果按钮点击失败，尝试点击整个行
                await row.click(timeout=3000)
        else:
            await row.click(timeout=3000)
        await asyncio.sleep(DELAY_AFTER_CLICK)

        # 等待弹窗出现
        modal = page.locator(".modal-dialog").first
        if not await wait_for_element(page, ".modal-dialog", timeout=5000):
            logger.warning(f"地址 {addr} 未出现弹窗")
            return []

        items = modal.locator(".item-content")
        total = await items.count()
        if MAX_CHANNELS_PER_IP > 0:
            total = min(total, MAX_CHANNELS_PER_IP)

        for i in range(total):
            try:
                name_elem = items.nth(i).locator(".item-title").first
                link_elem = items.nth(i).locator(".item-subtitle").first
                name = await name_elem.inner_text(timeout=2000)
                link = await link_elem.inner_text(timeout=2000)
                name = name.strip()
                link = link.strip()
                if not name or not link:
                    continue

                # 补全协议（如果链接缺少协议头）
                if not link.startswith(('http://', 'https://', 'rtsp://', 'rtmp://')):
                    link = DEFAULT_PROTOCOL + link

                norm = normalize_cctv(name)
                group = classify_channel(norm)
                if not group:
                    continue
                final_name = norm if group == "央视频道" else (clean_chinese_only(name) if ENABLE_CHINESE_CLEAN else name)
                if final_name:
                    entries.append((group, final_name, link))
            except Exception as e:
                logger.debug(f"提取频道项失败: {e}")
                continue

        if ENABLE_SCREENSHOTS:
            await page.screenshot(path=f"screenshots/addr_{addr}_{int(time.time())}.png")
    except Exception as e:
        logger.warning(f"提取地址 {addr} 时出错: {e}")
        if ENABLE_SCREENSHOTS:
            await page.screenshot(path=f"screenshots/addr_error_{ip_index}.png")

    return entries

async def wait_data(page):
    """等待数据加载完成（检测IP或域名出现）"""
    for _ in range(2):
        logger.info("等待30秒加载数据...")
        await asyncio.sleep(30)
        has = await page.evaluate('''()=>{
            for(let e of document.querySelectorAll('div.item-title')){
                if(e.innerText.trim()) return true;  // 只要出现非空标题即认为有数据
            }
            return false;
        }''')
        if has:
            logger.info("数据加载完成")
            return True
    logger.warning("数据加载超时")
    return False

# ============================================================================
# ============================= 主流程 =======================================
# ============================================================================

async def main():
    # 创建截图目录（如果需要）
    if ENABLE_SCREENSHOTS:
        Path("screenshots").mkdir(exist_ok=True)

    async with async_playwright() as p:
        browser = await getattr(p, BROWSER_TYPE).launch(
            headless=HEADLESS,
            args=["--no-sandbox", "--disable-setuid-sandbox"]
        )
        ctx = await browser.new_context(viewport={"width": 1920, "height": 1080})
        page = await ctx.new_page()

        try:
            # 访问目标页面
            logger.info(f"正在访问 {TARGET_URL}")
            await page.goto(TARGET_URL, timeout=PAGE_LOAD_TIMEOUT, wait_until="networkidle")

            # 点击引擎搜索
            if ENGINE_SELECTOR:
                eng = page.locator(ENGINE_SELECTOR).first
                if await eng.count() > 0:
                    await robust_click(eng)
                    await asyncio.sleep(0.5)

            # 点击组播提取标签
            if MCAST_SELECTOR:
                mcast = page.locator(MCAST_SELECTOR).first
                await robust_click(mcast)
                await asyncio.sleep(0.5)

            # 点击开始按钮
            if START_SELECTOR:
                start = page.locator(START_SELECTOR).first
                await robust_click(start)
                await asyncio.sleep(0.5)

            # 等待数据加载
            if not await wait_data(page):
                logger.error("数据加载失败，退出")
                await browser.close()
                return

            # 获取IP行列表
            rows = page.locator("div.ios-list-item").filter(has_text="频道:")
            total_rows = await rows.count()
            if total_rows == 0:
                logger.error("未找到任何地址行")
                await browser.close()
                return

            process_count = min(total_rows, MAX_IPS) if MAX_IPS > 0 else total_rows
            logger.info(f"找到 {total_rows} 个地址，将处理前 {process_count} 个")

            raw_entries = []
            for i in range(process_count):
                entries = await extract_one_ip(page, rows.nth(i), i+1)
                raw_entries.extend(entries)
                if i < process_count - 1:
                    await asyncio.sleep(DELAY_BETWEEN_IPS)

            logger.info(f"原始提取：共 {len(raw_entries)} 条频道-链接")

            # 构建频道映射
            channel_map = defaultdict(list)
            seen = set()
            for group, name, url in raw_entries:
                if ENABLE_DEDUPLICATION:
                    key = (group, name, url)
                    if key in seen:
                        continue
                    seen.add(key)
                channel_map[(group, name)].append(url)

            logger.info(f"去重后：{sum(len(v) for v in channel_map.values())} 条链接，{len(channel_map)} 个频道")

            # 测速过滤（带缓存）
            if ENABLE_SPEED_TEST and channel_map:
                channel_map = await run_speed_test(channel_map)

            # 重新整理为分组列表
            grouped = defaultdict(list)
            for (group, name), urls in channel_map.items():
                for url in urls:
                    grouped[group].append((name, url))

            # 导出M3U
            with open(OUTPUT_M3U_FILENAME, "w", encoding="utf-8") as f:
                f.write("#EXTM3U\n")
                for group in GROUP_ORDER:
                    for name, url in grouped.get(group, []):
                        f.write(f'#EXTINF:-1 group-title="{group}",{name}\n{url}\n')

            # 导出TXT
            with open(OUTPUT_TXT_FILENAME, "w", encoding="utf-8") as f:
                for group in GROUP_ORDER:
                    if group not in grouped:
                        continue
                    f.write(f"{group},#genre#\n")
                    for name, url in grouped[group]:
                        f.write(f"{name},{url}\n")
                    f.write("\n")

            total_links = sum(len(v) for v in grouped.values())
            logger.info(f"导出完成！共 {total_links} 条可用链接，保存至 {OUTPUT_M3U_FILENAME} 和 {OUTPUT_TXT_FILENAME}")

        except Exception as e:
            logger.exception("主流程发生未捕获异常")
            if ENABLE_SCREENSHOTS:
                await page.screenshot(path="screenshots/fatal_error.png")
        finally:
            await browser.close()

if __name__ == "__main__":
    # 直接运行主函数
    asyncio.run(main())
