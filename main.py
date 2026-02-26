#!/usr/bin/env python3
"""
IPTV 组播/域名源提取工具 - 通用版
修改说明：放宽了IP校验，支持抓取域名形式的源；保留实时日志功能
"""

import asyncio
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
TARGET_URL = "https://iptv.809899.xyz"          # 目标网站地址
HEADLESS = True                                  # 是否无头模式 (True=后台/False=显示窗口)
BROWSER_TYPE = "chromium"                        # 浏览器类型
MAX_SOURCES = 20                                  # 【修改】最多提取前N个源(不再叫MAX_IPS，因为可能是域名)
PAGE_LOAD_TIMEOUT = 180000                       # 页面加载超时(毫秒)

# 2. 输出文件设置 ------------------------------------------------------------
OUTPUT_M3U_FILENAME = "iptv_channels.m3u"        # 生成的M3U文件名
OUTPUT_TXT_FILENAME = "iptv_channels.txt"        # 生成的TXT文件名
MAX_LINKS_PER_CHANNEL = 10                        # 每个频道保留的最优链接数量

# 3. 测速总开关 --------------------------------------------------------------
ENABLE_SPEED_TEST = True                         # 是否进行速度测试

# 4. 测速并发控制 ------------------------------------------------------------
SPEED_TEST_CONCURRENCY = 5                      # 同时测速的协程数
SPEED_TEST_TIMEOUT = 10                          # 每个链接测速总超时(秒)

# 5. 测速采样参数 ------------------------------------------------------------
TS_SAMPLE_COUNT = 5                               # 每个HLS流下载的TS片段数量(越少越快)
TS_DOWNLOAD_TIMEOUT = 5                           # 单个TS片段下载超时(秒)
GENERIC_SAMPLE_SIZE = 1024 * 1024                 # 通用测速样本大小(字节)
GENERIC_DOWNLOAD_TIMEOUT = 10                     # 通用测速下载超时(秒)

# 6. 流畅度判定标准 ----------------------------------------------------------
MIN_STABLE_SPEED = 0.5                            # 最低要求速度(Mbps)，放宽要求

# 7. 分辨率过滤 (默认关闭) ---------------------------------------------------
ENABLE_RESOLUTION_FILTER = False                    # 是否过滤低清
MIN_RESOLUTION_WIDTH = 1280
MIN_RESOLUTION_HEIGHT = 720
FALLBACK_TO_SPEED_WHEN_NO_RESOLUTION = True        # 无分辨率信息时按速度通过

# 8. 页面操作延迟 ------------------------------------------------------------
DELAY_BETWEEN_SOURCES = 0.5                       # 处理完一个源后等待秒数
DELAY_AFTER_CLICK = 0.5                           # 点击后等待秒数
MAX_CHANNELS_PER_SOURCE = 0                         # 单个源最多提取频道数(0不限制)

# 9. 数据清洗与去重 ----------------------------------------------------------
ENABLE_CHINESE_CLEAN = False                        # 【修改】默认关闭中文名强制清洗，防止误删
ENABLE_DEDUPLICATION = True                          # 是否去重
ENABLE_SCREENSHOTS = False                           # 是否截图(调试用)
CCTV_USE_MAPPING = True                              # 是否格式化CCTV名称

# 10. 网络与安全设置 ---------------------------------------------------------
ENABLE_SSL_VERIFY = False                            # 是否验证SSL证书

# ============================================================================
# ============================ 频道分类规则 ==================================
# ============================================================================

OUTPUT_DIR = Path(__file__).parent

# 页面元素选择器配置 (增加容错)
PAGE_CONFIG = {
    "engine_search": ["引索搜索", "引擎搜索", "关键词搜索", "直播搜索"],
    "multicast_tab": ["组播提取", "酒店提取", "直播源"], # 增加可能的Tab名
    "start_button": ["开始播放", "开始搜索", "开始提取", "开始", "扫描"],
}

# 频道分类规则 (增加更多通用分类)
CATEGORY_RULES = [
    {"name": "4K专区",      "keywords": ["4k", "8k", "超清"]},
    {"name": "央视频道",    "keywords": ["cctv", "cetv", "中央"]},
    {"name": "卫视频道",    "keywords": ["卫视", "凤凰", "tvb", "湖南", "浙江", "江苏", "东方", "北京", "广东", "山东", "河南", "河北"]},
    {"name": "电影频道",    "keywords": ["电影", "影院", "chc", "院线"]},
    {"name": "轮播频道",    "keywords": ["轮播", "回放"]},
    {"name": "儿童频道",    "keywords": ["少儿", "动画", "卡通", "动漫"]},
    {"name": "新闻频道",    "keywords": ["新闻", "资讯"]},
    {"name": "体育频道",    "keywords": ["体育", "足球", "篮球"]},
    {"name": "其他频道",    "keywords": []}, # 兜底分类
]

# 输出分组顺序
GROUP_ORDER = [
    "央视频道", "卫视频道", "新闻频道", "体育频道", "电影频道", "4K专区", "儿童频道", "轮播频道", "其他频道"
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
# ============================= 日志配置 (实时版) =============================
# ============================================================================

class UnbufferedStreamHandler(logging.StreamHandler):
    """强制每次输出后立即刷新"""
    def emit(self, record):
        super().emit(record)
        self.flush()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        UnbufferedStreamHandler(sys.stdout),
        logging.FileHandler('iptv_extractor.log', encoding='utf-8')
    ]
)
logger = logging.getLogger('IPTV-Extractor')

# ============================================================================
# ============================= 工具函数 =====================================
# ============================================================================

# 【修改】移除了强制IP正则，保留但不强制使用
IP_PATTERN = re.compile(r'^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$')
CCTV_PATTERN = re.compile(r'(cctv)[-\s]?(\d{1,3})', re.IGNORECASE)
CETV_PATTERN = re.compile(r'(cetv)[-\s]?(\d)', re.IGNORECASE)
RESOLUTION_PATTERN = re.compile(r'(\d+)x(\d+)')
CHINESE_ONLY_PATTERN = re.compile(r'[^\u4e00-\u9fff]')

def build_classifier():
    """构建频道分类器，增加兜底逻辑"""
    compiled = []
    for rule in CATEGORY_RULES:
        if not rule["keywords"]:
            # 遇到空关键词的兜底分类，先存起来最后处理
            default_group = rule["name"]
            continue
        pattern = re.compile("|".join(re.escape(kw.lower()) for kw in rule["keywords"]))
        compiled.append((rule["name"], pattern))
    
    # 返回分类函数，如果都没匹配上且有兜底，返回兜底
    def classifier(name):
        for group, pat in compiled:
            if pat.search(name.lower()):
                return group
        # 如果没找到，检查是否定义了兜底分类
        return next((r["name"] for r in CATEGORY_RULES if not r["keywords"]), None)
    
    return classifier

classify_channel = build_classifier()

def normalize_cctv(name: str) -> str:
    """标准化CCTV/CETV频道名称"""
    name_lower = name.lower()
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
ENGINE_SELECTOR = build_selector(PAGE_CONFIG["engine_search"], "a.sidebar-link,button,div.segment-item,div[role='tab']")
MCAST_SELECTOR = build_selector(PAGE_CONFIG["multicast_tab"], "div.segment-item,div[role='tab'],button")
START_SELECTOR = build_selector(PAGE_CONFIG["start_button"], "button")

# ============================================================================
# ========================= 重试装饰器 =======================================
# ============================================================================

def retry_async(max_retries=2, delay=1.0, exceptions=(Exception,)):
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
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout), headers=headers or {}) as resp:
            if resp.status == 200:
                return await resp.read()
    except:
        pass
    return None

async def test_stream(session: aiohttp.ClientSession, url: str) -> Tuple[bool, float, Optional[Tuple[int, int]]]:
    resolution = None
    content = await fetch_url(session, url, 5)
    if content:
        txt = content.decode("utf-8", "ignore")
        lines = txt.splitlines()
        if any(line.startswith("#EXTM3U") for line in lines):
            base = url[:url.rfind('/')+1] if '/' in url else url
            ts_list = []
            for line in lines:
                if line.startswith("#EXT-X-STREAM-INF:"):
                    m = RESOLUTION_PATTERN.search(line)
                    if m:
                        resolution = (int(m.group(1)), int(m.group(2)))
                elif not line.startswith("#") and line.strip():
                    ts_list.append(urljoin(base, line.strip()))
            
            if ts_list:
                sample = ts_list[:min(TS_SAMPLE_COUNT, len(ts_list))]
                speeds = []
                for ts_url in sample:
                    t0 = time.monotonic()
                    data = await fetch_url(session, ts_url, TS_DOWNLOAD_TIMEOUT)
                    cost = time.monotonic() - t0
                    if data and cost > 0:
                        speed = (len(data) * 8) / cost / 1e6
                        speeds.append(speed)
                if speeds:
                    avg_speed = sum(speeds) / len(speeds)
                    return avg_speed >= MIN_STABLE_SPEED, avg_speed, resolution

    # 通用测速回退
    headers = {"Range": f"bytes=0-{GENERIC_SAMPLE_SIZE-1}"}
    t0 = time.monotonic()
    data = await fetch_url(session, url, GENERIC_DOWNLOAD_TIMEOUT, headers=headers)
    cost = time.monotonic() - t0
    if data and cost > 0:
        speed = (len(data) * 8) / cost / 1e6
        return speed >= MIN_STABLE_SPEED, speed, resolution
    return False, 0.0, resolution

async def test_speed_task(url: str, sem: asyncio.Semaphore, session: aiohttp.ClientSession):
    async with sem:
        try:
            ok, speed, resolution = await test_stream(session, url)
            if not ok:
                return None
            if ENABLE_RESOLUTION_FILTER and resolution:
                w, h = resolution
                if w < MIN_RESOLUTION_WIDTH or h < MIN_RESOLUTION_HEIGHT:
                    return None
            elif ENABLE_RESOLUTION_FILTER and not resolution and not FALLBACK_TO_SPEED_WHEN_NO_RESOLUTION:
                return None
            return (url, speed, resolution is not None)
        except:
            return None

async def run_speed_test(channel_map: Dict[Tuple[str, str], List[str]]) -> Dict[Tuple[str, str], List[str]]:
    if not channel_map:
        return {}

    conn = aiohttp.TCPConnector(ssl=ENABLE_SSL_VERIFY)
    async with aiohttp.ClientSession(connector=conn) as session:
        sem = asyncio.Semaphore(SPEED_TEST_CONCURRENCY)
        tasks = []
        for (group, name), urls in channel_map.items():
            for url in urls:
                tasks.append((group, name, url))

        logger.info(f"开始测速：共 {len(tasks)} 条链接")
        results = await asyncio.gather(*[test_speed_task(url, sem, session) for (_, _, url) in tasks])

        result_map = defaultdict(list)
        for i, res in enumerate(results):
            if res:
                group, name, _ = tasks[i]
                url, speed, has_res = res
                result_map[(group, name)].append((url, speed))

        final_map = {}
        for key, items in result_map.items():
            items.sort(key=lambda x: -x[1])
            final_map[key] = [url for url, _ in items[:MAX_LINKS_PER_CHANNEL]]

        logger.info(f"测速完成，保留 {sum(len(v) for v in final_map.values())} 条链接")
        return final_map

# ============================================================================
# ============================ 页面交互函数 (核心修改区) ======================
# ============================================================================

async def robust_click(locator, timeout=10000):
    try:
        await locator.scroll_into_view_if_needed(timeout=3000)
        await asyncio.sleep(0.2)
        await locator.click(force=True, timeout=timeout)
        return True
    except:
        try:
            await locator.evaluate("el => el.click()")
            return True
        except:
            return False

async def wait_for_element(page, selector, state="visible", timeout=10000):
    try:
        await page.wait_for_selector(selector, state=state, timeout=timeout)
        return True
    except PlaywrightTimeoutError:
        return False

@retry_async(max_retries=1, delay=0.5, exceptions=(Exception,))
async def extract_one_source(page, row, index):
    """
    【核心修改】提取单个源（支持IP或域名）
    移除了严格的IP正则校验
    """
    entries = []
    source_name = "Unknown"
    
    try:
        # 1. 获取标题（可能是IP，也可能是域名，或者是任意文字）
        title_elem = row.locator("div.item-title").first
        source_name = await title_elem.inner_text(timeout=2000)
        source_name = source_name.strip()
        logger.info(f"[{index}] 正在处理源: {source_name}")
    except Exception as e:
        logger.debug(f"[{index}] 无法获取源标题，尝试继续处理")
        # 即使获取不到标题，也不直接return，尝试点击看看

    try:
        # 2. 点击展开
        list_btn = row.locator("button:has(i.fa-list)").first
        clicked = False
        if await list_btn.count() > 0:
            clicked = await robust_click(list_btn)
        if not clicked:
            # 如果没有列表按钮或点击失败，尝试点击行本身
            try:
                await row.click(timeout=2000)
            except:
                pass
        
        await asyncio.sleep(DELAY_AFTER_CLICK)

        # 3. 等待弹窗 (使用更通用的选择器)
        modal = page.locator(".modal-dialog,div[role='dialog'],div[class*='modal']").first
        try:
            await modal.wait_for(state="visible", timeout=5000)
        except:
            # 也许没有弹窗，链接就在当前页面？这里暂时只处理弹窗逻辑
            logger.debug(f"[{index}] 未检测到弹窗")
            return []

        # 4. 提取内容 (使用更通用的选择器)
        # 尝试匹配所有可能的 item 容器
        items = modal.locator(".item-content,div[class*='item']")
        total = await items.count()
        
        if MAX_CHANNELS_PER_SOURCE > 0:
            total = min(total, MAX_CHANNELS_PER_SOURCE)
            
        logger.info(f"  -> 发现 {total} 个频道")

        for i in range(total):
            try:
                # 尝试多种组合获取名称和链接
                item = items.nth(i)
                
                # 获取名称
                name = ""
                name_candidates = [
                    item.locator(".item-title"),
                    item.locator("div[class*='title']"),
                    item.locator("span").first
                ]
                for cand in name_candidates:
                    if await cand.count() > 0:
                        try:
                            txt = await cand.inner_text(timeout=1000)
                            if txt and len(txt) > 1:
                                name = txt.strip()
                                break
                        except:
                            continue

                # 获取链接
                link = ""
                link_candidates = [
                    item.locator(".item-subtitle"),
                    item.locator("div[class*='subtitle']"),
                    item.locator("div[class*='link']"),
                    item.locator("a").first
                ]
                
                for cand in link_candidates:
                    if await cand.count() > 0:
                        try:
                            # 先尝试获取 href 属性
                            href = await cand.get_attribute('href', timeout=500)
                            if href and "://" in href:
                                link = href
                                break
                            # 如果没有href，再尝试获取文本
                            txt = await cand.inner_text(timeout=1000)
                            if txt and ("://" in txt or txt.startswith("http")):
                                link = txt.strip()
                                break
                        except:
                            continue

                # 最终校验
                if not name:
                    name = f"频道{i+1}"
                if not link or not ("://" in link):
                    continue

                # 标准化和分类
                norm = normalize_cctv(name)
                group = classify_channel(norm)
                
                # 如果没分到组，强制分到“其他频道”
                if not group:
                    group = "其他频道"

                final_name = norm if group == "央视频道" else (clean_chinese_only(name) if ENABLE_CHINESE_CLEAN else name)
                
                # 如果清洗后名字空了，用原名
                if not final_name:
                    final_name = name
                
                entries.append((group, final_name, link))
            except Exception as e:
                continue

    except Exception as e:
        logger.debug(f"[{index}] 处理源时出错: {e}")

    return entries

async def wait_data(page):
    """等待数据加载，稍微放宽检测条件"""
    logger.info("等待数据加载 (最多60秒)...")
    for attempt in range(4):
        await asyncio.sleep(15)
        # 检查页面上是否有看起来像列表项的东西
        has_items = await page.evaluate('''()=>{
            return document.querySelectorAll('div[class*="item"], div[class*="list"]').length > 5;
        }''')
        if has_items:
            logger.info("数据加载成功！")
            return True
        logger.info(f"  等待中... ({attempt+1}/4)")
    return False

# ============================================================================
# ============================= 主流程 =======================================
# ============================================================================

async def main():
    if ENABLE_SCREENSHOTS:
        Path("screenshots").mkdir(exist_ok=True)

    async with async_playwright() as p:
        browser = await getattr(p, BROWSER_TYPE).launch(
            headless=HEADLESS,
            args=["--no-sandbox"]
        )
        ctx = await browser.new_context(viewport={"width": 1920, "height": 1080})
        page = await ctx.new_page()

        try:
            logger.info(f"正在访问 {TARGET_URL}")
            try:
                await page.goto(TARGET_URL, timeout=PAGE_LOAD_TIMEOUT, wait_until="networkidle")
            except:
                logger.warning("页面加载超时，尝试继续操作...")

            # 尝试点击导航
            if ENGINE_SELECTOR:
                eng = page.locator(ENGINE_SELECTOR).first
                if await eng.count() > 0:
                    await robust_click(eng)
                    await asyncio.sleep(0.5)

            if MCAST_SELECTOR:
                mcast = page.locator(MCAST_SELECTOR).first
                if await mcast.count() > 0:
                    await robust_click(mcast)
                    await asyncio.sleep(0.5)

            if START_SELECTOR:
                start = page.locator(START_SELECTOR).first
                if await start.count() > 0:
                    await robust_click(start)
                    await asyncio.sleep(0.5)

            await wait_data(page)

            # 【修改】寻找列表行，使用更宽泛的过滤器
            # 不再只过滤包含"频道:"的行，尝试寻找所有看起来像列表项的行
            rows = page.locator("div.ios-list-item,div[class*='list-item'],div[class*='item']")
            
            # 进一步过滤：行内最好有按钮或者有看起来像链接的东西，或者有一定的高度
            # 这里简单的取前N个进行处理，避免太多无效点击
            total_rows = await rows.count()
            logger.info(f"页面扫描到 {total_rows} 个可能的列表项")

            if total_rows == 0:
                logger.error("未找到任何列表项")
                await browser.close()
                return

            process_count = min(total_rows, MAX_SOURCES)
            logger.info(f"将尝试处理前 {process_count} 个项")

            raw_entries = []
            for i in range(process_count):
                try:
                    entries = await extract_one_source(page, rows.nth(i), i+1)
                    raw_entries.extend(entries)
                except:
                    pass
                if i < process_count - 1:
                    await asyncio.sleep(DELAY_BETWEEN_SOURCES)

            logger.info(f"原始提取：共 {len(raw_entries)} 条记录")

            # 去重
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

            # 测速
            if ENABLE_SPEED_TEST and channel_map:
                channel_map = await run_speed_test(channel_map)

            # 整理输出
            grouped = defaultdict(list)
            for (group, name), urls in channel_map.items():
                for url in urls:
                    grouped[group].append((name, url))

            # 导出M3U
            with open(OUTPUT_M3U_FILENAME, "w", encoding="utf-8") as f:
                f.write("#EXTM3U\n")
                for g in GROUP_ORDER:
                    for n,u in grouped.get(g,[]):
                        f.write(f'#EXTINF:-1 group-title="{g}",{n}\n{u}\n')

            # 导出TXT
            with open(OUTPUT_TXT_FILENAME, "w", encoding="utf-8") as f:
                for g in GROUP_ORDER:
                    if g not in grouped: continue
                    f.write(f"{g},#genre#\n")
                    for n,u in grouped[g]:
                        f.write(f"{n},{u}\n")
                    f.write("\n")

            total_links = sum(len(v) for v in grouped.values())
            logger.info(f"===== 任务完成 =====")
            logger.info(f"共导出 {total_links} 条链接")
            logger.info(f"文件: {OUTPUT_M3U_FILENAME}")

        except Exception as e:
            logger.exception("发生严重错误")
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
