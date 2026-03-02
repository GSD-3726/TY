#!/usr/bin/env python3
"""
IPTV 组播提取工具 - 适配 GitHub Actions 优化版
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
import multiprocessing
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any
from urllib.parse import urljoin, urlparse
import functools

import aiohttp
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError


# ============================================================================
# ======================== 【配置区 · GitHub Actions 适配】 =====================
# ============================================================================

# -------------------------- 1. 基础爬取设置 --------------------------
TARGET_URL = "https://iptv.809899.xyz"
HEADLESS = True  # Actions 必须为 True
BROWSER_TYPE = "chromium"
MAX_IPS = 1  # Actions 资源有限，建议≤2
MAX_TOTAL_CHANNELS = 0
PAGE_LOAD_TIMEOUT = 120000

# -------------------------- 2. 文件输出设置 --------------------------
OUTPUT_DIR = Path(__file__).parent
OUTPUT_M3U_FILENAME = OUTPUT_DIR / "iptv_channels.m3u"
OUTPUT_TXT_FILENAME = OUTPUT_DIR / "iptv_channels.txt"
MAX_LINKS_PER_CHANNEL = 5  # Actions 缩短列表，提升效率

# -------------------------- 3. FFmpeg 测速设置 (Actions 核心优化) --------------------------
ENABLE_FFMPEG_TEST = True
FFMPEG_PATH = "ffmpeg"  # Actions 中通过 apt 安装，无需指定路径
FFMPEG_TEST_DURATION = 5  # 从10秒缩短为5秒（Actions 超时敏感）
FFMPEG_CONCURRENCY = 1  # Actions 强制低并发（避免资源耗尽）
MIN_AVG_FPS = 20.0  # 适度降低阈值（兼顾准确性和通过率）
MIN_FRAMES = 80  # 从210降为80（适配5秒测试时长）

# -------------------------- 4. 网页操作延时 --------------------------
DELAY_BETWEEN_IPS = 2.0  # Actions 网络较慢，适度增加延时
DELAY_AFTER_CLICK = 1.0
MAX_CHANNELS_PER_IP = 0

# -------------------------- 5. 数据清洗 --------------------------
ENABLE_CHINESE_CLEAN = True
ENABLE_DEDUPLICATION = True
ENABLE_SCREENSHOTS = False  # Actions 禁用截图（节省空间）
CCTV_USE_MAPPING = True

# -------------------------- 6. 网络协议 --------------------------
DEFAULT_PROTOCOL = "http://"

# -------------------------- 7. 缓存设置 (Actions 禁用) --------------------------
ENABLE_CACHE = False  # Actions 每次运行环境全新，禁用缓存更稳定
CACHE_FILE = OUTPUT_DIR / "iptv_speed_cache.json"
CACHE_EXPIRE_HOURS = 24

# -------------------------- 8. 更新时间显示 --------------------------
TIME_DISPLAY_AT_TOP = False
UPDATE_STREAM_URL = "https://gitee.com/bmg369/tvtest/raw/master/cg/index.m3u8"

# ============================================================================
# ============================ 频道分类规则 ==================================
# ============================================================================
PAGE_CONFIG = {
    "engine_search": ["引索搜索", "引擎搜索", "关键词搜索"],
    "multicast_tab": ["酒店提取"],
    "start_button": ["开始播放", "开始搜索", "开始提取"],
}

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
    "CCTV-1综合", "CCTV-2财经", "CCTV-3综艺", "CCTV-4国际", "CCTV-5体育",
    "CCTV-5+体育赛事", "CCTV-6电影", "CCTV-7国防军事", "CCTV-8电视剧",
    "CCTV-9纪录", "CCTV-10科教", "CCTV-11戏曲", "CCTV-12社会与法",
    "CCTV-13新闻", "CCTV-14少儿", "CCTV-15音乐", "CCTV-16奥林匹克",
    "CCTV-17农业农村", "CETV1", "CETV2", "CETV4", "CETV5"
]

# ============================================================================
# ============================= 日志配置 (Actions 友好) ============================
# ============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(OUTPUT_DIR / 'iptv_extractor.log', encoding='utf-8')
    ]
)
logger = logging.getLogger('IPTV-Extractor')

# ============================================================================
# ============================= 工具函数 =====================================
# ============================================================================
CCTV_PATTERN = re.compile(r'(cctv)[-\s]?(\d{1,3}|5\+)', re.IGNORECASE)
CHINESE_ONLY_PATTERN = re.compile(r'[^\u4e00-\u9fff]')

def build_classifier():
    compiled = []
    for rule in CATEGORY_RULES:
        if not rule["keywords"]:
            continue
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
    if not text_list:
        return ""
    if len(text_list) == 1:
        return f"{element_type}:has-text('{text_list[0]}')"
    pattern = "|".join(re.escape(t) for t in text_list)
    return f"{element_type}:text-matches('{pattern}')"

ENGINE_SELECTOR = build_selector(PAGE_CONFIG["engine_search"], "a.sidebar-link,button,div.segment-item")
MCAST_SELECTOR = build_selector(PAGE_CONFIG["multicast_tab"], "div.segment-item")
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
                    logger.warning(f"尝试 {attempt}/{max_retries} 失败: {e}，重试中...")
                    await asyncio.sleep(delay)
            return None
        return wrapper
    return decorator

# ============================================================================
# ========================= 进度条工具 (Actions 适配) ==========================
# ============================================================================
def print_progress_bar(current: int, total: int, success: int, failed: int, last_percent: int) -> int:
    if total == 0:
        return 0
    percent = current / total
    percent_int = int(percent * 100)
    should_print = (
        (percent_int % 5 == 0 and percent_int > last_percent) or  # Actions 每5%打印一次，减少日志量
        current == total or
        current == 0
    )
    if should_print:
        if percent_int == last_percent and current != total:
            return last_percent
        bar_length = 20
        filled_length = int(bar_length * percent)
        bar = '█' * filled_length + '░' * (bar_length - filled_length)
        logger.info(f"[{percent_int:3d}%] {bar} ({current}/{total}) | 成功:{success} | 失败:{failed}")
        return last_percent
    return last_percent

# ============================================================================
# ========================= FFmpeg 自动检测 (Actions 适配) ======================
# ============================================================================
def find_ffmpeg() -> Optional[str]:
    """自动搜索FFmpeg路径（适配GitHub Actions Linux环境）"""
    common_paths = [
        "ffmpeg",  # Actions 中 apt install ffmpeg 后默认路径
        "/usr/local/bin/ffmpeg",
        "/usr/bin/ffmpeg"
    ]
    for path in common_paths:
        if shutil.which(path):
            logger.info(f"找到FFmpeg: {path}")
            return path
    logger.error("未找到FFmpeg！GitHub Actions需先执行: sudo apt install -y ffmpeg")
    sys.exit(1)

# 初始化FFmpeg路径
FFMPEG_PATH = find_ffmpeg()

# ============================================================================
# ========================= 轻量预过滤 (Actions 核心优化) ======================
# ============================================================================
async def pre_check_url(url: str) -> bool:
    """
    轻量预检测：在调用FFmpeg前快速排除无效链接（减少Actions资源消耗）
    """
    try:
        # 1. 解析域名，3秒超时
        parsed = urlparse(url)
        if not parsed.hostname:
            return False
        await asyncio.get_event_loop().getaddrinfo(parsed.hostname, None, timeout=3)
        
        # 2. HTTP/HTTPS链接额外检测头信息
        if url.startswith(('http://', 'https://')):
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=3),
                connector=aiohttp.TCPConnector(verify_ssl=False)
            ) as session:
                async with session.head(url, allow_redirects=True) as resp:
                    return resp.status < 500  # 5xx直接排除
        return True
    except Exception as e:
        logger.debug(f"预过滤排除无效链接：{url} | 原因：{str(e)[:30]}")
        return False

# ============================================================================
# ========================= FFmpeg 解析优化 (多维度判定) ======================
# ============================================================================
def parse_ffmpeg_output(output: str) -> Tuple[int, float, float, float]:
    """
    优化FFmpeg输出解析：适配不同版本，新增比特率、丢包率判定
    """
    # 兼容不同FFmpeg版本的正则
    frame_pattern = re.compile(r'frame\s*=\s*(\d+)', re.IGNORECASE)
    fps_pattern = re.compile(r'(?:fps|avg_fps)\s*=\s*([\d.]+)', re.IGNORECASE)
    bitrate_pattern = re.compile(r'bitrate\s*=\s*([\d.]+)\s*kb/s', re.IGNORECASE)
    drop_pattern = re.compile(r'drop\s*=\s*(\d+)', re.IGNORECASE)
    
    frame_matches = frame_pattern.findall(output)
    fps_matches = fps_pattern.findall(output)
    bitrate_matches = bitrate_pattern.findall(output)
    drop_matches = drop_pattern.findall(output)
    
    # 基础解析
    frames = int(frame_matches[-1]) if frame_matches else 0
    avg_fps = float(fps_matches[-1]) if fps_matches else (frames / FFMPEG_TEST_DURATION)
    bitrate = float(bitrate_matches[-1]) if bitrate_matches else 0.0
    drop_count = int(drop_matches[-1]) if drop_matches else 0
    
    # 丢包率计算
    drop_rate = drop_count / frames if frames > 0 else 1.0
    
    return frames, avg_fps, bitrate, drop_rate

# ============================================================================
# ========================= 【核心】FFmpeg测速 (Actions 优化) ====================
# ============================================================================
async def test_stream_with_ffmpeg(url: str) -> Dict[str, Any]:
    """
    优化点：
    1. 动态缩短测试时长（3秒无有效帧直接终止）
    2. 双重超时保护（防止Actions进程卡死）
    3. 多维度判定（帧率+帧数+比特率+丢包率）
    """
    cmd = [
        FFMPEG_PATH, "-hide_banner", "-y",
        "-fflags", "nobuffer+flush_packets",  # 低延迟，快速刷新
        "-flags", "low_delay",
        "-rw_timeout", "3000000",  # 3秒连接超时（Actions网络敏感）
        "-i", url,
        "-t", str(FFMPEG_TEST_DURATION),
        "-vf", "fps=1",  # 每秒仅检测1帧，降低CPU占用
        "-f", "null", "-"
    ]

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE
        )

        # 优化1：进程监控（3秒无有效帧则强制终止）
        kill_trigger = False
        async def monitor_proc():
            nonlocal kill_trigger
            await asyncio.sleep(3)  # 3秒后检查
            if proc.returncode is None:
                try:
                    stderr_part = await proc.stderr.readline()
                    if b"frame=0" in stderr_part or b"Invalid data" in stderr_part:
                        kill_trigger = True
                        proc.kill()
                        logger.debug(f"提前终止无效链接：{url}（3秒无有效帧）")
                except:
                    pass

        monitor_task = asyncio.create_task(monitor_proc())

        # 优化2：双重超时保护（测试时长+5秒缓冲）
        try:
            _, stderr = await asyncio.wait_for(proc.communicate(), timeout=FFMPEG_TEST_DURATION + 5)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            return {"ok": False, "fps": 0.0, "message": "测速超时", "frames": 0}
        finally:
            monitor_task.cancel()
            # 确保进程彻底退出（Actions防止进程泄漏）
            if proc.returncode is None:
                proc.kill()
                await proc.wait()

        if kill_trigger:
            return {"ok": False, "fps": 0.0, "message": "无有效帧", "frames": 0}

        # 解析输出
        output = stderr.decode('utf-8', errors='ignore')
        frames, avg_fps, bitrate, drop_rate = parse_ffmpeg_output(output)

        # 优化3：多维度判定（适配Actions低资源）
        is_smooth = (
            frames >= MIN_FRAMES
            and avg_fps >= MIN_AVG_FPS
            and bitrate >= 80  # 最低比特率80kb/s
            and drop_rate < 0.15  # 丢包率<15%
        )

        # Actions 友好日志
        logger.debug(
            f"测速结果：{url[:50]} | 帧数={frames} | 帧率={avg_fps:.2f} | "
            f"比特率={bitrate:.2f}kb/s | 丢包率={drop_rate:.2%} | 有效={is_smooth}"
        )

        return {
            "ok": is_smooth, "fps": avg_fps, "frames": frames,
            "bitrate": bitrate, "drop_rate": drop_rate, "message": "成功"
        }
    except Exception as e:
        err_msg = str(e)[:50]
        logger.debug(f"测速失败：{url[:50]} | 原因：{err_msg}")
        return {"ok": False, "fps": 0.0, "message": err_msg, "frames": 0}

# ============================================================================
# ========================= 批量测速 (Actions 低并发) ========================
# ============================================================================
async def run_ffmpeg_test(channel_map: Dict[Tuple[str, str], List[str]]) -> Dict[Tuple[str, str], List[str]]:
    if not channel_map:
        return {}

    result_map = defaultdict(list)
    pending_tasks_data = []

    # 统计总链接数
    total_urls = sum(len(urls) for urls in channel_map.values())
    logger.info(f"总待处理链接：{total_urls} 条")

    # 1. 预过滤（核心优化：减少FFmpeg测试数量）
    for (group, name), urls in channel_map.items():
        for url in urls:
            if await pre_check_url(url):
                pending_tasks_data.append((group, name, url))
            else:
                logger.debug(f"预过滤排除：{url[:50]}")

    total_pending = len(pending_tasks_data)
    logger.info(f"预过滤后需要测速：{total_pending} 条（减少 {total_urls - total_pending} 条无效测试）")

    if total_pending == 0:
        return finalize_results(result_map)

    # 2. 低并发测速（Actions 强制1并发，避免资源耗尽）
    sem = asyncio.Semaphore(FFMPEG_CONCURRENCY)

    async def bound_test(item):
        group, name, url = item
        async with sem:
            result = await test_stream_with_ffmpeg(url)
            return (group, name, url, result)

    tasks = [bound_test(item) for item in pending_tasks_data]

    # 进度统计（Actions 友好）
    completed = 0
    success_count = 0
    failed_count = 0
    last_printed_percent = -100

    print_progress_bar(0, total_pending, 0, 0, last_printed_percent)

    for coro in asyncio.as_completed(tasks):
        group, name, url, res = await coro
        completed += 1

        if res["ok"]:
            success_count += 1
            result_map[(group, name)].append((url, res["fps"]))
        else:
            failed_count += 1

        last_printed_percent = print_progress_bar(completed, total_pending, success_count, failed_count, last_printed_percent)

    return finalize_results(result_map)

def finalize_results(result_map):
    final_map = {}
    for key, items in result_map.items():
        items.sort(key=lambda x: -x[1])
        final_map[key] = [url for url, _ in items[:MAX_LINKS_PER_CHANNEL]]

    total_final = sum(len(v) for v in final_map.values())
    logger.info(f"测速筛选完成，最终保留 {total_final} 条优质链接")
    return final_map

# ============================================================================
# ============================ 页面交互函数 (Actions 适配) =====================
# ============================================================================
async def robust_click(locator, timeout=10000):
    try:
        await locator.scroll_into_view_if_needed(timeout=3000)
        await asyncio.sleep(0.2)
        await locator.click(force=True, timeout=timeout)
        return True
    except Exception:
        try:
            await locator.evaluate("el => el.click()")
            return True
        except Exception:
            return False

async def wait_for_element(page, selector, timeout=30000):
    try:
        await page.wait_for_selector(selector, timeout=timeout)
        return True
    except PlaywrightTimeoutError:
        return False

@retry_async(max_retries=2, delay=1.0)
async def extract_one_ip(page, row, ip_index):
    entries = []
    addr = None
    try:
        addr_elem = row.locator("div.item-title").first
        addr = await addr_elem.inner_text(timeout=3000)
        addr = addr.strip()
        if not addr:
            logger.warning(f"第 {ip_index} 行地址为空，跳过")
            return []
        logger.info(f"处理地址 [{ip_index}]: {addr}")
    except Exception as e:
        logger.warning(f"提取第 {ip_index} 行地址失败: {e}")
        return []

    try:
        list_btn = row.locator("button:has(i.fa-list)").first
        if await list_btn.count() > 0:
            if not await robust_click(list_btn):
                await row.click(timeout=3000)
        else:
            await row.click(timeout=3000)
        await asyncio.sleep(DELAY_AFTER_CLICK)

        modal = page.locator(".modal-dialog").first
        if not await wait_for_element(page, ".modal-dialog", timeout=5000):
            logger.warning(f"第 {ip_index} 行地址 {addr} 未弹出频道弹窗，跳过")
            return []

        items = modal.locator(".item-content")
        total = await items.count()
        if total == 0:
            logger.warning(f"第 {ip_index} 行地址 {addr} 无频道数据，跳过")
            return []
            
        if MAX_CHANNELS_PER_IP > 0:
            total = min(total, MAX_CHANNELS_PER_IP)

        logger.info(f"第 {ip_index} 行地址 {addr} 共提取到 {total} 个频道")
        for i in range(total):
            try:
                name_elem = items.nth(i).locator(".item-title").first
                link_elem = items.nth(i).locator(".item-subtitle").first
                name = await name_elem.inner_text(timeout=2000)
                link = await link_elem.inner_text(timeout=2000)
                name, link = name.strip(), link.strip()
                if not name or not link:
                    continue

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
                logger.warning(f"提取第 {ip_index} 行第 {i+1} 个频道失败: {e}")
                continue
    except Exception as e:
        logger.warning(f"提取第 {ip_index} 行地址 {addr} 出错: {e}")
    return entries

async def wait_data(page):
    """优化数据等待逻辑，适配Actions网络延迟"""
    for retry in range(4):  # 最多等待120秒（比原来多1次重试）
        logger.info(f"等待30秒加载数据... (重试 {retry+1}/4)")
        await asyncio.sleep(30)
        has_data = await page.evaluate('''()=>{
            const items = document.querySelectorAll('div.ios-list-item');
            for(let item of items) {
                const title = item.querySelector('.item-title')?.innerText?.trim();
                const subtitle = item.querySelector('.item-subtitle')?.innerText?.trim();
                if(title && subtitle && subtitle.includes('频道:')) {
                    return true;
                }
            }
            return false;
        }''')
        if has_data:
            logger.info("数据加载完成")
            return True
    logger.error("数据加载超时（120秒）")
    return False

# ============================================================================
# ===================== 【导出】带时间戳的文件 (Actions 适配) ===================
# ============================================================================
def export_results_with_timestamp(channel_map: Dict[Tuple[str, str], List[str]]):
    now = datetime.datetime.now()
    time_str = now.strftime("%Y-%m-%d %H:%M:%S")
    update_url = UPDATE_STREAM_URL

    grouped = defaultdict(list)
    for (group, name), urls in channel_map.items():
        for url in urls:
            grouped[group].append((name, url))

    # --- 导出 M3U ---
    with open(OUTPUT_M3U_FILENAME, "w", encoding="utf-8") as f:
        f.write("#EXTM3U\n")
        if TIME_DISPLAY_AT_TOP:
            f.write(f'#EXTINF:-1 tvg-name="{time_str}" tvg-id="更新时间" tvg-logo="" group-title="更新时间", {time_str}\n')
            f.write(f"{update_url}\n\n")

        for group in GROUP_ORDER:
            if group not in grouped:
                continue

            if group == "央视频道":
                name_url_dict = {name: url for name, url in grouped[group]}
                sorted_channels = [(name, name_url_dict[name]) for name in CCTV_ORDER if name in name_url_dict]
            else:
                sorted_channels = grouped[group]

            for name, url in sorted_channels:
                f.write(f'#EXTINF:-1 group-title="{group}",{name}\n{url}\n')
            f.write("\n")

        if not TIME_DISPLAY_AT_TOP:
            f.write(f'#EXTINF:-1 tvg-name="{time_str}" tvg-id="更新时间" tvg-logo="" group-title="更新时间", {time_str}\n')
            f.write(f"{update_url}\n\n")

    # --- 导出 TXT ---
    with open(OUTPUT_TXT_FILENAME, "w", encoding="utf-8") as f:
        if TIME_DISPLAY_AT_TOP:
            f.write("更新时间,#genre#\n")
            f.write(f"{time_str},{update_url}\n\n")

        for group in GROUP_ORDER:
            if group not in grouped:
                continue

            if group == "央视频道":
                name_url_dict = {name: url for name, url in grouped[group]}
                sorted_channels = [(name, name_url_dict[name]) for name in CCTV_ORDER if name in name_url_dict]
            else:
                sorted_channels = grouped[group]

            f.write(f"{group},#genre#\n")
            for name, url in sorted_channels:
                f.write(f"{name},{url}\n")
            f.write("\n")

        if not TIME_DISPLAY_AT_TOP:
            f.write("更新时间,#genre#\n")
            f.write(f"{time_str},{update_url}\n\n")

    total_links = sum(len(v) for v in grouped.values()) + 1
    position_text = "顶部" if TIME_DISPLAY_AT_TOP else "底部"
    logger.info(f"导出完成！共 {total_links} 条链接（含更新时间），更新时间已放在{position_text}")

# ============================================================================
# ============================= 主流程 (Actions 适配) ==========================
# ============================================================================
async def main():
    async with async_playwright() as p:
        # Actions 中 Chromium 启动参数优化
        browser = await getattr(p, BROWSER_TYPE).launch(
            headless=HEADLESS, 
            args=[
                "--no-sandbox", "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",  # 解决Actions内存不足
                "--disable-gpu",  # Actions 无GPU，禁用减少资源占用
                "--single-process"  # 单进程模式，稳定
            ]
        )
        ctx = await browser.new_context(viewport={"width": 1920, "height": 1080})
        page = await ctx.new_page()

        try:
            logger.info(f"正在访问 {TARGET_URL}")
            await page.goto(TARGET_URL, timeout=PAGE_LOAD_TIMEOUT, wait_until="networkidle")

            if ENGINE_SELECTOR:
                eng = page.locator(ENGINE_SELECTOR).first
                if await eng.count() > 0:
                    logger.info("点击引擎搜索按钮")
                    await robust_click(eng)

            if MCAST_SELECTOR:
                mcast = page.locator(MCAST_SELECTOR).first
                logger.info("点击酒店提取标签")
                await robust_click(mcast)

            if START_SELECTOR:
                start = page.locator(START_SELECTOR).first
                logger.info("点击开始提取按钮")
                await robust_click(start)

            if not await wait_data(page):
                logger.error("数据加载失败，退出程序")
                return

            # 精准筛选IP行
            rows = page.locator("div.ios-list-item").filter(
                has=page.locator("div.item-subtitle:has-text('频道:')")
            )
            total_rows = await rows.count()
            logger.info(f"【精准筛选】找到包含频道信息的IP行总数：{total_rows}")
            
            if total_rows == 0:
                logger.error("未找到任何包含频道信息的IP行，退出程序")
                return

            process_count = min(total_rows, MAX_IPS) if MAX_IPS > 0 else total_rows
            logger.info(f"配置MAX_IPS={MAX_IPS}，实际将处理前 {process_count} 个IP行")

            raw_entries = []
            processed_ip_count = 0
            for i in range(process_count):
                row = rows.nth(i)
                entries = await extract_one_ip(page, row, i+1)
                if entries:
                    raw_entries.extend(entries)
                    processed_ip_count += 1
                
                if MAX_TOTAL_CHANNELS > 0 and len(raw_entries) >= MAX_TOTAL_CHANNELS:
                    raw_entries = raw_entries[:MAX_TOTAL_CHANNELS]
                    logger.info(f"已达到总频道数上限 {MAX_TOTAL_CHANNELS}，停止提取IP行")
                    break
                
                if i < process_count - 1:
                    await asyncio.sleep(DELAY_BETWEEN_IPS)

            logger.info(f"实际处理IP行数：{processed_ip_count} / {process_count}")
            logger.info(f"原始提取频道总数：{len(raw_entries)} 条")

            # 去重
            channel_map = defaultdict(list)
            seen = set()
            duplicate_count = 0
            for group, name, url in raw_entries:
                if ENABLE_DEDUPLICATION:
                    key = (group, name, url)
                    if key in seen:
                        duplicate_count += 1
                        continue
                    seen.add(key)
                channel_map[(group, name)].append(url)
            
            logger.info(f"去重完成，重复频道数：{duplicate_count} 条")
            logger.info(f"去重后频道数：{len(channel_map)} 个")

            # FFmpeg测速
            if ENABLE_FFMPEG_TEST and channel_map:
                logger.info("开始FFmpeg测速筛选（适配GitHub Actions）")
                channel_map = await run_ffmpeg_test(channel_map)
            else:
                logger.info("跳过FFmpeg测速")

            # 导出
            export_results_with_timestamp(channel_map)

        except Exception as e:
            logger.exception("主流程异常")
        finally:
            # Actions 确保浏览器彻底关闭
            await page.close()
            await ctx.close()
            await browser.close()

if __name__ == "__main__":
    # Actions 中设置事件循环策略（解决Linux异步问题）
    if sys.platform == 'linux':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy() if sys.platform == 'win32' else asyncio.DefaultEventLoopPolicy())
    asyncio.run(main())
 优化代码测速 要求测速准确
