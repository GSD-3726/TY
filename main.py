#!/usr/bin/env python3
"""
IPTV 组播提取工具
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
import pytz  # 【新增】导入时区处理库
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any
from urllib.parse import urljoin
import functools

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# ============================================================================
# ======================== 【配置区 · 全中文详细说明】 =========================
# ============================================================================

# -------------------------- 1. 基础爬取设置 --------------------------
TARGET_URL = "https://iptv.809899.xyz"          # 【必填】要爬取的目标网站地址
HEADLESS = True                                  # 【True/False】是否隐藏浏览器窗口 (True=后台运行, False=显示窗口)
BROWSER_TYPE = "chromium"                        # 【chromium/firefox/webkit】浏览器内核类型，推荐默认 chromium
MAX_IPS = 20                                     # 【数字】最多处理前N个IP/地址行 (0表示不限制)
PAGE_LOAD_TIMEOUT = 120000                       # 【毫秒】页面加载最长等待时间 (120秒)

# -------------------------- 2. 文件输出设置 --------------------------
OUTPUT_DIR = Path(__file__).parent               # 【路径】结果保存目录 (默认为脚本所在文件夹)
OUTPUT_M3U_FILENAME = OUTPUT_DIR / "iptv_channels.m3u"  # M3U播放列表文件名
OUTPUT_TXT_FILENAME = OUTPUT_DIR / "iptv_channels.txt"  # TXT格式文件名
MAX_LINKS_PER_CHANNEL = 10                       # 【数字】每个频道最多保留多少个最快的源

# -------------------------- 3. FFmpeg 测速设置 --------------------------
ENABLE_FFMPEG_TEST = True                         # 【True/False】总开关：是否开启测速 (False直接保存所有链接)
FFMPEG_PATH = "ffmpeg"                            # 【路径】FFmpeg程序位置 (Windows需写完整路径，如 r"C:\ffmpeg\bin\ffmpeg.exe")
FFMPEG_TEST_DURATION = 10                          # 【秒】单个链接的测试时长 (越长越准，但越慢)
FFMPEG_CONCURRENCY = 2                            # 【数字】同时测试的链接数 (GitHub Actions建议<=2，本地电脑建议<=CPU核心数)
MIN_AVG_FPS = 24.0                                # 【数字】最低平均帧率 (低于此值认为卡顿，丢弃)
MIN_FRAMES = 210                                    # 【数字】最低解码帧数 (防止只有几秒数据就误判为成功)

# -------------------------- 3.1 分辨率优先设置 (新增) --------------------------
PREFER_1080P = True                               # 【True/False】是否优先1920x1080分辨率 (True=1080p排在最前, False=仅按帧率排序)
PREFER_RESOLUTION_WIDTH = 1920                   # 【数字】优先分辨率宽度
PREFER_RESOLUTION_HEIGHT = 1080                  # 【数字】优先分辨率高度

# -------------------------- 4. 网页操作延时 --------------------------
DELAY_BETWEEN_IPS = 1.0                             # 【秒】处理完一个IP后等待多久
DELAY_AFTER_CLICK = 0.5                             # 【秒】点击按钮后等待弹窗多久
MAX_CHANNELS_PER_IP = 0                              # 【数字】单个IP最多提取多少个频道 (0表示不限制)

# -------------------------- 5. 数据清洗 --------------------------
ENABLE_CHINESE_CLEAN = True                         # 【True/False】是否移除频道名中的非中文字符 (仅对非央视频道)
ENABLE_DEDUPLICATION = True                          # 【True/False】是否去重 (相同的频道名+链接只保留一个)
ENABLE_SCREENSHOTS = False                           # 【True/False】是否在关键步骤截图 (用于调试)
CCTV_USE_MAPPING = True                              # 【True/False】是否将CCTV数字转为中文 (如 "CCTV-1" 变为 "CCTV-1综合")

# -------------------------- 6. 网络协议 --------------------------
DEFAULT_PROTOCOL = "http://"                         # 【http:///https:///rtsp://】当链接缺少协议头时，自动补全

# -------------------------- 7. 缓存设置 --------------------------
ENABLE_CACHE = True                                  # 【True/False】是否启用缓存 (开启后，测过的链接24小时内不再重测)
CACHE_FILE = OUTPUT_DIR / "iptv_speed_cache.json"  # 【路径】缓存文件保存位置
CACHE_EXPIRE_HOURS = 24                              # 【小时】缓存过期时间 (0表示永不过期)

# -------------------------- 8. 更新时间显示 --------------------------
TIME_DISPLAY_AT_TOP = False                          # 【True/False】更新时间显示位置 (True=文件最上面, False=文件最后面)

# -------------------------- 9. 更新时间条目占位流 --------------------------
UPDATE_STREAM_URL = "https://gitee.com/bmg369/tvtest/blob/master/20250924141550.mp4"

# ============================================================================
# ============================ 频道分类规则 ==================================
# ============================================================================

# 页面元素定位关键词 (如果网站改版了，修改这里的文字即可)
PAGE_CONFIG = {
    "engine_search": ["引索搜索", "引擎搜索", "关键词搜索"],
    "multicast_tab": ["酒店提取"],
    "start_button": ["开始播放", "开始搜索", "开始提取"],
}

# 频道自动分类规则 (按关键词匹配)
CATEGORY_RULES = [
    {"name": "4K专区",      "keywords": ["4k"]},
    {"name": "央视频道",    "keywords": ["cctv", "cetv", "中央"]},
    {"name": "卫视频道",    "keywords": ["卫视", "凤凰", "tvb", "湖南", "浙江", "江苏", "东方"]},
    {"name": "电影频道",    "keywords": ["电影", "影院", "chc"]},
    {"name": "轮播频道",    "keywords": ["轮播"]},
    {"name": "儿童频道",    "keywords": ["少儿", "动画", "卡通"]},
]

# 导出文件时的分组排序
GROUP_ORDER = ["央视频道", "卫视频道", "电影频道", "4K专区", "儿童频道", "轮播频道"]

# CCTV 台标映射表 & 排序顺序
CCTV_NAME_MAPPING = {
    "1": "综合", "2": "财经", "3": "综艺", "4": "国际", "5": "体育",
    "5+": "体育赛事", "6": "电影", "7": "国防军事", "8": "电视剧",
    "9": "纪录", "10": "科教", "11": "戏曲", "12": "社会与法",
    "13": "新闻", "14": "少儿", "15": "音乐", "16": "奥林匹克",
    "17": "农业农村",
}

# 【央视严格排序】1,2,3,4,5,5+,6,7,8,9,10,11,12,13,14,15,16,17
CCTV_ORDER = ["1", "2", "3", "4", "5", "5+", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17"]

# ============================================================================
# ============================= 日志配置 =====================================
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
# ============================= 缓存工具函数 ==================================
# ============================================================================

def load_cache() -> Dict[str, Dict[str, Any]]:
    if not ENABLE_CACHE: return {}
    if not CACHE_FILE.exists(): return {}
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            cache = json.load(f)
        now = time.time()
        expire_seconds = CACHE_EXPIRE_HOURS * 3600
        valid_cache = {}
        for url, data in cache.items():
            if expire_seconds == 0 or (now - data.get("timestamp", 0)) < expire_seconds:
                valid_cache[url] = data
        return valid_cache
    except Exception as e:
        logger.warning(f"加载缓存失败: {e}")
        return {}

def save_cache(cache: Dict[str, Dict[str, Any]]) -> None:
    if not ENABLE_CACHE: return
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning(f"保存缓存失败: {e}")

# ============================================================================
# ============================= 工具函数 =====================================
# ============================================================================

CCTV_PATTERN = re.compile(r'(cctv)[-\s]?(\d{1,3}\+?)', re.IGNORECASE)
CHINESE_ONLY_PATTERN = re.compile(r'[^\u4e00-\u9fff]')

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
        return "CCTV-5+体育赛事" if CCTV_USE_MAPPING else "CCTV-5+"
    match = CCTV_PATTERN.search(name_lower)
    if match:
        num = match.group(2)
        if CCTV_USE_MAPPING and num in CCTV_NAME_MAPPING:
            return f"CCTV-{num}{CCTV_NAME_MAPPING[num]}"
        return f"CCTV-{num}"
    return name

def clean_chinese_only(name: str) -> str:
    return CHINESE_ONLY_PATTERN.sub('', name)

def build_selector(text_list, element_type="button"):
    if not text_list: return ""
    if len(text_list) == 1: return f"{element_type}:has-text('{text_list[0]}')"
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
                    if attempt == max_retries: raise
                    logger.warning(f"尝试 {attempt}/{max_retries} 失败: {e}，重试中...")
                    await asyncio.sleep(delay)
            return None
        return wrapper
    return decorator

# ============================================================================
# ========================= 进度条打印工具 (2%步进) ==========================
# ============================================================================

def print_progress_bar(current: int, total: int, success: int, failed: int, last_percent: int) -> int:
    """
    打印进度条，每2%刷新一次 (0%, 2%, 4%, ... 100%)
    """
    if total == 0: return 0
    
    percent = current / total
    percent_int = int(percent * 100)
    
    should_print = (
        (percent_int % 2 == 0 and percent_int > last_percent) or 
        current == total or 
        current == 0
    )
    
    if should_print:
        if percent_int == last_percent and current != total:
            return last_percent
            
        bar_length = 20
        filled_length = int(bar_length * percent)
        bar = '█' * filled_length + '░' * (bar_length - filled_length)
        
        logger.info(f"[{percent_int:3d}%] {bar} ({current}/{total}) | 有效:{success} | 共：{current}")
        return percent_int
    
    return last_percent

# ============================================================================
# ========================= 【核心】FFmpeg测速代码 (修改版) ===================
# ============================================================================

async def test_stream_with_ffmpeg(url: str) -> Dict[str, Any]:
    if not shutil.which(FFMPEG_PATH):
        logger.error(f"未找到FFmpeg: {FFMPEG_PATH}")
        return {"ok": False, "fps": 0.0, "message": "FFmpeg未安装"}

    cmd = [
        FFMPEG_PATH, "-hide_banner", "-y",
        "-fflags", "nobuffer",
        "-rw_timeout", "5000000",
        "-i", url,
        "-t", str(FFMPEG_TEST_DURATION),
        "-f", "null", "-"
    ]

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE
        )
        
        try:
            _, stderr = await asyncio.wait_for(proc.communicate(), timeout=FFMPEG_TEST_DURATION + 5)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            return {"ok": False, "fps": 0.0, "message": "连接超时"}

        output = stderr.decode('utf-8', errors='ignore')
        
        frame_matches = re.findall(r'frame=\s*(\d+)', output)
        fps_matches = re.findall(r'fps=\s*([\d.]+)', output)
        
        # 【新增】解析视频分辨率
        width, height = 0, 0
        # 匹配 Stream #0:0: Video: h264 (Main), yuv420p, 1920x1080 [SAR 1:1 DAR 16:9], 25 fps, 25 tbr, 1k tbn, 50 tbc
        res_match = re.search(r',\s*(\d{3,4})x(\d{3,4})\s*[, \[]', output)
        if res_match:
            width = int(res_match.group(1))
            height = int(res_match.group(2))
        
        frames = int(frame_matches[-1]) if frame_matches else 0
        avg_fps = float(fps_matches[-1]) if fps_matches else 0.0
        is_smooth = frames >= MIN_FRAMES and avg_fps >= MIN_AVG_FPS
        
        return {"ok": is_smooth, "fps": avg_fps, "frames": frames, "width": width, "height": height}
    except Exception as e:
        return {"ok": False, "fps": 0.0, "message": f"异常: {str(e)[:50]}", "width": 0, "height": 0}

async def run_ffmpeg_test(channel_map: Dict[Tuple[str, str], List[str]]) -> Dict[Tuple[str, str], List[str]]:
    if not channel_map: return {}

    cache = load_cache()
    new_cache_entries = {}
    result_map = defaultdict(list)
    pending_tasks_data = [] 

    for (group, name), urls in channel_map.items():
        for url in urls:
            if url in cache:
                if cache[url].get("ok"):
                    # 从缓存读取分辨率，如果没有则默认为0
                    w = cache[url].get("width", 0)
                    h = cache[url].get("height", 0)
                    result_map[(group, name)].append((url, cache[url].get("fps", 0), w, h))
            else:
                pending_tasks_data.append((group, name, url))

    total_pending = len(pending_tasks_data)
    cached_count = len([item for sublist in result_map.values() for item in sublist])
    logger.info(f"缓存命中 {cached_count} 条，需测速 {total_pending} 条")

    if total_pending == 0:
        return finalize_results(result_map)

    sem = asyncio.Semaphore(FFMPEG_CONCURRENCY)
    
    async def bound_test(item):
        group, name, url = item
        async with sem:
            result = await test_stream_with_ffmpeg(url)
            return (group, name, url, result)

    tasks = [bound_test(item) for item in pending_tasks_data]
    
    completed = 0
    success_count = 0
    failed_count = 0
    last_printed_percent = -100

    print_progress_bar(0, total_pending, 0, 0, last_printed_percent)

    for coro in asyncio.as_completed(tasks):
        group, name, url, res = await coro
        completed += 1
        
        new_cache_entries[url] = {
            "ok": res["ok"], "fps": res["fps"], 
            "frames": res.get("frames", 0), 
            "width": res.get("width", 0),
            "height": res.get("height", 0),
            "timestamp": time.time()
        }

        if res["ok"]:
            success_count += 1
            # 传递分辨率给结果集
            result_map[(group, name)].append((url, res["fps"], res["width"], res["height"]))
        else:
            failed_count += 1

        last_printed_percent = print_progress_bar(completed, total_pending, success_count, failed_count, last_printed_percent)

    if new_cache_entries:
        cache.update(new_cache_entries)
        save_cache(cache)
        logger.info(f"缓存更新：新增 {len(new_cache_entries)} 条记录 (含分辨率)")

    return finalize_results(result_map)

def finalize_results(result_map):
    final_map = {}
    for key, items in result_map.items():
        # 【修改】排序逻辑：优先判断是否为目标分辨率，然后再按帧率排序
        if PREFER_1080P:
            items.sort(
                key=lambda x: (
                    # x[2] = width, x[3] = height
                    # 如果是目标分辨率，优先级为0，否则为1 (0排在前面)
                    0 if (x[2] == PREFER_RESOLUTION_WIDTH and x[3] == PREFER_RESOLUTION_HEIGHT) else 1,
                    -x[1] # 然后按帧率倒序
                )
            )
        else:
            # 原逻辑：仅按帧率排序
            items.sort(key=lambda x: -x[1])
        
        # 只保留URL，丢弃fps和分辨率用于最终输出
        final_map[key] = [url for url, _, _, _ in items[:MAX_LINKS_PER_CHANNEL]]
    
    total_final = sum(len(v) for v in final_map.values())
    logger.info(f"测速筛选完成，最终保留 {total_final} 条优质链接 (1080p优先: {PREFER_1080P})")
    return final_map

# ============================================================================
# ============================ 页面交互函数 ==================================
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
        if not addr: return []
        logger.info(f"处理地址 [{ip_index}]: {addr}")
    except Exception as e:
        logger.warning(f"提取地址失败: {e}")
        return []

    try:
        list_btn = row.locator("button:has(i.fa-list)").first
        if await list_btn.count() > 0:
            if not await robust_click(list_btn): await row.click(timeout=3000)
        else:
            await row.click(timeout=3000)
        await asyncio.sleep(DELAY_AFTER_CLICK)

        modal = page.locator(".modal-dialog").first
        if not await wait_for_element(page, ".modal-dialog", timeout=5000): return []

        items = modal.locator(".item-content")
        total = await items.count()
        if MAX_CHANNELS_PER_IP > 0: total = min(total, MAX_CHANNELS_PER_IP)

        for i in range(total):
            try:
                name_elem = items.nth(i).locator(".item-title").first
                link_elem = items.nth(i).locator(".item-subtitle").first
                name = await name_elem.inner_text(timeout=2000)
                link = await link_elem.inner_text(timeout=2000)
                name, link = name.strip(), link.strip()
                if not name or not link: continue

                if not link.startswith(('http://', 'https://', 'rtsp://', 'rtmp://')):
                    link = DEFAULT_PROTOCOL + link

                norm = normalize_cctv(name)
                group = classify_channel(norm)
                if not group: continue
                final_name = norm if group == "央视频道" else (clean_chinese_only(name) if ENABLE_CHINESE_CLEAN else name)
                if final_name: entries.append((group, final_name, link))
            except Exception:
                continue
    except Exception as e:
        logger.warning(f"提取出错 {addr}: {e}")
    return entries

async def wait_data(page):
    for _ in range(2):
        logger.info("等待30秒加载数据...")
        await asyncio.sleep(30)
        has = await page.evaluate('''()=>{
            for(let e of document.querySelectorAll('div.item-title')){
                if(e.innerText.trim()) return true;
            }
            return false;
        }''')
        if has:
            logger.info("数据加载完成")
            return True
    logger.warning("数据加载超时")
    return False

# ============================================================================
# ======================== 【央视排序核心函数】 ===============================
# ============================================================================

def sort_cctv_channels(channels):
    def get_cctv_sort_key(name):
        match = CCTV_PATTERN.search(name)
        if not match:
            return (999, name)
        num = match.group(2)
        if num in CCTV_ORDER:
            return (CCTV_ORDER.index(num), name)
        return (998, name)
    return sorted(channels, key=lambda x: get_cctv_sort_key(x[0]))

# ============================================================================
# ===================== 【导出】带时间戳的文件 ================================
# ============================================================================

def export_results_with_timestamp(channel_map: Dict[Tuple[str, str], List[str]]):
    # 【核心修改】设置为UTC+8（北京时间）
    tz_beijing = pytz.timezone('Asia/Shanghai')  # 定义北京时间时区
    now = datetime.datetime.now(tz_beijing)      # 获取当前北京时间（UTC+8）
    time_str = now.strftime("%Y-%m-%d %H:%M:%S") # 格式化时间字符串
    update_url = UPDATE_STREAM_URL

    grouped = defaultdict(list)
    for (group, name), urls in channel_map.items():
        for url in urls:
            grouped[group].append((name, url))

    with open(OUTPUT_M3U_FILENAME, "w", encoding="utf-8") as f:
        f.write("#EXTM3U\n")
        if TIME_DISPLAY_AT_TOP:
            f.write(f'#EXTINF:-1 tvg-name="{time_str}" tvg-id="更新时间" tvg-logo="" group-title="更新时间", {time_str}\n')
            f.write(f"{update_url}\n\n")
        
        for group in GROUP_ORDER:
            if group not in grouped:
                continue
            if group == "央视频道":
                sorted_chans = sort_cctv_channels(grouped[group])
            else:
                sorted_chans = sorted(grouped[group], key=lambda x: x[0])
            
            for name, url in sorted_chans:
                f.write(f'#EXTINF:-1 group-title="{group}",{name}\n{url}\n')
            f.write("\n")
        
        if not TIME_DISPLAY_AT_TOP:
            f.write(f'#EXTINF:-1 tvg-name="{time_str}" tvg-id="更新时间" tvg-logo="" group-title="更新时间", {time_str}\n')
            f.write(f"{update_url}\n\n")

    with open(OUTPUT_TXT_FILENAME, "w", encoding="utf-8") as f:
        if TIME_DISPLAY_AT_TOP:
            f.write("更新时间,#genre#\n")
            f.write(f"{time_str},{update_url}\n\n")
        
        for group in GROUP_ORDER:
            if group not in grouped:
                continue
            f.write(f"{group},#genre#\n")
            
            if group == "央视频道":
                sorted_chans = sort_cctv_channels(grouped[group])
            else:
                sorted_chans = sorted(grouped[group], key=lambda x: x[0])
            
            for name, url in sorted_chans:
                f.write(f"{name},{url}\n")
            f.write("\n")
        
        if not TIME_DISPLAY_AT_TOP:
            f.write("更新时间,#genre#\n")
            f.write(f"{time_str},{update_url}\n\n")

    total_links = sum(len(v) for v in grouped.values()) + 1
    position_text = "顶部" if TIME_DISPLAY_AT_TOP else "底部"
    logger.info(f"导出完成！共 {total_links} 条链接（含更新时间），更新时间已放在{position_text}")

# ============================================================================
# ============================= 主流程 =======================================
# ============================================================================

async def main():
    if ENABLE_SCREENSHOTS:
        (OUTPUT_DIR / "screenshots").mkdir(exist_ok=True)

    async with async_playwright() as p:
        browser = await getattr(p, BROWSER_TYPE).launch(
            headless=HEADLESS, args=["--no-sandbox", "--disable-setuid-sandbox"]
        )
        ctx = await browser.new_context(viewport={"width": 1920, "height": 1080})
        page = await ctx.new_page()

        try:
            logger.info(f"正在访问 {TARGET_URL}")
            await page.goto(TARGET_URL, timeout=PAGE_LOAD_TIMEOUT, wait_until="networkidle")

            if ENGINE_SELECTOR:
                eng = page.locator(ENGINE_SELECTOR).first
                if await eng.count() > 0: await robust_click(eng)
            
            if MCAST_SELECTOR:
                mcast = page.locator(MCAST_SELECTOR).first
                await robust_click(mcast)
            
            if START_SELECTOR:
                start = page.locator(START_SELECTOR).first
                await robust_click(start)

            if not await wait_data(page):
                logger.error("数据加载失败")
                return

            rows = page.locator("div.ios-list-item").filter(has_text="频道:")
            total_rows = await rows.count()
            if total_rows == 0:
                logger.error("未找到任何地址")
                return

            process_count = min(total_rows, MAX_IPS) if MAX_IPS > 0 else total_rows
            logger.info(f"找到 {total_rows} 个地址，处理前 {process_count} 个")

            raw_entries = []
            for i in range(process_count):
                entries = await extract_one_ip(page, rows.nth(i), i+1)
                raw_entries.extend(entries)
                if i < process_count - 1: await asyncio.sleep(DELAY_BETWEEN_IPS)

            logger.info(f"原始提取：{len(raw_entries)} 条")

            channel_map = defaultdict(list)
            seen = set()
            for group, name, url in raw_entries:
                if ENABLE_DEDUPLICATION:
                    key = (group, name, url)
                    if key in seen: continue
                    seen.add(key)
                channel_map[(group, name)].append(url)

            if ENABLE_FFMPEG_TEST and channel_map:
                channel_map = await run_ffmpeg_test(channel_map)

            export_results_with_timestamp(channel_map)

        except Exception as e:
            logger.exception("主流程异常")
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
