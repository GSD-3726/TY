#!/usr/bin/env python3
"""
IPTV 组播提取工具
"""

import asyncio
import aiohttp
import json
import logging
import os
import re
import sys
import time
import shutil
import datetime
import pytz
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any
from urllib.parse import urljoin
import functools
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# ============================================================================
# ================================ 配置区 ====================================
# ============================================================================

# 基础爬取设置
TARGET_URL = "https://iptv.809899.xyz"    # 目标网站地址
HEADLESS = True                               # 是否隐藏浏览器窗口 (True=后台运行, False=显示窗口)
BROWSER_TYPE = "chromium"                     # 浏览器内核类型 (chromium/firefox/webkit)
MAX_IPS = 10                                   # 最多处理前N个IP/地址 (0表示不限制)
PAGE_LOAD_TIMEOUT = 120000                    # 页面加载最长等待时间 (毫秒)

# 文件输出设置
OUTPUT_DIR = Path(__file__).parent            # 结果保存目录 (默认为脚本所在文件夹)
OUTPUT_M3U_FILENAME = OUTPUT_DIR / "iptv_channels.m3u"  # M3U播放列表文件名
OUTPUT_TXT_FILENAME = OUTPUT_DIR / "iptv_channels.txt"  # TXT格式文件名
MAX_LINKS_PER_CHANNEL = 10                    # 单个频道最多保留的源数量

# FFmpeg 测速设置
ENABLE_FFMPEG_TEST = True                      # 是否开启测速 (False直接保存所有链接)
FFMPEG_PATH = "ffmpeg"                         # FFmpeg程序路径
FFMPEG_CONCURRENCY = 3                         # 同时测试的链接数

# HLS 预检设置
ENABLE_HLS_PRECHECK = True                     # 是否开启轻量级预检 (快速淘汰死链)
HLS_MIN_BANDWIDTH_MBPS = 0.125                # 最低带宽要求 (MB/s)，低于此值直接淘汰

# FFmpeg 短测设置
FFMPEG_TEST_DURATION_SHORT = 5                 # 短测时长 (秒)
MIN_FRAMES_SHORT = 80                          # 短测最低解码帧数

# FFmpeg 长测设置
FFMPEG_TEST_DURATION_FULL = 15                 # 长测时长 (秒)
MIN_FRAMES_FULL = 300                          # 长测最低解码帧数

# 通用帧率要求
MIN_AVG_FPS = 24.0                             # 最低平均帧率 (fps)

# 测速白名单
TEST_ONLY_GROUPS = [                            # 只对这些分组的频道进行测速
    "央视频道",
    "卫视频道",
    "电影频道",
    "轮播频道",
    "儿童频道"
]
KEEP_GROUPS = TEST_ONLY_GROUPS                 # 最终只保留这些分组的频道

# 分辨率优先设置
PREFER_1080P = True                             # 是否优先1920x1080分辨率 (True=1080p排在最前)
PREFER_RESOLUTION_WIDTH = 1920                 # 优先分辨率宽度
PREFER_RESOLUTION_HEIGHT = 1080                # 优先分辨率高度

# 网页操作延时
DELAY_BETWEEN_IPS = 1.0                         # 处理完一个IP后等待多久 (秒)
DELAY_AFTER_CLICK = 0.5                         # 点击按钮后等待弹窗多久 (秒)
MAX_CHANNELS_PER_IP = 0                         # 单个IP最多提取多少个频道 (0表示不限制)

# 数据清洗
ENABLE_CHINESE_CLEAN = True                     # 是否移除频道名中的非中文字符
ENABLE_DEDUPLICATION = True                      # 是否去重 (相同的频道名+链接只保留一个)
ENABLE_SCREENSHOTS = False                       # 是否在关键步骤截图 (用于调试)
CCTV_USE_MAPPING = True                          # 是否将CCTV数字转为中文 (如 "CCTV-1" 变为 "CCTV-1综合")

# 网络协议
DEFAULT_PROTOCOL = "http://"                     # 当链接缺少协议头时，自动补全

# 缓存设置
ENABLE_CACHE = True                               # 是否启用缓存 (开启后，测过的链接不再重测)
CACHE_FILE = OUTPUT_DIR / "iptv_speed_cache.json"  # 缓存文件保存位置
CACHE_EXPIRE_HOURS = 48                           # 缓存过期时间 (小时)

# 更新时间显示
TIME_DISPLAY_AT_TOP = False                       # 更新时间显示位置 (True=文件最上面, False=文件最后面)

# 更新时间条目占位流
UPDATE_STREAM_URL = "https://gitee.com/bmg369/tvtest/raw/master/cg/index.m3u8"

# ============================================================================
# ============================= 频道分类规则 ==================================
# ============================================================================

PAGE_CONFIG = {
    "engine_search": ["引索搜索", "引擎搜索", "关键词搜索"],
    "multicast_tab": ["酒店提取"],
    "start_button": ["开始播放", "开始搜索", "开始提取"],
}

CATEGORY_RULES = [
    {"name": "央视频道",    "keywords": ["cctv", "cetv", "中央"]},
    {"name": "卫视频道",    "keywords": ["卫视", "凤凰", "tvb", "湖南", "浙江", "江苏", "东方"]},
    {"name": "4K专区",      "keywords": ["4k"]},
    {"name": "电影频道",    "keywords": ["电影", "影院", "chc"]},
    {"name": "轮播频道",    "keywords": ["轮播"]},
    {"name": "儿童频道",    "keywords": ["少儿", "动画", "卡通"]},
]

GROUP_ORDER = ["央视频道", "卫视频道", "电影频道", "轮播频道", "儿童频道"]

CCTV_NAME_MAPPING = {
    "1": "综合", "2": "财经", "3": "综艺", "4": "国际", "5": "体育",
    "5+": "体育赛事", "6": "电影", "7": "国防军事", "8": "电视剧",
    "9": "纪录", "10": "科教", "11": "戏曲", "12": "社会与法",
    "13": "新闻", "14": "少儿", "15": "音乐", "16": "奥林匹克",
    "17": "农业农村",
}

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
SUFFIX_REMOVE_PATTERN = re.compile(r'(4K|高清|超清|标清|HD|SD)$', re.IGNORECASE)

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

def clean_channel_suffix(name: str) -> str:
    while True:
        new_name = SUFFIX_REMOVE_PATTERN.sub('', name).strip()
        if new_name == name:
            break
        name = new_name
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
# ========================= 进度条打印工具 ====================================
# ============================================================================

def print_progress_bar(current: int, total: int, success: int, failed: int, last_percent: int, stage: str = "测速中") -> int:
    if total == 0: return 0
    percent = current / total
    percent_int = int(percent * 100)
    should_print = (
        (percent_int % 5 == 0 and percent_int > last_percent) or
        current == total or current == 0
    )
    if should_print:
        if percent_int == last_percent and current != total:
            return last_percent
        bar_length = 20
        filled_length = int(bar_length * percent)
        bar = '█' * filled_length + '░' * (bar_length - filled_length)
        logger.info(f"[{stage}] [{percent_int:3d}%] {bar} ({current}/{total}) | 有效:{success} | 共：{total}条")
        return percent_int
    return last_percent

# ============================================================================
# ========================= 【核心】三层测速逻辑 ===============================
# ============================================================================

async def hls_precheck(url: str) -> Tuple[bool, Dict[str, Any]]:
    if not ENABLE_HLS_PRECHECK:
        return True, {"bandwidth": 9999}

    try:
        async with aiohttp.ClientSession() as sess:
            async with sess.get(url, timeout=10) as r:
                if r.status != 200:
                    return False, {}
                text = await r.text()
    except Exception:
        return False, {}

    segments = []
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if not line.startswith("http"):
            line = urljoin(url, line)
        segments.append(line)
        if len(segments) >= 2:
            break

    if not segments:
        return False, {}

    speeds = []
    try:
        async with aiohttp.ClientSession() as sess:
            for seg in segments:
                t0 = time.time()
                async with sess.get(seg, timeout=10) as r:
                    if r.status != 200:
                        return False, {}
                    data = await r.read()
                dt = time.time() - t0
                if dt > 0:
                    kbps = len(data) * 8 / dt / 1000
                    speeds.append(kbps)
    except:
        return False, {}

    if not speeds:
        return False, {}

    avg_bandwidth = statistics.mean(speeds)
    min_kbps = HLS_MIN_BANDWIDTH_MBPS * 1000
    return avg_bandwidth > min_kbps, {"bandwidth": avg_bandwidth}

async def ffmpeg_test_wrapper(url: str, duration: int) -> Dict[str, Any]:
    if not shutil.which(FFMPEG_PATH):
        return {"ok": False, "fps": 0.0, "message": "FFmpeg未安装", "width": 0, "height": 0}

    cmd = [
        FFMPEG_PATH, "-hide_banner", "-y",
        "-fflags", "nobuffer",
        "-flags", "low_delay",
        "-rw_timeout", "5000000",
        "-i", url,
        "-t", str(duration),
        "-f", "null", "-"
    ]

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE
        )
        try:
            _, stderr = await asyncio.wait_for(proc.communicate(), timeout=duration + 10)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            return {"ok": False, "fps": 0.0, "message": "Timeout", "width": 0, "height": 0}

        output = stderr.decode('utf-8', errors='ignore')

        frame_matches = re.findall(r'frame=\s*(\d+)', output)
        fps_matches = re.findall(r'fps=\s*([\d.]+)', output)

        width, height = 0, 0
        res_match = re.search(r',\s*(\d{3,4})x(\d{3,4})\s*[, \[]', output)
        if res_match:
            width = int(res_match.group(1))
            height = int(res_match.group(2))

        frames = int(frame_matches[-1]) if frame_matches else 0
        avg_fps = float(fps_matches[-1]) if fps_matches else 0.0

        return {"ok": True, "fps": avg_fps, "frames": frames, "width": width, "height": height, "raw": output[-100:]}
    except Exception as e:
        return {"ok": False, "fps": 0.0, "message": str(e)[:50], "width": 0, "height": 0}

# ============================================================================
# ========================= 【调度】三层测速流水线 =============================
# ============================================================================

async def run_ffmpeg_test(channel_map: Dict[Tuple[str, str], List[str]]) -> Dict[Tuple[str, str], List[str]]:
    if not channel_map: return {}

    cache = load_cache()
    new_cache_entries = {}
    result_map = defaultdict(list)
    
    pending_tasks = []
    cache_hit_ok = 0
    cache_hit_fail = 0

    for (group, name), urls in channel_map.items():
        if group not in TEST_ONLY_GROUPS: continue
        for url in urls:
            if url in cache:
                res = cache[url]
                if res.get("passed_final"):
                    result_map[(group, name)].append((url, res.get("fps", 0), res.get("width", 0), res.get("height", 0)))
                    cache_hit_ok += 1
                else:
                    cache_hit_fail += 1
            else:
                pending_tasks.append((group, name, url))

    total_tasks = len(pending_tasks)
    logger.info(f"缓存命中(有效): {cache_hit_ok}, 需重新检测: {total_tasks}")
    if total_tasks == 0:
        return finalize_results(result_map)

    sem = asyncio.Semaphore(FFMPEG_CONCURRENCY)
    
    stats = {"pre": 0, "short": 0, "full": 0, "fail": 0}
    completed = 0
    last_pct = -1

    async def worker(item):
        nonlocal completed, last_pct
        group, name, url = item
        
        final_result = None
        stage_pass = False

        async with sem:
            try:
                if ENABLE_HLS_PRECHECK:
                    is_ok, pre_info = await hls_precheck(url)
                    if not is_ok:
                        final_result = {"passed_final": False, "stage": "precheck", "timestamp": time.time()}
                        stats["fail"] += 1
                        return (group, name, url, None, final_result)

                short_res = await ffmpeg_test_wrapper(url, FFMPEG_TEST_DURATION_SHORT)
                if not short_res["ok"] or \
                   short_res["fps"] < MIN_AVG_FPS or \
                   short_res["frames"] < MIN_FRAMES_SHORT:
                    final_result = {"passed_final": False, "stage": "short", "fps": short_res["fps"], "timestamp": time.time()}
                    stats["fail"] += 1
                    return (group, name, url, None, final_result)

                long_res = await ffmpeg_test_wrapper(url, FFMPEG_TEST_DURATION_FULL)
                if long_res["ok"] and \
                   long_res["fps"] >= MIN_AVG_FPS and \
                   long_res["frames"] >= MIN_FRAMES_FULL:
                    
                    final_result = {
                        "passed_final": True, "stage": "full",
                        "fps": long_res["fps"], "frames": long_res["frames"],
                        "width": long_res["width"], "height": long_res["height"],
                        "timestamp": time.time()
                    }
                    stats["full"] += 1
                    return (group, name, url, long_res, final_result)
                else:
                    final_result = {"passed_final": False, "stage": "full", "fps": long_res["fps"], "timestamp": time.time()}
                    stats["fail"] += 1
                    return (group, name, url, None, final_result)

            except Exception as e:
                logger.debug(f"Worker exception {url}: {e}")
                return (group, name, url, None, {"passed_final": False, "timestamp": time.time()})
            finally:
                completed += 1
                total_ok = stats["full"]
                total_fail = stats["fail"]
                last_pct = print_progress_bar(completed, total_tasks, total_ok, total_fail, last_pct, stage="三层过滤")

    tasks = [worker(item) for item in pending_tasks]
    results = await asyncio.gather(*tasks)

    for group, name, url, data, cache_entry in results:
        new_cache_entries[url] = cache_entry
        if data and cache_entry.get("passed_final"):
            result_map[(group, name)].append((url, data["fps"], data["width"], data["height"]))

    if new_cache_entries:
        cache.update(new_cache_entries)
        save_cache(cache)
        logger.info(f"缓存更新完毕，新增 {len(new_cache_entries)} 条记录")

    return finalize_results(result_map)

def finalize_results(result_map):
    final_map = {}
    for key, items in result_map.items():
        group, _ = key
        if group in TEST_ONLY_GROUPS:
            if PREFER_1080P:
                items.sort(
                    key=lambda x: (
                        0 if (x[2] == PREFER_RESOLUTION_WIDTH and x[3] == PREFER_RESOLUTION_HEIGHT) else 1,
                        -x[1]
                    )
                )
            else:
                items.sort(key=lambda x: -x[1])
            final_map[key] = [url for url, _, _, _ in items[:MAX_LINKS_PER_CHANNEL]]
        else:
            final_map[key] = [url for url, _, _, _ in items]
    
    total_final = sum(len(v) for v in final_map.values())
    logger.info(f"全部流程结束，最终保留 {total_final} 条优质链接")
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
                
                if group not in KEEP_GROUPS:
                    continue

                if group == "央视频道":
                    final_name = norm
                else:
                    temp_name = clean_chinese_only(name) if ENABLE_CHINESE_CLEAN else name
                    final_name = clean_channel_suffix(temp_name)
                
                if final_name: 
                    entries.append((group, final_name, link))
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
    tz_beijing = pytz.timezone('Asia/Shanghai')
    now = datetime.datetime.now(tz_beijing)
    time_str = now.strftime("%Y-%m-%d %H:%M:%S")
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

    total_links = sum(len(v) for v in grouped.values())
    position_text = "顶部" if TIME_DISPLAY_AT_TOP else "底部"
    logger.info(f"导出完成！共 {total_links} 条有效频道链接，更新时间已放在{position_text}")

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

            logger.info(f"原始提取：{len(raw_entries)} 条未筛选的频道链接")

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
