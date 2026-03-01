#!/usr/bin/env python3
"""
IPTV 组播提取工具（配置文件版）
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
import configparser
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any, Union
from urllib.parse import urljoin
import functools
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# ============================================================================
# ============================= 读取配置文件 =================================
# ============================================================================

# 初始化配置解析器
config = configparser.ConfigParser(allow_no_value=True, comment_prefixes='#')
config.optionxform = str  # 保留配置项大小写

# 读取配置文件
CONFIG_FILE = Path(__file__).parent / "iptv_config.ini"
if not CONFIG_FILE.exists():
    print(f"配置文件不存在：{CONFIG_FILE}")
    sys.exit(1)

config.read(CONFIG_FILE, encoding='utf-8')

# -------------------------- 基础配置 --------------------------
TARGET_URL = config.get('基础配置', 'TARGET_URL')
HEADLESS = config.getboolean('基础配置', 'HEADLESS')
BROWSER_TYPE = config.get('基础配置', 'BROWSER_TYPE')
MAX_IPS = config.getint('基础配置', 'MAX_IPS')
PAGE_LOAD_TIMEOUT = config.getint('基础配置', 'PAGE_LOAD_TIMEOUT')
DELAY_BETWEEN_IPS = config.getfloat('基础配置', 'DELAY_BETWEEN_IPS')
DELAY_AFTER_CLICK = config.getfloat('基础配置', 'DELAY_AFTER_CLICK')
MAX_CHANNELS_PER_IP = config.getint('基础配置', 'MAX_CHANNELS_PER_IP')
DEFAULT_PROTOCOL = config.get('基础配置', 'DEFAULT_PROTOCOL')

# -------------------------- 页面配置 --------------------------
def get_config_list(section, key):
    """将配置文件中的逗号分隔字符串转为列表"""
    value = config.get(section, key, fallback='')
    return [item.strip() for item in value.split(',') if item.strip()]

ENGINE_SEARCH = get_config_list('页面配置', 'ENGINE_SEARCH')
MULTICAST_TAB = get_config_list('页面配置', 'MULTICAST_TAB')
START_BUTTON = get_config_list('页面配置', 'START_BUTTON')

PAGE_CONFIG = {
    "engine_search": ENGINE_SEARCH,
    "multicast_tab": MULTICAST_TAB,
    "start_button": START_BUTTON,
}

# -------------------------- 输出配置 --------------------------
OUTPUT_DIR = config.get('输出配置', 'OUTPUT_DIR') or Path(__file__).parent
OUTPUT_DIR = Path(OUTPUT_DIR)
OUTPUT_M3U_FILENAME = OUTPUT_DIR / config.get('输出配置', 'OUTPUT_M3U_FILENAME')
OUTPUT_TXT_FILENAME = OUTPUT_DIR / config.get('输出配置', 'OUTPUT_TXT_FILENAME')
MAX_LINKS_PER_CHANNEL = config.getint('输出配置', 'MAX_LINKS_PER_CHANNEL')
TIME_DISPLAY_AT_TOP = config.getboolean('输出配置', 'TIME_DISPLAY_AT_TOP')
UPDATE_STREAM_URL = config.get('输出配置', 'UPDATE_STREAM_URL')

# -------------------------- 自定义源配置 --------------------------
ENABLE_CUSTOM_SOURCES = config.getboolean('自定义源配置', 'ENABLE_CUSTOM_SOURCES')
# 解析自定义源列表（处理换行分隔）
CUSTOM_SOURCES_RAW = config.get('自定义源配置', 'CUSTOM_SOURCES', fallback='')
CUSTOM_SOURCES = [line.strip() for line in CUSTOM_SOURCES_RAW.split('\n') if line.strip()]

# -------------------------- FFmpeg测速配置 --------------------------
ENABLE_FFMPEG_TEST = config.getboolean('FFmpeg测速配置', 'ENABLE_FFMPEG_TEST')
FFMPEG_PATH = config.get('FFmpeg测速配置', 'FFMPEG_PATH')
FFMPEG_CONCURRENCY = config.getint('FFmpeg测速配置', 'FFMPEG_CONCURRENCY')
FFMPEG_TEST_DURATION_SHORT = config.getint('FFmpeg测速配置', 'FFMPEG_TEST_DURATION_SHORT')
MIN_FRAMES_SHORT = config.getint('FFmpeg测速配置', 'MIN_FRAMES_SHORT')
FFMPEG_TEST_DURATION_FULL = config.getint('FFmpeg测速配置', 'FFMPEG_TEST_DURATION_FULL')
MIN_FRAMES_FULL = config.getint('FFmpeg测速配置', 'MIN_FRAMES_FULL')
MIN_AVG_FPS = config.getfloat('FFmpeg测速配置', 'MIN_AVG_FPS')

# -------------------------- HLS预检配置 --------------------------
ENABLE_HLS_PRECHECK = config.getboolean('HLS预检配置', 'ENABLE_HLS_PRECHECK')
HLS_MIN_BANDWIDTH_MBPS = config.getfloat('HLS预检配置', 'HLS_MIN_BANDWIDTH_MBPS')

# -------------------------- 过滤与分类配置 --------------------------
TEST_ONLY_GROUPS = get_config_list('过滤与分类配置', 'TEST_ONLY_GROUPS')
KEEP_GROUPS = get_config_list('过滤与分类配置', 'KEEP_GROUPS')
PREFER_1080P = config.getboolean('过滤与分类配置', 'PREFER_1080P')
PREFER_RESOLUTION_WIDTH = config.getint('过滤与分类配置', 'PREFER_RESOLUTION_WIDTH')
PREFER_RESOLUTION_HEIGHT = config.getint('过滤与分类配置', 'PREFER_RESOLUTION_HEIGHT')
ENABLE_CHINESE_CLEAN = config.getboolean('过滤与分类配置', 'ENABLE_CHINESE_CLEAN')
ENABLE_DEDUPLICATION = config.getboolean('过滤与分类配置', 'ENABLE_DEDUPLICATION')
ENABLE_SCREENSHOTS = config.getboolean('过滤与分类配置', 'ENABLE_SCREENSHOTS')
CCTV_USE_MAPPING = config.getboolean('过滤与分类配置', 'CCTV_USE_MAPPING')

# -------------------------- 缓存配置 --------------------------
ENABLE_CACHE = config.getboolean('缓存配置', 'ENABLE_CACHE')
CACHE_FILE = OUTPUT_DIR / config.get('缓存配置', 'CACHE_FILE')
CACHE_EXPIRE_HOURS = config.getint('缓存配置', 'CACHE_EXPIRE_HOURS')

# ============================================================================
# ============================= 频道分类规则 ==================================
# ============================================================================

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

# -------------------------- 自定义源解析工具函数 --------------------------
async def fetch_content(source: str) -> Optional[str]:
    """
    获取自定义源内容（适配M3U文件流下载场景）
    支持网络链接+本地文件，二进制读取+多编码适配+请求头+重定向
    """
    try:
        if source.startswith(('http://', 'https://')):
            # 网络链接：添加浏览器请求头+允许重定向+延长超时
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': '*/*',
                'Connection': 'keep-alive'
            }
            async with aiohttp.ClientSession(headers=headers, timeout=aiohttp.ClientTimeout(total=60)) as sess:
                async with sess.get(source, allow_redirects=True, max_redirects=5) as r:
                    if r.status != 200:
                        logger.warning(f"获取网络源失败 {source}: HTTP {r.status}")
                        return None
                    
                    # 二进制读取内容（适配文件流下载的M3U）
                    content_bytes = await r.read()
                    if not content_bytes:
                        logger.warning(f"获取到的内容为空: {source}")
                        return None
                    
                    # 尝试多种编码解码（解决中文乱码/编码错误）
                    content = None
                    for encoding in ['utf-8', 'gbk', 'gb2312', 'latin-1']:
                        try:
                            content = content_bytes.decode(encoding)
                            break
                        except UnicodeDecodeError:
                            continue
                    
                    if not content:
                        # 最后尝试忽略错误解码，确保内容不丢失
                        content = content_bytes.decode('utf-8', errors='ignore')
                    
                    return content
        else:
            # 本地文件：同样用二进制读取适配不同编码
            path = Path(source)
            if path.exists():
                with open(path, 'rb') as f:
                    content_bytes = f.read()
                # 尝试多种编码解码
                content = None
                for encoding in ['utf-8', 'gbk', 'gb2312', 'latin-1']:
                    try:
                        content = content_bytes.decode(encoding)
                        break
                    except UnicodeDecodeError:
                        continue
                if not content:
                    content = content_bytes.decode('utf-8', errors='ignore')
                return content
            else:
                logger.warning(f"本地文件不存在: {source}")
                return None
    except asyncio.TimeoutError:
        logger.warning(f"获取源内容超时（60秒）: {source}")
        return None
    except aiohttp.ClientError as e:
        logger.warning(f"网络请求错误: {source} - {str(e)}")
        return None
    except Exception as e:
        logger.warning(f"获取源内容失败: {source} - {str(e)}")
        return None

def parse_m3u(content: str) -> List[Tuple[str, str, str]]:
    """
    解析M3U格式内容（兼容非标准M3U）
    返回 (分组, 频道名, 链接) 列表
    """
    entries = []
    current_group = ""
    lines = content.splitlines()
    i = 0
    
    while i < len(lines):
        line = lines[i].strip()
        i += 1
        
        # 跳过空行和非核心注释行
        if not line or (line.startswith('#') and not line.startswith(('#EXTINF:', '#EXTGRP:'))):
            continue
        
        # 解析分组行（#EXTGRP）
        if line.startswith('#EXTGRP:'):
            current_group = line[8:].strip()
            continue
        
        # 解析核心频道行（#EXTINF）
        if line.startswith('#EXTINF:'):
            # 提取分组（group-title）
            group_match = re.search(r'group-title="([^"]+)"', line)
            if group_match:
                current_group = group_match.group(1)
            
            # 提取频道名（逗号后内容）
            channel_name = "未知频道"
            if ',' in line:
                channel_name = line.split(',', 1)[1].strip()
            
            # 寻找下一行非注释行作为频道链接
            channel_url = None
            while i < len(lines):
                url_line = lines[i].strip()
                i += 1
                if url_line and not url_line.startswith('#'):
                    channel_url = url_line
                    break
            
            # 有效频道才加入列表
            if channel_url and channel_name:
                entries.append((current_group, channel_name, channel_url))
    
    logger.info(f"解析M3U完成，共提取 {len(entries)} 个频道")
    return entries

def parse_txt(content: str) -> List[Tuple[str, str, str]]:
    """解析TXT格式内容，返回 (分组, 频道名, 链接) 列表"""
    entries = []
    current_group = ""
    lines = content.splitlines()
    for line in lines:
        line = line.strip()
        if not line:
            continue
        if ',#genre#' in line.lower():
            # 分组行
            current_group = line.split(',', 1)[0].strip()
        else:
            # 频道行：频道名,链接
            if ',' in line:
                name, url = line.split(',', 1)
                name = name.strip()
                url = url.strip()
                if name and url:
                    entries.append((current_group, name, url))
    return entries

async def process_custom_sources() -> List[Tuple[str, str, str]]:
    """处理所有自定义源，返回 (分组, 频道名, 链接) 列表（取消自定义频道，自动归类）"""
    if not ENABLE_CUSTOM_SOURCES:
        return []
    
    all_entries = []
    for source in CUSTOM_SOURCES:
        # 处理直接指定的频道（格式：分组|频道名|链接）
        if '|' in source and len(source.split('|')) == 3:
            group, name, url = source.split('|', 2)
            group = group.strip()
            name = name.strip()
            url = url.strip()
            all_entries.append((group, name, url))
            logger.info(f"添加自定义频道: [{group}] {name}")
            continue
        
        # 否则是M3U/TXT文件/链接，需要解析
        content = await fetch_content(source)
        if not content:
            continue
        
        # 判断是m3u还是txt
        if '#EXTM3U' in content:
            entries = parse_m3u(content)
            logger.info(f"解析M3U源 {source}: 得到 {len(entries)} 个频道")
        else:
            entries = parse_txt(content)
            logger.info(f"解析TXT源 {source}: 得到 {len(entries)} 个频道")
        
        all_entries.extend(entries)
    
    # 对自定义源进行标准化清洗和自动分类（取消自定义频道）
    classified_entries = []
    for group, name, url in all_entries:
        # 补全协议头
        if not url.startswith(('http://', 'https://', 'rtsp://', 'rtmp://')):
            url = DEFAULT_PROTOCOL + url
        
        # 标准化频道名（比如CCTV转中文）
        norm_name = normalize_cctv(name)
        
        # 自动分类（优先级：手动指定分组 > 关键词分类）
        if group:
            new_group = group
        else:
            new_group = classify_channel(norm_name)
        
        # 如果无法分类，跳过（不加入自定义频道）
        if not new_group:
            logger.warning(f"自定义频道 {name} 无法自动分类，已跳过")
            continue
        
        # 清理频道名
        if new_group == "央视频道":
            final_name = norm_name
        else:
            temp_name = clean_chinese_only(name) if ENABLE_CHINESE_CLEAN else name
            final_name = clean_channel_suffix(temp_name)
        
        # 仅保留在KEEP_GROUPS中的频道
        if final_name and new_group in KEEP_GROUPS:
            classified_entries.append((new_group, final_name, url))
    
    logger.info(f"自定义源处理完成，共 {len(classified_entries)} 个有效频道")
    return classified_entries

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
            raw_entries = []

            # 1. 网页爬取逻辑
            if TARGET_URL:
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

                if await wait_data(page):
                    rows = page.locator("div.ios-list-item").filter(has_text="频道:")
                    total_rows = await rows.count()
                    
                    if total_rows > 0:
                        process_count = min(total_rows, MAX_IPS) if MAX_IPS > 0 else total_rows
                        logger.info(f"找到 {total_rows} 个地址，处理前 {process_count} 个")

                        for i in range(process_count):
                            entries = await extract_one_ip(page, rows.nth(i), i+1)
                            raw_entries.extend(entries)
                            if i < process_count - 1: await asyncio.sleep(DELAY_BETWEEN_IPS)

                        logger.info(f"网页原始提取：{len(raw_entries)} 条未筛选的频道链接")
                    else:
                        logger.warning("未在网页上找到任何地址")
                else:
                    logger.error("网页数据加载失败")

            # 2. 合并自定义直播源
            if ENABLE_CUSTOM_SOURCES:
                custom_entries = await process_custom_sources()
                raw_entries.extend(custom_entries)
                logger.info(f"合并自定义源后，共 {len(raw_entries)} 条未筛选的频道链接")

            if not raw_entries:
                logger.error("没有获取到任何频道链接（网页+自定义均为空）")
                return

            # 3. 去重与构建频道映射
            channel_map = defaultdict(list)
            seen = set()
            for group, name, url in raw_entries:
                if ENABLE_DEDUPLICATION:
                    key = (group, name, url)
                    if key in seen: continue
                    seen.add(key)
                channel_map[(group, name)].append(url)

            # 4. 测速
            if ENABLE_FFMPEG_TEST and channel_map:
                channel_map = await run_ffmpeg_test(channel_map)

            # 5. 导出
            export_results_with_timestamp(channel_map)

        except Exception as e:
            logger.exception("主流程异常")
        finally:
            await browser.close()

if __name__ == "__main__":
    # 兼容Python 3.8的asyncio.run
    try:
        asyncio.run(main())
    except RuntimeError as e:
        if "asyncio.run() cannot be called from a running event loop" in str(e):
            loop = asyncio.get_event_loop()
            loop.run_until_complete(main())
    except AttributeError:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())
