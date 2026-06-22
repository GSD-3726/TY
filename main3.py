import asyncio
import json
import logging
import re
import os
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
# 所有可调参数均集中于此，分类整理，方便修改

# -------------------------- 1. 基础设置 ------------------------------------
TARGET_URL = "https://iptv.809899.xyz"          # 目标网站地址
HEADLESS = True                                 # 无头模式（GitHub运行必须True）
BROWSER_TYPE = "chromium"                       # 浏览器内核
OUTPUT_DIR = Path(__file__).parent              # 输出目录（当前脚本目录）
OUTPUT_M3U_FILENAME = OUTPUT_DIR / "iptv_channels.m3u"
OUTPUT_TXT_FILENAME = OUTPUT_DIR / "iptv_channels.txt"
LOCAL_SOURCE_FILENAME = OUTPUT_DIR / "local_source.txt"
MAX_LINKS_PER_CHANNEL = 8                       # 每个频道最多保留几条链接
DEFAULT_PROTOCOL = "http://"                    # 默认协议（用于补全链接）

# -------------------------- 2. 爬取控制 ------------------------------------
EXTRACT_MODE = "酒店提取"                       # "酒店提取" 或 "组播提取"
ENABLE_WEB_SCRAPING = True                      # 是否启用网站爬取
MAX_IPS = 100                                   # 最多处理多少个IP
MAX_TOTAL_CHANNELS = 0                          # 总频道上限（0=不限制）
MAX_CHANNELS_PER_IP = 0                         # 单个IP最多提取频道数
DELAY_BETWEEN_IPS = 0.1                         # 切换IP间隔（秒）
DELAY_AFTER_CLICK = 0.3                         # 点击弹窗后等待（秒）
MODAL_WAIT_TIMEOUT = 1                          # 等待模态框出现（秒）

# -------------------------- 3. 超时与等待 ----------------------------------
PAGE_LOAD_TIMEOUT = 120                         # 页面加载超时（秒）
DATA_LOAD_TIMEOUT = 60                          # 数据加载总超时（秒）
AFTER_START_WAIT = 30                           # 点击【开始提取】后等待秒数
IP_ADDR_TIMEOUT = 0.1                           # 读取IP地址超时（秒）
CHANNEL_NAME_TIMEOUT = 0.1                      # 读取频道名称超时（秒）
CHANNEL_URL_TIMEOUT = 0.1                       # 读取频道链接超时（秒）
SCROLL_TIMEOUT = 0.1                            # 滚动到元素视野的超时（秒）
CLICK_TIMEOUT = 0.1                             # 点击元素的超时（秒）
WAIT_FOR_ELEMENT_TIMEOUT = 30                   # wait_for_element默认超时（秒）
DATA_CHECK_INTERVAL = 10                        # 数据加载检查间隔（秒）

# -------------------------- 4. GitHub源订阅 --------------------------------
ENABLE_GITHUB_SOURCES = True                   # 是否启用GitHub源
GITHUB_M3U_LINKS = [
    "https://gh-proxy.com/https://raw.githubusercontent.com/mzky/checklist/refs/heads/master/itvlist.m3u",
    "https://gh-proxy.com/https://raw.githubusercontent.com/ajqubbs/zhiboyuan/main/%E9%A6%99%E9%9B%A8%E7%9B%B4%E6%92%AD.txt",
    "https://gh-proxy.com/https://raw.githubusercontent.com/vinkerq/iptv-api/refs/heads/master/iptv.txt"
]

# -------------------------- 5. FFmpeg测速设置 -------------------------------
ENABLE_FFMPEG_TEST = True                       # 是否启用FFmpeg测速
FFMPEG_PATH = "ffmpeg"                          # FFmpeg 程序路径
FFMPEG_TEST_DURATION = 15                       # 每个链接测试时长（秒）
FFMPEG_CONCURRENCY = 6                          # 并发测速数量
MIN_AVG_FPS = 25                                # 最低平均帧率 - 直播标准25fps(PAL)
MIN_FRAMES = 350                                # 最低解码帧数 - 配合15秒: 15*25=375,留余量
MIN_SPEED = 0.95                                # 最低解码速度 - speed<0.95说明解码跟不上
MAX_DROP_RATIO = 0.03                           # 最大丢帧率 - 超过3%认为卡顿

# -------------------------- 6. 缓存设置 ------------------------------------
ENABLE_CACHE = True                             # 启用测速缓存
CACHE_FILE = OUTPUT_DIR / "iptv_speed_cache.json"
CACHE_EXPIRE_HOURS = 72                         # 缓存过期小时
CACHE_EXPIRE_HOURS_GOOD = 168                   # 高质量源缓存过期小时（7天）
CACHE_EXPIRE_HOURS_BAD = 24                     # 低质量源缓存过期小时（1天）

# -------------------------- 7. 数据处理 ------------------------------------
ENABLE_CHINESE_CLEAN = True                     # 清理非中文字符（仅对非央视）
ENABLE_DEDUPLICATION = True                     # 全局去重
CCTV_USE_MAPPING = True                         # CCTV映射中文名称
ENABLE_MIGU_FILTER = True                       # 过滤包含"migu"的链接
SKIP_INTERNAL_IP = True                         # 跳过内网IP
ENABLE_SATELLITE_CLEAN = True                   # 卫视名称清洗

# -------------------------- 8. 频道分类规则 --------------------------------
CATEGORY_RULES = [
    {"name": "4K专区", "keywords": ["4k", "4K", "超高清", "2160p"]},
    {"name": "央视频道", "keywords": ["cctv", "cetv", "央视", "中央", "CCTV", "CETV", "央视频道"]},
    {"name": "卫视频道", "keywords": ["卫视", "卫视高清"]},
    {"name": "电影频道", "keywords": ["电影", "影院", "chc", "动作", "剧场", "映画", "影视", "大片", "影视频道"]},
    {"name": "轮播频道", "keywords": ["轮播", "滚动", "循环"]},
    {"name": "儿童频道", "keywords": ["少儿", "动画", "动漫", "卡通", "亲子", "儿童", "宝贝"]},
]
GROUP_ORDER = ["央视频道", "卫视频道", "电影频道", "4K专区", "儿童频道", "轮播频道"]

CCTV_NAME_MAPPING = {
    "1": "综合", "2": "财经", "3": "综艺", "4": "国际",
    "5": "体育", "5+": "体育赛事",
    "6": "电影", "7": "国防军事", "8": "电视剧",
    "9": "纪录", "10": "科教", "11": "戏曲",
    "12": "社会与法", "13": "新闻", "14": "少儿",
    "15": "音乐", "16": "奥林匹克", "17": "农业农村",
}
CCTV_ORDER = [
    "CCTV-1综合", "CCTV-2财经", "CCTV-3综艺", "CCTV-4国际",
    "CCTV-5体育", "CCTV-5+体育赛事",
    "CCTV-6电影", "CCTV-7国防军事", "CCTV-8电视剧",
    "CCTV-9纪录", "CCTV-10科教", "CCTV-11戏曲",
    "CCTV-12社会与法", "CCTV-13新闻", "CCTV-14少儿",
    "CCTV-15音乐", "CCTV-16奥林匹克", "CCTV-17农业农村",
    "CETV-1", "CETV-2", "CETV-3", "CETV-4", "CETV-5"
]

# -------------------------- 9. 页面按钮匹配 --------------------------------
PAGE_CONFIG = {
    "engine_search": ["引擎搜索", "关键词搜索"],
    "hotel": ["酒店提取"],
    "multicast": ["组播提取"],
    "start_button": ["开始播放", "开始搜索", "开始提取"],
}

# -------------------------- 10. 日志与更新 --------------------------------
TIME_DISPLAY_AT_TOP = False
UPDATE_STREAM_URL = "https://gitee.com/bmg369/tvtest/raw/master/cg/index.m3u8"

# -------------------------- 11. 连通性/预检配置 -----------------------------
CONNECTIVITY_CONCURRENCY = 20                   # 连通性预检并发数
CONNECTIVITY_TIMEOUT = 3                        # 连通性超时（秒）

# -------------------------- 12. 增量更新配置 --------------------------------
ENABLE_INCREMENTAL_UPDATE = True
QUALITY_THRESHOLD = 4
REQUIRED_CHANNELS_FILE = OUTPUT_DIR / "频道.txt"

# -------------------------- 13. 早停优化配置 --------------------------------
EARLY_TERMINATE_SECONDS = 8                    # 启动缓冲期，前8秒不做早停判断
EARLY_TERMINATE_MIN_FPS = 10                    # 最低帧率阈值
EARLY_TERMINATE_MIN_FRAMES = 50                 # 最少解码帧数才触发判断

# ============================================================================
# ============================= 日志配置（北京时间） ===========================
# ============================================================================
log_level = logging.INFO

class BeijingFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.datetime.fromtimestamp(
            record.created, datetime.timezone(datetime.timedelta(hours=8))
        )
        s = dt.strftime("%Y-%m-%d %H:%M:%S")
        return f"{s},{int(record.msecs):03d}"

logger = logging.getLogger('IPTV-Extractor')
logger.setLevel(log_level)
logger.handlers.clear()
stdout_h = logging.StreamHandler(sys.stdout)
formatter = BeijingFormatter("%(asctime)s - %(levelname)s - %(message)s")
stdout_h.setFormatter(formatter)
logger.addHandler(stdout_h)

# ============================================================================
# ========================= 工具函数 =========================================
# ============================================================================
CCTV_PATTERN = re.compile(r'(cctv)[-\s]?(\d{1,3}|5\+)', re.IGNORECASE)
CHINESE_ONLY_PATTERN = re.compile(r'[^\u4e00-\u9fff]')
INTERNAL_IP_PATTERN = re.compile(r'^(192\.168\.|10\.|172\.(1[6-9]|2[0-9]|3[0-1])\.|127\.0\.0\.1)')
CHINESE_NUM_MAP = {
    '一': '1', '二': '2', '三': '3', '四': '4', '五': '5',
    '六': '6', '七': '7', '八': '8', '九': '9', '十': '10',
    '十一': '11', '十二': '12', '十三': '13', '十四': '14',
    '十五': '15', '十六': '16', '十七': '17'
}
CHINESE_NUM_PATTERN = '|'.join(sorted(CHINESE_NUM_MAP.keys(), key=len, reverse=True))

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
    # 4K
    if re.search(r'cctv[-\s]?4k', name_lower):
        return "CCTV-4K"
    # 5+ 变体
    if re.search(r'cctv[-\s]?5\+?|cctv5plus|cctv5p', name_lower):
        return "CCTV-5+体育赛事" if CCTV_USE_MAPPING else "CCTV5+"
    # CETV
    cetv_match = re.search(r'cetv[-\s]?(\d+)', name_lower)
    if cetv_match:
        num = cetv_match.group(1)
        return f"CETV-{num}"
    # 常规 CCTV
    cctv_match = CCTV_PATTERN.search(name_lower)
    if cctv_match:
        num = cctv_match.group(2)
        if CCTV_USE_MAPPING and num in CCTV_NAME_MAPPING:
            return f"CCTV-{num}{CCTV_NAME_MAPPING[num]}"
        return f"CCTV-{num}"
    # 中文数字
    chinese_pattern = rf'中央\s*({CHINESE_NUM_PATTERN})'
    ch_match = re.search(chinese_pattern, name)
    if ch_match:
        ch_num = ch_match.group(1)
        num = CHINESE_NUM_MAP.get(ch_num)
        if num:
            if CCTV_USE_MAPPING and num in CCTV_NAME_MAPPING:
                return f"CCTV-{num}{CCTV_NAME_MAPPING[num]}"
            return f"CCTV-{num}"
    return name

def clean_chinese_only(name: str) -> str:
    return CHINESE_ONLY_PATTERN.sub('', name)

def build_selector(text_list, element_type="button"):
    if not text_list:
        return ""
    pattern = "|".join(re.escape(t) for t in text_list)
    elements = [el.strip() for el in element_type.split(',')]
    selectors = [f"{el}:text-matches('{pattern}')" for el in elements]
    return ",".join(selectors)

def is_internal_ip(url: str) -> bool:
    try:
        host = urlparse(url).hostname
        if not host:
            return False
        return bool(INTERNAL_IP_PATTERN.match(host))
    except Exception:
        return False

def clean_satellite_name(name: str) -> str:
    if not ENABLE_SATELLITE_CLEAN:
        return name
    pattern = re.compile(
        r'(.*?卫视)'
        r'(?:\s*[（(]?)\s*'
        r'(移动|高清|HD|超高清|4K|标清|测试)'
        r'(?:\s*[）)]?)\s*',
        re.IGNORECASE
    )
    while True:
        new_name = pattern.sub(r'\1', name)
        if new_name == name:
            break
        name = new_name
    return name

def clean_final_name(group: str, name: str) -> str:
    """根据分组和开关返回最终频道名"""
    if group == "央视频道":
        return name
    if ENABLE_CHINESE_CLEAN:
        return clean_chinese_only(name)
    return name

# ============================================================================
# ========================= 缓存管理 ==========================================
# ============================================================================
CACHE_EXPIRE_SECONDS = CACHE_EXPIRE_HOURS * 3600
CACHE_EXPIRE_SECONDS_GOOD = CACHE_EXPIRE_HOURS_GOOD * 3600
CACHE_EXPIRE_SECONDS_BAD = CACHE_EXPIRE_HOURS_BAD * 3600

def _is_high_quality_cache(data: dict) -> bool:
    if not data.get("ok", False):
        return False
    fps = data.get("fps", 0.0)
    speed = data.get("speed", 0.0)
    drop = data.get("drop", 0)
    frames = data.get("frames", 1)
    drop_ratio = drop / max(frames, 1)
    return fps >= 28 and speed >= 1.0 and drop_ratio <= 0.01

def _get_cache_expire_seconds(data: dict) -> int:
    if _is_high_quality_cache(data):
        return CACHE_EXPIRE_SECONDS_GOOD
    elif data.get("ok", False):
        return CACHE_EXPIRE_SECONDS
    else:
        return CACHE_EXPIRE_SECONDS_BAD

def load_cache():
    if not ENABLE_CACHE or not CACHE_FILE.exists():
        return {}
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            cache = json.load(f)
        now = time.time()
        valid_cache = {}
        good_count = normal_count = bad_count = 0
        for url, data in cache.items():
            if isinstance(data, dict) and "ok" in data and "timestamp" in data:
                if "width" not in data:
                    data["width"] = 0
                if "height" not in data:
                    data["height"] = 0
                if "drop" not in data:
                    data["drop"] = 0
                if "speed" not in data:
                    data["speed"] = 0.0
                if "quality_score" not in data:
                    data["quality_score"] = 0.0
                expire_seconds = _get_cache_expire_seconds(data)
                if now - data["timestamp"] < expire_seconds:
                    valid_cache[url] = data
                    if _is_high_quality_cache(data):
                        good_count += 1
                    elif data["ok"]:
                        normal_count += 1
                    else:
                        bad_count += 1
            elif isinstance(data, (int, float)):
                valid_cache[url] = {
                    "ok": data > 0,
                    "fps": 0.0,
                    "frames": 0,
                    "drop": 0,
                    "speed": 0.0,
                    "width": 0,
                    "height": 0,
                    "quality_score": 0.0,
                    "timestamp": now
                }
        logger.info(f"缓存加载完成，有效条目数: {len(valid_cache)} (高质量:{good_count} 普通:{normal_count} 低质量:{bad_count})")
        return valid_cache
    except Exception as e:
        logger.debug(f"加载缓存异常: {e}，将重新创建")
        return {}

def save_cache(cache):
    if not ENABLE_CACHE:
        return
    try:
        now = time.time()
        cleaned_cache = {}
        for url, data in cache.items():
            if isinstance(data, dict) and "timestamp" in data:
                expire_seconds = _get_cache_expire_seconds(data)
                if now - data["timestamp"] < expire_seconds:
                    cleaned_cache[url] = data
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cleaned_cache, f, ensure_ascii=False, indent=2)
        logger.info(f"缓存保存完成，清理了 {len(cache) - len(cleaned_cache)} 条过期缓存")
    except Exception as e:
        logger.warning(f"保存缓存失败: {e}")

def is_cache_valid(timestamp, data=None):
    if data:
        expire_seconds = _get_cache_expire_seconds(data)
    else:
        expire_seconds = CACHE_EXPIRE_SECONDS
    return time.time() - timestamp < expire_seconds

# ============================================================================
# ========================= 连通性测试函数 ====================================
# ============================================================================
def extract_domain(url: str) -> str:
    try:
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}/"
    except Exception:
        return ""

async def check_url_connectivity(url: str, timeout: int) -> bool:
    """单个GET请求测试连通性，总超时=timeout"""
    if not url.startswith(('http://', 'https://')):
        return True
    try:
        domain = extract_domain(url)
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        if domain:
            headers['Referer'] = domain
        timeout_obj = aiohttp.ClientTimeout(total=timeout)
        async with aiohttp.ClientSession(timeout=timeout_obj) as session:
            async with session.get(url, headers=headers, allow_redirects=True) as resp:
                if resp.status != 200:
                    return False
                # 只读少量数据即可
                data = await resp.content.read(8192)
                return len(data) > 0
    except Exception:
        return False

# ============================================================================
# ========================= 重试、进度、FFmpeg测速 =============================
# ============================================================================
def retry_async(max_retries=2, delay=1.0, exceptions=(Exception,)):
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            for attempt in range(1, max_retries+1):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_retries:
                        raise
                    logger.warning(f"尝试 {attempt}/{max_retries} 失败: {e}，重试")
                    await asyncio.sleep(delay)
            return None
        return wrapper
    return decorator

def print_progress_bar(current: int, total: int, success: int, failed: int, last_percent: int, early_stopped: int = 0) -> int:
    if total == 0:
        return 0
    percent_int = int((current / total) * 100)
    if not ((percent_int % 5 == 0 and percent_int > last_percent) or current == total):
        return last_percent
    bar = '█' * int(20 * current / total) + '░' * (20 - int(20 * current / total))
    extra_info = f" | 早期终止:{early_stopped}" if early_stopped > 0 else ""
    logger.info(f"[{percent_int:3d}%] {bar} ({current}/{total}) | 成功:{success} | 失败:{failed}{extra_info}")
    sys.stdout.flush()
    return percent_int

async def test_stream_with_ffmpeg(url: str) -> Dict[str, Any]:
    """优化版 FFmpeg 测速：缓冲期+连续两次检测避免误杀，兼容speed格式"""
    if not shutil.which(FFMPEG_PATH):
        logger.error(f"未找到FFmpeg: {FFMPEG_PATH}")
        return {"ok": False, "fps": 0.0, "frames": 0, "width": 0, "height": 0, "drop": 0, "speed": 0.0, "quality_score": 0.0, "message": "FFmpeg未安装"}
    domain = extract_domain(url)
    referer = domain if domain else "https://www.miguvideo.com/"
    headers = (
        f"User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36\r\n"
        f"Referer: {referer}\r\n"
    )
    cmd = [
        FFMPEG_PATH,
        "-hide_banner",
        "-y",
        "-headers", headers,
        "-fflags", "nobuffer",
        "-rw_timeout", "5000000",
        "-i", url,
        "-t", str(FFMPEG_TEST_DURATION),
        "-f", "null",
        "-"
    ]
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE
        )
        start_time = time.time()
        stderr_data = b""
        early_terminated = False
        last_fps = None
        try:
            while True:
                try:
                    chunk = await asyncio.wait_for(proc.stderr.read(4096), timeout=1.0)
                    if not chunk:
                        break
                    stderr_data += chunk
                    full_output = stderr_data.decode('utf-8', errors='ignore')
                    elapsed = time.time() - start_time
                    if elapsed >= EARLY_TERMINATE_SECONDS:
                        fps_matches = re.findall(r'fps=\s*([\d.]+)', full_output)
                        frame_matches = re.findall(r'frame=\s*(\d+)', full_output)
                        if fps_matches and frame_matches:
                            current_fps = float(fps_matches[-1])
                            current_frames = int(frame_matches[-1])
                            if current_frames >= EARLY_TERMINATE_MIN_FRAMES:
                                if last_fps is not None and last_fps < EARLY_TERMINATE_MIN_FPS and current_fps < EARLY_TERMINATE_MIN_FPS:
                                    logger.debug(f"早期终止: {url[:50]}... 连续帧率过低({current_fps:.1f}<{EARLY_TERMINATE_MIN_FPS})")
                                    early_terminated = True
                                    proc.kill()
                                    await proc.wait()
                                    return {"ok": False, "fps": current_fps, "frames": current_frames, "width": 0, "height": 0, "drop": 0, "speed": 0.0, "quality_score": 0.0, "message": "早期终止：连续帧率过低"}
                                last_fps = current_fps
                except asyncio.TimeoutError:
                    if proc.returncode is not None:
                        break
                elapsed = time.time() - start_time
                if elapsed >= FFMPEG_TEST_DURATION + 5:
                    break
            if not early_terminated:
                _, stderr_remaining = await asyncio.wait_for(
                    proc.communicate(),
                    timeout=FFMPEG_TEST_DURATION + 10 - (time.time() - start_time)
                )
                stderr_data += stderr_remaining
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            return {"ok": False, "fps": 0.0, "frames": 0, "width": 0, "height": 0, "drop": 0, "speed": 0.0, "quality_score": 0.0, "message": "连接超时"}
        if early_terminated:
            return {"ok": False, "fps": 0.0, "frames": 0, "width": 0, "height": 0, "drop": 0, "speed": 0.0, "quality_score": 0.0, "message": "早期终止"}
        output = stderr_data.decode('utf-8', errors='ignore')
        error_indicators = ["Connection refused", "Connection timed out", "No route to host", "404 Not Found", "403 Forbidden"]
        for error in error_indicators:
            if error.lower() in output.lower():
                return {"ok": False, "fps": 0.0, "frames": 0, "width": 0, "height": 0, "drop": 0, "speed": 0.0, "quality_score": 0.0, "message": f"连接错误: {error}"}
        frame_matches = re.findall(r'frame=\s*(\d+)', output)
        frames = int(frame_matches[-1]) if frame_matches else 0
        fps_matches = re.findall(r'fps=\s*([\d.]+)', output)
        avg_fps = float(fps_matches[-1]) if fps_matches else 0.0
        drop_matches = re.findall(r'drop=\s*(\d+)', output)
        drop_frames = int(drop_matches[-1]) if drop_matches else 0
        speed_matches = re.findall(r'speed=\s*([\d.]+)x?', output)
        speed = float(speed_matches[-1]) if speed_matches else 0.0
        width, height = 0, 0
        video_stream_match = re.search(r'Stream #0:\d+.*Video:.*? (\d+)x(\d+)', output, re.IGNORECASE)
        if video_stream_match:
            width = int(video_stream_match.group(1))
            height = int(video_stream_match.group(2))
        drop_ratio = drop_frames / max(frames, 1)
        is_smooth = (frames >= MIN_FRAMES and avg_fps >= MIN_AVG_FPS and speed >= MIN_SPEED and drop_ratio <= MAX_DROP_RATIO)
        is_acceptable = (frames >= MIN_FRAMES * 0.8 and avg_fps >= MIN_AVG_FPS * 0.9 and speed >= MIN_SPEED * 0.9 and drop_ratio <= MAX_DROP_RATIO * 1.5)
        if not is_smooth and is_acceptable:
            logger.debug(f"源降级接受: {url[:50]}... 帧数:{frames} 帧率:{avg_fps:.1f} 速度:{speed:.2f} 丢帧率:{drop_ratio:.1%}")
        if not is_smooth:
            reasons = []
            if frames < MIN_FRAMES:
                reasons.append(f"帧数不足({frames}<{MIN_FRAMES})")
            if avg_fps < MIN_AVG_FPS:
                reasons.append(f"帧率低({avg_fps:.1f}<{MIN_AVG_FPS})")
            if speed < MIN_SPEED:
                reasons.append(f"解码慢({speed:.2f}<{MIN_SPEED})")
            if drop_ratio > MAX_DROP_RATIO:
                reasons.append(f"丢帧率高({drop_ratio:.1%}>{MAX_DROP_RATIO:.0%})")
            logger.debug(f"源被过滤: {url[:60]}... 原因: {', '.join(reasons)}")
        quality_score = (
            avg_fps * 0.3 +
            (width * height / 1000000) * 0.3 +
            speed * 100 * 0.2 +
            (1 - drop_ratio) * 100 * 0.2
        )
        return {
            "ok": is_smooth or is_acceptable,
            "fps": avg_fps,
            "frames": frames,
            "width": width,
            "height": height,
            "drop": drop_frames,
            "speed": speed,
            "degraded": not is_smooth and is_acceptable,
            "quality_score": quality_score
        }
    except Exception as e:
        return {"ok": False, "fps": 0.0, "frames": 0, "width": 0, "height": 0, "drop": 0, "speed": 0.0, "quality_score": 0.0, "message": f"异常: {str(e)[:50]}"}

async def run_ffmpeg_test(channel_map: Dict[Tuple[str, str], List[str]],
                          required_map: Optional[Dict[Tuple[str, str], int]] = None,
                          cache: Optional[Dict] = None) -> Tuple[Dict[Tuple[str, str], List[str]], int]:
    """
    FFmpeg测速主函数，支持前置检查避免无效测速，传入缓存对象复用。
    若 required_map 非空（增量模式），则仅对 required_map 中的频道进行测速，
    其他频道保留原链接（不测速）。
    """
    if not channel_map:
        return {}, 0
    if cache is None:
        cache = load_cache() if ENABLE_CACHE else {}
    new_cache = {}
    result_map = defaultdict(list)
    total = sum(len(us) for us in channel_map.values())
    cached_ok = 0
    cached_failed_skipped = 0
    pending = []
    total_early_stopped = 0

    # 确定哪些频道需要测速
    if required_map:
        to_test = required_map
        # 非必需频道直接保留原链接（但不测速），包装为元组
        for (g, n), us in channel_map.items():
            if (g, n) not in to_test:
                result_map[(g, n)].extend((u, 0.0, 0, 0, 0.0) for u in us)
    else:
        to_test = {k: MAX_LINKS_PER_CHANNEL for k in channel_map}

    # 对需要测速的频道，构建待测列表
    for (g, n), needed in to_test.items():
        if needed <= 0:
            continue
        us = channel_map.get((g, n), [])
        for u in us:
            if len(result_map[(g, n)]) >= needed:
                break
            cache_item = cache.get(u)
            if cache_item and isinstance(cache_item, dict) and "ok" in cache_item:
                if is_cache_valid(cache_item.get("timestamp", 0), cache_item):
                    if cache_item["ok"]:
                        result_map[(g, n)].append((
                            u,
                            cache_item.get("fps", 0.0),
                            cache_item.get("width", 0),
                            cache_item.get("height", 0),
                            cache_item.get("quality_score", 0.0)
                        ))
                        cached_ok += 1
                    else:
                        cached_failed_skipped += 1
                    continue
            if SKIP_INTERNAL_IP and is_internal_ip(u):
                continue
            pending.append((g, n, u))

    logger.info(f"需要测速的频道数: {len(to_test)}，总链接: {total} 缓存有效且成功: {cached_ok} 需处理: {len(pending)}")

    if not pending:
        final = {}
        for k, vs in result_map.items():
            vs.sort(key=lambda x: (-x[4], -x[2]*x[3], -x[1]))
            max_links = required_map.get(k, MAX_LINKS_PER_CHANNEL) if required_map else MAX_LINKS_PER_CHANNEL
            final[k] = [u for u, _, _, _, _ in vs[:max_links]]
        return final, 0

    # 优先级排序
    def _get_priority(item):
        g, n, u = item
        if u in cache and cache[u].get("ok", False):
            return 0
        domain = extract_domain(u)
        known_domains = ["live.bilibili.com", "huya.com", "douyu.com", "youtube.com", "twitch.tv"]
        if any(d in domain for d in known_domains):
            return 1
        return 2
    pending.sort(key=_get_priority)

    sem = asyncio.Semaphore(FFMPEG_CONCURRENCY)
    connectivity_sem = asyncio.Semaphore(CONNECTIVITY_CONCURRENCY)
    completed_counts = {k: len(vs) for k, vs in result_map.items()}
    lock = asyncio.Lock()

    # 全局平摊分批
    BATCH_SIZE = 100
    batches = [pending[i:i+BATCH_SIZE] for i in range(0, len(pending), BATCH_SIZE)]
    logger.info(f"分为 {len(batches)} 批处理")

    for batch_idx, batch in enumerate(batches, 1):
        # 过滤已达标频道的链接
        filtered_batch = []
        for item in batch:
            g, n, u = item
            async with lock:
                if completed_counts.get((g, n), 0) >= (required_map.get((g, n), MAX_LINKS_PER_CHANNEL) if required_map else MAX_LINKS_PER_CHANNEL):
                    continue
            filtered_batch.append(item)
        if not filtered_batch:
            logger.info(f"第 {batch_idx}/{len(batches)} 批所有频道已达标，跳过")
            continue
        logger.info(f"--- 处理第 {batch_idx}/{len(batches)} 批，共 {len(filtered_batch)} 个链接 ---")
        early_stopped = await _process_ffmpeg_batch(
            filtered_batch, sem, connectivity_sem, result_map, completed_counts,
            required_map, new_cache, lock
        )
        total_early_stopped += early_stopped

    if ENABLE_CACHE and new_cache:
        cache.update(new_cache)
        save_cache(cache)

    final = {}
    for k, vs in result_map.items():
        vs.sort(key=lambda x: (-x[4], -x[2]*x[3], -x[1]))
        max_links = required_map.get(k, MAX_LINKS_PER_CHANNEL) if required_map else MAX_LINKS_PER_CHANNEL
        final[k] = [u for u, _, _, _, _ in vs[:max_links]]

    logger.info(f"测速完成，共 {len(final)} 个频道通过筛选")
    return final, total_early_stopped

async def _process_ffmpeg_batch(pending, sem, connectivity_sem, result_map, completed_counts,
                                required_map, new_cache, lock):
    """处理一批 FFmpeg 测速任务，前置检查频道是否达标，返回早期终止数量"""
    async def test_one(item):
        g, n, u = item
        async with lock:
            if completed_counts.get((g, n), 0) >= (required_map.get((g, n), MAX_LINKS_PER_CHANNEL) if required_map else MAX_LINKS_PER_CHANNEL):
                return g, n, u, {"ok": False, "fps": 0.0, "frames": 0, "width": 0, "height": 0, "drop": 0, "speed": 0.0, "quality_score": 0.0, "message": "频道已达标，跳过", "skipped": True}
        # 连通性预检（受信号量控制）
        async with connectivity_sem:
            if u.startswith(('http://', 'https://')):
                if not await check_url_connectivity(u, CONNECTIVITY_TIMEOUT):
                    return g, n, u, {"ok": False, "fps": 0.0, "frames": 0, "width": 0, "height": 0, "drop": 0, "speed": 0.0, "quality_score": 0.0, "message": "连通性预检失败", "skipped": False}
        # 正式测速
        async with sem:
            try:
                res = await test_stream_with_ffmpeg(u)
                return g, n, u, res
            except asyncio.CancelledError:
                raise

    tasks = [asyncio.create_task(test_one(item)) for item in pending]
    completed, success, failed, last_percent, early_stopped = 0, 0, 0, -100, 0
    print_progress_bar(0, len(tasks), success, failed, last_percent, early_stopped)

    for coro in asyncio.as_completed(tasks):
        try:
            g, n, u, res = await coro
        except asyncio.CancelledError:
            completed += 1
            continue
        except Exception as e:
            logger.error(f"测速任务异常: {e}")
            completed += 1
            continue
        if res.get("skipped", False):
            completed += 1
            continue
        completed += 1
        if res["ok"]:
            success += 1
            async with lock:
                result_map[(g, n)].append((
                    u,
                    res.get("fps", 0.0),
                    res.get("width", 0),
                    res.get("height", 0),
                    res.get("quality_score", 0.0)
                ))
                completed_counts[(g, n)] = completed_counts.get((g, n), 0) + 1
        else:
            failed += 1
            if res.get("message", "").startswith("早期终止"):
                early_stopped += 1
        if ENABLE_CACHE:
            new_cache[u] = {
                "ok": res.get("ok", False),
                "fps": res.get("fps", 0.0),
                "frames": res.get("frames", 0),
                "drop": res.get("drop", 0),
                "speed": res.get("speed", 0.0),
                "width": res.get("width", 0),
                "height": res.get("height", 0),
                "degraded": res.get("degraded", False),
                "quality_score": res.get("quality_score", 0.0),
                "timestamp": time.time()
            }
        last_percent = print_progress_bar(completed, len(tasks), success, failed, last_percent, early_stopped)
    return early_stopped

# ============================================================================
# ===================== 解析频道.txt =========================================
# ============================================================================
def parse_required_channels(filepath: Path) -> Dict[str, set]:
    required = defaultdict(set)
    current_group = None
    if not filepath.exists():
        logger.info(f"频道.txt 文件不存在: {filepath}，将不进行频道缺失检查")
        return {}
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                if line.endswith('#genre#'):
                    current_group = line.split(',')[0].strip()
                    continue
                if current_group:
                    name_cleaned = clean_satellite_name(line)
                    normalized = normalize_cctv(name_cleaned)
                    required[current_group].add(normalized)
        logger.info(f"从 {filepath} 读取到 {sum(len(v) for v in required.values())} 个要求频道")
    except Exception as e:
        logger.warning(f"解析 频道.txt 失败: {e}")
    return required

# ============================================================================
# ========================= GitHub M3U 解析 ===================================
# ============================================================================
@retry_async(max_retries=3, delay=2)
async def download_github_m3u(url, session: Optional[aiohttp.ClientSession] = None):
    close_session = False
    if session is None:
        session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30))
        close_session = True
    try:
        async with session.get(url, headers={'User-Agent': 'Mozilla/5.0'}) as r:
            if r.status == 200:
                t = await r.text()
                logger.info(f"下载成功 {url}")
                return t
    except Exception as e:
        logger.debug(f"下载失败 {url}: {e}")
    finally:
        if close_session:
            await session.close()
    return ""

def parse_m3u_file(content):
    channels = []
    name = ""
    current_group = "未分类"
    for line in content.splitlines():
        line = line.strip()
        if line.startswith("#EXTINF"):
            # 先提取频道名（逗号之后）
            name_match = re.search(r'#EXTINF:.*?,(.+)', line)
            if name_match:
                raw_name = name_match.group(1).strip()
            else:
                raw_name = ""
            # 再提取 group-title
            group_match = re.search(r'group-title="([^"]*)"', line)
            if group_match:
                current_group = group_match.group(1).strip()
            else:
                current_group = "未分类"
            name = raw_name
        elif line.startswith("http"):
            url = line.strip()
            if name and url:
                name_cleaned = clean_satellite_name(name)
                nn = normalize_cctv(name_cleaned)
                gr = classify_channel(nn)
                if not gr and current_group != "未分类":
                    gr = current_group
                if not gr:
                    gr = "未分类"
                final_name = clean_final_name(gr, nn)
                channels.append((gr, final_name, url))
                name = ""
                current_group = "未分类"
    return channels

def parse_txt_content(content: str, default_group: str = "未分类") -> List[Tuple[str, str, str]]:
    channels = []
    for line in content.splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        if line.endswith('#genre#'):
            continue
        if ',' in line:
            parts = line.split(',', 1)
            if len(parts) == 2:
                name = parts[0].strip()
                url_part = parts[1].strip()
                if '$' in url_part:
                    url = url_part.split('$')[0].strip()
                else:
                    url = url_part
                if name and url:
                    name_cleaned = clean_satellite_name(name)
                    normalized = normalize_cctv(name_cleaned)
                    group = classify_channel(normalized)
                    if group:
                        final_name = clean_final_name(group, normalized)
                        channels.append((group, final_name, url))
    return channels

def parse_iptv_txt_file(filepath: Path) -> List[Tuple[str, str, str]]:
    if not filepath.exists():
        return []
    channels = []
    current_group = "未分类"
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                if line.endswith('#genre#'):
                    current_group = line.split(',')[0].strip()
                    continue
                if ',' in line:
                    parts = line.split(',', 1)
                    if len(parts) == 2:
                        name, url = parts[0].strip(), parts[1].strip()
                        if name and url:
                            name_cleaned = clean_satellite_name(name)
                            nn = normalize_cctv(name_cleaned)
                            gr = classify_channel(nn) or current_group
                            final_name = clean_final_name(gr, nn)
                            channels.append((gr, final_name, url))
        logger.info(f"从 {filepath} 解析到 {len(channels)} 个链接")
    except Exception as e:
        logger.error(f"解析文件失败: {e}")
    return channels

# ============================================================================
# ========================= 页面点击与提取 ===================================
# ============================================================================
async def robust_click(loc, timeout=CLICK_TIMEOUT):
    try:
        await loc.scroll_into_view_if_needed(timeout=SCROLL_TIMEOUT*1000)
        await asyncio.sleep(0.2)
        await loc.click(force=True, timeout=timeout*1000)
        return True
    except Exception:
        try:
            await loc.evaluate("el=>el.click()")
            return True
        except Exception:
            return False

async def wait_for_element(page, sel, timeout=WAIT_FOR_ELEMENT_TIMEOUT):
    try:
        await page.wait_for_selector(sel, timeout=timeout*1000)
        return True
    except Exception:
        return False

@retry_async(max_retries=2, delay=1)
async def extract_one_ip(page, row, idx):
    entries = []
    try:
        addr = await row.locator("div.item-title").first.inner_text(timeout=IP_ADDR_TIMEOUT*1000)
        addr = addr.strip()
        if not addr:
            return []
        logger.info(f"处理IP [{idx}]: {addr}")
    except Exception:
        return []

    try:
        btn = row.locator("button:has(i.fa-list)").first
        if await btn.count() > 0:
            if not await robust_click(btn):
                await row.scroll_into_view_if_needed(timeout=SCROLL_TIMEOUT*1000)
                await row.click()
        else:
            await row.scroll_into_view_if_needed(timeout=SCROLL_TIMEOUT*1000)
            await row.click()
        await asyncio.sleep(DELAY_AFTER_CLICK)
        if not await wait_for_element(page, ".modal-dialog", MODAL_WAIT_TIMEOUT):
            return []

        # 等待模态框内容加载
        try:
            await page.wait_for_selector(".modal-dialog .item-content", timeout=2000)
        except Exception:
            logger.debug(f"IP [{idx}] 模态框内无内容，跳过")
            await close_modal(page)
            return []

        items = page.locator(".modal-dialog .item-content")
        total = await items.count()
        if total == 0:
            await close_modal(page)
            return []

        if MAX_CHANNELS_PER_IP > 0:
            total = min(total, MAX_CHANNELS_PER_IP)

        for i in range(total):
            try:
                n = await items.nth(i).locator(".item-title").inner_text(timeout=CHANNEL_NAME_TIMEOUT*1000)
                u = await items.nth(i).locator(".item-subtitle").inner_text(timeout=CHANNEL_URL_TIMEOUT*1000)
                n, u = n.strip(), u.strip()
                if not n or not u:
                    continue
                if not u.startswith(('http://', 'https://', 'rtsp://', 'rtmp://')):
                    u = DEFAULT_PROTOCOL + u
                n_cleaned = clean_satellite_name(n)
                nn = normalize_cctv(n_cleaned)
                g = classify_channel(nn)
                if not g:
                    continue
                final_name = clean_final_name(g, nn)
                entries.append((g, final_name, u))
            except Exception:
                continue

        await close_modal(page)

    except Exception as e:
        logger.debug(f"提取IP数据异常: {e}")
    return entries

async def close_modal(page):
    """关闭当前打开的模态框"""
    try:
        close_btn = page.locator(".modal-dialog .close, .modal-dialog button[data-dismiss='modal']").first
        if await close_btn.count() > 0:
            await robust_click(close_btn)
            await asyncio.sleep(0.2)
            return
        await page.keyboard.press("Escape")
        await asyncio.sleep(0.2)
    except Exception:
        pass

async def wait_data(page):
    logger.info("等待数据加载...")
    async def data_ready():
        return await page.evaluate('''()=>{
            for(let i of document.querySelectorAll('div.ios-list-item')){
                let s=i.querySelector('.item-subtitle')?.innerText||'';
                if(s.includes('频道:'))return true;
            }return false;
        }''')
    if await data_ready():
        logger.info("数据加载完成")
        return True
    for _ in range(DATA_LOAD_TIMEOUT // DATA_CHECK_INTERVAL + 1):
        await asyncio.sleep(DATA_CHECK_INTERVAL)
        if await data_ready():
            logger.info("数据加载完成")
            return True
    logger.error("数据加载超时，爬取失败")
    return False

# ============================================================================
# ================= URL 去重函数 ============================================
# ============================================================================
def deduplicate_urls_per_channel(channel_map: Dict[Tuple[str, str], List[str]]) -> Dict[Tuple[str, str], List[str]]:
    url_to_channels = defaultdict(list)
    for (group, name), urls in channel_map.items():
        for url in urls:
            url_to_channels[url].append((group, name))
    url_to_chosen = {}
    for url, channels in url_to_channels.items():
        if len(channels) == 1:
            url_to_chosen[url] = channels[0]
        else:
            plus_channels = [ch for ch in channels if '+' in ch[1].lower() or 'plus' in ch[1].lower()]
            if plus_channels:
                chosen = plus_channels[0]
            else:
                chosen = max(channels, key=lambda ch: len(ch[1]))
            url_to_chosen[url] = chosen
    new_map = defaultdict(list)
    for (group, name), urls in channel_map.items():
        for url in urls:
            if url_to_chosen[url] == (group, name):
                new_map[(group, name)].append(url)
    return dict(new_map)

# ============================================================================
# ========================= 结果导出 =========================================
# ============================================================================
def _sort_cctv_channels(chs):
    name_to_urls = defaultdict(list)
    for name, url in chs:
        name_to_urls[name].append(url)
    name_to_std = {}
    for name in name_to_urls.keys():
        std = None
        if name in CCTV_ORDER:
            std = name
        else:
            cctv_match = re.search(r'(cctv|cetv)[-\s]?(\d+)', name, re.IGNORECASE)
            if cctv_match:
                prefix = cctv_match.group(1).upper()
                num = cctv_match.group(2)
                for std_candidate in CCTV_ORDER:
                    if std_candidate.startswith(prefix) and re.search(rf'{prefix}[-\s]?{num}(?:\D|$)', std_candidate, re.IGNORECASE):
                        std = std_candidate
                        break
        name_to_std[name] = std
    std_to_urls = defaultdict(list)
    remaining = []
    for name, urls in name_to_urls.items():
        std = name_to_std.get(name)
        if std:
            std_to_urls[std].extend(urls)
        else:
            for url in urls:
                remaining.append((name, url))
    ordered_chs = []
    for std_name in CCTV_ORDER:
        if std_name in std_to_urls:
            for url in std_to_urls[std_name]:
                ordered_chs.append((std_name, url))
    remaining.sort(key=lambda x: x[0])
    ordered_chs.extend(remaining)
    return ordered_chs

def export_results_with_timestamp(channel_map):
    now = (datetime.datetime.utcnow() + datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")
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
                chs = _sort_cctv_channels(chs)
            else:
                chs = sorted(chs, key=lambda x: x[0])
            chs = [(n, u) for n, u in chs if n.strip()]
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
                chs = _sort_cctv_channels(chs)
            else:
                chs = sorted(chs, key=lambda x: x[0])
            chs = [(n, u) for n, u in chs if n.strip()]
            for n, u in chs:
                f.write(f"{n},{u}\n")
            f.write("\n")
        if not TIME_DISPLAY_AT_TOP:
            f.write("更新时间,#genre#\n")
            f.write(f"{now},{gu}\n")

    logger.info(f"导出完成：{len(channel_map)} 个频道")

# ============================================================================
# ===================== 分源统计打印函数 ======================================
# ============================================================================
def print_source_stats(**sources):
    logger.info("=" * 50)
    logger.info("各数据来源统计")
    logger.info("-" * 50)
    total_ch = 0
    total_url = 0
    for source_name, channel_map in sources.items():
        if not channel_map:
            continue
        ch_count = len(channel_map)
        url_count = sum(len(urls) for urls in channel_map.values())
        logger.info(f"{source_name}: {ch_count} 个频道 / {url_count} 条链接")
        total_ch += ch_count
        total_url += url_count
    logger.info("-" * 50)
    logger.info(f"合计: {total_ch} 个频道 / {total_url} 条链接")
    logger.info("=" * 50)

# ============================================================================
# ===================== 网站爬取主逻辑 ========================================
# ============================================================================
async def scrape_iptv_site() -> Dict[Tuple[str, str], List[str]]:
    channel_map = defaultdict(list)
    if not ENABLE_WEB_SCRAPING:
        return channel_map
    logger.info(f"启动网站爬取，模式：{EXTRACT_MODE}，目标地址：{TARGET_URL}")
    try:
        async with async_playwright() as p:
            browser = await getattr(p, BROWSER_TYPE).launch(
                headless=HEADLESS,
                args=["--no-sandbox", "--disable-dev-shm-usage"]
            )
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                viewport={"width": 1280, "height": 900}
            )
            page = await context.new_page()
            page.set_default_timeout(PAGE_LOAD_TIMEOUT * 1000)

            try:
                await page.goto(TARGET_URL, wait_until="domcontentloaded")
                logger.info("目标页面加载成功")
            except Exception as e:
                logger.error(f"页面加载失败：{e}")
                await browser.close()
                return channel_map

            mode_texts = PAGE_CONFIG["hotel"] if EXTRACT_MODE == "酒店提取" else PAGE_CONFIG["multicast"]
            mode_selector = build_selector(mode_texts, "button,div,span")
            if mode_selector:
                try:
                    mode_btn = page.locator(mode_selector).first
                    if await mode_btn.count() > 0:
                        await robust_click(mode_btn)
                        await asyncio.sleep(0.5)
                        logger.info(f"已切换到【{EXTRACT_MODE}】模式")
                except Exception as e:
                    logger.debug(f"模式切换按钮点击异常：{e}")

            start_selector = build_selector(PAGE_CONFIG["start_button"], "button,div")
            if start_selector:
                try:
                    start_btn = page.locator(start_selector).first
                    if await start_btn.count() > 0:
                        await robust_click(start_btn)
                        logger.info("已点击【开始提取】按钮")
                    else:
                        logger.warning("未检测到开始按钮，直接等待数据加载")
                except Exception as e:
                    logger.debug(f"点击开始按钮异常：{e}")

            await asyncio.sleep(AFTER_START_WAIT)
            if not await wait_data(page):
                logger.error("数据加载超时，终止爬取")
                await browser.close()
                return channel_map

            ip_items = page.locator("div.ios-list-item")
            total_ip = await ip_items.count()
            process_num = min(MAX_IPS, total_ip)
            logger.info(f"检测到 {total_ip} 个IP节点，将处理前 {process_num} 个")

            for idx in range(process_num):
                try:
                    row = ip_items.nth(idx)
                    entries = await extract_one_ip(page, row, idx + 1)
                    for group, name, url in entries:
                        if ENABLE_MIGU_FILTER and "migu" in url.lower():
                            continue
                        if SKIP_INTERNAL_IP and is_internal_ip(url):
                            continue
                        channel_map[(group, name)].append(url)
                    await asyncio.sleep(DELAY_BETWEEN_IPS)
                except Exception as e:
                    logger.debug(f"处理第 {idx+1} 个IP时出错：{e}")
                    continue
                if MAX_TOTAL_CHANNELS > 0 and len(channel_map) >= MAX_TOTAL_CHANNELS:
                    logger.info(f"已达到总频道上限 {MAX_TOTAL_CHANNELS}，停止爬取")
                    break

            await browser.close()
    except Exception as e:
        logger.error(f"网站爬取流程异常：{e}", exc_info=True)

    for key in list(channel_map.keys()):
        channel_map[key] = list(dict.fromkeys(channel_map[key]))

    logger.info(f"网站爬取完成，共获取 {len(channel_map)} 个有效频道")
    return channel_map

# ============================================================================
# ===================== GitHub 订阅源处理 =====================================
# ============================================================================
async def fetch_github_sources() -> Dict[Tuple[str, str], List[str]]:
    channel_map = defaultdict(list)
    if not ENABLE_GITHUB_SOURCES or not GITHUB_M3U_LINKS:
        return channel_map
    logger.info(f"开始下载 GitHub 订阅源，共 {len(GITHUB_M3U_LINKS)} 个地址")
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
        tasks = [download_github_m3u(url, session) for url in GITHUB_M3U_LINKS]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for idx, content in enumerate(results):
            if isinstance(content, Exception):
                logger.warning(f"GitHub源 {idx+1} 下载失败：{content}")
                continue
            if not content.strip():
                continue
            if content.lstrip().startswith("#EXTM3U"):
                parsed = parse_m3u_file(content)
            else:
                parsed = parse_txt_content(content)
            for group, name, url in parsed:
                if ENABLE_MIGU_FILTER and "migu" in url.lower():
                    continue
                if SKIP_INTERNAL_IP and is_internal_ip(url):
                    continue
                channel_map[(group, name)].append(url)
    for key in list(channel_map.keys()):
        channel_map[key] = list(dict.fromkeys(channel_map[key]))
    logger.info(f"GitHub 源处理完成，共 {len(channel_map)} 个频道")
    return channel_map

# ============================================================================
# ===================== 本地源加载 ============================================
# ============================================================================
def load_local_source_file() -> Dict[Tuple[str, str], List[str]]:
    channel_map = defaultdict(list)
    if not LOCAL_SOURCE_FILENAME.exists():
        logger.debug(f"本地源文件不存在：{LOCAL_SOURCE_FILENAME}，跳过")
        return channel_map
    logger.info(f"加载本地源文件：{LOCAL_SOURCE_FILENAME}")
    parsed = parse_iptv_txt_file(LOCAL_SOURCE_FILENAME)
    for group, name, url in parsed:
        if ENABLE_MIGU_FILTER and "migu" in url.lower():
            continue
        if SKIP_INTERNAL_IP and is_internal_ip(url):
            continue
        channel_map[(group, name)].append(url)
    for key in list(channel_map.keys()):
        channel_map[key] = list(dict.fromkeys(channel_map[key]))
    logger.info(f"本地源加载完成，共 {len(channel_map)} 个频道")
    return channel_map

# ============================================================================
# ===================== 主流程 ================================================
# ============================================================================
async def main():
    logger.info("=" * 60)
    logger.info("IPTV 频道采集与质量检测工具 启动")
    logger.info(f"提取模式：{EXTRACT_MODE} | 网站爬取：{'开' if ENABLE_WEB_SCRAPING else '关'}")
    logger.info(f"FFmpeg测速：{'开' if ENABLE_FFMPEG_TEST else '关'} | 增量更新：{'开' if ENABLE_INCREMENTAL_UPDATE else '关'}")
    logger.info("=" * 60)

    speed_cache = load_cache() if ENABLE_CACHE else {}

    web_channels = await scrape_iptv_site()
    github_channels = await fetch_github_sources()
    local_channels = load_local_source_file()

    print_source_stats(
        网站爬取=web_channels,
        GitHub订阅=github_channels,
        本地文件=local_channels
    )

    merged = defaultdict(list)
    for source in [web_channels, github_channels, local_channels]:
        for (group, name), urls in source.items():
            merged[(group, name)].extend(urls)

    if ENABLE_DEDUPLICATION:
        for key in merged:
            merged[key] = list(dict.fromkeys(merged[key]))
        merged = deduplicate_urls_per_channel(merged)
        logger.info(f"全局去重完成，剩余 {len(merged)} 个频道")

    merged = {k: v for k, v in merged.items() if v}
    total_links = sum(len(v) for v in merged.values())
    logger.info(f"数据合并完成，总计 {len(merged)} 个频道，{total_links} 条链接")
    if not merged:
        logger.error("未获取到任何有效频道，程序退出")
        return

    final_result = {}
    early_stop_count = 0
    if ENABLE_FFMPEG_TEST:
        logger.info("===== 开始 FFmpeg 质量测速 =====")
        required_map = None
        if ENABLE_INCREMENTAL_UPDATE:
            required_dict = parse_required_channels(REQUIRED_CHANNELS_FILE)
            if required_dict:
                required_map = {}
                for group, names in required_dict.items():
                    for name in names:
                        required_map[(group, name)] = QUALITY_THRESHOLD
                logger.info(f"增量模式：将优先保障 {len(required_map)} 个必需频道，每个保留 {QUALITY_THRESHOLD} 条优质源")
            else:
                logger.warning("未读取到必需频道配置，将执行全量测速")

        final_result, early_stop_count = await run_ffmpeg_test(
            merged,
            required_map=required_map,
            cache=speed_cache
        )
    else:
        logger.info("FFmpeg 测速已禁用，直接输出原始链接（仅去重截断）")
        final_result = {}
        for k, urls in merged.items():
            final_result[k] = urls[:MAX_LINKS_PER_CHANNEL]

    if final_result:
        export_results_with_timestamp(final_result)
        logger.info("✅ 结果文件已生成：")
        logger.info(f"  M3U 格式：{OUTPUT_M3U_FILENAME}")
        logger.info(f"  TXT 格式：{OUTPUT_TXT_FILENAME}")
    else:
        logger.error("❌ 无符合质量要求的频道，未生成输出文件")

    logger.info("=" * 60)
    logger.info("处理结束")
    logger.info(f"最终有效频道：{len(final_result)} 个")
    if ENABLE_FFMPEG_TEST:
        final_link_count = sum(len(urls) for urls in final_result.values())
        logger.info(f"最终有效链接：{final_link_count} 条")
        if early_stop_count > 0:
            logger.info(f"早期终止低质量源：{early_stop_count} 条")
    logger.info("=" * 60)

# ============================================================================
# ===================== 程序入口 ==============================================
# ============================================================================
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("用户手动中断程序")
    except Exception as e:
        logger.error(f"程序运行异常：{e}", exc_info=True)
        sys.exit(1)
