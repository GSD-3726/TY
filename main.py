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
TARGET_URL            = "https://iptv.809899.xyz"  # 目标IPTV源爬取网站地址
HEADLESS              = True                       # 浏览器无头模式(True=后台运行不显示界面,False=显示浏览器窗口)
BROWSER_TYPE          = "chromium"                 # 使用的浏览器类型(chromium/firefox/webkit)
OUTPUT_DIR            = Path(__file__).parent      # 输出文件保存目录(默认脚本所在目录)
OUTPUT_M3U_FILENAME   = OUTPUT_DIR / "iptv_channels.m3u"  # 输出M3U格式播放列表文件名
OUTPUT_TXT_FILENAME   = OUTPUT_DIR / "iptv_channels.txt"  # 输出TXT格式播放列表文件名
MAX_LINKS_PER_CHANNEL = 10                         # 单个频道最多保留多少个可用直播源
DEFAULT_PROTOCOL      = "http://"                  # 无协议前缀时默认补充的协议

EXTRACT_MODE          = "酒店提取"                 # 提取模式,可选:"酒店提取"/"组播提取"/"引擎搜索"
MAX_IPS               = 100                        # 最多处理多少个IP地址(酒店/组播模式)
MAX_TOTAL_CHANNELS    = 0                          # 总最大频道数限制(0=不限制)
MAX_CHANNELS_PER_IP   = 0                          # 每个IP最多提取多少个频道(0=不限制)
DELAY_BETWEEN_IPS     = 0.1                        # 处理不同IP之间的延迟(秒)
DELAY_AFTER_CLICK     = 0.3                        # 点击按钮后等待页面响应的时间(秒)
MODAL_WAIT_TIMEOUT    = 1                          # 等待弹窗出现的超时时间(秒)

PAGE_LOAD_TIMEOUT      = 120                       # 页面加载超时时间(秒)
DATA_LOAD_TIMEOUT      = 60                        # 等待频道数据加载的超时时间(秒)
AFTER_START_WAIT       = 30                        # 点击开始按钮后初始等待时间(秒)
IP_ADDR_TIMEOUT        = 0.1                       # 读取IP地址超时时间(秒)
CHANNEL_NAME_TIMEOUT   = 0.1                       # 读取频道名称超时时间(秒)
CHANNEL_URL_TIMEOUT    = 0.1                       # 读取频道地址超时时间(秒)
SCROLL_TIMEOUT         = 0.1                       # 页面滚动超时时间(秒)
CLICK_TIMEOUT          = 0.1                       # 点击元素超时时间(秒)
WAIT_FOR_ELEMENT_TIMEOUT = 30                      # 等待页面元素出现的超时时间(秒)
DATA_CHECK_INTERVAL    = 30                        # 检查数据是否加载完成的间隔时间(秒)

ENABLE_GITHUB_SOURCES = True                       # 是否启用GitHub公共源补充
GITHUB_M3U_LINKS = [                                # GitHub上的公共M3U/TXT源地址列表
    "https://gh-proxy.com/https://github.com/kimwang1978/collect-txt/blob/main/bbxx.txt"
]

ENABLE_FFMPEG_TEST     = True                      # 是否启用FFmpeg流媒体质量测速
FFMPEG_PATH            = "ffmpeg"                  # FFmpeg可执行文件路径(默认系统PATH中查找)
FFMPEG_TEST_DURATION   = 10                        # 每个直播流测速时长(秒)
FFMPEG_CONCURRENCY     = 8                         # 同时进行测速的最大并发数
MIN_AVG_FPS            = 24                        # 最低平均帧率要求(低于此值判定为不可用)
MIN_FRAMES             = 210                       # 最低总帧数要求(低于此值判定为不可用)

ENABLE_CACHE           = True                      # 是否启用测速结果缓存
CACHE_FILE             = OUTPUT_DIR / "iptv_speed_cache.json"  # 缓存文件保存路径
CACHE_EXPIRE_HOURS     = 72                        # 缓存过期时间(小时)

ENABLE_CHINESE_CLEAN   = True                      # 是否清理频道名称中的非中文字符
ENABLE_DEDUPLICATION   = True                      # 是否启用直播源去重(同一个URL只保留在一个频道)
CCTV_USE_MAPPING       = True                      # 是否使用标准CCTV频道名称映射
ENABLE_MIGU_FILTER     = True                      # 是否过滤咪咕源(部分地区无法访问)
SKIP_INTERNAL_IP       = True                      # 是否跳过内网IP地址(192.168.x.x/10.x.x.x等)
ENABLE_SATELLITE_CLEAN = True                      # 是否清理卫视频道名称中的后缀(如"高清"/"移动"/"测试")

CATEGORY_RULES = [                                  # 频道自动分类规则(按优先级顺序匹配)
    {"name": "4K专区",      "keywords": ["4k", "4K", "超高清", "2160p"]},
    {"name": "央视频道",    "keywords": ["cctv", "cetv", "央视", "中央", "CCTV", "CETV", "央视频道"]},
    {"name": "卫视频道",    "keywords": ["卫视", "卫视高清"]},
    {"name": "电影频道",    "keywords": ["电影", "影院", "chc", "动作", "剧场", "映画", "影视", "大片", "影视频道"]},
    {"name": "轮播频道",    "keywords": ["轮播", "滚动", "循环"]},
    {"name": "儿童频道",    "keywords": ["少儿", "动画", "动漫", "卡通", "亲子", "儿童", "宝贝"]},
]

GROUP_ORDER = ["央视频道", "卫视频道", "电影频道", "4K专区", "儿童频道", "轮播频道"]  # 输出时的分组顺序

CCTV_NAME_MAPPING = {                               # CCTV频道数字到标准名称的映射
    "1": "综合", "2": "财经", "3": "综艺", "4": "国际", "5": "体育",
    "5+": "体育赛事", "6": "电影", "7": "国防军事", "8": "电视剧",
    "9": "纪录", "10": "科教", "11": "戏曲", "12": "社会与法",
    "13": "新闻", "14": "少儿", "15": "音乐", "16": "奥林匹克",
    "17": "农业农村",
}

CCTV_ORDER = [                                      # 央视频道输出时的排序
    "CCTV-1综合", "CCTV-2财经", "CCTV-3综艺", "CCTV-4国际",
    "CCTV-5体育", "CCTV-5+体育赛事", "CCTV-6电影", "CCTV-7国防军事",
    "CCTV-8电视剧", "CCTV-9纪录", "CCTV-10科教", "CCTV-11戏曲",
    "CCTV-12社会与法", "CCTV-13新闻", "CCTV-14少儿", "CCTV-15音乐",
    "CCTV-16奥林匹克", "CCTV-17农业农村", "CETV1", "CETV2", "CETV4", "CETV5"
]

PAGE_CONFIG = {                                     # 页面元素文本匹配规则(适配不同网站布局)
    "engine_search": ["引索搜索", "引擎搜索", "关键词搜索"],
    "hotel":        ["酒店提取"],
    "multicast":    ["组播提取"],
    "start_button": ["开始播放", "开始搜索", "开始提取"],
}

TIME_DISPLAY_AT_TOP    = False                      # 更新时间显示在文件顶部(True=顶部,False=底部)
UPDATE_STREAM_URL      = "https://gitee.com/bmg369/tvtest/raw/master/cg/index.m3u8"  # 更新时间对应的占位流地址
ENABLE_VERBOSE_LOGGING = False                      # 是否启用详细日志输出

CONNECTIVITY_CONCURRENCY = 20                       # 基础连通性测试的最大并发数
CONNECTIVITY_TIMEOUT     = 1                        # 基础连通性测试超时时间(秒)

# ============================================================================
# ============================= 日志配置 ===========================
# ============================================================================

log_level = logging.INFO

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
        if not rule["keywords"]: continue
        pattern = re.compile("|".join(re.escape(kw.lower()) for kw in rule["keywords"]))
        compiled.append((rule["name"], pattern))
    return lambda name: next((group for group, pat in compiled if pat.search(name.lower())), None)

classify_channel = build_classifier()

def normalize_cctv(name: str) -> str:
    name_lower = name.lower()
    if re.search(r'cctv[-\s]?4k', name_lower):
        return "CCTV-4K"
    if "cctv5+" in name_lower:
        return "CCTV-5+体育赛事" if CCTV_USE_MAPPING else "CCTV5+"
    
    cctv_match = CCTV_PATTERN.search(name_lower)
    if cctv_match:
        num = cctv_match.group(2)
        if CCTV_USE_MAPPING and num in CCTV_NAME_MAPPING:
            return f"CCTV-{num}{CCTV_NAME_MAPPING[num]}"
        return f"CCTV-{num}"
    
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
    if not text_list: return ""
    if len(text_list) == 1:
        return f"{element_type}:has-text('{text_list[0]}')"
    pattern = "|".join(re.escape(t) for t in text_list)
    return f"{element_type}:text-matches('{pattern}')"

def is_internal_ip(url: str) -> bool:
    try:
        host = urlparse(url).hostname
        if not host:
            return False
        return bool(INTERNAL_IP_PATTERN.match(host))
    except:
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

# ============================================================================
# ========================= 缓存管理 ==========================================
# ============================================================================
CACHE_EXPIRE_SECONDS = CACHE_EXPIRE_HOURS * 3600

def load_cache():
    if not ENABLE_CACHE or not CACHE_FILE.exists():
        return {}
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            cache = json.load(f)
        now = time.time()
        valid_cache = {}
        for url, data in cache.items():
            if isinstance(data, dict) and "ok" in data and "timestamp" in data:
                if now - data["timestamp"] < CACHE_EXPIRE_SECONDS:
                    if "width" not in data: data["width"] = 0
                    if "height" not in data: data["height"] = 0
                    if "bitrate" not in data: data["bitrate"] = 0
                    valid_cache[url] = data
            else:
                if isinstance(data, (int, float)):
                    valid_cache[url] = {
                        "ok": data > 0, "fps": 0.0, "frames": 0,
                        "width": 0, "height": 0, "bitrate": 0, "timestamp": now
                    }
        logger.info(f"缓存加载完成，有效条目数: {len(valid_cache)}")
        return valid_cache
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
# ========================= 基础连通性测试 ====================================
# ============================================================================
async def check_url_connectivity(url: str, timeout: int) -> bool:
    if not url.startswith(('http://', 'https://')):
        return True
    try:
        timeout_obj = aiohttp.ClientTimeout(total=timeout)
        async with aiohttp.ClientSession(timeout=timeout_obj) as session:
            async with session.get(url, headers={'User-Agent': 'Mozilla/5.0'}, allow_redirects=True) as resp:
                if resp.status != 200:
                    return False
                try:
                    await resp.content.readexactly(1024)
                except asyncio.IncompleteReadError as e:
                    return len(e.partial) > 0
                except Exception:
                    return False
                return True
    except Exception:
        return False

# ============================================================================
# ========================= 重试 & 进度条 =====================================
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

# ============================================================================
# ========================= FFmpeg 核心测速 ==================================
# ============================================================================
async def test_stream_with_ffmpeg(url: str, duration: int = 10) -> Dict[str, Any]:
    if not shutil.which(FFMPEG_PATH):
        logger.error(f"未找到FFmpeg: {FFMPEG_PATH}")
        return {"ok": False, "fps": 0.0, "frames": 0, "width": 0, "height": 0, "bitrate": 0, "message": "FFmpeg未安装"}

    headers = (
        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36\r\n"
        "Referer: https://www.miguvideo.com/\r\n"
    )

    cmd = [
        FFMPEG_PATH, "-hide_banner", "-y",
        "-headers", headers,
        "-fflags", "nobuffer",
        "-analyzeduration", "3000000", "-probesize", "3000000",
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
            _, stderr = await asyncio.wait_for(proc.communicate(), timeout=duration + 8)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            return {"ok": False, "fps": 0.0, "frames": 0, "width": 0, "height": 0, "bitrate": 0, "message": "连接超时"}

        output = stderr.decode('utf-8', errors='ignore')
        frame_matches = re.findall(r'frame=\s*(\d+)', output)
        fps_matches = re.findall(r'fps=\s*([\d.]+)', output)
        drop_matches = re.findall(r'drop=\s*(\d+)', output)
        bitrate_matches = re.findall(r'bitrate=\s*([\d.]+)\s*kbits/s', output)
        
        frames = int(frame_matches[-1]) if frame_matches else 0
        avg_fps = float(fps_matches[-1]) if fps_matches else 0.0
        dropped_frames = int(drop_matches[-1]) if drop_matches else 0
        bitrate = float(bitrate_matches[-1]) if bitrate_matches else 0.0

        width, height = 0, 0
        video_matches = re.finditer(r'Stream #0:(\d+).*Video:.*? (\d+)x(\d+)', output, re.IGNORECASE)
        for match in video_matches:
            w, h = int(match.group(2)), int(match.group(3))
            if w > 0 and h > 0:
                width, height = w, h
                break
        if width == 0 or height == 0:
            generic_match = re.search(r'Video:.*? (\d+)x(\d+)', output, re.IGNORECASE)
            if generic_match:
                width, height = int(generic_match.group(1)), int(generic_match.group(2))

        min_required_frames = max(MIN_FRAMES, int(duration * MIN_AVG_FPS * 0.8))
        is_basic_ok = frames >= min_required_frames and avg_fps >= (MIN_AVG_FPS * 0.8)
        drop_rate = dropped_frames / frames if frames > 0 else 1.0
        is_stable = drop_rate <= 0.02
        is_bandwidth_ok = (bitrate > 300) if bitrate > 0 else True 
        is_smooth = is_basic_ok and is_stable and is_bandwidth_ok

        return {
            "ok": is_smooth,
            "fps": avg_fps,
            "frames": frames,
            "width": width,
            "height": height,
            "bitrate": bitrate,
            "drop_rate": drop_rate,
            "needs_retest": (not is_smooth) and is_basic_ok
        }
    except Exception as e:
        return {"ok": False, "fps": 0.0, "frames": 0, "width": 0, "height": 0, "bitrate": 0, "message": f"异常: {str(e)[:50]}"}

# ============================================================================
# ================== 【核心新函数】智能测速：达标即停 ==========================
# ============================================================================
async def smart_test_channel_urls(
    channel_key: Tuple[str, str],
    url_list: List[str],
    cache: dict,
    sem: asyncio.Semaphore
) -> Tuple[List[str], List[str], dict]:
    """
    智能测速单个频道：
    1. 先测速现有URL
    2. 达标 MAX_LINKS_PER_CHANNEL 立刻停止
    3. 返回 达标URL列表 + 未测速URL列表 + 更新缓存
    """
    group, name = channel_key
    qualified = []
    remaining_urls = []
    new_cache = {}
    now = time.time()

    for url in url_list:
        # 已达标，直接剩余
        if len(qualified) >= MAX_LINKS_PER_CHANNEL:
            remaining_urls.append(url)
            continue

        # 缓存检查
        cache_item = cache.get(url)
        if cache_item and isinstance(cache_item, dict) and "ok" in cache_item:
            if is_cache_valid(cache_item.get("timestamp", 0)):
                if cache_item["ok"]:
                    qualified.append(url)
                continue

        # 并发测速
        async with sem:
            res = await test_stream_with_ffmpeg(url, FFMPEG_TEST_DURATION)
            if res.get("needs_retest"):
                res = await test_stream_with_ffmpeg(url, FFMPEG_TEST_DURATION + 10)

        # 记录缓存
        new_cache[url] = {
            "ok": res["ok"], "fps": res["fps"], "frames": res.get("frames", 0),
            "width": res.get("width", 0), "height": res.get("height", 0),
            "bitrate": res.get("bitrate", 0), "timestamp": now
        }

        if res["ok"]:
            qualified.append(url)

    return qualified[:MAX_LINKS_PER_CHANNEL], remaining_urls, new_cache

# ============================================================================
# ========================= M3U解析（读取本地历史文件）=======================
# ============================================================================
def parse_existing_m3u(m3u_path: Path) -> Dict[Tuple[str, str], List[str]]:
    """读取上一次输出的 m3u 文件，恢复频道+链接"""
    channel_map = defaultdict(list)
    if not m3u_path.exists():
        logger.info("本地无历史M3U文件，跳过")
        return channel_map

    current_group = ""
    current_name = ""
    with open(m3u_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line.startswith("#EXTINF"):
                match = re.search(r'group-title="([^"]+)"', line)
                current_group = match.group(1) if match else "未分类"
                name_match = re.search(r',([^,]+)$', line)
                current_name = name_match.group(1).strip() if name_match else ""
            elif line.startswith("http") and current_group and current_name:
                channel_map[(current_group, current_name)].append(line)
                current_name = ""

    logger.info(f"✅ 读取历史M3U：共 {len(channel_map)} 个频道")
    return channel_map

# ============================================================================
# ========================= GitHub / 网站 / TXT 解析 ==========================
# ============================================================================
@retry_async(max_retries=3, delay=2)
async def download_github_m3u(url, session=None):
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
    ch = []
    g = n = u = ""
    for l in content.splitlines():
        l = l.strip()
        if l.startswith("#EXTINF"):
            m = re.search(r'#EXTINF:-1.*?group-title="[^"]*",(.+)', l)
            if not m:
                m = re.search(r'#EXTINF:-1.*?,(.+)', l)
            if m:
                n = m.group(1).strip()
        elif l.startswith("http"):
            u = l.strip()
            if n and u:
                n_cleaned = clean_satellite_name(n)
                nn = normalize_cctv(n_cleaned)
                gr = classify_channel(nn)
                if gr:
                    fn = nn if gr == "央视频道" else (clean_chinese_only(n_cleaned) if ENABLE_CHINESE_CLEAN else n_cleaned)
                    ch.append((gr, fn, u))
            g = n = u = ""
    return ch

def parse_txt_content(content: str, default_group="未分类"):
    channels = []
    for line in content.splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        if ',' in line:
            parts = line.split(',', 1)
            if len(parts) == 2:
                name, url = parts[0].strip(), parts[1].strip()
                if '$' in url:
                    url = url.split('$')[0].strip()
                if name and url:
                    n_cleaned = clean_satellite_name(name)
                    nn = normalize_cctv(n_cleaned)
                    gr = classify_channel(nn)
                    if gr:
                        fn = nn if gr == "央视频道" else (clean_chinese_only(n_cleaned) if ENABLE_CHINESE_CLEAN else n_cleaned)
                        channels.append((gr, fn, url))
    return channels

async def robust_click(loc, timeout=CLICK_TIMEOUT):
    try:
        await loc.scroll_into_view_if_needed(timeout=SCROLL_TIMEOUT*1000)
        await asyncio.sleep(0.2)
        await loc.click(force=True, timeout=timeout*1000)
        return True
    except:
        try:
            await loc.evaluate("el=>el.click()")
            return True
        except:
            return False

async def wait_for_element(page, sel, timeout=WAIT_FOR_ELEMENT_TIMEOUT):
    try:
        await page.wait_for_selector(sel, timeout=timeout*1000)
        return True
    except:
        return False

@retry_async(max_retries=2, delay=1)
async def extract_one_ip(page, row, idx):
    e = []
    try:
        addr = await row.locator("div.item-title").first.inner_text(timeout=IP_ADDR_TIMEOUT*1000)
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
        if not await wait_for_element(page, ".modal-dialog", MODAL_WAIT_TIMEOUT):
            return []
        items = page.locator(".modal-dialog .item-content")
        total = await items.count()
        if total == 0:
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
                fn = nn if g == "央视频道" else (clean_chinese_only(n_cleaned) if ENABLE_CHINESE_CLEAN else n_cleaned)
                e.append((g, fn, u))
            except:
                continue
    except:
        pass
    return e

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

def deduplicate_urls_per_channel(channel_map):
    url_to_channels = defaultdict(list)
    for (g, n), urls in channel_map.items():
        for url in urls:
            url_to_channels[url].append((g, n))
    url_to_chosen = {}
    for url, channels in url_to_channels.items():
        if len(channels) == 1:
            url_to_chosen[url] = channels[0]
        else:
            chosen = max(channels, key=lambda ch: len(ch[1]))
            url_to_chosen[url] = chosen
    new_map = defaultdict(list)
    for (g, n), urls in channel_map.items():
        for url in urls:
            if url_to_chosen[url] == (g, n):
                new_map[(g, n)].append(url)
    return dict(new_map)

# ============================================================================
# ========================= 导出结果 ==========================================
# ============================================================================
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
            if gro not in g: continue
            chs = g[gro]
            if gro == "央视频道":
                name_to_urls = defaultdict(list)
                for name, url in chs: name_to_urls[name].append(url)
                ordered_chs = []
                for std_name in CCTV_ORDER:
                    if std_name in name_to_urls:
                        for url in name_to_urls[std_name]:
                            ordered_chs.append((std_name, url))
                chs = ordered_chs
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
            if gro not in g: continue
            f.write(f"{gro},#genre#\n")
            chs = g[gro]
            if gro == "央视频道":
                name_to_urls = defaultdict(list)
                for name, url in chs: name_to_urls[name].append(url)
                ordered_chs = []
                for std_name in CCTV_ORDER:
                    if std_name in name_to_urls:
                        for url in name_to_urls[std_name]:
                            ordered_chs.append((std_name, url))
                chs = ordered_chs
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
# ========================= 主流程（全新逻辑）================================
# ============================================================================
async def main():
    overall_start = time.time()
    cache = load_cache() if ENABLE_CACHE else {}
    sem = asyncio.Semaphore(FFMPEG_CONCURRENCY)

    # ===================== 步骤1：优先读取本地历史M3U =====================
    logger.info("="*50)
    logger.info("📌 步骤1：读取本地历史 M3U 频道")
    logger.info("="*50)
    final_channel_map = parse_existing_m3u(OUTPUT_M3U_FILENAME)
    all_qualified = defaultdict(list)

    # ===================== 步骤2：对本地源智能测速（达标即停）=================
    logger.info("="*50)
    logger.info("📌 步骤2：智能测速本地源（达标即停）")
    logger.info("="*50)
    tasks = []
    channel_keys = list(final_channel_map.keys())

    for key in channel_keys:
        urls = final_channel_map[key]
        tasks.append(smart_test_channel_urls(key, urls, cache, sem))

    results = await asyncio.gather(*tasks)
    for i, (qualified, remaining, new_cache) in enumerate(results):
        key = channel_keys[i]
        all_qualified[key] = qualified
        cache.update(new_cache)

    # 统计达标情况
    total_needs = 0
    total_ok = 0
    for key, qs in all_qualified.items():
        need = max(0, MAX_LINKS_PER_CHANNEL - len(qs))
        total_needs += need
        total_ok += len(qs)
    logger.info(f"✅ 本地源测速完成：达标 {total_ok} 条，需补充 {total_needs} 条")

    if total_needs <= 0:
        logger.info("🎉 所有频道已满足要求，直接输出！")
        save_cache(cache)
        export_results_with_timestamp(all_qualified)
        return

    # ===================== 步骤3：从 GitHub + 网站爬取补充源 =================
    logger.info("="*50)
    logger.info("📌 步骤3：从 GitHub + 网站 补充新源")
    logger.info("="*50)
    supplementary_entries = []

    # GitHub
    if ENABLE_GITHUB_SOURCES:
        async with aiohttp.ClientSession() as session:
            tasks = [download_github_m3u(u, session) for u in GITHUB_M3U_LINKS]
            res = await asyncio.gather(*tasks, return_exceptions=True)
            for txt in res:
                if isinstance(txt, str):
                    if txt.startswith("#EXTM3U"):
                        supplementary_entries.extend(parse_m3u_file(txt))
                    else:
                        supplementary_entries.extend(parse_txt_content(txt))

    # 网站爬取
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS, args=["--no-sandbox"])
        ctx = await browser.new_context(viewport={"width": 1920, "height": 1080})
        page = await ctx.new_page()
        try:
            await page.goto(TARGET_URL, timeout=PAGE_LOAD_TIMEOUT*1000)
            eng_sel = build_selector(PAGE_CONFIG["engine_search"], "a,button")
            if await page.locator(eng_sel).count() > 0:
                await robust_click(page.locator(eng_sel).first)
            tab_sel = build_selector(PAGE_CONFIG[EXTRACT_MODE == "酒店提取" and "hotel" or "multicast"], "div")
            await robust_click(page.locator(tab_sel).first)
            start_btn = page.locator(build_selector(PAGE_CONFIG["start_button"])).first
            await robust_click(start_btn)
            await asyncio.sleep(AFTER_START_WAIT)
            if await wait_data(page):
                rows = page.locator("div.ios-list-item").filter(has=page.locator("div.item-subtitle:has-text('频道:')"))
                cnt = min(await rows.count(), MAX_IPS)
                for i in range(cnt):
                    supplementary_entries.extend(await extract_one_ip(page, rows.nth(i), i+1))
                    await asyncio.sleep(DELAY_BETWEEN_IPS)
        except Exception as e:
            logger.error(f"爬取失败：{e}")
        finally:
            await browser.close()

    # 过滤补充源
    supplementary_map = defaultdict(list)
    for g, n, u in supplementary_entries:
        if ENABLE_MIGU_FILTER and 'migu' in u.lower():
            continue
        if SKIP_INTERNAL_IP and is_internal_ip(u):
            continue
        supplementary_map[(g, n)].append(u)

    supplementary_map = deduplicate_urls_per_channel(supplementary_map)
    logger.info(f"✅ 补充源获取完成：{len(supplementary_map)} 个频道可用")

    # ===================== 步骤4：补充测速（只测未达标的频道）=================
    logger.info("="*50)
    logger.info("📌 步骤4：补充测速（仅未达标频道）")
    logger.info("="*50)
    tasks = []
    keys_to_supplement = []

    for key, qs in all_qualified.items():
        need = MAX_LINKS_PER_CHANNEL - len(qs)
        if need <= 0:
            continue
        urls = supplementary_map.get(key, [])
        if not urls:
            continue
        keys_to_supplement.append(key)
        tasks.append(smart_test_channel_urls(key, urls, cache, sem))

    if tasks:
        supple_results = await asyncio.gather(*tasks)
        for i, (new_qs, _, new_cache) in enumerate(supple_results):
            key = keys_to_supplement[i]
            all_qualified[key].extend(new_qs)
            cache.update(new_cache)

    # ===================== 步骤5：兜底排序（全部不达标按质量输出）=============
    final_output = {}
    for key, urls in all_qualified.items():
        # 截断到目标数量
        final = urls[:MAX_LINKS_PER_CHANNEL]
        final_output[key] = final

    # 保存 & 导出
    save_cache(cache)
    export_results_with_timestamp(final_output)
    logger.info(f"总耗时：{time.time() - overall_start:.2f}s")
    logger.info("🎉 全部完成！")

if __name__ == "__main__":
    if sys.platform.startswith('linux'):
        asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())
    elif sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
