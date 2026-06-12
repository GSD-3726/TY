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
from playwright.async_api import async_playwright

# TS检测依赖（可选，如果无法导入则禁用TS检测）
try:
    from m3u8 import M3U8
    M3U8_AVAILABLE = True
except ImportError:
    M3U8_AVAILABLE = False
    print("警告: 未安装m3u8库，将禁用TS分片检测，请运行: pip install m3u8")

# ============================================================================
# ======================== 【中文配置区】=====================================
# ============================================================================

TARGET_URL = "https://iptv.809899.xyz"                     # 目标IPTV源爬取网站地址（仅当 ENABLE_WEB_CRAWL = True 时使用）
HEADLESS = True                                            # 浏览器无头模式(True=后台运行不显示界面,False=显示浏览器窗口)
OUTPUT_DIR = Path(__file__).parent                         # 输出文件保存目录(默认脚本所在目录)
OUTPUT_M3U_FILENAME = OUTPUT_DIR / "iptv_channels.m3u"     # 输出M3U格式播放列表文件名
OUTPUT_TXT_FILENAME = OUTPUT_DIR / "iptv_channels.txt"     # 输出TXT格式播放列表文件名
MAX_LINKS_PER_CHANNEL = 10                                 # 单个频道最多保留多少个可用直播源
DEFAULT_PROTOCOL = "http://"                               # 无协议前缀时默认补充的协议（如 rtmp:// 等）

# ----- FFmpeg配置 -----
FFMPEG_PATH = "ffmpeg"                                     # FFmpeg可执行文件路径(默认使用系统PATH中的ffmpeg)
ENABLE_FFMPEG_FINAL = True                                 # 是否启用FFmpeg最终验证（对通过TS检测的源进行短时测试）
FFMPEG_TEST_DURATION = 5                                   # FFmpeg最终验证时长（秒），缩短可加快速度
FFMPEG_CONCURRENCY = 6                                     # FFmpeg最大并发数（同时测试的流数量）
MIN_AVG_FPS = 24                                           # 最低平均帧率要求（低于此值判定为不可用）
MIN_FRAMES = 110                                           # 最低总帧数要求（5秒*24帧=120帧）

# ----- 网页爬取相关配置（仅在 ENABLE_WEB_CRAWL = True 时生效）-----
ENABLE_WEB_CRAWL = False                                    # 是否启用网页爬取（从 TARGET_URL 获取源）
EXTRACT_MODE = "酒店提取"                                   # 提取模式，可选:"酒店提取"/"组播提取"/"引擎搜索"
MAX_IPS = 100                                              # 最多处理多少个IP地址(酒店/组播模式)
MAX_CHANNELS_PER_IP = 0                                    # 每个IP最多提取多少个频道(0=不限制)
DELAY_BETWEEN_IPS = 0.1                                    # 处理不同IP之间的延迟(秒)
DELAY_AFTER_CLICK = 0.3                                    # 点击按钮后等待页面响应的时间(秒)
MODAL_WAIT_TIMEOUT = 1                                     # 等待弹窗出现的超时时间(秒)
PAGE_LOAD_TIMEOUT = 120                                    # 页面加载超时时间(秒)
DATA_LOAD_TIMEOUT = 60                                     # 等待频道数据加载的超时时间(秒)
AFTER_START_WAIT = 30                                      # 点击开始按钮后初始等待时间(秒)
IP_ADDR_TIMEOUT = 0.1                                      # 读取IP地址超时时间(秒)
CHANNEL_NAME_TIMEOUT = 0.1                                 # 读取频道名称超时时间(秒)
CHANNEL_URL_TIMEOUT = 0.1                                  # 读取频道地址超时时间(秒)
SCROLL_TIMEOUT = 0.1                                       # 页面滚动超时时间(秒)
CLICK_TIMEOUT = 0.1                                        # 点击元素超时时间(秒)
WAIT_FOR_ELEMENT_TIMEOUT = 30                              # 等待页面元素出现的超时时间(秒)
DATA_CHECK_INTERVAL = 30                                   # 检查数据是否加载完成的间隔时间(秒)

# ----- 外部源配置 -----
ENABLE_GITHUB_SOURCES = True                               # 是否启用GitHub公共源补充
GITHUB_M3U_LINKS = [                                       # GitHub上的公共M3U/TXT源地址列表
    "https://gh-proxy.com/https://github.com/kimwang1978/collect-txt/blob/main/bbxx.txt"
]

# ----- 质量检测配置 -----
ENABLE_TS_CHECK = True                                     # 是否启用TS分片快速检测（需安装m3u8库）
TS_SAMPLE_SEGMENTS = 5                                     # 下载前几个TS片段进行测速
TS_MAX_AVG_TIME = 0.5                                      # TS片段平均响应时间阈值（秒），超过则判定为慢
TS_TIMEOUT = 2.0                                           # 单个TS片段下载超时(秒)

# ----- 缓存配置 -----
ENABLE_CACHE = True                                        # 是否启用测速结果缓存
CACHE_FILE = OUTPUT_DIR / "iptv_speed_cache.json"          # 缓存文件保存路径
CACHE_EXPIRE_HOURS = 72                                    # 缓存过期时间(小时)

# ----- 频道名称处理配置 -----
ENABLE_CHINESE_CLEAN = True                                # 是否清理频道名称中的非中文字符
ENABLE_DEDUPLICATION = True                                # 是否启用直播源去重(同一个URL只保留在一个频道)
CCTV_USE_MAPPING = True                                    # 是否使用标准CCTV频道名称映射（如 CCTV-1综合）
ENABLE_MIGU_FILTER = True                                  # 是否过滤咪咕源(部分地区无法访问)
SKIP_INTERNAL_IP = True                                    # 是否跳过内网IP地址(192.168.x.x/10.x.x.x等)
ENABLE_SATELLITE_CLEAN = True                              # 是否清理卫视频道名称中的后缀(如"高清"/"移动"/"测试")

# ----- 频道分类规则（按优先级顺序匹配）-----
CATEGORY_RULES = [                                         # 频道自动分类规则
    {"name": "4K专区",      "keywords": ["4k", "4K", "超高清", "2160p"]},
    {"name": "央视频道",    "keywords": ["cctv", "cetv", "央视", "中央", "CCTV", "CETV", "央视频道"]},
    {"name": "卫视频道",    "keywords": ["卫视", "卫视高清"]},
    {"name": "电影频道",    "keywords": ["电影", "影院", "chc", "动作", "剧场", "映画", "影视", "大片", "影视频道"]},
    {"name": "轮播频道",    "keywords": ["轮播", "滚动", "循环"]},
    {"name": "儿童频道",    "keywords": ["少儿", "动画", "动漫", "卡通", "亲子", "儿童", "宝贝"]},
]
GROUP_ORDER = ["央视频道", "卫视频道", "电影频道", "4K专区", "儿童频道", "轮播频道"]  # 输出时的分组顺序

# ----- CCTV频道名称映射 -----
CCTV_NAME_MAPPING = {                                      # CCTV频道数字到标准名称的映射
    "1": "综合", "2": "财经", "3": "综艺", "4": "国际", "5": "体育",
    "5+": "体育赛事", "6": "电影", "7": "国防军事", "8": "电视剧",
    "9": "纪录", "10": "科教", "11": "戏曲", "12": "社会与法",
    "13": "新闻", "14": "少儿", "15": "音乐", "16": "奥林匹克",
    "17": "农业农村",
}
CCTV_ORDER = [                                             # 央视频道输出时的排序
    "CCTV-1综合", "CCTV-2财经", "CCTV-3综艺", "CCTV-4国际",
    "CCTV-5体育", "CCTV-5+体育赛事", "CCTV-6电影", "CCTV-7国防军事",
    "CCTV-8电视剧", "CCTV-9纪录", "CCTV-10科教", "CCTV-11戏曲",
    "CCTV-12社会与法", "CCTV-13新闻", "CCTV-14少儿", "CCTV-15音乐",
    "CCTV-16奥林匹克", "CCTV-17农业农村", "CETV1", "CETV2", "CETV4", "CETV5"
]

# ----- 页面元素匹配配置 -----
PAGE_CONFIG = {                                            # 页面元素文本匹配规则(适配不同网站布局)
    "engine_search": ["引索搜索", "引擎搜索", "关键词搜索"],
    "hotel":        ["酒店提取"],
    "multicast":    ["组播提取"],
    "start_button": ["开始播放", "开始搜索", "开始提取"],
}

# ----- 输出与日志配置 -----
TIME_DISPLAY_AT_TOP = False                                # 更新时间显示在文件顶部(True=顶部,False=底部)
UPDATE_STREAM_URL = "https://gitee.com/bmg369/tvtest/raw/master/cg/index.m3u8"  # 更新时间对应的占位流地址
ENABLE_VERBOSE_LOGGING = False                             # 是否启用详细日志输出

# ----- 快速连通性测试配置 -----
CONNECTIVITY_CONCURRENCY = 100                              # 基础连通性测试的最大并发数
CONNECTIVITY_TIMEOUT = 2                                 # 基础连通性测试超时时间(秒) - 设为0.5秒以快速过滤死链

# ----- 质量评分权重（仅用于FFmpeg结果）-----
SCORE_WEIGHT_FPS = 0.4                                     # 帧率权重
SCORE_WEIGHT_DROP = 0.4                                    # 丢帧率权重
SCORE_WEIGHT_BITRATE = 0.2                                 # 码率权重

# ============================================================================
# ============================= 日志配置 =====================================
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
# ========================= 全局 HTTP 会话 ===================================
# ============================================================================
_global_session: Optional[aiohttp.ClientSession] = None

async def get_global_session() -> aiohttp.ClientSession:
    global _global_session
    if _global_session is None or _global_session.closed:
        connector = aiohttp.TCPConnector(limit=200, limit_per_host=50, ttl_dns_cache=300)
        _global_session = aiohttp.ClientSession(connector=connector, timeout=aiohttp.ClientTimeout(total=30))
    return _global_session

async def close_global_session():
    global _global_session
    if _global_session and not _global_session.closed:
        await _global_session.close()

# ============================================================================
# ========================= 进度条管理 =======================================
# ============================================================================
class ProgressBar:
    def __init__(self, total: int, title: str = "测速进度"):
        self.total = total
        self.title = title
        self.current = 0
        self.success = 0
        self.failed = 0
        self.last_percent = -1
        self.lock = asyncio.Lock()

    async def update(self, success: bool = False):
        async with self.lock:
            self.current += 1
            if success:
                self.success += 1
            else:
                self.failed += 1
            self._print()

    def _print(self):
        if self.total == 0:
            return
        percent_int = int((self.current / self.total) * 100)
        if not ((percent_int % 5 == 0 and percent_int > self.last_percent) or self.current == self.total):
            return
        self.last_percent = percent_int
        
        bar_length = 30
        filled_length = int(bar_length * self.current // self.total)
        bar = '█' * filled_length + '░' * (bar_length - filled_length)
        
        logger.info(
            f"[{self.title}] {percent_int:3d}% |{bar}| "
            f"({self.current}/{self.total}) | 成功:{self.success} | 失败:{self.failed}"
        )
        sys.stdout.flush()

# ============================================================================
# ========================= 缓存管理 =========================================
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
                    if "score" not in data: data["score"] = 0.0
                    valid_cache[url] = data
        logger.info(f"缓存加载完成，有效条目数: {len(valid_cache)}")
        return valid_cache
    except Exception as e:
        logger.debug(f"加载缓存异常: {e}")
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
# ========================= 快速连通性测试 ====================================
# ============================================================================
async def check_url_connectivity(url: str, timeout: float) -> bool:
    if not url.startswith(('http://', 'https://')):
        return True
    try:
        session = await get_global_session()
        timeout_obj = aiohttp.ClientTimeout(total=timeout, connect=timeout, sock_read=timeout)
        async with session.get(url, timeout=timeout_obj, headers={'User-Agent': 'Mozilla/5.0'}, allow_redirects=True) as resp:
            if resp.status != 200:
                return False
            try:
                await resp.content.readexactly(1024)
            except asyncio.IncompleteReadError as e:
                return len(e.partial) > 0
            return True
    except Exception:
        return False

async def filter_online_urls(url_list: List[str], timeout: float = CONNECTIVITY_TIMEOUT, concurrency: int = CONNECTIVITY_CONCURRENCY) -> List[str]:
    sem = asyncio.Semaphore(concurrency)
    async def test_one(url: str) -> bool:
        async with sem:
            return await check_url_connectivity(url, timeout)
    tasks = [test_one(url) for url in url_list]
    results = await asyncio.gather(*tasks)
    return [url for url, ok in zip(url_list, results) if ok]

# ============================================================================
# ========================= TS分片快速检测 ====================================
# ============================================================================
async def check_ts_speed(m3u8_url: str) -> Tuple[bool, float]:
    """
    检测m3u8源的TS分片响应速度
    返回 (是否通过, 综合评分0-100)
    """
    if not M3U8_AVAILABLE or not ENABLE_TS_CHECK:
        return True, 50.0  # 如果禁用TS检测，默认通过

    try:
        session = await get_global_session()
        # 获取m3u8文件
        async with session.get(m3u8_url, headers={'User-Agent': 'Mozilla/5.0'}) as resp:
            if resp.status != 200:
                return False, 0.0
            content = await resp.text()
        
        # 解析m3u8
        playlist = M3U8(content, base_uri=m3u8_url)
        if not playlist.segments:
            return False, 0.0
        
        # 取前N个TS片段
        ts_urls = [seg.uri for seg in playlist.segments[:TS_SAMPLE_SEGMENTS]]
        if not ts_urls:
            return False, 0.0
        
        # 并发下载片段头（只读前1KB）
        sem = asyncio.Semaphore(5)
        async def fetch_one(url):
            async with sem:
                start = time.time()
                try:
                    timeout = aiohttp.ClientTimeout(total=TS_TIMEOUT)
                    async with session.get(url, timeout=timeout) as r:
                        if r.status != 200:
                            return None
                        await r.content.readexactly(1024)
                        elapsed = time.time() - start
                        return elapsed
                except Exception:
                    return None
        
        tasks = [fetch_one(url) for url in ts_urls]
        results = await asyncio.gather(*tasks)
        valid_times = [t for t in results if t is not None]
        if len(valid_times) < max(1, TS_SAMPLE_SEGMENTS // 2):
            return False, 0.0
        
        avg_time = sum(valid_times) / len(valid_times)
        # 评分：平均响应时间 < TS_MAX_AVG_TIME 得100分，否则线性递减
        if avg_time <= TS_MAX_AVG_TIME:
            score = 100.0
        else:
            score = max(0, 100 * (TS_MAX_AVG_TIME / avg_time))
        passed = avg_time <= TS_MAX_AVG_TIME
        return passed, score
    except Exception as e:
        logger.debug(f"TS检测失败 {m3u8_url}: {e}")
        return False, 0.0

# ============================================================================
# ========================= FFmpeg 深度验证 ===================================
# ============================================================================
async def test_stream_ffmpeg_quick(url: str, duration: int = FFMPEG_TEST_DURATION) -> Dict[str, Any]:
    """快速FFmpeg测试（短时）"""
    # 检查FFmpeg是否可用
    ffmpeg_path = shutil.which(FFMPEG_PATH)
    if not ffmpeg_path:
        logger.warning(f"未找到FFmpeg可执行文件: {FFMPEG_PATH}，将跳过FFmpeg验证")
        return {"ok": True, "fps": 25.0, "frames": 125, "score": 70.0}  # 默认通过
    
    headers = (
        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36\r\n"
        "Referer: https://www.miguvideo.com/\r\n"
    )
    cmd = [
        ffmpeg_path, "-hide_banner", "-y",
        "-headers", headers,
        "-fflags", "nobuffer",
        "-analyzeduration", "3000000", "-probesize", "3000000",
        "-rw_timeout", "5000000",
        "-i", url,
        "-t", str(duration),
        "-f", "null", "-"
    ]
    try:
        proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE)
        try:
            _, stderr = await asyncio.wait_for(proc.communicate(), timeout=duration + 5)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            return {"ok": False, "fps": 0.0, "frames": 0, "score": 0.0}
        output = stderr.decode('utf-8', errors='ignore')
        frame_matches = re.findall(r'frame=\s*(\d+)', output)
        fps_matches = re.findall(r'fps=\s*([\d.]+)', output)
        drop_matches = re.findall(r'drop=\s*(\d+)', output)
        bitrate_matches = re.findall(r'bitrate=\s*([\d.]+)\s*kbits/s', output)
        
        frames = int(frame_matches[-1]) if frame_matches else 0
        avg_fps = float(fps_matches[-1]) if fps_matches else 0.0
        dropped = int(drop_matches[-1]) if drop_matches else 0
        bitrate = float(bitrate_matches[-1]) if bitrate_matches else 0.0
        
        min_frames_req = max(MIN_FRAMES, int(duration * MIN_AVG_FPS * 0.8))
        is_ok = frames >= min_frames_req and avg_fps >= (MIN_AVG_FPS * 0.8)
        drop_rate = dropped / frames if frames > 0 else 1.0
        if drop_rate > 0.05:
            is_ok = False
        
        # 评分
        fps_score = min(50, (avg_fps / MIN_AVG_FPS) * 50) if avg_fps > 0 else 0
        drop_score = max(0, 40 * (1 - drop_rate))
        bitrate_score = min(10, (bitrate / 2000) * 10) if bitrate > 0 else 0
        score = fps_score + drop_score + bitrate_score
        return {"ok": is_ok, "fps": avg_fps, "frames": frames, "score": score}
    except Exception as e:
        logger.debug(f"FFmpeg测试异常 {url}: {e}")
        return {"ok": False, "fps": 0.0, "frames": 0, "score": 0.0}

# ============================================================================
# ================== 智能测速（三级筛选 + 达标即停）==========================
# ============================================================================
async def smart_test_channel_urls(
    channel_key: Tuple[str, str],
    url_list: List[str],
    cache: dict,
    sem_ffmpeg: asyncio.Semaphore,
    progress: Optional[ProgressBar] = None
) -> Tuple[List[Tuple[str, float]], List[str], dict]:
    """
    对频道内的URL列表进行三级筛选：
    1. 连通性（已在外部做）
    2. TS分片检测
    3. FFmpeg深度验证（可选）
    返回 (url, score) 列表，未测完的列表，新缓存
    """
    group, name = channel_key
    qualified = []      # (url, score)
    remaining_urls = []
    new_cache = {}
    now = time.time()
    
    for url in url_list:
        if len(qualified) >= MAX_LINKS_PER_CHANNEL:
            remaining_urls.append(url)
            continue
        
        # 缓存检查
        cache_item = cache.get(url)
        if cache_item and isinstance(cache_item, dict) and "ok" in cache_item:
            if is_cache_valid(cache_item.get("timestamp", 0)):
                if cache_item["ok"]:
                    qualified.append((url, cache_item.get("score", 0.0)))
                continue
        
        # Level 2: TS检测（仅对http/https且可能是m3u8的链接）
        ts_passed = True
        ts_score = 50.0
        if url.lower().endswith('.m3u8') or '/m3u8' in url.lower():
            ts_passed, ts_score = await check_ts_speed(url)
            if not ts_passed:
                # TS检测失败，标记为不可用
                new_cache[url] = {"ok": False, "score": 0.0, "timestamp": now}
                if progress:
                    await progress.update(success=False)
                continue
        
        # Level 3: FFmpeg最终验证（可选）
        if ENABLE_FFMPEG_FINAL:
            async with sem_ffmpeg:
                ff_result = await test_stream_ffmpeg_quick(url, FFMPEG_TEST_DURATION)
            ok = ff_result["ok"]
            score = ff_result["score"]
            # 结合TS评分（如果TS检测通过，可以适当提高权重，但简单取平均或取最小）
            if ts_passed and ts_score > 50:
                score = (score + ts_score) / 2
        else:
            ok = ts_passed
            score = ts_score
        
        new_cache[url] = {
            "ok": ok, "score": score,
            "fps": ff_result.get("fps", 0) if ENABLE_FFMPEG_FINAL else 0,
            "timestamp": now
        }
        if ok:
            qualified.append((url, score))
        if progress:
            await progress.update(success=ok)
    
    qualified.sort(key=lambda x: x[1], reverse=True)
    return qualified[:MAX_LINKS_PER_CHANNEL], remaining_urls, new_cache

# ============================================================================
# ========================= M3U解析（读取本地历史文件）=======================
# ============================================================================
def parse_existing_m3u(m3u_path: Path) -> Dict[Tuple[str, str], List[str]]:
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
# ========================= GitHub / TXT 解析 ================================
# ============================================================================
@functools.lru_cache(maxsize=128)
async def download_github_m3u(url: str) -> str:
    session = await get_global_session()
    try:
        async with session.get(url, headers={'User-Agent': 'Mozilla/5.0'}) as r:
            if r.status == 200:
                return await r.text()
    except Exception as e:
        logger.debug(f"下载失败 {url}: {e}")
    return ""

def parse_m3u_file(content: str) -> List[Tuple[str, str, str]]:
    channels = []
    n = ""
    for line in content.splitlines():
        line = line.strip()
        if line.startswith("#EXTINF"):
            m = re.search(r'#EXTINF:-1.*?group-title="[^"]*",(.+)', line)
            if not m:
                m = re.search(r'#EXTINF:-1.*?,(.+)', line)
            if m:
                n = m.group(1).strip()
        elif line.startswith("http"):
            u = line
            if n and u:
                n_cleaned = clean_satellite_name(n)
                nn = normalize_cctv(n_cleaned)
                gr = classify_channel(nn)
                if gr:
                    fn = nn if gr == "央视频道" else (clean_chinese_only(n_cleaned) if ENABLE_CHINESE_CLEAN else n_cleaned)
                    channels.append((gr, fn, u))
            n = ""
    return channels

def parse_txt_content(content: str) -> List[Tuple[str, str, str]]:
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

# ============================================================================
# ========================= 网站爬取相关函数 ==================================
# ============================================================================
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

async def extract_one_ip(page, row, idx):
    entries = []
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
                entries.append((g, fn, u))
            except:
                continue
    except:
        pass
    return entries

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
# ========================= 导出结果 =========================================
# ============================================================================
def export_results_with_timestamp(channel_map: Dict[Tuple[str, str], List[str]]):
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
                ordered = []
                for std in CCTV_ORDER:
                    if std in name_to_urls:
                        for url in name_to_urls[std]:
                            ordered.append((std, url))
                chs = ordered
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
                ordered = []
                for std in CCTV_ORDER:
                    if std in name_to_urls:
                        for url in name_to_urls[std]:
                            ordered.append((std, url))
                chs = ordered
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
    overall_start = time.time()
    cache = load_cache() if ENABLE_CACHE else {}
    sem_ffmpeg = asyncio.Semaphore(FFMPEG_CONCURRENCY)

    logger.info("="*50)
    logger.info("📌 步骤1：读取本地历史 M3U 频道")
    logger.info("="*50)
    final_channel_map = parse_existing_m3u(OUTPUT_M3U_FILENAME)
    all_qualified = defaultdict(list)  # key -> list of (url, score)

    logger.info("="*50)
    logger.info("📌 步骤2：智能测速本地源（三级筛选+达标即停）")
    logger.info("="*50)
    tasks = []
    channel_keys = list(final_channel_map.keys())
    
    total_to_test = 0
    for key in channel_keys:
        urls = final_channel_map[key]
        for url in urls:
            item = cache.get(url)
            if not (item and isinstance(item, dict) and item.get("ok") and is_cache_valid(item.get("timestamp", 0))):
                total_to_test += 1
    local_progress = ProgressBar(total_to_test, title="本地源测速")
    logger.info(f"需测速URL总数: {total_to_test}")

    for key in channel_keys:
        urls = final_channel_map[key]
        tasks.append(smart_test_channel_urls(key, urls, cache, sem_ffmpeg, local_progress))
    results = await asyncio.gather(*tasks)
    for i, (qualified, _, new_cache) in enumerate(results):
        all_qualified[channel_keys[i]] = qualified
        cache.update(new_cache)

    total_need = 0
    total_ok = 0
    for qs in all_qualified.values():
        total_need += max(0, MAX_LINKS_PER_CHANNEL - len(qs))
        total_ok += len(qs)
    logger.info(f"✅ 本地源测速完成：达标 {total_ok} 条，需补充 {total_need} 条")

    if total_need <= 0:
        logger.info("🎉 所有频道已满足要求，直接输出！")
        save_cache(cache)
        final_out = {k: [url for url, _ in v] for k, v in all_qualified.items()}
        export_results_with_timestamp(final_out)
        await close_global_session()
        return

    # ===================== 步骤3：获取补充源 =================================
    logger.info("="*50)
    logger.info("📌 步骤3：获取补充源（GitHub + 可选网页爬取）")
    logger.info("="*50)
    supplementary_entries = []

    # GitHub 源
    if ENABLE_GITHUB_SOURCES:
        tasks = [download_github_m3u(u) for u in GITHUB_M3U_LINKS]
        res = await asyncio.gather(*tasks, return_exceptions=True)
        for txt in res:
            if isinstance(txt, str) and txt:
                if txt.startswith("#EXTM3U"):
                    supplementary_entries.extend(parse_m3u_file(txt))
                else:
                    supplementary_entries.extend(parse_txt_content(txt))

    # 网页爬取（仅当开关打开时）
    if ENABLE_WEB_CRAWL:
        logger.info("开始网页爬取...")
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
    else:
        logger.info("网页爬取已禁用（ENABLE_WEB_CRAWL=False），仅使用 GitHub 源")

    # 过滤补充源
    supplementary_map = defaultdict(list)
    for g, n, u in supplementary_entries:
        if ENABLE_MIGU_FILTER and 'migu' in u.lower():
            continue
        if SKIP_INTERNAL_IP and is_internal_ip(u):
            continue
        supplementary_map[(g, n)].append(u)
    supplementary_map = deduplicate_urls_per_channel(supplementary_map)
    logger.info(f"✅ 补充源获取完成（去重后）：{len(supplementary_map)} 个频道可用")

    # 快速连通性筛选
    logger.info("="*50)
    logger.info("📌 步骤3.5：快速连通性筛选（剔除无效链接）")
    logger.info("="*50)
    filtered_map = defaultdict(list)
    total_before = 0
    total_after = 0
    for key, urls in supplementary_map.items():
        total_before += len(urls)
        if urls:
            online = await filter_online_urls(urls, timeout=CONNECTIVITY_TIMEOUT, concurrency=CONNECTIVITY_CONCURRENCY)
            filtered_map[key] = online
            total_after += len(online)
    logger.info(f"连通性筛选：{total_before} → {total_after} 有效（剔除 {total_before - total_after}）")
    supplementary_map = filtered_map

    # 补充测速
    logger.info("="*50)
    logger.info("📌 步骤4：补充测速（仅未达标频道）")
    logger.info("="*50)
    tasks = []
    keys_to_supp = []
    total_supp_urls = 0
    for key, qs in all_qualified.items():
        need = MAX_LINKS_PER_CHANNEL - len(qs)
        if need <= 0:
            continue
        urls = supplementary_map.get(key, [])
        if not urls:
            continue
        for url in urls:
            item = cache.get(url)
            if not (item and isinstance(item, dict) and item.get("ok") and is_cache_valid(item.get("timestamp", 0))):
                total_supp_urls += 1
    supp_progress = ProgressBar(total_supp_urls, title="补充源测速")
    logger.info(f"需补充测速URL总数: {total_supp_urls}")

    for key, qs in all_qualified.items():
        need = MAX_LINKS_PER_CHANNEL - len(qs)
        if need <= 0:
            continue
        urls = supplementary_map.get(key, [])
        if not urls:
            continue
        keys_to_supp.append(key)
        tasks.append(smart_test_channel_urls(key, urls, cache, sem_ffmpeg, supp_progress))
    if tasks:
        supp_results = await asyncio.gather(*tasks)
        for i, (new_qs, _, new_cache) in enumerate(supp_results):
            key = keys_to_supp[i]
            all_qualified[key].extend(new_qs)
            cache.update(new_cache)

    # 最终输出
    final_output = {}
    for key, qs in all_qualified.items():
        sorted_qs = sorted(qs, key=lambda x: x[1], reverse=True)
        final_output[key] = [url for url, _ in sorted_qs[:MAX_LINKS_PER_CHANNEL]]

    save_cache(cache)
    export_results_with_timestamp(final_output)
    await close_global_session()
    logger.info(f"总耗时：{time.time() - overall_start:.2f}s")
    logger.info("🎉 全部完成！")

if __name__ == "__main__":
    if sys.platform.startswith('linux'):
        asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())
    elif sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
