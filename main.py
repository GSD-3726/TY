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

# TS检测依赖（可选）
try:
    from m3u8 import M3U8
    M3U8_AVAILABLE = True
except ImportError:
    M3U8_AVAILABLE = False
    print("警告: 未安装m3u8库，已禁用TS分片检测，执行：pip install m3u8")

# ============================================================================
# ======================== 【中文配置区】优化版（保源+提速+稳源）=====================
# ============================================================================
TARGET_URL = "https://iptv.809899.xyz"
HEADLESS = True
OUTPUT_DIR = Path(__file__).parent
OUTPUT_M3U_FILENAME = OUTPUT_DIR / "iptv_channels.m3u"
OUTPUT_TXT_FILENAME = OUTPUT_DIR / "iptv_channels.txt"
MAX_LINKS_PER_CHANNEL = 8        # 单频道保留8个备用源，提升稳定性
DEFAULT_PROTOCOL = "http://"

# ----- FFmpeg 配置（平衡稳定+速度，轻微放宽判定，保留低卡顿源）-----
FFMPEG_PATH = "ffmpeg"
ENABLE_FFMPEG_FINAL = True
FFMPEG_TEST_DURATION = 3         # 测试时长3秒，兼顾速度与真实性
FFMPEG_CONCURRENCY = 12          # 高并发提速
MIN_AVG_FPS = 21                 # 略低于标准帧率，保留轻微降帧但稳定源
MIN_FRAMES = 55                  # 帧数阈值微调，减少误删

# ----- 网页爬取配置（适度延时，保证采集完整，避免漏源）-----
ENABLE_WEB_CRAWL = True
EXTRACT_MODE = "酒店提取"
MAX_IPS = 150
MAX_CHANNELS_PER_IP = 0
DELAY_BETWEEN_IPS = 0.05
DELAY_AFTER_CLICK = 0.2
MODAL_WAIT_TIMEOUT = 1.5
PAGE_LOAD_TIMEOUT = 120
DATA_LOAD_TIMEOUT = 60
AFTER_START_WAIT = 25
IP_ADDR_TIMEOUT = 0.1
CHANNEL_NAME_TIMEOUT = 0.1
CHANNEL_URL_TIMEOUT = 0.1
SCROLL_TIMEOUT = 0.1
CLICK_TIMEOUT = 0.1
WAIT_FOR_ELEMENT_TIMEOUT = 30
DATA_CHECK_INTERVAL = 20

# ----- 外部源配置 -----
ENABLE_GITHUB_SOURCES = True
GITHUB_M3U_LINKS = [
    "https://gh-proxy.com/https://github.com/kimwang1978/collect-txt/blob/main/bbxx.txt"
]

# ----- TS分片检测（放宽阈值，保留慢速但稳定流；减少采样提速）-----
ENABLE_TS_CHECK = True
TS_SAMPLE_SEGMENTS = 3           # 仅采样3个分片，提速
TS_MAX_AVG_TIME = 0.9            # 响应阈值放宽，慢但不卡的源保留
TS_TIMEOUT = 3.0                 # 分片超时延长

# ----- 缓存配置（延长缓存，避免重复测速，优先复用已验证稳定源）-----
ENABLE_CACHE = True
CACHE_FILE = OUTPUT_DIR / "iptv_speed_cache.json"
CACHE_EXPIRE_HOURS = 96          # 缓存4天，稳定源长期复用

# ----- 频道过滤开关（按需开启/关闭）-----
ENABLE_CHINESE_CLEAN = True
ENABLE_DEDUPLICATION = True
CCTV_USE_MAPPING = True
ENABLE_MIGU_FILTER = False       # 关闭咪咕过滤，增加源数量
SKIP_INTERNAL_IP = False        # 关闭内网IP过滤，保留局域网稳定源
ENABLE_SATELLITE_CLEAN = True

# ----- 频道白名单【仅匹配以下分类才会保留+测速】-----
CATEGORY_RULES = [
    {"name": "4K专区",      "keywords": ["4k", "4K", "超高清", "2160p"]},
    {"name": "央视频道",    "keywords": ["cctv", "cetv", "央视", "中央", "CCTV", "CETV", "央视频道"]},
    {"name": "卫视频道",    "keywords": ["卫视", "卫视高清"]},
    {"name": "电影频道",    "keywords": ["电影", "影院", "chc", "动作", "剧场", "映画", "影视", "大片", "影视频道"]},
    {"name": "轮播频道",    "keywords": ["轮播", "滚动", "循环"]},
    {"name": "儿童频道",    "keywords": ["少儿", "动画", "动漫", "卡通", "亲子", "儿童", "宝贝"]},
]
GROUP_ORDER = ["央视频道", "卫视频道", "电影频道", "4K专区", "儿童频道", "轮播频道"]

# ----- CCTV 名称标准化映射 -----
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

# ----- 页面元素匹配 -----
PAGE_CONFIG = {
    "engine_search": ["引索搜索", "引擎搜索", "关键词搜索"],
    "hotel":        ["酒店提取"],
    "multicast":    ["组播提取"],
    "start_button": ["开始播放", "开始搜索", "开始提取"],
}

# ----- 输出与日志 -----
TIME_DISPLAY_AT_TOP = False
UPDATE_STREAM_URL = "https://gitee.com/bmg369/tvtest/raw/master/cg/index.m3u8"
ENABLE_VERBOSE_LOGGING = False

# ----- 连通性检测（高并发+放宽超时，减少误杀弱网稳定源）-----
CONNECTIVITY_CONCURRENCY = 150
CONNECTIVITY_TIMEOUT = 2.5

# ----- 评分权重 -----
SCORE_WEIGHT_FPS = 0.4
SCORE_WEIGHT_DROP = 0.4
SCORE_WEIGHT_BITRATE = 0.2

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
        return f"{dt.strftime('%Y-%m-%d %H:%M:%S')},{dt.microsecond // 1000:03d}"

logger = logging.getLogger('IPTV-Collector')
logger.setLevel(log_level)
logger.handlers.clear()

stdout_handler = logging.StreamHandler(sys.stdout)
formatter = BeijingFormatter("%(asctime)s - %(levelname)s - %(message)s")
stdout_handler.setFormatter(formatter)
logger.addHandler(stdout_handler)

# ============================================================================
# ========================= 工具函数 =========================================
# ============================================================================
CCTV_PATTERN = re.compile(r'(cctv)[-\s]?(\d{1,3}|5\+)', re.IGNORECASE)
CHINESE_ONLY_PATTERN = re.compile(r'[^\u4e00-\u9fff]')
INTERNAL_IP_PATTERN = re.compile(r'^(192\.168\.|10\.|172\.(1[6-9]|2\d|3[0-1])\.|127\.0\.0\.1)')

CHINESE_NUM_MAP = {
    '一': '1', '二': '2', '三': '3', '四': '4', '五': '5',
    '六': '6', '七': '7', '八': '8', '九': '9', '十': '10',
    '十一': '11', '十二': '12', '十三': '13', '十四': '14',
    '十五': '15', '十六': '16', '十七': '17'
}
CHINESE_NUM_PATTERN = '|'.join(sorted(CHINESE_NUM_MAP.keys(), key=len, reverse=True))

def build_classifier():
    compiled_rules = []
    for rule in CATEGORY_RULES:
        if not rule["keywords"]:
            continue
        pattern = re.compile("|".join(re.escape(kw.lower()) for kw in rule["keywords"]))
        compiled_rules.append((rule["name"], pattern))
    return lambda name: next((g for g, p in compiled_rules if p.search(name.lower())), None)

classify_channel = build_classifier()

def normalize_cctv(name: str) -> str:
    name_lower = name.lower()
    if re.search(r'cctv[-\s]?4k', name_lower):
        return "CCTV-4K"
    if "cctv5+" in name_lower:
        return "CCTV-5+体育赛事" if CCTV_USE_MAPPING else "CCTV-5+"

    cctv_match = CCTV_PATTERN.search(name_lower)
    if cctv_match:
        num = cctv_match.group(2)
        if CCTV_USE_MAPPING and num in CCTV_NAME_MAPPING:
            return f"CCTV-{num}{CCTV_NAME_MAPPING[num]}"
        return f"CCTV-{num}"

    chn_match = re.search(CHINESE_NUM_PATTERN, name)
    if chn_match:
        chn_num = chn_match.group()
        num = CHINESE_NUM_MAP.get(chn_num)
        if num and num in CCTV_NAME_MAPPING:
            return f"CCTV-{num}{CCTV_NAME_MAPPING[num]}"
    return name

def clean_chinese_only(name: str) -> str:
    return CHINESE_ONLY_PATTERN.sub('', name)

def build_selector(text_list: List[str], elem_type: str = "button") -> str:
    if not text_list:
        return ""
    if len(text_list) == 1:
        return f"{elem_type}:has-text('{text_list[0]}')"
    pattern = "|".join(re.escape(t) for t in text_list)
    return f"{elem_type}:text-matches('{pattern}')"

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
        r'(?:\s*[（(])?\s*'
        r'(移动|高清|HD|超高清|4K|标清|测试)'
        r'(?:\s*[）)])?\s*',
        re.IGNORECASE
    )
    return pattern.sub(r'\1', name)

# ============================================================================
# ========================= 全局HTTP会话 ===================================
# ============================================================================
_global_session: Optional[aiohttp.ClientSession] = None

async def get_global_session() -> aiohttp.ClientSession:
    global _global_session
    if _global_session is None or _global_session.closed:
        connector = aiohttp.TCPConnector(limit=200, limit_per_host=50, ttl_dns_cache=300)
        timeout = aiohttp.ClientTimeout(total=30)
        _global_session = aiohttp.ClientSession(connector=connector, timeout=timeout)
    return _global_session

async def close_global_session():
    global _global_session
    if _global_session and not _global_session.closed:
        await _global_session.close()
        _global_session = None

# ============================================================================
# ========================= 进度条 =======================================
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
        percent = int((self.current / self.total) * 100)
        if percent % 5 != 0 and self.current != self.total:
            return
        self.last_percent = percent
        bar_len = 30
        filled = int(bar_len * self.current / self.total)
        bar = "█" * filled + "░" * (bar_len - filled)
        logger.info(f"[{self.title}] {percent:3d}% |{bar}| ({self.current}/{self.total}) 正常:{self.success} 失效:{self.failed}")

# ============================================================================
# ========================= 缓存管理 =========================================
# ============================================================================
CACHE_EXPIRE_SECONDS = CACHE_EXPIRE_HOURS * 3600

def load_cache() -> dict:
    if not ENABLE_CACHE or not CACHE_FILE.exists():
        return {}
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            cache = json.load(f)
        now = time.time()
        valid_cache = {}
        for url, data in cache.items():
            if not isinstance(data, dict) or "timestamp" not in data:
                continue
            if now - data["timestamp"] < CACHE_EXPIRE_SECONDS:
                if "score" not in data:
                    data["score"] = 0.0
                valid_cache[url] = data
        logger.info(f"加载缓存成功，有效缓存条目：{len(valid_cache)}")
        return valid_cache
    except Exception as e:
        logger.warning(f"加载缓存失败：{str(e)}")
        return {}

def save_cache(cache: dict):
    if not ENABLE_CACHE:
        return
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning(f"保存缓存失败：{str(e)}")

def is_cache_valid(timestamp: float) -> bool:
    return (time.time() - timestamp) < CACHE_EXPIRE_SECONDS

# ============================================================================
# ========================= 连通性检测 ====================================
# ============================================================================
async def check_url_connectivity(url: str, timeout: float) -> bool:
    if not url.startswith(("http://", "https://")):
        return True
    try:
        session = await get_global_session()
        req_timeout = aiohttp.ClientTimeout(total=timeout, connect=timeout, sock_read=timeout)
        async with session.get(url, timeout=req_timeout, headers={"User-Agent": "Mozilla/5.0"}, allow_redirects=True) as resp:
            if resp.status < 400:
                await resp.content.read(1024)
                return True
            return False
    except Exception:
        return False

async def filter_online_urls(url_list: List[str], timeout: float, concurrency: int) -> List[str]:
    sem = asyncio.Semaphore(concurrency)
    async def task(url: str) -> Optional[str]:
        async with sem:
            if await check_url_connectivity(url, timeout):
                return url
            return None
    tasks = [task(u) for u in url_list]
    results = await asyncio.gather(*tasks)
    return [u for u in results if u is not None]

# ============================================================================
# ========================= TS分片检测 ====================================
# ============================================================================
async def check_ts_speed(m3u8_url: str) -> Tuple[bool, float]:
    if not M3U8_AVAILABLE or not ENABLE_TS_CHECK:
        return True, 50.0
    try:
        session = await get_global_session()
        async with session.get(m3u8_url, timeout=aiohttp.ClientTimeout(total=TS_TIMEOUT), headers={"User-Agent": "Mozilla/5.0"}) as resp:
            if resp.status >= 400:
                return False, 0.0
            content = await resp.text()
        playlist = M3U8(content, base_uri=m3u8_url)
        if not playlist.segments:
            return False, 0.0

        ts_urls = [seg.uri for seg in playlist.segments[:TS_SAMPLE_SEGMENTS]]
        sem = asyncio.Semaphore(5)
        async def get_cost(ts_url: str) -> Optional[float]:
            async with sem:
                st = time.time()
                try:
                    async with session.get(ts_url, timeout=aiohttp.ClientTimeout(total=TS_TIMEOUT)) as r:
                        if r.status < 400:
                            await r.content.read(1024)
                            return time.time() - st
                except Exception:
                    pass
                return None

        tasks = [get_cost(u) for u in ts_urls]
        costs = await asyncio.gather(*tasks)
        valid_costs = [c for c in costs if c is not None]
        if len(valid_costs) < max(1, TS_SAMPLE_SEGMENTS // 2):
            return False, 0.0

        avg_cost = sum(valid_costs) / len(valid_costs)
        if avg_cost <= TS_MAX_AVG_TIME:
            score = 100.0
        else:
            score = max(0.0, 100.0 * (TS_MAX_AVG_TIME / avg_cost))
        return avg_cost <= TS_MAX_AVG_TIME, score
    except Exception:
        return True, 50.0

# ============================================================================
# ========================= FFmpeg 流稳定性检测 ===============================
# 核心：检测帧率、丢包，判断是否卡顿
# ============================================================================
async def test_stream_ffmpeg(url: str, duration: int) -> Dict[str, Any]:
    ffmpeg_bin = shutil.which(FFMPEG_PATH)
    if not ffmpeg_bin:
        logger.warning("未找到ffmpeg，跳过视频流检测")
        return {"ok": True, "fps": 25.0, "frames": 120, "drop_rate": 0.0, "score": 70.0}

    headers = "User-Agent: Mozilla/5.0\r\nReferer: https://www.miguvideo.com/\r\n"
    cmd = [
        ffmpeg_bin, "-hide_banner", "-y",
        "-headers", headers,
        "-fflags", "nobuffer",
        "-analyzeduration", "3000000",
        "-probesize", "3000000",
        "-rw_timeout", "5000000",
        "-i", url,
        "-t", str(duration),
        "-f", "null", "-"
    ]

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE
        )
        _, stderr_data = await proc.communicate(timeout=duration + 8)
        stderr_text = stderr_data.decode("utf-8", errors="ignore")

        frame_reg = re.compile(r'frame=\s*(\d+)')
        fps_reg = re.compile(r'fps=\s*(\d+\.?\d*)')
        drop_reg = re.compile(r'drop=\s*(\d+)')

        frame_match = frame_reg.search(stderr_text)
        fps_match = fps_reg.search(stderr_text)
        drop_match = drop_reg.search(stderr_text)

        frames = int(frame_match.group(1)) if frame_match else 0
        avg_fps = float(fps_match.group(1)) if fps_match else 0.0
        drop = int(drop_match.group(1)) if drop_match else 0

        drop_rate = drop / (frames + 1) if frames > 0 else 1.0

        # 判定规则：放宽标准，保留轻微波动源
        ok_flag = frames >= MIN_FRAMES and avg_fps >= MIN_AVG_FPS and drop_rate <= 0.08

        # 综合评分
        fps_score = min(50.0, (avg_fps / 25) * 50)
        drop_score = max(0.0, 40.0 * (1 - drop_rate))
        bitrate_score = 10.0
        total_score = fps_score + drop_score + bitrate_score

        return {
            "ok": ok_flag,
            "fps": avg_fps,
            "frames": frames,
            "drop_rate": drop_rate,
            "score": round(total_score, 2)
        }
    except Exception as e:
        logger.debug(f"FFmpeg检测异常 {url}: {str(e)}")
        return {"ok": True, "fps": 0.0, "frames": 0, "drop_rate": 1.0, "score": 40.0}

# ============================================================================
# ========================= 综合测速逻辑 ====================================
# ============================================================================
async def smart_test_channel_urls(
    channel_key: Tuple[str, str],
    url_list: List[str],
    cache: dict,
    sem_ffmpeg: asyncio.Semaphore,
    progress: Optional[ProgressBar] = None
) -> Tuple[List[Tuple[str, float]], List[str], dict]:
    qualified = []
    remaining = []
    new_cache = {}
    now_ts = time.time()

    for url in url_list:
        if len(qualified) >= MAX_LINKS_PER_CHANNEL:
            remaining.append(url)
            continue

        # 读取缓存
        cache_item = cache.get(url)
        if cache_item and isinstance(cache_item, dict) and cache_item.get("ok") and is_cache_valid(cache_item.get("timestamp", 0)):
            qualified.append((url, cache_item.get("score", 0.0)))
            continue

        # 第二步：TS分片检测（仅m3u8流）
        ts_ok, ts_score = True, 50.0
        if url.lower().endswith(".m3u8") or "m3u8" in url.lower():
            ts_ok, ts_score = await check_ts_speed(url)
            if not ts_ok:
                new_cache[url] = {"ok": False, "score": 0.0, "timestamp": now_ts}
                if progress:
                    await progress.update(success=False)
                continue

        # 第三步：FFmpeg 稳定性检测
        ffmpeg_ok = True
        ffmpeg_score = 50.0
        async with sem_ffmpeg:
            ffmpeg_res = await test_stream_ffmpeg(url, FFMPEG_TEST_DURATION)
            ffmpeg_ok = ffmpeg_res["ok"]
            ffmpeg_score = ffmpeg_res["score"]

        # 综合判定 & 写入缓存
        final_ok = ts_ok and ffmpeg_ok
        final_score = (ts_score + ffmpeg_score) / 2

        new_cache[url] = {
            "ok": final_ok,
            "score": final_score,
            "timestamp": now_ts
        }

        if final_ok:
            qualified.append((url, final_score))
            if progress:
                await progress.update(success=True)
        else:
            if progress:
                await progress.update(success=False)

    # 按分数降序排序
    qualified.sort(key=lambda x: x[1], reverse=True)
    return qualified[:MAX_LINKS_PER_CHANNEL], remaining, new_cache

# ============================================================================
# ========================= M3U 文件解析 ====================================
# ============================================================================
def parse_existing_m3u(m3u_path: Path) -> Dict[Tuple[str, str], List[str]]:
    channel_map = defaultdict(list)
    if not m3u_path.exists():
        logger.info("本地M3U文件不存在")
        return dict(channel_map)

    current_name = ""
    current_group = ""
    try:
        with open(m3u_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                if line.startswith("#EXTINF"):
                    name_match = re.search(r',(.+)$', line)
                    group_match = re.search(r'tvg-group="([^"]+)"', line)
                    if name_match:
                        current_name = name_match.group(1).strip()
                    if group_match:
                        current_group = group_match.group(1).strip()
                elif line.startswith(("http://", "https://")):
                    chn_name = current_name
                    if ENABLE_SATELLITE_CLEAN:
                        chn_name = clean_satellite_name(chn_name)
                    if ENABLE_CHINESE_CLEAN:
                        chn_name = clean_chinese_only(chn_name)
                    chn_name = normalize_cctv(chn_name)
                    # 二次校验白名单
                    group = classify_channel(chn_name)
                    if group:
                        channel_map[(group, chn_name)].append(line)
    except Exception as e:
        logger.warning(f"解析本地M3U失败：{str(e)}")
    logger.info(f"解析本地M3U完成，有效白名单频道数：{len(channel_map)}")
    return dict(channel_map)

# ============================================================================
# ========================= 网络源解析 ====================================
# ============================================================================
@functools.lru_cache(maxsize=128)
async def download_github_m3u(url: str) -> str:
    try:
        session = await get_global_session()
        async with session.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=aiohttp.ClientTimeout(total=20)) as resp:
            if resp.status < 400:
                return await resp.text()
    except Exception as e:
        logger.debug(f"下载外部源失败 {url}: {str(e)}")
    return ""

def parse_m3u_file(content: str) -> List[Tuple[str, str, str]]:
    res = []
    curr_name = ""
    for line in content.splitlines():
        line = line.strip()
        if line.startswith("#EXTINF"):
            name_mat = re.search(r',(.+)$', line)
            if name_mat:
                curr_name = name_mat.group(1).strip()
        elif line.startswith(("http://", "https://")):
            name = curr_name
            if ENABLE_SATELLITE_CLEAN:
                name = clean_satellite_name(name)
            if ENABLE_CHINESE_CLEAN:
                name = clean_chinese_only(name)
            name = normalize_cctv(name)
            group = classify_channel(name)
            if group:
                res.append((group, name, line))
    return res

def parse_txt_content(content: str) -> List[Tuple[str, str, str]]:
    res = []
    for line in content.splitlines():
        line = line.strip()
        if not line or ',' not in line:
            continue
        parts = line.split(',', 1)
        if len(parts) != 2:
            continue
        name, url = parts
        if not url.startswith(("http://", "https://")):
            continue
        name = name.strip()
        if ENABLE_SATELLITE_CLEAN:
            name = clean_satellite_name(name)
        if ENABLE_CHINESE_CLEAN:
            name = clean_chinese_only(name)
        name = normalize_cctv(name)
        group = classify_channel(name)
        if group:
            res.append((group, name, url))
    return res

# ============================================================================
# ========================= 网页爬取函数 ====================================
# ============================================================================
async def robust_click(locator, timeout: float = CLICK_TIMEOUT) -> bool:
    try:
        await locator.scroll_into_view_if_needed(timeout=timeout * 1000)
        await asyncio.sleep(0.2)
        await locator.click(force=True, timeout=timeout * 1000)
        return True
    except Exception:
        try:
            await locator.evaluate("el => el.click()")
            return True
        except Exception:
            return False

async def wait_for_element(page, selector: str, timeout: float = WAIT_FOR_ELEMENT_TIMEOUT) -> bool:
    try:
        await page.wait_for_selector(selector, timeout=timeout * 1000)
        return True
    except Exception:
        return False

async def extract_one_ip(page, row, idx: int) -> List[Tuple[str, str, str]]:
    entries = []
    try:
        addr = await row.locator("div.item-title").first.inner_text(timeout=IP_ADDR_TIMEOUT * 1000)
        addr = addr.strip()
        if not addr:
            return []
        logger.info(f"正在处理IP [{idx}]: {addr}")
    except Exception:
        return []

    # 点击展开
    btn = row.locator("button:has(i.fa-list)").first
    if await btn.count() > 0:
        await robust_click(btn)
    else:
        await robust_click(row)
    await asyncio.sleep(DELAY_AFTER_CLICK)

    if not await wait_for_element(page, ".modal-dialog", MODAL_WAIT_TIMEOUT):
        return []

    items = page.locator(".modal-dialog .item-content")
    total = await items.count()
    if MAX_CHANNELS_PER_IP > 0:
        total = min(total, MAX_CHANNELS_PER_IP)

    for i in range(total):
        try:
            name = await items.nth(i).locator("div.item-title").inner_text(timeout=CHANNEL_NAME_TIMEOUT * 1000)
            url = await items.nth(i).locator("div.item-subtitle").inner_text(timeout=CHANNEL_URL_TIMEOUT * 1000)
            name = name.strip()
            url = url.strip()
            if not name or not url:
                continue
            if not url.startswith(("http://", "https://", "rtsp://", "rtmp://")):
                url = f"{DEFAULT_PROTOCOL}{url}"

            # 名称标准化 + 白名单过滤
            clean_name = clean_satellite_name(name)
            clean_name = clean_chinese_only(clean_name)
            std_name = normalize_cctv(clean_name)
            group = classify_channel(std_name)
            if group:
                entries.append((group, std_name, url))
        except Exception:
            continue
    return entries

async def wait_data_ready(page) -> bool:
    logger.info("等待频道数据加载...")
    for _ in range(int(DATA_LOAD_TIMEOUT / DATA_CHECK_INTERVAL)):
        flag = await page.evaluate('''() => {
            const items = document.querySelectorAll('div.ios-list-item div.item-subtitle');
            for(let el of items) {
                if(el.textContent.includes('频道:')) return true;
            }
            return false;
        }''')
        if flag:
            return True
        await asyncio.sleep(DATA_CHECK_INTERVAL)
    return False

# ============================================================================
# ========================= 去重处理 ====================================
# ============================================================================
def deduplicate_urls_per_channel(channel_map: Dict[Tuple[str, str], List[str]]) -> Dict[Tuple[str, str], List[str]]:
    url_map = defaultdict(list)
    for key, urls in channel_map.items():
        for u in urls:
            url_map[u].append(key)

    keep_map = {}
    for url, keys in url_map.items():
        if not keys:
            continue
        # 优先保留第一个匹配的频道
        target_key = keys[0]
        if url not in keep_map.setdefault(target_key, []):
            keep_map[target_key].append(url)

    final_map = defaultdict(list)
    for k, v in keep_map.items():
        final_map[k] = v
    return dict(final_map)

# ============================================================================
# ========================= 结果输出 ====================================
# ============================================================================
def export_results_with_timestamp(channel_map: Dict[Tuple[str, str], List[str]]):
    now = (datetime.datetime.utcnow() + datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")
    group_dict = defaultdict(list)
    for (group, name), urls in channel_map.items():
        for u in urls:
            group_dict[group].append((name, u))

    # 输出 M3U
    with open(OUTPUT_M3U_FILENAME, "w", encoding="utf-8") as f:
        f.write("#EXTM3U\n")
        if TIME_DISPLAY_AT_TOP:
            f.write(f'#EXTINF:-1,tvg-group="更新时间",更新时间:{now}\n{UPDATE_STREAM_URL}\n\n')
        for group in GROUP_ORDER:
            if group not in group_dict:
                continue
            for name, url in group_dict[group]:
                f.write(f'#EXTINF:-1,tvg-group="{group}",{name}\n{url}\n')
            f.write("\n")
    logger.info(f"M3U 文件已生成：{OUTPUT_M3U_FILENAME}")

    # 输出 TXT
    with open(OUTPUT_TXT_FILENAME, "w", encoding="utf-8") as f:
        if TIME_DISPLAY_AT_TOP:
            f.write(f"更新时间:{now},{UPDATE_STREAM_URL}\n\n")
        for group in GROUP_ORDER:
            if group not in group_dict:
                continue
            f.write(f"# {group}\n")
            for name, url in group_dict[group]:
                f.write(f"{name},{url}\n")
            f.write("\n")
    logger.info(f"TXT 文件已生成：{OUTPUT_TXT_FILENAME}")
    logger.info(f"共输出 {len(channel_map)} 个有效频道")

# ============================================================================
# ========================= 主逻辑入口（核心改造：无本地文件则全新采集）======
# ============================================================================
async def main():
    total_start = time.time()
    cache = load_cache() if ENABLE_CACHE else {}
    ffmpeg_sem = asyncio.Semaphore(FFMPEG_CONCURRENCY)

    logger.info("=" * 50)
    logger.info("📌 步骤1：检测本地历史 M3U 文件")
    logger.info("=" * 50)

    # 核心判断：本地文件不存在则清空历史，直接全新采集
    if OUTPUT_M3U_FILENAME.exists():
        local_channel_map = parse_existing_m3u(OUTPUT_M3U_FILENAME)
    else:
        logger.info("未检测到本地 iptv_channels.m3u，跳过本地历史，开始全新采集")
        local_channel_map = {}

    all_qualified = defaultdict(list)

    # 有本地文件：先测速本地源
    if local_channel_map:
        logger.info("=" * 50)
        logger.info("📌 步骤2：测速本地历史频道（仅白名单频道）")
        logger.info("=" * 50)
        task_list = []
        key_list = list(local_channel_map.keys())
        test_total = 0

        for key in key_list:
            urls = local_channel_map[key]
            for u in urls:
                ci = cache.get(u)
                if not (ci and ci.get("ok") and is_cache_valid(ci.get("timestamp", 0))):
                    test_total += 1

        progress = ProgressBar(test_total, title="本地源测速")
        logger.info(f"待测速链接总数：{test_total}")

        for k in key_list:
            task_list.append(smart_test_channel_urls(k, local_channel_map[k], cache, ffmpeg_sem, progress))

        task_results = await asyncio.gather(*task_list)
        for idx, (qualified, _, new_cache) in enumerate(task_results):
            all_qualified[key_list[idx]] = qualified
            cache.update(new_cache)

        need_count = 0
        ok_count = 0
        for qs in all_qualified.values():
            ok_count += len(qs)
            need_count += max(0, MAX_LINKS_PER_CHANNEL - len(qs))
        logger.info(f"本地源测速完成：可用链接 {ok_count} 条，还需补充 {need_count} 条")

        if need_count <= 0:
            logger.info("🎉 本地源已满足数量要求，直接输出结果")
            save_cache(cache)
            final_out = {k: [u for u, _ in v] for k, v in all_qualified.items()}
            export_results_with_timestamp(final_out)
            await close_global_session()
            logger.info(f"脚本总耗时：{time.time() - total_start:.2f} 秒")
            return
    else:
        logger.info("无本地历史频道，直接进入全网采集流程")

    # 步骤3：采集 GitHub + 网页源
    logger.info("=" * 50)
    logger.info("📌 步骤3：采集全网源（GitHub + 网页爬取，仅保留白名单频道）")
    logger.info("=" * 50)
    supplement_entries = []

    # 拉取 GitHub 源
    if ENABLE_GITHUB_SOURCES:
        gh_tasks = [download_github_m3u(link) for link in GITHUB_M3U_LINKS]
        gh_results = await asyncio.gather(*gh_tasks, return_exceptions=True)
        for content in gh_results:
            if isinstance(content, str) and content:
                if content.startswith("#EXTM3U"):
                    supplement_entries.extend(parse_m3u_file(content))
                else:
                    supplement_entries.extend(parse_txt_content(content))

    # 网页爬取
    if ENABLE_WEB_CRAWL:
        logger.info("开始网页爬取...")
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=HEADLESS, args=["--no-sandbox", "--disable-gpu"])
            ctx = await browser.new_context(viewport={"width": 1920, "height": 1080})
            page = await ctx.new_page()
            try:
                await page.goto(TARGET_URL, timeout=PAGE_LOAD_TIMEOUT * 1000)
                eng_sel = build_selector(PAGE_CONFIG["engine_search"], "a,button")
                if await page.locator(eng_sel).count() > 0:
                    await robust_click(page.locator(eng_sel).first)

                mode_key = "hotel" if EXTRACT_MODE == "酒店提取" else "multicast"
                tab_sel = build_selector(PAGE_CONFIG[mode_key], "div")
                await robust_click(page.locator(tab_sel).first)

                start_sel = build_selector(PAGE_CONFIG["start_button"])
                await robust_click(page.locator(start_sel).first)
                await asyncio.sleep(AFTER_START_WAIT)

                if await wait_data_ready(page):
                    row_locator = page.locator("div.ios-list-item").filter(has=page.locator("div.item-subtitle:has-text('频道:')"))
                    row_cnt = min(await row_locator.count(), MAX_IPS)
                    for i in range(row_cnt):
                        row = row_locator.nth(i)
                        supplement_entries.extend(await extract_one_ip(page, row, i + 1))
                        await asyncio.sleep(DELAY_BETWEEN_IPS)
            except Exception as e:
                logger.error(f"网页爬取异常：{str(e)}")
            finally:
                await browser.close()
    else:
        logger.info("网页爬取已关闭，仅使用 GitHub 外部源")

    # 过滤：咪咕、内网IP
    supplement_map = defaultdict(list)
    for group, name, url in supplement_entries:
        if ENABLE_MIGU_FILTER and "migu" in url.lower():
            continue
        if SKIP_INTERNAL_IP and is_internal_ip(url):
            continue
        supplement_map[(group, name)].append(url)

    # URL去重
    supplement_map = deduplicate_urls_per_channel(supplement_map)
    logger.info(f"采集+去重完成，共获取 {len(supplement_map)} 个白名单频道")

    # 连通性粗筛（剔除完全打不开的链接）
    logger.info("=" * 50)
    logger.info("📌 步骤4：连通性粗筛（剔除死链）")
    logger.info("=" * 50)
    filtered_map = defaultdict(list)
    total_before = 0
    total_after = 0
    for key, urls in supplement_map.items():
        total_before += len(urls)
        if urls:
            online_urls = await filter_online_urls(urls, CONNECTIVITY_TIMEOUT, CONNECTIVITY_CONCURRENCY)
            filtered_map[key] = online_urls
            total_after += len(online_urls)
    supplement_map = filtered_map
    logger.info(f"粗筛结果：{total_before} → {total_after} 条有效链接，剔除 {total_before - total_after} 条死链")

    # 补充测速
    logger.info("=" * 50)
    logger.info("📌 步骤5：对白名单频道执行稳定性测速")
    logger.info("=" * 50)
    test_tasks = []
    test_keys = []
    supp_test_total = 0

    # 统计待测速数量
    for key, urls in supplement_map.items():
        for u in urls:
            ci = cache.get(u)
            if not (ci and ci.get("ok") and is_cache_valid(ci.get("timestamp", 0))):
                supp_test_total += 1

    supp_progress = ProgressBar(supp_test_total, title="全网源测速")
    logger.info(f"待测速链接总数：{supp_test_total}")

    # 生成测速任务
    for key in supplement_map:
        test_keys.append(key)
        test_tasks.append(smart_test_channel_urls(key, supplement_map[key], cache, ffmpeg_sem, supp_progress))

    if test_tasks:
        supp_results = await asyncio.gather(*test_tasks)
        for idx, (qualified, _, new_cache) in enumerate(supp_results):
            k = test_keys[idx]
            all_qualified[k].extend(qualified)
            cache.update(new_cache)

    # 最终整理：按分数排序、截取最大数量
    final_output_map = {}
    for key, url_score_list in all_qualified.items():
        # 分数降序，优先高分稳定源
        url_score_list.sort(key=lambda x: x[1], reverse=True)
        final_urls = [u for u, _ in url_score_list[:MAX_LINKS_PER_CHANNEL]]
        if final_urls:
            final_output_map[key] = final_urls

    # 保存缓存 & 输出文件
    save_cache(cache)
    export_results_with_timestamp(final_output_map)

    await close_global_session()
    logger.info(f"✅ 全部任务完成！总耗时：{time.time() - total_start:.2f} 秒")

if __name__ == "__main__":
    # 兼容 Windows / Linux 事件循环
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    else:
        asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())
    asyncio.run(main())
