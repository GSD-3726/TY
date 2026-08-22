import asyncio
import json
import logging
import random
import re
import sys
import time
import argparse
import datetime
import os
from collections import defaultdict
from pathlib import Path
from urllib.parse import urlparse, quote
from typing import Dict, List, Tuple, Optional, Any
import aiohttp
from playwright.async_api import async_playwright

# ============================================================================
# 目标站抓取配置
# ============================================================================
TARGET_URL = "https://iptv.cqshushu.com/index.php"  # 抓取源地址
DEFAULT_PROTOCOL = "http://"                        # 补全URL缺失的协议头
SCRAPE_SOURCE_FILTER = "hotel"                      # 默认抓取类型：all/hotel/multicast/migu/other
ENABLE_SCRAPE = False                                # 是否启用目标站抓取（可与 --skip-scrape 配合）
MAX_IPS = 0                                         # 最大处理IP数量，0表示无限制
MAX_PAGES = 10                                      # IP列表最大翻页数
IPS_PER_PAGE = 10                                   # 每页IP数量（页面实际可能不同）
PAGE_DELAY_MIN = 5.0                                # IP列表页翻页最小延迟（秒）
PAGE_DELAY_MAX = 8.0                                # IP列表页翻页最大延迟（秒）
IP_DELAY_MIN = 2.0                                  # 不同IP之间的最小延迟（秒）
IP_DELAY_MAX = 4.0                                  # 不同IP之间的最大延迟（秒）
MAX_DETAIL_PAGES = 40                               # 每个IP详情页最大翻页数
DETAIL_PAGE_TIMEOUT = 30000                         # 详情页加载超时（毫秒）
DETAIL_IDLE_TIMEOUT = 5000                          # 详情页空闲超时（毫秒）
DETAIL_MAX_SECONDS = 60                             # 单个详情页采集最大时长（秒）
DETAIL_PAGE_DELAY_MIN = 1.0                         # 详情页翻页最小延迟（秒）
DETAIL_PAGE_DELAY_MAX = 2.0                         # 详情页翻页最大延迟（秒）
DETAIL_WAIT_MIN = 2.0                               # 详情页加载后最小等待（秒）
DETAIL_WAIT_MAX = 4.0                               # 详情页加载后最大等待（秒）
HEADLESS = True                                     # 是否使用无头模式
CHROME_PATH = ""                                    # Chrome/Chromium 可执行文件路径，留空自动查找
PAGE_TIMEOUT = 60000                                # 页面加载超时（毫秒）
IDLE_TIMEOUT = 15000                                # 页面空闲超时（毫秒）

# ============================================================================
# 测速筛选配置（快速连通性预检 + 稳定长测）
# ============================================================================
ENABLE_FFMPEG = True                                # 是否启用FFmpeg测速
FFMPEG_PATH = "ffmpeg"                              # FFmpeg可执行文件路径

# 快速连通性预检（仅检测HTTP状态）
CONN_TIMEOUT = 2.0                                  # 连接超时（秒）
CONN_CONCURRENCY = 80                               # 并发连接数

# 稳定长测参数（播放10秒，严格筛选）
STABLE_FFMPEG_DURATION = 10                         # 测试时长（秒）
STABLE_PROC_TIMEOUT = 13                            # 进程超时（秒）
STABLE_FFMPEG_CONCURRENCY = 16                      # 并发数，适配低性能环境
MIN_AVG_FPS = 15                                    # 最低平均帧率
MIN_FRAMES = 150                                    # 最少帧数
MIN_REALTIME_FACTOR = 0.65                          # 实时性因子最小值
MIN_NET_FEED_RATIO = 0.70                           # 网络喂入比率最小值
MIN_LATE_SPEED = 0.70                               # 后半段平均速度最小值（倍速）
MIN_SPEED_MIN = 0.50                                # 全程最低速度最小值（倍速）
MIN_AVG_SPEED = 0.75                                # 平均速度最小值（倍速）
MAX_FPS_JITTER = 0.4                                # 帧率抖动最大值（归一化）
FFMPEG_RETRIES = 1                                  # 重试次数（目前未使用）

# ============================================================================
# 缓存配置
# ============================================================================
ENABLE_CACHE = True
CACHE_FILE = Path(__file__).parent / "iptv_speed_cache.json"
CACHE_EXPIRE_HOURS = 72
CACHE_EXPIRE_SEC = CACHE_EXPIRE_HOURS * 3600

# ============================================================================
# GitHub源配置
# ============================================================================
ENABLE_GITHUB = True
GITHUB_URLS = [
    "https://gh-proxy.com/https://raw.githubusercontent.com/vbskycn/iptv/master/tv/iptv4.txt",
    "https://live.zbds.top/tv/iptv4.txt",
    "https://gitee.com/OscarWilde/itv/raw/master/tv.txt",
    "https://raw.githubusercontent.com/suxuang/myIPTV/main/ipv4.m3u",
    "https://gh.927223.xyz/https://raw.githubusercontent.com/Kimentanm/aptv/master/m3u/iptv.m3u",
    "https://gh-proxy.com/raw.githubusercontent.com/yoursmile66/TVBox/main/live.txt",
    "https://gh.927223.xyz/https://raw.githubusercontent.com/develop202/migu_video/refs/heads/main/interface.txt",
    "http://www.52top.com.cn:678/downloads/migu.txt",
    "http://cyh92.cn/list.m3u",
    "https://4kkj.cn/uploads/ok专线接口/[直播]/央视.txt",
    "http://rihou.cc:567/gggg.nzk",
    "http://wangziduoqing.com/yuan/zb.txt",
    "https://gitee.com/main-stream/tv/raw/master/BOSS.json",
    "http://183.131.83.37:40782/移动魔百和直播源_2025-04-08_湖南_未知_未知.txt",
    "https://cnb.cool/ms511/PG/-/git/raw/main/sub/live.txt",
]
MAX_TEST_URLS_PER_CHANNEL = 8                       # 每个频道最多测试的链接数
MAX_LINKS_PER_CHANNEL = 8                           # 每个频道最终保留的最大有效链接数（与测试数一致，可独立调整）
GITHUB_TIMEOUT = 30
GITHUB_RETRIES = 3

# ============================================================================
# 输出配置
# ============================================================================
OUTPUT_DIR = Path(__file__).parent
OUTPUT_M3U = OUTPUT_DIR / "iptv_channels.m3u"
OUTPUT_TXT = OUTPUT_DIR / "iptv_channels.txt"

# ============================================================================
# 频道分类规则
# ============================================================================
CATEGORY_RULES = [
    {"name": "央视频道", "keywords": ["cctv", "cetv", "央视"]},
    {"name": "卫视频道", "keywords": ["卫视"]},
    {"name": "影视频道", "keywords": ["影视", "影院", "chc", "电影", "经典影"]},
    {"name": "体育频道", "keywords": ["体育", "赛事", "高尔夫", "劲爆"]},
    {"name": "纪实频道", "keywords": ["纪实", "探索", "记录", "人文", "自然"]},
]
GROUP_ORDER = ["央视频道", "卫视频道", "影视频道", "体育频道"]

# ============================================================================
# 辅助正则与映射（无需修改）
# ============================================================================
CCTV_MAP = {
    "1": "综合", "2": "财经", "3": "综艺", "4": "国际", "5": "体育",
    "5+": "体育赛事", "6": "电影", "7": "军事农业", "8": "电视剧",
    "9": "纪录", "10": "科教", "11": "戏曲", "12": "社会与法",
    "13": "新闻", "14": "少儿", "15": "音乐", "16": "奥林匹克", "17": "农业农村",
}
CCTV_ORDER = [f"CCTV-{k}{v}" for k, v in CCTV_MAP.items() if k != "5+"]
CCTV_ORDER.insert(5, "CCTV-5+体育赛事")
CCTV_ORDER.append("CCTV-4K")
CCTV_RE = re.compile(r'(cctv)[-\s]?(5\+|\d{1,3})', re.IGNORECASE)
CHINESE_ONLY = re.compile(r'[^\u4e00-\u9fff]')
INTERNAL_IP = re.compile(r'^(192\.168\.|10\.|172\.(1[6-9]|2[0-9]|3[0-1])\.|127\.0\.0\.1)')
CLEAR_SUFFIX_RE = re.compile(r'[\s\-_]*(高清|标清|4K|超清|蓝光|HD|FHD|UHD|2K|流畅|原画|精品|720P|1080P|2160P)', re.IGNORECASE)

# 反检测脚本
STEALTH_JS = """ // Step 1: Delete webdriver getter from prototype, redefine as data property // paer.js checks Object.getOwnPropertyDescriptor(Navigator.prototype, 'webdriver') // If there's a getter -> flags webdriver_spoof. Data property (no getter) passes. delete Navigator.prototype.webdriver; Object.defineProperty(Navigator.prototype, 'webdriver', { value: undefined, writable: false, configurable: true }); // Step 2: Real chrome.runtime (paer.js checks chrome_runtime_missing) if (!window.chrome) window.chrome = {}; window.chrome.runtime = { connect: function() { return { onMessage: {addListener:function(){}}, postMessage:function(){}, onDisconnect: {addListener:function(){}} }; }, sendMessage: function() {}, onConnect: {addListener:function(){}, removeListener:function(){}, hasListener:function(){return false;}}, onMessage: {addListener:function(){}, removeListener:function(){}, hasListener:function(){return false;}}, getURL: function(p) { return 'chrome-extension://invalid/'+p; }, id: undefined }; // Step 3: Clean automation traces for (let k in window) { if (k.startsWith('cdc_') || k.startsWith('__webdriver') || k.startsWith('__driver') || k.startsWith('__selenium')) delete window[k]; } // Step 4: Permissions const origQuery = window.navigator.permissions.query; window.navigator.permissions.query = (p) => p.name==='notifications' ? Promise.resolve({state:Notification.permission}) : origQuery(p); """

# FFmpeg 输出解析正则
RE_FRAME = re.compile(r'frame=\s*(\d+)')
RE_SPEED = re.compile(r'speed=\s*([\d.]+)x')
RE_VIDEO_RES = re.compile(r'Video:.*?(\d{3,})x(\d{3,})', re.IGNORECASE)
RE_TIME = re.compile(r'time=(\d+):(\d+):([\d.]+)')
RE_ERROR = re.compile(
    r'(overrun|corrupt|missing|error while decoding|Invalid data found|PES packet size mismatch'
    r'|non-existing|timestamp|Connection reset|Broken pipe|Server returned 40[34]|404 Not Found'
    r'|Connection timed out|HTTP error|timeout|Input/output error|Connection refused'
    r'|buffer underflow|End of file|could not find codec parameters|Unable to open URL)',
    re.IGNORECASE
)

# ============================================================================
# 日志配置（实时刷新）
# ============================================================================
class BJFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.datetime.fromtimestamp(
            record.created,
            datetime.timezone(datetime.timedelta(hours=8))
        )
        return dt.strftime("%Y-%m-%d %H:%M:%S")

class FlushStreamHandler(logging.StreamHandler):
    def emit(self, record):
        super().emit(record)
        self.flush()

logger = logging.getLogger('IPTV')
logger.setLevel(logging.INFO)
logger.handlers.clear()
_h = FlushStreamHandler(sys.stdout)
_h.setFormatter(BJFormatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(_h)

# ============================================================================
# 辅助函数（分类、去重、规范化等）
# ============================================================================
def build_classifier():
    compiled = []
    for rule in CATEGORY_RULES:
        pat = re.compile("|".join(re.escape(k.lower()) for k in rule["keywords"]))
        compiled.append((rule["name"], pat))
    return lambda name: next((g for g, p in compiled if p.search(name.lower())), None)

classify = build_classifier()

def norm_cctv(name: str) -> str:
    low = name.lower()
    if re.search(r'cctv[-\s]?4k', low):
        return "CCTV-4K"
    if re.search(r'cctv[-\s]?5\+', low):
        return "CCTV-5+体育赛事"
    m = CCTV_RE.search(low)
    if m:
        num = m.group(2)
        if num in CCTV_MAP:
            return f"CCTV-{num}{CCTV_MAP[num]}"
        return f"CCTV-{num}"
    return name

def unify_channel_name(raw_name: str) -> str:
    std_name = norm_cctv(raw_name)
    std_name = CLEAR_SUFFIX_RE.sub("", std_name)
    std_name = re.sub(r'[\s\-_]+$', "", std_name).strip()
    return std_name

def clean_cn(name: str) -> str:
    return CHINESE_ONLY.sub('', name)

def is_internal(url: str) -> bool:
    try:
        host = urlparse(url).hostname
        return bool(host and INTERNAL_IP.match(host))
    except:
        return False

def norm_type(t: str) -> str:
    m = {
        "all": "all", "全部": "all",
        "hotel": "hotel", "酒店": "hotel",
        "multicast": "multicast", "组播": "multicast",
        "migu": "migu", "咪咕": "migu",
        "other": "other", "其他": "other",
    }
    return m.get(t.strip().lower(), "all")

def progress_bar(cur: int, total: int, ok: int, fail: int, last_pct: int) -> int:
    if total == 0:
        return 0
    pct = int(cur / total * 100)
    if pct == last_pct and cur != total:
        return last_pct
    bar = '█' * (pct // 5) + '-' * (20 - pct // 5)
    logger.info(f"({pct}%) {bar} ({cur}/{total}) 成功{ok} 失败{fail}")
    sys.stdout.flush()
    return pct

# ============================================================================
# 模拟人类行为（反检测）
# ============================================================================
async def human_scroll(page):
    d = random.randint(150, 400)
    for _ in range(random.randint(3, 6)):
        await page.evaluate(f'window.scrollBy(0, {d // 3})')
        await asyncio.sleep(random.uniform(0.05, 0.15))
    await asyncio.sleep(random.uniform(0.3, 0.8))

async def random_mouse(page):
    await page.mouse.move(random.randint(100, 800), random.randint(100, 600))
    await asyncio.sleep(random.uniform(0.1, 0.3))

# ============================================================================
# 缓存读写
# ============================================================================
def load_cache() -> dict:
    if not ENABLE_CACHE or not CACHE_FILE.exists():
        return {}
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            cache = json.load(f)
        now = time.time()
        valid = {}
        for url, data in cache.items():
            if isinstance(data, dict) and "ok" in data and "ts" in data:
                if now - data["ts"] < CACHE_EXPIRE_SEC:
                    valid[url] = data
        logger.info(f"缓存加载: {len(valid)} 条有效")
        return valid
    except Exception as e:
        logger.debug(f"缓存加载异常: {e}")
        return {}

def save_cache(cache: dict):
    if not ENABLE_CACHE:
        return
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning(f"保存缓存失败: {e}")

# ============================================================================
# 测速相关函数（快速连通性 + 稳定长测）
# ============================================================================
def parse_ffmpeg_time(time_str_h: str, time_str_m: str, time_str_s: str) -> float:
    try:
        return int(time_str_h) * 3600 + int(time_str_m) * 60 + float(time_str_s)
    except (ValueError, TypeError):
        return 0.0

async def quick_connectivity_test(urls: List[str]) -> List[str]:
    """快速连通预检：仅检测HTTP状态，不下载body"""
    semaphore = asyncio.Semaphore(CONN_CONCURRENCY)
    
    async def _check_one(url: str):
        async with semaphore:
            try:
                timeout = aiohttp.ClientTimeout(total=CONN_TIMEOUT, connect=1.5)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.get(url, headers={
                        "User-Agent": "Mozilla/5.0",
                        "Referer": "https://www.miguvideo.com/"
                    }) as resp:
                        if resp.status in (200, 206, 301, 302):
                            return url
            except Exception:
                pass
            return None
    
    tasks = [asyncio.ensure_future(_check_one(u)) for u in urls]
    alive = []
    for coro in asyncio.as_completed(tasks):
        result = await coro
        if result:
            alive.append(result)
    
    logger.info(f"快速连通预检: {len(urls)} -> {len(alive)} 存活")
    return alive

async def stable_ffmpeg_test(url: str) -> Dict[str, Any]:
    """
    稳定长测：播放10秒，检测帧率、速度、实时性等指标
    采用严格的判定标准，筛除卡顿、低画质或失效源
    """
    headers = (
        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64)\r\n"
        "Referer: https://www.miguvideo.com/\r\n"
    )
    
    cmd = [
        FFMPEG_PATH, "-hide_banner", "-y",
        "-headers", headers,
        "-fflags", "+genpts+nobuffer+discardcorrupt+ignidx",
        "-flags", "low_delay",
        "-max_delay", "1000000",
        "-analyzeduration", "800000",
        "-probesize", "800000",
        "-rw_timeout", "5000000",
        "-err_detect", "bitstream",
        "-i", url,
        "-t", str(STABLE_FFMPEG_DURATION),
        "-f", "null", "-"
    ]
    
    start_time = time.perf_counter()
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE
        )

        all_speeds = []
        all_frames = []
        time_points = []
        has_errors = False
        low_speed_duration = 0.0
        last_check_time = start_time
        w, h = 0, 0
        resolution_parsed = False

        try:
            async for raw_line in proc.stderr:
                line = raw_line.decode('utf-8', errors='ignore').strip()
                elapsed = time.perf_counter() - start_time

                if RE_ERROR.search(line):
                    has_errors = True
                    proc.kill()
                    break

                if not resolution_parsed:
                    res_match = RE_VIDEO_RES.search(line)
                    if res_match:
                        w, h = int(res_match.group(1)), int(res_match.group(2))
                        resolution_parsed = True

                speed_match = RE_SPEED.search(line)
                if speed_match:
                    speed = float(speed_match.group(1))
                    all_speeds.append(speed)
                    if elapsed > 3.0:
                        if speed < MIN_SPEED_MIN:
                            low_speed_duration += elapsed - last_check_time
                            if low_speed_duration >= 2.0:
                                proc.kill()
                                break
                        else:
                            low_speed_duration = 0.0
                    last_check_time = time.perf_counter()

                frame_match = RE_FRAME.search(line)
                if frame_match:
                    all_frames.append(int(frame_match.group(1)))

                time_match = RE_TIME.search(line)
                if time_match:
                    play_sec = parse_ffmpeg_time(*time_match.groups())
                    time_points.append((elapsed, play_sec))

                if elapsed >= STABLE_PROC_TIMEOUT:
                    proc.kill()
                    break

        except Exception as e:
            logger.debug(f"读取输出异常 {url[:60]}: {e}")
            try: proc.kill()
            except: pass

        try:
            await asyncio.wait_for(proc.wait(), timeout=2.0)
        except asyncio.TimeoutError:
            try: proc.kill()
            except: pass

        elapsed_total = time.perf_counter() - start_time

        frames = all_frames[-1] if all_frames else 0
        avg_speed = sum(all_speeds[-3:]) / len(all_speeds[-3:]) if all_speeds else 0.0

        if len(all_speeds) >= 5:
            split_idx = int(len(all_speeds) * 0.4)
            late_speeds = all_speeds[split_idx:]
            late_avg_speed = sum(late_speeds) / len(late_speeds)
            min_speed = min(all_speeds)
        else:
            late_avg_speed = avg_speed
            min_speed = avg_speed if all_speeds else 0.0

        actual_play_time = 0.0
        if len(time_points) >= 2:
            actual_play_time = max(time_points[-1][1] - time_points[0][1], 0.1)
        else:
            actual_play_time = STABLE_FFMPEG_DURATION if frames > MIN_FRAMES else 0.0

        effective_test_time = max(elapsed_total - 1.0, 0.1)
        net_feed_ratio = actual_play_time / effective_test_time if effective_test_time > 0 else 0.0

        actual_fps = frames / actual_play_time if actual_play_time > 0 else 0.0
        fps_jitter = 0.0
        if len(all_frames) >= 4:
            frame_deltas = [all_frames[i] - all_frames[i-1] for i in range(1, len(all_frames))]
            if frame_deltas and max(frame_deltas) > 0:
                fps_jitter = (max(frame_deltas) - min(frame_deltas)) / max(frame_deltas)

        realtime_factor = actual_fps * avg_speed / 25.0 if avg_speed > 0 else 0.0

        # 综合判定 —— 使用配置变量 MIN_AVG_SPEED 替代硬编码 0.75
        is_ok = (
            frames >= MIN_FRAMES
            and actual_fps >= MIN_AVG_FPS
            and avg_speed >= MIN_AVG_SPEED
            and late_avg_speed >= MIN_LATE_SPEED
            and min_speed >= MIN_SPEED_MIN
            and net_feed_ratio >= MIN_NET_FEED_RATIO
            and elapsed_total < STABLE_FFMPEG_DURATION * 2.5
            and not has_errors
            and fps_jitter <= MAX_FPS_JITTER
            and realtime_factor >= MIN_REALTIME_FACTOR
        )

        return {
            "ok": is_ok,
            "fps": round(actual_fps, 2),
            "frames": frames,
            "width": w,
            "height": h,
            "speed": round(avg_speed, 2),
            "late_speed": round(late_avg_speed, 2),
            "elapsed": round(elapsed_total, 2),
            "realtime": round(realtime_factor, 3),
            "has_errors": has_errors,
            "fps_jitter": round(fps_jitter, 3)
        }

    except Exception as e:
        logger.debug(f"稳定测试异常 {url[:60]}: {e}")
        return {
            "ok": False, "fps": 0, "frames": 0, "width": 0, "height": 0,
            "speed": 0, "elapsed": 0, "realtime": 0, "has_errors": True
        }

def stream_quality_score(item: tuple) -> float:
    """综合评分（帧率30% + 速度35% + 耗时10% + 分辨率25%）"""
    _url, fps, w, _h, speed, elapsed = item
    fps_score = min(fps / 25.0, 1.0) if fps > 0 else 0.0
    speed_score = min(speed, 1.5) / 1.5 if speed > 0 else 0.0
    time_score = max(0.0, 1.0 - (max(elapsed - 3.0, 0) / 5.0))
    
    if w >= 1920:
        res_score = 1.0
    elif w >= 1280:
        res_score = 0.7
    elif w >= 720:
        res_score = 0.4
    else:
        res_score = 0.1
    
    return fps_score * 0.30 + speed_score * 0.35 + time_score * 0.10 + res_score * 0.25

def _finalize_result(result_map):
    final = {}
    for k, vs in result_map.items():
        vs.sort(key=stream_quality_score, reverse=True)
        final[k] = [u for u, _, _, _, _, _ in vs[:MAX_LINKS_PER_CHANNEL]]  # 使用正确变量
    return final

# ============================================================================
# 批量测试流水线
# ============================================================================
async def batch_test_pipeline(channel_map: Dict[Tuple[str, str], List[str]]
) -> Dict[Tuple[str, str], List[str]]:
    """对每个频道的前N个链接进行稳定长测，筛选有效链接"""
    if not channel_map:
        return {}

    if MAX_TEST_URLS_PER_CHANNEL > 0:
        channel_map = {k: v[:MAX_TEST_URLS_PER_CHANNEL] for k, v in channel_map.items()}
        capped_total = sum(len(v) for v in channel_map.values())
        logger.info(f"按配置截取: 每频道最多{MAX_TEST_URLS_PER_CHANNEL}个, 共{capped_total}个链接待测")
    
    cache = load_cache() if ENABLE_CACHE else {}
    new_cache = {}
    result_map = defaultdict(list)
    
    all_urls = []
    url_to_channel = {}
    cached_ok = 0
    
    for (g, n), urls in channel_map.items():
        for u in urls:
            if is_internal(u):
                continue
            ci = cache.get(u)
            if ci and isinstance(ci, dict) and "ok" in ci:
                if time.time() - ci.get("ts", 0) < CACHE_EXPIRE_SEC:
                    if ci["ok"]:
                        result_map[(g, n)].append((
                            u, ci.get("fps", 0), ci.get("w", 0),
                            ci.get("h", 0), ci.get("speed", 0), ci.get("elapsed", 0)
                        ))
                        cached_ok += 1
                    continue
            all_urls.append(u)
            url_to_channel[u] = (g, n)
    
    if not all_urls:
        logger.info(f"全部命中缓存: {cached_ok} 条")
        return _finalize_result(result_map)
    
    logger.info(f"=== 快速连通预检 ({len(all_urls)} 条) ===")
    alive_urls = await quick_connectivity_test(all_urls)
    dead_urls = set(all_urls) - set(alive_urls)
    for u in dead_urls:
        new_cache[u] = {"ok": False, "ts": time.time()}
    
    fast_passed = [(url, None) for url in alive_urls]
    logger.info(f"跳过快速首帧测试，直接进入稳定长测（共 {len(fast_passed)} 条）")
    
    logger.info(f"=== 稳定测试：10秒长测 ({len(fast_passed)} 条, 并发:{STABLE_FFMPEG_CONCURRENCY}) ===")
    stable_sem = asyncio.Semaphore(STABLE_FFMPEG_CONCURRENCY)
    
    async def _stable_test(url: str, fast_res: dict):
        async with stable_sem:
            res = await stable_ffmpeg_test(url)
            return url, fast_res, res
    
    stable_tasks = [asyncio.ensure_future(_stable_test(u, r)) for u, r in fast_passed]
    done = ok = fail = 0
    lp = -1
    
    for coro in asyncio.as_completed(stable_tasks):
        url, fast_res, res = await coro
        done += 1
        g, n = url_to_channel[url]
        
        if res["ok"]:
            ok += 1
            result_map[(g, n)].append((
                url, res["fps"], res["width"], res["height"],
                res["speed"], res["elapsed"]
            ))
        else:
            fail += 1
        
        new_cache[url] = {
            "ok": res["ok"], "fps": res["fps"], "frames": res["frames"],
            "w": res["width"], "h": res["height"], "speed": res["speed"],
            "elapsed": res["elapsed"], "ts": time.time()
        }
        lp = progress_bar(done, len(stable_tasks), ok, fail, lp)
    
    if ENABLE_CACHE and new_cache:
        cache.update(new_cache)
        save_cache(cache)
    
    logger.info(f"稳定测试完成: 通过 {ok}, 失败 {fail}")
    return _finalize_result(result_map)

# ============================================================================
# GitHub源下载与解析
# ============================================================================
async def download_github(url: str, session: aiohttp.ClientSession) -> str:
    req_url = quote(url, safe=":/?&=%#[]@!$'()*+,;-._~") if any(ord(c) > 127 for c in url) else url
    for attempt in range(1, GITHUB_RETRIES + 1):
        try:
            async with session.get(req_url, headers={'User-Agent': 'Mozilla/5.0'}) as r:
                if r.status == 200:
                    text = await r.text()
                    if not text or len(text.strip()) < 50:
                        logger.warning(f"GitHub内容过短({len(text)}字符): {url[:80]}")
                        continue
                    head = text[:500].lower()
                    if '<!doctype' in head or '<html' in head or '<head>' in head:
                        logger.warning(f"GitHub返回的是HTML页面: {url[:80]}")
                        continue
                    return text
                else:
                    logger.warning(f"GitHub状态码{r.status}: {url[:80]}")
        except Exception as e:
            logger.warning(f"GitHub下载失败({attempt}/{GITHUB_RETRIES}): {url[:80]} {type(e).__name__}")
            await asyncio.sleep(1)
    return ""

def parse_m3u_content(content: str) -> List[Tuple[str, str, str]]:
    channels = []
    name = ""
    for line in content.splitlines():
        line = line.strip()
        if line.startswith("#EXTINF"):
            m = re.search(r'group-title="([^"]*)",(.+)', line)
            if m:
                name = m.group(2).strip()
            else:
                m2 = re.search(r'#EXTINF:-1.*?,(.+)', line)
                name = m2.group(1).strip() if m2 else ""
        elif line.startswith("http") and name:
            url = line.strip()
            std_ch = unify_channel_name(name)
            g = classify(std_ch)
            if g:
                fn = std_ch if g == "央视频道" else clean_cn(std_ch)
                channels.append((g, fn, url))
            name = ""
    return channels

def parse_txt_content(content: str) -> List[Tuple[str, str, str]]:
    channels = []
    for line in content.splitlines():
        line = line.strip()
        if not line or line.startswith('#') or line.endswith('#genre#'):
            continue
        if ',' in line:
            parts = line.split(',', 1)
            if len(parts) == 2:
                name = parts[0].strip()
                url = parts[1].strip()
                if '$' in url:
                    url = url.split('$')[0].strip()
                if name and url:
                    std_ch = unify_channel_name(name)
                    g = classify(std_ch)
                    if g:
                        fn = std_ch if g == "央视频道" else clean_cn(std_ch)
                        channels.append((g, fn, url))
    return channels

async def fetch_github_sources() -> Tuple[List[Tuple[str, str, str]], List[set]]:
    if not ENABLE_GITHUB or not GITHUB_URLS:
        return [], []
    all_channels = []
    source_urls_list = []
    timeout = aiohttp.ClientTimeout(total=GITHUB_TIMEOUT)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = [download_github(url, session) for url in GITHUB_URLS]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for i, result in enumerate(results):
            source_name = f"GitHub-{i+1}"
            if isinstance(result, Exception) or not result:
                logger.warning(f"{source_name}: 下载失败")
                source_urls_list.append(set())
                continue
            content = result.strip()
            if content.startswith('#EXTM3U') or '#EXTINF' in content:
                channels = parse_m3u_content(content)
            else:
                channels = parse_txt_content(content)
            logger.debug(f"{source_name}: 解析到 {len(channels)} 个频道")
            url_set = {url for _, _, url in channels}
            source_urls_list.append(url_set)
            all_channels.extend(channels)
    logger.info(f"GitHub 源合计: {len(all_channels)} 条原始链接")
    return all_channels, source_urls_list

# ============================================================================
# 目标站抓取（Playwright）
# ============================================================================
async def scrape_ips_playwright(ctx, filter_type: str, max_pages: int) -> list:
    entries = []
    seen = set()
    target_url = f"{TARGET_URL}?t={filter_type}&province=all&limit={IPS_PER_PAGE}" if filter_type != "all" else f"{TARGET_URL}?province=all&limit={IPS_PER_PAGE}"
    page = None
    filter_applied = False
    for attempt in range(5):
        try:
            if page is None or page.is_closed():
                page = await ctx.new_page()
                await page.add_init_script(STEALTH_JS)
            await page.goto(target_url, timeout=PAGE_TIMEOUT, wait_until="commit")
            await asyncio.sleep(random.uniform(5, 8))
            try:
                await page.wait_for_load_state("networkidle", timeout=IDLE_TIMEOUT)
            except:
                pass
            if filter_type != "all":
                current_filter = await page.evaluate("() => document.querySelector('#typeSelect')?.value")
                if current_filter == filter_type:
                    filter_applied = True
                    break
                else:
                    await asyncio.sleep(random.uniform(2, 4))
            else:
                filter_applied = True
                break
        except Exception as e:
            logger.warning(f"[PW] 目标页加载失败 {attempt+1}/5")
            page = None
            await asyncio.sleep(3)
    if page is None or page.is_closed():
        logger.error("[PW] 无法加载目标页，放弃抓取")
        return entries

    current_page = 1
    while current_page <= max_pages:
        await human_scroll(page)
        await random_mouse(page)
        try:
            page_entries = await page.evaluate(r""" () => { const rows = document.querySelectorAll('table.iptv-table tbody tr'); return Array.from(rows).map(row => { const cells = row.querySelectorAll('td'); if (cells.length < 6) return null; const a = cells[0].querySelector('a'); if (!a) return null; const onclick = a.getAttribute('onclick') || ''; const m = onclick.match(/gotoIP\('([^']+)',\s*'([^']+)'\)/); return { ip: a.innerText.trim(), hash: m ? m[1] : '', type: m ? m[2] : '', channel_count: cells[1].innerText.trim(), type_info: cells[2].innerText.trim(), online_time: cells[3].innerText.trim(), update_time: cells[4].innerText.trim(), status: cells[5].innerText.trim() }; }).filter(x => x && x.ip && x.hash); } """)
        except Exception as e:
            logger.warning(f"[PW] 第{current_page}页数据提取失败: {e}")
            break

        new_count = 0
        for entry in page_entries:
            if filter_type != 'all' and entry['type'] != filter_type:
                continue
            if entry['ip'] in seen:
                continue
            if '失效' in entry['status']:
                continue
            seen.add(entry['ip'])
            entries.append(entry)
            new_count += 1

        if new_count == 0 and current_page > 1:
            break

        try:
            nxt = await page.query_selector('a:has-text("下一页")')
            if not nxt:
                break
            href = await nxt.get_attribute('href') or ''
            if 'page=' not in href:
                break
        except Exception as e:
            logger.warning(f"[PW] 下一页按钮获取失败: {e}")
            break

        delay = random.uniform(PAGE_DELAY_MIN, PAGE_DELAY_MAX)
        await asyncio.sleep(delay)
        try:
            await nxt.click()
            try:
                await page.wait_for_load_state("networkidle", timeout=IDLE_TIMEOUT)
            except:
                pass
            await asyncio.sleep(random.uniform(PAGE_DELAY_MIN, PAGE_DELAY_MAX))
        except Exception as e:
            logger.warning(f"[PW] 翻页失败: {e}")
            break
        current_page += 1

    logger.info(f"[PW] 共抓取 {len(entries)} 个IP")
    return entries

async def extract_detail_channels_playwright(ctx, detail_url: str) -> list:
    channels = []
    page = None
    start_time = time.perf_counter()
    def is_overtime():
        return time.perf_counter() - start_time > DETAIL_MAX_SECONDS
    try:
        page = await ctx.new_page()
        await page.add_init_script(STEALTH_JS)

        await page.goto(detail_url, timeout=DETAIL_PAGE_TIMEOUT, wait_until="domcontentloaded")
        await asyncio.sleep(random.uniform(DETAIL_WAIT_MIN, DETAIL_WAIT_MAX))
        try:
            await page.wait_for_load_state("networkidle", timeout=DETAIL_IDLE_TIMEOUT)
        except:
            pass
        page_title = await page.title()
        page_text = ""
        try:
            page_text = (await page.inner_text("body"))[:500]
        except:
            pass
        if "站点禁止" in page_title or "访问被拒绝" in page_text or "站点禁止" in page_text:
            logger.debug(f"[PW] 详情页被拒绝: {detail_url[:60]}")
            return channels

        s_hash = None
        channel_list_url = None
        s_link = await page.evaluate(r"""
            () => {
                const links = document.querySelectorAll('a[href*="?s="]');
                for (const a of links) {
                    const href = a.getAttribute('href') || '';
                    if (href.includes('?s=')) {
                        return href;
                    }
                }
                return null;
            }
        """)
        if s_link:
            m = re.search(r'[?&]s=([^&]+)', s_link)
            if m:
                s_hash = m.group(1)
                t_match = re.search(r'[?&]t=([^&]+)', detail_url)
                t_type = t_match.group(1) if t_match else 'hotel'
                channel_list_url = f"{TARGET_URL}?s={s_hash}&t={t_type}&page_size=100"
                logger.debug(f"[PW] 构造频道列表URL: {channel_list_url[:80]}")

        if not channel_list_url:
            for sel in ['a:has-text("查看频道列表")', 'a.btn-play', '.btn-play']:
                try:
                    btn = await page.query_selector(sel)
                    if btn:
                        href = await btn.get_attribute("href") or ""
                        if '?s=' in href:
                            m = re.search(r'[?&]s=([^&]+)', href)
                            if m:
                                s_hash = m.group(1)
                                t_match = re.search(r'[?&]t=([^&]+)', detail_url)
                                t_type = t_match.group(1) if t_match else 'hotel'
                                channel_list_url = f"{TARGET_URL}?s={s_hash}&t={t_type}&page_size=100"
                                break
                except:
                    continue

        if not channel_list_url:
            logger.debug(f"[PW] 未找到频道列表链接: {detail_url[:60]}")
            return channels

        await page.goto(channel_list_url, timeout=DETAIL_PAGE_TIMEOUT, wait_until="domcontentloaded")
        await asyncio.sleep(random.uniform(3, 5))
        try:
            await page.wait_for_load_state("networkidle", timeout=DETAIL_IDLE_TIMEOUT)
        except:
            pass
        await asyncio.sleep(random.uniform(1, 2))

        seen_page_urls = set()
        for page_num in range(1, MAX_DETAIL_PAGES + 1):
            if is_overtime():
                logger.debug(f"详情页超时(>{DETAIL_MAX_SECONDS}s)强制停止: {detail_url[:60]}")
                break

            table_loaded = False
            try:
                await page.wait_for_selector('table.iptv-table tbody tr', timeout=10000)
                table_loaded = True
            except:
                try:
                    await page.wait_for_selector('table tbody tr', timeout=5000)
                    table_loaded = True
                except:
                    pass
            if not table_loaded:
                if page_num == 1:
                    logger.debug(f"[PW] 未找到频道表格: {detail_url[:60]}")
                break

            page_channels = await page.evaluate(r"""
                () => {
                    const results = [];
                    const rows = document.querySelectorAll('table.iptv-table tbody tr, table tbody tr');
                    for (const row of rows) {
                        const cells = row.querySelectorAll('td');
                        if (cells.length < 3) continue;
                        const name = cells[1] ? cells[1].innerText.trim() : '';
                        let url = '';
                        const a = cells[2] ? cells[2].querySelector('a') : null;
                        if (a) {
                            url = a.getAttribute('href') || a.innerText.trim();
                        } else if (cells[2]) {
                            url = cells[2].innerText.trim();
                        }
                        if (name && url) {
                            results.push({name: name, url: url});
                        }
                    }
                    return results;
                }
            """)
            if not page_channels:
                break

            for ch in page_channels:
                name = ch.get('name', '').strip()
                url = ch.get('url', '').strip()
                if name and url:
                    url = url.replace('&', '&')
                    if not url.startswith(('http://', 'https://')):
                        url = DEFAULT_PROTOCOL + url
                    channels.append((name, url))

            current_page_url = page.url
            if current_page_url in seen_page_urls:
                logger.debug(f"[PW] URL重复，停止翻页")
                break
            seen_page_urls.add(current_page_url)

            if page_num >= MAX_DETAIL_PAGES:
                break

            nxt = None
            try:
                pagination_btns = await page.query_selector_all('.pagination-btn')
                for btn in pagination_btns:
                    btn_text = (await btn.inner_text()).strip()
                    btn_href = await btn.get_attribute('href') or ''
                    if btn_text == '下一页' and btn_href:
                        nxt = btn
                        break
            except:
                pass

            if not nxt:
                try:
                    current_url = page.url
                    if 'page=' in current_url:
                        m = re.search(r'page=(\d+)', current_url)
                        if m:
                            next_page = int(m.group(1)) + 1
                            next_url = re.sub(r'page=\d+', f'page={next_page}', current_url)
                            await page.goto(next_url, timeout=DETAIL_PAGE_TIMEOUT, wait_until="domcontentloaded")
                            await asyncio.sleep(random.uniform(DETAIL_WAIT_MIN, DETAIL_WAIT_MAX))
                            try:
                                await page.wait_for_load_state("networkidle", timeout=DETAIL_IDLE_TIMEOUT)
                            except:
                                pass
                            continue
                except:
                    pass
                break

            try:
                disabled = await nxt.get_attribute("disabled") or ""
                cls = await nxt.get_attribute("class") or ""
                if disabled or "disabled" in cls:
                    break
            except:
                pass

            await asyncio.sleep(random.uniform(DETAIL_PAGE_DELAY_MIN, DETAIL_PAGE_DELAY_MAX))
            try:
                await nxt.click()
                try:
                    await page.wait_for_load_state("networkidle", timeout=DETAIL_IDLE_TIMEOUT)
                except:
                    pass
                await asyncio.sleep(random.uniform(1, 2))
            except Exception as e:
                logger.debug(f"翻页失败: {e}")
                break
    except Exception as e:
        logger.debug(f"[PW] 提取频道异常: {e}")
    finally:
        if page and not page.is_closed():
            try:
                await page.close()
            except:
                pass

    seen = set()
    unique = []
    for name, url in channels:
        if url not in seen:
            seen.add(url)
            unique.append((name, url))
    return unique

# ============================================================================
# URL去重
# ============================================================================
def deduplicate_urls(ch_map: Dict[Tuple[str, str], List[str]]) -> Dict[Tuple[str, str], List[str]]:
    url_to_ch = defaultdict(list)
    for (g, n), urls in ch_map.items():
        for u in urls:
            url_to_ch[u].append((g, n))
    url_chosen = {}
    for url, chs in url_to_ch.items():
        if len(chs) == 1:
            url_chosen[url] = chs[0]
        else:
            plus = [c for c in chs if '+' in c[1].lower()]
            url_chosen[url] = plus[0] if plus else max(chs, key=lambda c: len(c[1]))
    new_map = defaultdict(list)
    for (g, n), urls in ch_map.items():
        for u in urls:
            if url_chosen[u] == (g, n):
                new_map[(g, n)].append(u)
    return dict(new_map)

# ============================================================================
# 导出M3U/TXT
# ============================================================================
def export(ch_map: Dict[Tuple[str, str], List[str]]):
    now = datetime.datetime.now(
        datetime.timezone(datetime.timedelta(hours=8))
    ).strftime("%Y-%m-%d %H:%M:%S")
    groups = defaultdict(list)
    for (g, n), urls in ch_map.items():
        for u in urls:
            groups[g].append((n, u))

    cctv_weight = {name: idx for idx, name in enumerate(CCTV_ORDER)}

    with open(OUTPUT_M3U, 'w', encoding='utf-8') as f:
        f.write("#EXTM3U\n")
        for grp in GROUP_ORDER:
            if grp not in groups:
                continue
            chs = groups[grp]
            if grp == "央视频道":
                chs_sorted = sorted(chs, key=lambda x: cctv_weight.get(x[0], 9999))
            else:
                chs_sorted = sorted(chs, key=lambda x: x[0])
            for n, u in chs_sorted:
                if n.strip():
                    f.write(f'#EXTINF:-1 group-title="{grp}",{n}\n{u}\n')
        f.write("\n")
        f.write(f'#EXTINF:-1 group-title="更新时间",{now}\nhttps://example.com\n')

    with open(OUTPUT_TXT, 'w', encoding='utf-8') as f:
        for grp in GROUP_ORDER:
            if grp not in groups:
                continue
            f.write(f"{grp},#genre#\n")
            chs = groups[grp]
            if grp == "央视频道":
                chs_sorted = sorted(chs, key=lambda x: cctv_weight.get(x[0], 9999))
            else:
                chs_sorted = sorted(chs, key=lambda x: x[0])
            for n, u in chs_sorted:
                if n.strip():
                    f.write(f"{n},{u}\n")
        f.write("\n")
        f.write(f"更新时间,#genre#\n{now},https://example.com\n")

    logger.info(f"导出完成: {len(ch_map)} 个频道")

# ============================================================================
# 主函数
# ============================================================================
async def main():
    parser = argparse.ArgumentParser(description="IPTV源抓取工具（稳定长测版）")
    parser.add_argument("--type", default="all", help="抓取源类型: all/hotel/multicast/migu/other")
    parser.add_argument("--max-pages", type=int, default=MAX_PAGES, help="最大翻页数")
    parser.add_argument("--max-ips", type=int, default=MAX_IPS, help="最大IP数量, 0=无限制")
    parser.add_argument("--headless", default="true", help="无头模式: true/false")
    parser.add_argument("--skip-ffmpeg", action="store_true", help="跳过FFmpeg测速")
    parser.add_argument("--chrome-path", default="", help="Chrome路径")
    parser.add_argument("--skip-scrape", action="store_true", help="跳过目标站抓取")
    parser.add_argument("--skip-github", action="store_true", help="跳过GitHub源")
    args = parser.parse_args()

    config_raw_type = SCRAPE_SOURCE_FILTER
    cmd_raw_type = args.type
    if cmd_raw_type and cmd_raw_type.strip().lower() != "all":
        ft = norm_type(cmd_raw_type)
        logger.info(f"使用命令行指定类型: {ft}")
    else:
        ft = norm_type(config_raw_type)
        logger.info(f"使用配置默认类型: {ft}")

    max_pages = args.max_pages
    max_ips = args.max_ips
    headless = args.headless.lower() != "false" if args.headless else HEADLESS
    do_ffmpeg = ENABLE_FFMPEG and not args.skip_ffmpeg
    do_scrape = ENABLE_SCRAPE and not args.skip_scrape

    start_time = time.time()
    logger.info("=" * 60)
    logger.info("IPTV 源抓取工具启动")
    logger.info(f" 类型: {ft} | 抓取目标站: {'开启' if do_scrape else '关闭'} | GitHub: {'开启' if ENABLE_GITHUB and not args.skip_github else '关闭'} | FFmpeg: {'开启' if do_ffmpeg else '关闭'}")
    logger.info("=" * 60)

    all_channels = []
    github_sources_urls = []
    scrape_urls_set = set()

    # GitHub源
    if ENABLE_GITHUB and not args.skip_github:
        github_chs, github_sources_urls = await fetch_github_sources()
        for g, n, u in github_chs:
            all_channels.append((g, n, u))

    # 目标站抓取（Playwright）
    if do_scrape:
        logger.info("--- 开始目标站抓取 ---")
        entries = []
        try:
            chrome_path = args.chrome_path or CHROME_PATH
            if not chrome_path:
                candidates = [
                    str(Path(__file__).parent / ".openclaw/tmp/browser/chrome-linux64/chrome"),
                    "/usr/bin/google-chrome-stable",
                    "/usr/bin/google-chrome",
                    "/usr/bin/chromium-browser",
                    "/usr/bin/chromium",
                ]
                for c in candidates:
                    if Path(c).exists() and Path(c).is_file():
                        chrome_path = c
                        break
            if chrome_path:
                logger.info(f"Chrome路径: {chrome_path}")
            else:
                logger.info("Chrome路径: 使用Playwright默认")

            async with async_playwright() as p:
                launch_opts = {
                    "headless": headless,
                    "args": [
                        "--no-sandbox", "--disable-setuid-sandbox",
                        "--disable-dev-shm-usage", "--disable-gpu",
                        "--disable-features=IsolateOrigins,site-per-process",
                        "--disable-blink-features=AutomationControlled",
                        "--disable-web-security",
                        "--disable-features=BlockInsecurePrivateNetworkRequests",
                    ]
                }
                if chrome_path:
                    launch_opts["executable_path"] = chrome_path

                browser = await p.chromium.launch(**launch_opts)
                ctx = await browser.new_context(
                    viewport={"width": 1920, "height": 1080},
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                )
                await ctx.add_init_script(STEALTH_JS)

                try:
                    entries = await scrape_ips_playwright(ctx, ft, max_pages)
                except Exception as e:
                    logger.warning(f"Playwright IP列表抓取失败: {e}")

                if max_ips > 0:
                    entries = entries[:max_ips]

                if entries:
                    for i, entry in enumerate(entries):
                        try:
                            detail_url = f"{TARGET_URL}?p={entry['hash']}&t={entry['type']}"
                            chs = await extract_detail_channels_playwright(ctx, detail_url)
                            for name, url in chs:
                                std_ch = unify_channel_name(name)
                                g = classify(std_ch)
                                if g:
                                    fn = std_ch if g == "央视频道" else clean_cn(std_ch)
                                    all_channels.append((g, fn, url))
                                    scrape_urls_set.add(url)
                            await asyncio.sleep(random.uniform(IP_DELAY_MIN, IP_DELAY_MAX))
                        except Exception as e:
                            logger.warning(f"IP {entry['ip']} 详情提取失败")

                try: await ctx.close()
                except: pass
                try: await browser.close()
                except: pass
        except Exception as e:
            logger.warning(f"Playwright整体失败: {e}")

    # 过滤内网IP
    before = len(all_channels)
    all_channels = [(g, n, u) for g, n, u in all_channels if not is_internal(u)]
    if before != len(all_channels):
        logger.info(f"过滤内网IP: {before} -> {len(all_channels)}")

    ch_map = defaultdict(list)
    for g, n, u in all_channels:
        ch_map[(g, n)].append(u)
    ch_map = deduplicate_urls(ch_map)

    allowed = set(GROUP_ORDER)
    ch_map = {k: v for k, v in ch_map.items() if k[0] in allowed}

    total_links_before_test = sum(len(v) for v in ch_map.values())
    logger.info(f"去重后: {len(ch_map)} 个频道, {total_links_before_test} 条链接")

    # === 测速筛选 ===
    if do_ffmpeg and ch_map:
        logger.info("--- 开始测速筛选 ---")
        ff_start = time.time()
        ch_map = await batch_test_pipeline(ch_map)
        logger.info(f"测速耗时: {time.time() - ff_start:.1f}s")

    # 导出
    export(ch_map)

    # === 统计信息 ===
    final_urls = set()
    for urls in ch_map.values():
        final_urls.update(urls)

    logger.info("=" * 60)
    logger.info("来源有效性统计:")
    for i, url_set in enumerate(github_sources_urls, start=1):
        raw = len(url_set)
        effective = len(url_set & final_urls)
        pct = (effective / raw * 100) if raw else 0
        logger.info(f"  GitHub 源{i}原始链接: 共{raw}条, 有效{effective}条, 有效率{pct:.1f}%")

    raw_scrape = len(scrape_urls_set)
    effective_scrape = len(scrape_urls_set & final_urls)
    pct_scrape = (effective_scrape / raw_scrape * 100) if raw_scrape else 0
    logger.info(f"  目标站抓取原始链接: 共{raw_scrape}条, 有效{effective_scrape}条, 有效率{pct_scrape:.1f}%")
    logger.info("=" * 60)

    total_time = time.time() - start_time
    logger.info(f"总耗时: {total_time:.1f}s")
    logger.info("=" * 60)

if __name__ == "__main__":
    asyncio.run(main())
