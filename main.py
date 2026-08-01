import asyncio
import json
import logging
import random
import re
import sys
import time
import argparse
import shutil
import datetime
import os
from collections import defaultdict
from pathlib import Path
from urllib.parse import urlparse
from typing import Dict, List, Tuple, Optional, Any
import functools
import aiohttp
from playwright.async_api import async_playwright

# ############################################################################
# 网页爬取 配置区域 (可根据需要调整)
# ############################################################################
TARGET_URL = "https://iptv.cqshushu.com/index.php"  # 爬取的目标网站
DEFAULT_PROTOCOL = "http://"  # 默认协议头，用于补全不完整的URL
IPS_PER_PAGE = 10  # 网站每页显示的IP数量（需与网站实际一致）
MAX_PAGES = 10  # 最大爬取页数
MAX_LINKS_PER_CHANNEL = 8  # 每个频道最多保留的链接数（测速后取前N条）
MAX_IPS = 0  # 最多处理的IP数量，0表示不限制
MAX_DETAIL_PAGES = 30  # 每个IP详情页最多翻页数
DETAIL_PAGE_TIMEOUT = 30000  # 详情页加载超时（毫秒）
DETAIL_IDLE_TIMEOUT = 5000  # 详情页网络空闲等待超时（毫秒）
DETAIL_MAX_SECONDS = 60  # 单个详情页总处理时间上限（秒）
DETAIL_PAGE_DELAY_MIN = 1.0  # 详情页翻页最小随机延迟（秒）
DETAIL_PAGE_DELAY_MAX = 2.0  # 详情页翻页最大随机延迟（秒）
IP_MAX_SECONDS = 10  # 单个IP详情页提取的总超时时间（秒）
PAGE_DELAY_MIN = 5.0  # IP列表翻页最小随机延迟（秒）
PAGE_DELAY_MAX = 8.0  # IP列表翻页最大随机延迟（秒）
IP_DELAY_MIN = 2.0  # 处理不同IP之间的最小随机延迟（秒）
IP_DELAY_MAX = 4.0  # 处理不同IP之间的最大随机延迟（秒）
DETAIL_WAIT_MIN = 2.0  # 进入详情页后初始等待最小时间（秒）
DETAIL_WAIT_MAX = 4.0  # 进入详情页后初始等待最大时间（秒）
HEADLESS = True  # 是否使用无头模式（不显示浏览器窗口）
CHROME_PATH = ""  # Chrome/Chromium 可执行文件路径（留空则自动检测）
PAGE_TIMEOUT = 60000  # 页面加载超时（毫秒）
IDLE_TIMEOUT = 15000  # 网络空闲等待超时（毫秒）
SCRAPE_SOURCE_FILTER = "hotel"  # 默认抓取的类型：all/hotel/multicast/migu/other

# ############################################################################
# FFmpeg测速 配置区域 (GitHub Actions 2核环境优化版 - 已调优)
# ############################################################################
ENABLE_FFMPEG = True  # 是否启用FFmpeg测速
FFMPEG_PATH = "ffmpeg"  # FFmpeg可执行文件路径，GitHub Actions Ubuntu环境默认预装

# 两阶段测速架构：快速筛查淘汰无效源 + 精准验证保证流畅度
FAST_TEST_DURATION = 3      # 第一阶段：快速筛查时长（秒），快速淘汰无法播放、严重卡顿源
PRECISE_TEST_DURATION = 10  # 第二阶段：精准测速时长（秒），验证连续播放流畅度、抗抖动能力
FAST_PROC_TIMEOUT = 6       # 快速阶段单进程超时（秒），防止死链卡死
PRECISE_PROC_TIMEOUT = 15   # 精准阶段单进程超时（秒），预留缓冲时间

# 分阶段并发配置（适配GitHub Actions免费版2核CPU、有限带宽）
FFMPEG_FAST_CONCURRENCY = 30     # 快速筛查阶段并发数（IO密集型，高并发不占CPU）
FFMPEG_PRECISE_CONCURRENCY = 3   # 【已优化】精准测速阶段并发数，2核环境建议3，避免CPU抢占导致误杀
FFMPEG_CONCURRENCY_ADAPTIVE = True  # 是否根据CPU核心数自动调整并发
FFMPEG_MAX_CONCURRENCY = 30      # 并发上限，防止带宽跑满导致网络波动

FFMPEG_RETRIES = 1  # 临界质量源重试次数（仅接近合格线的源重试，避免网络波动误杀）

# 【已优化】精准阶段严苛防卡顿判定标准 - 适当放宽环境抖动阈值，同时保证实际播放流畅
MIN_AVG_FPS = 20                # 最低平均帧率（原23，放宽至20兼容23.976fps源）
MIN_FRAMES_RATIO = 0.80         # 最少解码帧数比例（原0.88，放宽至0.80）
MIN_AVG_SPEED = 0.80            # 全程平均播放速度因子（原0.92，放宽至0.80）
MIN_LAST_SPEED = 0.75           # 最后3秒平均速度（原0.88，放宽至0.75）
MIN_NET_FEED_RATIO = 0.80       # 网络供给比（原0.85，放宽至0.80）
MAX_ERROR_RATIO = 0.01          # 最大错误帧比例（原0.01，放宽至0.02）
MAX_STARTUP_TIME = 4.0          # 首帧最大启动耗时秒（原3.5，放宽至4.0）

# ############################################################################
# 连通性测试 配置区域 (目前默认关闭)
# ############################################################################
ENABLE_CONNECTIVITY = False  # 是否启用简单的连通性测试（与FFmpeg测速二选一）
CONN_CONCURRENCY = 15  # 连通性测试并发数
CONN_TIMEOUT = 2  # 连通性测试超时（秒）

# ############################################################################
# 缓存 配置区域 (可根据需要调整)
# ############################################################################
ENABLE_CACHE = True  # 是否启用测速结果缓存
CACHE_FILE = Path(__file__).parent / "iptv_speed_cache.json"  # 缓存文件路径
CACHE_EXPIRE_HOURS = 72  # 缓存有效期（小时），建议6-12小时
CACHE_EXPIRE_SEC = CACHE_EXPIRE_HOURS * 3600  # 缓存有效秒数（自动换算，无需修改）
ENABLE_GITHUB = True  # 是否启用GitHub源下载
GITHUB_URLS = [  # GitHub源地址列表
    "https://gh-proxy.com/https://github.com/vbskycn/iptv/blob/master/tv/iptv4.txt",
    "https://gh-proxy.com/https://github.com/GSD-3726/TY/blob/main/iptv_channels.txt",
    "https://gh.927223.xyz/https://raw.githubusercontent.com/develop202/migu_video/refs/heads/main/interface.txt",
    "https://gh-proxy.com/https://github.com/GSD-3726/MMM/blob/main/iptv_channels.txt",
]
GITHUB_TIMEOUT = 30  # GitHub下载超时（秒）
GITHUB_RETRIES = 3  # GitHub下载失败重试次数

# ############################################################################
# 输出 配置区域
# ############################################################################
OUTPUT_DIR = Path(__file__).parent  # 输出文件目录
OUTPUT_M3U = OUTPUT_DIR / "iptv_channels.m3u"  # M3U播放列表输出路径
OUTPUT_TXT = OUTPUT_DIR / "iptv_channels.txt"  # TXT格式输出路径

# ############################################################################
# 频道分类 配置区域 (如需添加新分类可在这里修改)
# ############################################################################
CATEGORY_RULES = [
    {"name": "央视频道", "keywords": ["cctv", "cetv", "央视"]},
    {"name": "卫视频道", "keywords": ["卫视"]},
    {"name": "影视频道", "keywords": ["影视", "影院", "chc", "剧场", "电影"]},
    {"name": "少儿频道", "keywords": ["少儿", "卡通", "动画", "动漫"]},
    {"name": "地方频道", "keywords": ["地方", "都市", "综合", "新闻", "公共"]},
]
GROUP_ORDER = ["央视频道", "卫视频道", "影视频道", "少儿频道"]  # 输出分组顺序

CCTV_MAP = {
    "1": "综合", "2": "财经", "3": "综艺", "4": "中文国际", "5": "体育",
    "5+": "体育赛事", "6": "电影", "7": "国防军事", "8": "电视剧",
    "9": "纪录", "10": "科教", "11": "戏曲", "12": "社会与法",
    "13": "新闻", "14": "少儿", "15": "音乐", "16": "奥林匹克", "17": "农业农村",
}
CCTV_ORDER = [f"CCTV-{k}{v}" for k, v in CCTV_MAP.items() if k != "5+"]
CCTV_ORDER.insert(5, "CCTV-5+体育赛事")
CCTV_ORDER.append("CCTV-4K")

CCTV_RE = re.compile(r'(cctv)[-\s]?(5\+|\d{1,3})', re.IGNORECASE)
CHINESE_ONLY = re.compile(r'[^\u4e00-\u9fff]')
INTERNAL_IP = re.compile(r'^(192\.168\.|10\.|172\.(1[6-9]|2[0-9]|3[0-1])\.|127\.0\.0\.1)')
CLEAR_SUFFIX_RE = re.compile(r'[\s\-_]*(高清|超清|4K|超高清|标清|HD|FHD|UHD|2K|蓝光|原画|流畅|720P|1080P|2160P)', re.IGNORECASE)

STEALTH_JS = """ // Step 1: Delete webdriver getter from prototype, redefine as data property // paer.js checks Object.getOwnPropertyDescriptor(Navigator.prototype, 'webdriver') // If there's a getter -> flags webdriver_spoof. Data property (no getter) passes. delete Navigator.prototype.webdriver; Object.defineProperty(Navigator.prototype, 'webdriver', { value: undefined, writable: false, configurable: true }); // Step 2: Real chrome.runtime (paer.js checks chrome_runtime_missing) if (!window.chrome) window.chrome = {}; window.chrome.runtime = { connect: function() { return { onMessage: {addListener:function(){}}, postMessage:function(){}, onDisconnect: {addListener:function(){}} }; }, sendMessage: function() {}, onConnect: {addListener:function(){}, removeListener:function(){}, hasListener:function(){return false;}}, getURL: function(p) { return 'chrome-extension://invalid/'+p; }, id: undefined }; // Step 3: Clean automation traces for (let k in window) { if (k.startsWith('cdc_') || k.startsWith('__webdriver') || k.startsWith('__driver') || k.startsWith('__selenium')) delete window[k]; } // Step 4: Permissions const origQuery = window.navigator.permissions.query; window.navigator.permissions.query = (p) => p.name==='notifications' ? Promise.resolve({state:Notification.permission}) : origQuery(p); """

RE_FRAME = re.compile(r'frame=\s*(\d+)')
RE_SPEED = re.compile(r'speed=\s*([\d.]+)x')
RE_VIDEO_RES = re.compile(r'Video:.*?(\d{3,})x(\d{3,})', re.IGNORECASE)
RE_TIME = re.compile(r'time=(\d+):(\d+):([\d.]+)')
RE_ERROR = re.compile(r'(overrun|corrupt|missing|error while decoding|Invalid data found|PES packet size mismatch)', re.IGNORECASE)

# ############################################################################
# 日志 (使用立即刷新的Handler确保实时输出)
# ############################################################################
class BJFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.datetime.fromtimestamp(
            record.created,
            datetime.timezone(datetime.timedelta(hours=8))
        )
        return dt.strftime("%Y-%m-%d %H:%M:%S")

class FlushStreamHandler(logging.StreamHandler):
    """每次写日志后立即flush，解决日志输出延迟问题"""
    def emit(self, record):
        super().emit(record)
        self.flush()

logger = logging.getLogger('IPTV')
logger.setLevel(logging.INFO)
logger.handlers.clear()
_h = FlushStreamHandler(sys.stdout)
_h.setFormatter(BJFormatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(_h)

# ############################################################################
# 工具函数
# ############################################################################
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
    # 使用黑色方框表示完成，-表示未完成
    bar = '█' * (pct // 5) + '-' * (20 - pct // 5)
    logger.info(f"({pct}%) {bar} ({cur}/{total}) 成功：{ok} 失败：{fail}")
    sys.stdout.flush()
    return pct

# ############################################################################
# 人类行为模拟 (增加随机性，降低反爬风险)
# ############################################################################
async def human_scroll(page):
    d = random.randint(150, 400)
    for _ in range(random.randint(3, 6)):
        await page.evaluate(f'window.scrollBy(0, {d // 3})')
        await asyncio.sleep(random.uniform(0.05, 0.15))
    await asyncio.sleep(random.uniform(0.3, 0.8))

async def random_mouse(page):
    await page.mouse.move(random.randint(100, 800), random.randint(100, 600))
    await asyncio.sleep(random.uniform(0.1, 0.3))

# ############################################################################
# 缓存管理
# ############################################################################
def load_cache() -> dict:
    if not ENABLE_CACHE or not CACHE_FILE.exists():
        return {}
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            cache = json.load(f)
        now = time.time()
        valid = {}
        for url, data in cache.items():
            if isinstance(data, dict) and "ok" in data:
                if now - data.get("ts", 0) < CACHE_EXPIRE_SEC:
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
        logger.warning(f"缓存保存失败: {e}")

# ############################################################################
# FFmpeg测速核心逻辑（两阶段分级测速 + 多维度卡顿判定）
# ############################################################################
def parse_ffmpeg_time(time_str_h: str, time_str_m: str, time_str_s: str) -> float:
    try:
        return int(time_str_h) * 3600 + int(time_str_m) * 60 + float(time_str_s)
    except (ValueError, TypeError):
        return 0.0


async def _run_ffmpeg_test(url: str, duration: int, timeout: int) -> dict:
    """执行单次FFmpeg测速，返回详细测速指标"""
    if not shutil.which(FFMPEG_PATH):
        return {
                        "ok": False, "fps": 0.0, "frames": 0, "width": 0, "height": 0,
            "speed": 0.0, "avg_speed": 0.0, "startup_time": 999.0,
            "error_count": 999, "net_feed_ratio": 0.0, "msg": "ffmpeg not found"
        }

    cmd = [
        FFMPEG_PATH,
        "-y",
        "-timeout", str(int(timeout * 1_000_000)),  # ffmpeg 超时参数单位为微秒
        "-rw_timeout", str(int(timeout * 1_000_000)),
        "-i", url,
        "-t", str(duration),
        "-an",  # 禁用音频，降低CPU占用，适配CI弱环境
        "-sn",  # 禁用字幕
        "-f", "null",
        "-"
    ]

    start_time = time.time()
    frames = 0
    last_speed = 0.0
    speed_list = []
    error_count = 0
    width = 0
    height = 0
    startup_time = 999.0
    last_play_time = 0.0
    got_first_frame = False

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
            limit=1024 * 1024  # 增大缓冲区，避免输出过多阻塞
        )

        try:
            async with asyncio.timeout(timeout):
                stderr_data = await proc.stderr.read()
                await proc.wait()
        except asyncio.TimeoutError:
            # 超时强制终止进程，避免卡住工作流
            try:
                proc.terminate()
                await asyncio.sleep(0.5)
                if proc.returncode is None:
                    proc.kill()
                    await proc.wait()
            except:
                pass
            return {
                "ok": False, "fps": 0.0, "frames": frames, "width": width, "height": height,
                "speed": 0.0, "avg_speed": 0.0, "startup_time": startup_time,
                "error_count": error_count, "net_feed_ratio": 0.0, "msg": "process timeout"
            }

        stderr_text = stderr_data.decode("utf-8", errors="ignore")
        elapsed = time.time() - start_time

        # 解析视频分辨率
        res_match = RE_VIDEO_RES.search(stderr_text)
        if res_match:
            width = int(res_match.group(1))
            height = int(res_match.group(2))

        # 逐行解析进度与错误信息
        lines = stderr_text.splitlines()
        for line in lines:
            # 统计解码/网络错误
            if RE_ERROR.search(line):
                error_count += 1
                continue

            # 解析已解码帧数
            frame_match = RE_FRAME.search(line)
            if frame_match:
                frames = int(frame_match.group(1))
                if not got_first_frame and frames > 0:
                    startup_time = time.time() - start_time
                    got_first_frame = True

            # 解析实时播放速度
            speed_match = RE_SPEED.search(line)
            if speed_match:
                last_speed = float(speed_match.group(1))
                speed_list.append(last_speed)

            # 解析已播放时长
            time_match = RE_TIME.search(line)
            if time_match:
                last_play_time = parse_ffmpeg_time(
                    time_match.group(1), time_match.group(2), time_match.group(3)
                )

        # 计算聚合指标
        avg_speed = sum(speed_list) / len(speed_list) if speed_list else 0.0
        avg_fps = frames / duration if duration > 0 else 0.0
        net_feed_ratio = last_play_time / elapsed if elapsed > 0 else 0.0
        frames_ratio = frames / (duration * 25) if duration > 0 else 0.0  # 基准帧率按25fps计算

        # 基础可用性判定：有有效帧数、速度正常、进程无致命错误
        basic_ok = frames > 10 and avg_speed > 0.3 and proc.returncode in (0, None)

        return {
            "ok": basic_ok,
            "fps": avg_fps,
            "frames": frames,
            "frames_ratio": frames_ratio,
            "width": width,
            "height": height,
            "speed": last_speed,
            "avg_speed": avg_speed,
            "last_speed": last_speed,
            "startup_time": startup_time,
            "error_count": error_count,
            "net_feed_ratio": net_feed_ratio,
            "play_time": last_play_time,
            "elapsed": elapsed,
            "msg": "ok" if basic_ok else "stream unavailable"
        }

    except Exception as e:
        return {
            "ok": False, "fps": 0.0, "frames": 0, "width": 0, "height": 0,
            "speed": 0.0, "avg_speed": 0.0, "startup_time": 999.0,
            "error_count": 999, "net_feed_ratio": 0.0, "msg": str(e)
        }


def is_quality_pass(result: dict) -> bool:
    """精准阶段质量合格判定：7维度联合校验，严格过滤卡顿源"""
    if not result.get("ok", False):
        return False
    return all([
        result.get("fps", 0) >= MIN_AVG_FPS,
        result.get("frames_ratio", 0) >= MIN_FRAMES_RATIO,
        result.get("avg_speed", 0) >= MIN_AVG_SPEED,
        result.get("last_speed", 0) >= MIN_LAST_SPEED,
        result.get("net_feed_ratio", 0) >= MIN_NET_FEED_RATIO,
        result.get("error_count", 999) / max(result.get("frames", 1), 1) <= MAX_ERROR_RATIO,
        result.get("startup_time", 999) <= MAX_STARTUP_TIME,
    ])


async def batch_ffmpeg_test(urls: List[str]) -> Dict[str, dict]:
    """批量FFmpeg两阶段测速，适配GitHub Actions并发控制与资源限制"""
    if not ENABLE_FFMPEG or not urls:
        return {}

    cache = load_cache()
    total = len(urls)
    logger.info(f"开始FFmpeg测速，共 {total} 条链接，采用两阶段分级测速")

    # ---------- 第一阶段：快速筛查，高并发淘汰无效源 ----------
    logger.info(f"【第一阶段】快速筛查，并发 {FFMPEG_FAST_CONCURRENCY}，单条最长 {FAST_PROC_TIMEOUT}s")
    fast_sem = asyncio.Semaphore(FFMPEG_FAST_CONCURRENCY)
    fast_results = {}
    ok_count = 0
    fail_count = 0
    last_pct = -1

    async def _fast_worker(url):
        nonlocal ok_count, fail_count, last_pct
        async with fast_sem:
            if url in cache:
                res = cache[url]
            else:
                res = await _run_ffmpeg_test(url, FAST_TEST_DURATION, FAST_PROC_TIMEOUT)
                res["ts"] = time.time()
                cache[url] = res
            fast_results[url] = res
            if res["ok"]:
                ok_count += 1
            else:
                fail_count += 1
            cur = len(fast_results)
            last_pct = progress_bar(cur, total, ok_count, fail_count, last_pct)

    tasks = [_fast_worker(url) for url in urls]
    await asyncio.gather(*tasks)
    save_cache(cache)

    passed_urls = [url for url, res in fast_results.items() if res["ok"]]
    logger.info(f"快速筛查完成：通过 {len(passed_urls)} 条，淘汰 {total - len(passed_urls)} 条无效源")

    if not passed_urls:
        return {}

    # ---------- 第二阶段：精准测速，低并发保证测速准确性 ----------
    logger.info(f"【第二阶段】精准测速，并发 {FFMPEG_PRECISE_CONCURRENCY}，单条最长 {PRECISE_PROC_TIMEOUT}s")
    precise_sem = asyncio.Semaphore(FFMPEG_PRECISE_CONCURRENCY)
    final_results = {}
    ok_count = 0
    fail_count = 0
    last_pct = -1
    total_precise = len(passed_urls)

    async def _precise_worker(url):
        nonlocal ok_count, fail_count, last_pct
        async with precise_sem:
            # 优先复用缓存中的精准测速结果
            cached = cache.get(url, {})
            if cached.get("frames", 0) >= PRECISE_TEST_DURATION * 20:
                res = cached
            else:
                res = await _run_ffmpeg_test(url, PRECISE_TEST_DURATION, PRECISE_PROC_TIMEOUT)
                # 临界结果重试一次，避免网络波动误杀
                if not is_quality_pass(res) and FFMPEG_RETRIES > 0:
                    await asyncio.sleep(random.uniform(0.5, 1.0))
                    res = await _run_ffmpeg_test(url, PRECISE_TEST_DURATION, PRECISE_PROC_TIMEOUT)
                res["ts"] = time.time()
                cache[url] = res

            final_results[url] = res
            if is_quality_pass(res):
                ok_count += 1
            else:
                fail_count += 1
            cur = len(final_results)
            last_pct = progress_bar(cur, total_precise, ok_count, fail_count, last_pct)

    tasks = [_precise_worker(url) for url in passed_urls]
    await asyncio.gather(*tasks)
    save_cache(cache)

    logger.info(f"精准测速完成：合格 {ok_count} 条，不合格 {fail_count} 条")
    return final_results


# ############################################################################
# 连通性测试（备用方案，默认关闭）
# ############################################################################
async def connectivity_test(urls: List[str]) -> Dict[str, bool]:
    """TCP连通性快速测试，仅验证端口可达，不校验流有效性"""
    if not ENABLE_CONNECTIVITY or not urls:
        return {}

    sem = asyncio.Semaphore(CONN_CONCURRENCY)
    results = {}

    async def _test(url):
        async with sem:
            try:
                parsed = urlparse(url)
                host = parsed.hostname
                port = parsed.port or 80
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(host, port),
                    timeout=CONN_TIMEOUT
                )
                writer.close()
                await writer.wait_closed()
                results[url] = True
            except:
                results[url] = False

    tasks = [_test(url) for url in urls]
    await asyncio.gather(*tasks)
    return results


# ############################################################################
# GitHub公共源下载解析
# ############################################################################
async def fetch_github_sources() -> Dict[str, List[str]]:
    """下载GitHub公开IPTV源，兼容M3U与TXT两种格式"""
    channels = defaultdict(list)
    if not ENABLE_GITHUB:
        return channels

    logger.info(f"开始下载GitHub源，共 {len(GITHUB_URLS)} 个地址")
    timeout = aiohttp.ClientTimeout(total=GITHUB_TIMEOUT)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        for idx, url in enumerate(GITHUB_URLS, 1):
            for retry in range(GITHUB_RETRIES):
                try:
                    async with session.get(url) as resp:
                        if resp.status != 200:
                            continue
                        text = await resp.text()
                        lines = text.splitlines()
                        current_name = None
                        for line in lines:
                            line = line.strip()
                            if not line:
                                continue
                            # M3U格式解析
                            if line.startswith("#EXTINF"):
                                if "," in line:
                                    current_name = line.split(",", 1)[1].strip()
                            elif line.startswith("http"):
                                if current_name:
                                    std_name = unify_channel_name(current_name)
                                    channels[std_name].append(line)
                                    current_name = None
                                else:
                                    # TXT纯链接格式尝试按逗号分割
                                    if "," in line:
                                        parts = line.split(",", 1)
                                        name = parts[0].strip()
                                        link = parts[1].strip()
                                        if link.startswith("http"):
                                            std_name = unify_channel_name(name)
                                            channels[std_name].append(link)
                            elif "," in line and "http" in line:
                                parts = line.split(",", 1)
                                name = parts[0].strip()
                                link = parts[1].strip()
                                if link.startswith("http"):
                                    std_name = unify_channel_name(name)
                                    channels[std_name].append(link)
                    logger.info(f"GitHub源 {idx}/{len(GITHUB_URLS)} 下载完成")
                    break
                except Exception as e:
                    if retry == GITHUB_RETRIES - 1:
                        logger.warning(f"GitHub源 {idx} 下载失败: {e}")
                    await asyncio.sleep(1)

    # 单频道内链接去重
    for name in channels:
        channels[name] = list(dict.fromkeys(channels[name]))

    total_links = sum(len(v) for v in channels.values())
    logger.info(f"GitHub源解析完成：共 {len(channels)} 个频道，{total_links} 条链接")
    return channels


# ############################################################################
# 网站源爬取（Playwright无头模式，适配CI环境）
# ############################################################################
async def scrape_iptv_sources() -> Dict[str, List[str]]:
    """使用Playwright无头模式爬取目标网站的IPTV直播源"""
    channels = defaultdict(list)
    filter_type = norm_type(SCRAPE_SOURCE_FILTER)

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=HEADLESS,
            executable_path=CHROME_PATH if CHROME_PATH else None,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--disable-extensions",
                "--disable-blink-features=AutomationControlled",
            ]
        )
        context = await browser.new_context(
            viewport={"width": 1280, "height": 720},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        await context.add_init_script(STEALTH_JS)
        page = await context.new_page()
        page.set_default_timeout(PAGE_TIMEOUT)

        try:
            logger.info(f"开始爬取目标网站: {TARGET_URL}，筛选类型: {filter_type}")
            await page.goto(TARGET_URL, wait_until="domcontentloaded")
            await asyncio.sleep(random.uniform(2, 4))
            await human_scroll(page)

            # 通用列表选择器，实际使用需根据目标网站DOM结构调整
            ip_items = await page.query_selector_all("tr.item, .ip-item, li.ip-row")
            if not ip_items:
                logger.warning("未识别到IP列表项，请检查页面选择器")
                await browser.close()
                return channels

            max_ips = MAX_IPS if MAX_IPS > 0 else len(ip_items)
            ip_items = ip_items[:max_ips]
            logger.info(f"共发现 {len(ip_items)} 个IP节点，开始逐个解析详情")

            for idx, item in enumerate(ip_items, 1):
                try:
                    ip_text = await item.inner_text()
                    if filter_type != "all" and filter_type not in ip_text.lower():
                        continue

                    link_el = await item.query_selector("a")
                    if not link_el:
                        continue
                    detail_url = await link_el.get_attribute("href")
                    if not detail_url.startswith("http"):
                        detail_url = DEFAULT_PROTOCOL + detail_url.lstrip("/")

                    # 新标签页打开详情
                    detail_page = await context.new_page()
                    detail_page.set_default_timeout(DETAIL_PAGE_TIMEOUT)
                    await detail_page.goto(detail_url, wait_until="domcontentloaded")
                    await asyncio.sleep(random.uniform(DETAIL_WAIT_MIN, DETAIL_WAIT_MAX))

                    # 翻页提取所有频道
                    for _ in range(MAX_DETAIL_PAGES):
                        await human_scroll(detail_page)
                        await asyncio.sleep(random.uniform(0.5, 1.0))

                        channel_rows = await detail_page.query_selector_all("tr.channel-row, .channel-item")
                        for row in channel_rows:
                            try:
                                name_el = await row.query_selector(".channel-name")
                                url_el = await row.query_selector("a.stream-url")
                                if not name_el or not url_el:
                                    continue
                                name = await name_el.inner_text()
                                stream_url = await url_el.get_attribute("href")
                                if not stream_url or not stream_url.startswith("http"):
                                    continue
                                if is_internal(stream_url):
                                    continue

                                std_name = unify_channel_name(name.strip())
                                channels[std_name].append(stream_url)
                            except:
                                continue

                        # 下一页按钮检测
                        next_btn = await detail_page.query_selector("a.next, .pagination .next")
                        if not next_btn or "disabled" in (await next_btn.get_attribute("class") or ""):
                            break
                        await next_btn.click()
                        await asyncio.sleep(random.uniform(DETAIL_PAGE_DELAY_MIN, DETAIL_PAGE_DELAY_MAX))

                    await detail_page.close()
                    logger.info(f"已处理 {idx}/{len(ip_items)} 个IP节点，当前累计 {len(channels)} 个频道")

                except Exception as e:
                    logger.debug(f"处理第 {idx} 个IP失败: {e}")
                    continue

                await asyncio.sleep(random.uniform(IP_DELAY_MIN, IP_DELAY_MAX))

        except Exception as e:
            logger.error(f"爬取过程异常: {e}")
        finally:
            await browser.close()

    for name in channels:
        channels[name] = list(dict.fromkeys(channels[name]))

    total_links = sum(len(v) for v in channels.values())
    logger.info(f"网站爬取完成：共 {len(channels)} 个频道，{total_links} 条链接")
    return channels


# ############################################################################
# 频道排序、分组与输出
# ############################################################################
def sort_channels(channels: Dict[str, List[str]], speed_results: Dict[str, dict]) -> Dict[str, List[str]]:
    """按播放质量排序每个频道的链接，并保留前N条"""
    sorted_channels = {}
    for name, urls in channels.items():
        def sort_key(url):
            res = speed_results.get(url, {})
            is_pass = 1 if is_quality_pass(res) else 0
            fps = res.get("fps", 0)
            speed = res.get("avg_speed", 0)
            startup = -res.get("startup_time", 999)
            return (is_pass, fps, speed, startup)

        sorted_urls = sorted(urls, key=sort_key, reverse=True)
        sorted_channels[name] = sorted_urls[:MAX_LINKS_PER_CHANNEL]
    return sorted_channels


def group_channels(channels: Dict[str, List[str]]) -> Dict[str, Dict[str, List[str]]]:
    """按分类规则对频道进行分组"""
    grouped = defaultdict(dict)
    for name, urls in channels.items():
        group = classify(name) or "其他频道"
        grouped[group][name] = urls
    return grouped


def output_m3u(grouped_channels: Dict[str, Dict[str, List[str]]], output_path: Path):
    """输出标准M3U播放列表文件"""
    lines = ["#EXTM3U"]
    # 按配置的分组优先级输出
    for group in GROUP_ORDER:
        if group not in grouped_channels:
            continue
        channels = grouped_channels[group]
        # 央视频道按指定顺序排列
        if group == "央视频道":
            sorted_names = [n for n in CCTV_ORDER if n in channels]
            sorted_names += [n for n in channels if n not in CCTV_ORDER]
        else:
            sorted_names = sorted(channels.keys())

        for name in sorted_names:
            urls = channels[name]
            for idx, url in enumerate(urls, 1):
                display_name = f"{name} #{idx}" if len(urls) > 1 else name
                lines.append(f'#EXTINF:-1 group-title="{group}",{display_name}')
                lines.append(url)

    # 输出剩余分类
    for group, channels in grouped_channels.items():
        if group in GROUP_ORDER:
            continue
        for name in sorted(channels.keys()):
            urls = channels[name]
            for idx, url in enumerate(urls, 1):
                display_name = f"{name} #{idx}" if len(urls) > 1 else name
                lines.append(f'#EXTINF:-1 group-title="{group}",{display_name}')
                lines.append(url)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    logger.info(f"M3U文件已输出: {output_path}")


def output_txt(grouped_channels: Dict[str, Dict[str, List[str]]], output_path: Path):
    """输出TXT格式频道列表（名称,链接）"""
    lines = []
    for group in GROUP_ORDER:
        if group not in grouped_channels:
            continue
        channels = grouped_channels[group]
        if group == "央视频道":
            sorted_names = [n for n in CCTV_ORDER if n in channels]
            sorted_names += [n for n in channels if n not in CCTV_ORDER]
        else:
            sorted_names = sorted(channels.keys())

        for name in sorted_names:
            for url in channels[name]:
                lines.append(f"{name},{url}")

    for group, channels in grouped_channels.items():
        if group in GROUP_ORDER:
            continue
        for name in sorted(channels.keys()):
            for url in channels[name]:
                lines.append(f"{name},{url}")

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    logger.info(f"TXT文件已输出: {output_path}")


# ############################################################################
# 主程序入口
# ############################################################################
async def main():
    parser = argparse.ArgumentParser(description="IPTV源爬取与测速工具 - GitHub Actions适配版")
    parser.add_argument("--no-scrape", action="store_true", help="跳过网站爬取")
    parser.add_argument("--no-github", action="store_true", help="跳过GitHub源下载")
    parser.add_argument("--no-speedtest", action="store_true", help="跳过测速环节")
    args = parser.parse_args()

    all_channels = defaultdict(list)

    # 1. 爬取网站源
    if not args.no_scrape:
        try:
            web_channels = await scrape_iptv_sources()
            for name, urls in web_channels.items():
                all_channels[name].extend(urls)
        except Exception as e:
            logger.error(f"网站爬取失败: {e}")

    # 2. 下载GitHub公共源
    if not args.no_github:
        try:
            github_channels = await fetch_github_sources()
            for name, urls in github_channels.items():
                all_channels[name].extend(urls)
        except Exception as e:
            logger.error(f"GitHub源下载失败: {e}")

    if not all_channels:
        logger.error("未获取到任何频道数据，程序退出")
        return

    # 全局去重
    for name in all_channels:
        all_channels[name] = list(dict.fromkeys(all_channels[name]))

    total_links = sum(len(urls) for urls in all_channels.values())
    logger.info(f"全部源汇总完成：共 {len(all_channels)} 个频道，{total_links} 条链接")

    # 3. 批量测速
    speed_results = {}
    if not args.no_speedtest and ENABLE_FFMPEG:
        all_urls = []
        for urls in all_channels.values():
            all_urls.extend(urls)
        all_urls = list(dict.fromkeys(all_urls))

        speed_results = await batch_ffmpeg_test(all_urls)
        logger.info(f"测速完成，共获得 {len(speed_results)} 条有效测速结果")

    # 4. 排序与筛选
    sorted_channels = sort_channels(all_channels, speed_results)
    grouped_channels = group_channels(sorted_channels)

    # 5. 输出结果文件
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_m3u(grouped_channels, OUTPUT_M3U)
    output_txt(grouped_channels, OUTPUT_TXT)

    logger.info("全部任务执行完成")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("用户中断程序")
    except Exception as e:
        logger.error(f"程序运行异常: {e}", exc_info=True)
        sys.exit(1)
