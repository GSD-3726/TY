import asyncio
import json
import logging
import random
import re
import sys
import argparse
import shutil
import datetime
import os
import time
from collections import defaultdict
from pathlib import Path
from urllib.parse import urlparse
from typing import Dict, List, Tuple, Optional, Any

import aiohttp
from playwright.async_api import async_playwright

# ############################################################################
#                          网页抓取 基础配置
# ############################################################################

TARGET_URL = "https://iptv.cqshushu.com/index.php"
DEFAULT_PROTOCOL = "http://"
IPS_PER_PAGE = 10
MAX_PAGES = 6
MAX_LINKS_PER_CHANNEL = 8
MAX_IPS = 0
MAX_DETAIL_PAGES = 3
DETAIL_PAGE_TIMEOUT = 30000
DETAIL_IDLE_TIMEOUT = 5000
DETAIL_MAX_SECONDS = 45
DETAIL_PAGE_DELAY_MIN = 1.0
DETAIL_PAGE_DELAY_MAX = 2.0
IP_MAX_SECONDS = 10

PAGE_DELAY_MIN = 3.0
PAGE_DELAY_MAX = 5.0
IP_DELAY_MIN = 1.0
IP_DELAY_MAX = 2.0
DETAIL_WAIT_MIN = 2.0
DETAIL_WAIT_MAX = 3.0

HEADLESS = True
CHROME_PATH = ""
PAGE_TIMEOUT = 60000
IDLE_TIMEOUT = 15000

SCRAPE_SOURCE_FILTER = "hotel"

# ############################################################################
#                          三层筛选测速配置 (GitHub Actions 优化)
# ############################################################################

ENABLE_FFMPEG = True
FFMPEG_PATH = "ffmpeg"

# 三层筛选参数
CONN_TIMEOUT = 3.0
CONN_CONCURRENCY = 100
FAST_FFMPEG_DURATION = 6                              # 6秒，给慢启动源留余地
STABLE_FFMPEG_DURATION = 20
FAST_PROC_TIMEOUT = 12                                # 匹配6秒+余量
STABLE_PROC_TIMEOUT = 30
FAST_FFMPEG_CONCURRENCY = 30
STABLE_FFMPEG_CONCURRENCY = 15

FFMPEG_RETRIES = 1

# 稳定测试通过阈值 (20秒标准)
MIN_AVG_FPS = 15
MIN_FRAMES = 250
MIN_REALTIME_FACTOR = 0.60
MIN_NET_FEED_RATIO = 0.65

# ############################################################################
#                          简单连通性测试配置
# ############################################################################

ENABLE_CONNECTIVITY = True
CONN_TEST_TIMEOUT = 3.0

# ############################################################################
#                          缓存 相关配置
# ############################################################################

ENABLE_CACHE = True
CACHE_FILE = Path(__file__).parent / "iptv_speed_cache.json"
CACHE_EXPIRE_HOURS = 6
CACHE_EXPIRE_SEC = CACHE_EXPIRE_HOURS * 3600

# ############################################################################
#                          GitHub 源配置
# ############################################################################

ENABLE_GITHUB = True
GITHUB_URLS = [
    "https://gh-proxy.com/https://github.com/vbskycn/iptv/blob/master/tv/iptv4.txt",
    "https://gh-proxy.com/https://github.com/GSD-3726/TY/blob/main/iptv_channels.txt",
    "https://gh-proxy.com/https://github.com/GSD-3726/MMM/blob/main/iptv_channels.txt",
]
GITHUB_TIMEOUT = 30
GITHUB_RETRIES = 3

# ############################################################################
#                          输出配置
# ############################################################################

OUTPUT_DIR = Path(__file__).parent
OUTPUT_M3U = OUTPUT_DIR / "iptv_channels.m3u"
OUTPUT_TXT = OUTPUT_DIR / "iptv_channels.txt"

# ############################################################################
#                          频道分类 相关配置
# ############################################################################

CATEGORY_RULES = [
    {"name": "央视频道", "keywords": ["cctv", "cetv", "央视"]},
    {"name": "卫视频道", "keywords": ["卫视"]},
    {"name": "影视频道", "keywords": ["影视", "影院", "chc", "剧场", "电影"]},
    {"name": "体育频道", "keywords": ["体育", "体坛", "足球", "篮球"]},
    {"name": "少儿频道", "keywords": ["卡通", "动画", "少儿", "金鹰", "动漫"]},
]
GROUP_ORDER = ["央视频道", "卫视频道", "影视频道", "少儿频道"]

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

CLEAR_SUFFIX_RE = re.compile(r'[\s\-_]*(高清|超清|4K|标清|流畅|HD|FHD|UHD|2K|蓝光|原画|画中画|720P|1080P|2160P)', re.IGNORECASE)

STEALTH_JS = """
delete Navigator.prototype.webdriver;
Object.defineProperty(Navigator.prototype, 'webdriver', {
    value: undefined, writable: false, configurable: true
});
if (!window.chrome) window.chrome = {};
window.chrome.runtime = {
    connect: function() { return { onMessage: {addListener:function(){}}, postMessage:function(){}, onDisconnect:{addListener:function(){}} }; },
    sendMessage: function() {},
    onConnect: {addListener:function(){}, removeListener:function(){}, hasListener:function(){return false;}},
    onMessage: {addListener:function(){}, removeListener:function(){}, hasListener:function(){return false;}},
    getURL: function(p) { return 'chrome-extension://invalid/'+p; },
    id: undefined
};
for (let k in window) { if (k.startsWith('cdc_') || k.startsWith('__webdriver') || k.startsWith('__driver') || k.startsWith('__selenium')) delete window[k]; }
const origQuery = window.navigator.permissions.query;
window.navigator.permissions.query = (p) => p.name==='notifications' ? Promise.resolve({state:Notification.permission}) : origQuery(p);
"""

RE_FRAME = re.compile(r'frame=\s*(\d+)')
RE_SPEED = re.compile(r'speed=\s*([\d.]+)x')
RE_VIDEO_RES = re.compile(r'Video:.*?(\d{3,})x(\d{3,})', re.IGNORECASE)
RE_TIME = re.compile(r'time=(\d+):(\d+):([\d.]+)')
# 匹配 FFmpeg 实际 stderr 中常见的错误/异常模式
RE_ERROR = re.compile(
    r'(overrun|corrupt|missing|error while decoding|Invalid data found|'
    r'PES packet size mismatch|non-existing|timestamp|Connection reset|'
    r'Broken pipe|Server returned 403|404 Not Found|Connection timed out|'
    r'HTTP error|timeout)',
    re.IGNORECASE
)

# ############################################################################
#                          日志
# ############################################################################

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

# ############################################################################
#                          辅助函数
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
    bar = '█' * (pct // 5) + '?' * (20 - pct // 5)
    logger.info(f"[{pct:3d}%] {bar} ({cur}/{total}) 成功:{ok} 失败:{fail}")
    sys.stdout.flush()
    return pct

# ############################################################################
#                          人类行为模拟 (反检测)
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
#                          缓存读写
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
        logger.warning(f"缓存保存失败: {e}")

# ############################################################################
#                          三层筛选测速核心逻辑
# ############################################################################

def parse_ffmpeg_time(time_str_h: str, time_str_m: str, time_str_s: str) -> float:
    try:
        return int(time_str_h) * 3600 + int(time_str_m) * 60 + float(time_str_s)
    except (ValueError, TypeError):
        return 0.0


async def quick_connectivity_test(urls: List[str]) -> List[str]:
    semaphore = asyncio.Semaphore(CONN_CONCURRENCY)
    alive = []
    
    async def _check_one(url: str):
        async with semaphore:
            try:
                timeout = aiohttp.ClientTimeout(total=CONN_TIMEOUT, connect=1.5)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.get(url, headers={
                        "User-Agent": "Mozilla/5.0",
                        "Referer": "https://www.miguvideo.com/"
                    }, read_bufsize=1024) as resp:
                        if resp.status in (200, 206, 301, 302):
                            chunk = await resp.content.read(512)
                            if chunk and b'<html' not in chunk.lower()[:100]:
                                return url
            except Exception:
                pass
            return None
    
    tasks = [asyncio.ensure_future(_check_one(u)) for u in urls]
    for coro in asyncio.as_completed(tasks):
        result = await coro
        if result:
            alive.append(result)
    
    logger.info(f"连通性预检: {len(urls)} -> {len(alive)} 存活")
    return alive


async def fast_ffmpeg_probe(url: str) -> Optional[Dict[str, Any]]:
    """第二层：6秒快速探测。过滤首帧极慢、解码卡顿边缘的源，但保留慢启动优质源"""
    if not shutil.which(FFMPEG_PATH):
        return None
    
    cmd = [
        FFMPEG_PATH, "-hide_banner", "-y",
        "-fflags", "+genpts+nobuffer+discardcorrupt",
        "-rw_timeout", "5000000",
        "-analyzeduration", "800000",
        "-probesize", "800000",
        "-i", url,
        "-t", str(FAST_FFMPEG_DURATION),
        "-f", "null", "-"
    ]
    
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE
        )
        _, stderr = await asyncio.wait_for(proc.communicate(), timeout=FAST_PROC_TIMEOUT)
        output = stderr.decode('utf-8', errors='ignore')
        
        frames = int(RE_FRAME.findall(output)[-1]) if RE_FRAME.findall(output) else 0
        speed_strs = RE_SPEED.findall(output)
        avg_speed = sum(float(s) for s in speed_strs[-3:]) / len(speed_strs[-3:]) if speed_strs else 0
        
        # 门槛: 6秒至少45帧(~7.5fps)，且处理速度≥0.75x
        if frames >= 45 and avg_speed >= 0.75:
            w, h = 0, 0
            vm = RE_VIDEO_RES.search(output)
            if vm:
                w, h = int(vm.group(1)), int(vm.group(2))
            return {
                "pass": True, "frames": frames, "speed": avg_speed,
                "width": w, "height": h, "output": output
            }
    except Exception:
        pass
    return None


async def stable_ffmpeg_test(url: str) -> Dict[str, Any]:
    """第三层：20秒稳定测试。精准过滤周期性抖动与后置限流源"""
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
        "-analyzeduration", "1000000",
        "-probesize", "1000000",
        "-rw_timeout", "8000000",
        "-i", url,
        "-t", str(STABLE_FFMPEG_DURATION),
        "-f", "null", "-"
    ]
    
    start_time = time.perf_counter()
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE
        )
        _, stderr = await asyncio.wait_for(
            proc.communicate(), timeout=STABLE_PROC_TIMEOUT
        )
        elapsed = time.perf_counter() - start_time
        output = stderr.decode('utf-8', errors='ignore')
        
        has_errors = bool(RE_ERROR.search(output))
        frame_matches = RE_FRAME.findall(output)
        frames = int(frame_matches[-1]) if frame_matches else 0
        
        w, h = 0, 0
        vm = RE_VIDEO_RES.search(output)
        if vm:
            w, h = int(vm.group(1)), int(vm.group(2))
        
        speed_strs = RE_SPEED.findall(output)
        all_speeds = [float(s) for s in speed_strs]
        avg_speed = sum(all_speeds[-3:]) / len(all_speeds[-3:]) if all_speeds else 0
        
        time_matches = RE_TIME.findall(output)
        actual_play_time = STABLE_FFMPEG_DURATION
        if len(time_matches) >= 2:
            first = parse_ffmpeg_time(*time_matches[0])
            last = parse_ffmpeg_time(*time_matches[-1])
            actual_play_time = max(last - first, 0.1)
        
        actual_fps = frames / actual_play_time if actual_play_time > 0 else 0
        startup_offset = 1.5 if elapsed > 3.0 else 0.8
        net_feed_ratio = actual_play_time / (elapsed - startup_offset) if (elapsed - startup_offset) > 0 else 0
        
        stalled = actual_play_time < STABLE_FFMPEG_DURATION * 0.80
        
        # 捕捉测速末尾(稳定播放期)的速度尖峰
        recent_speeds = all_speeds[-5:] if len(all_speeds) >= 5 else all_speeds
        # 若记录不足3条，视为信息不足，不直接判死刑（返回False交由其他指标判断）
        has_stall_spikes = any(s < 0.6 for s in recent_speeds) if len(recent_speeds) >= 3 else False
        
        realtime_factor = actual_fps * avg_speed / 25.0
        
        is_ok = (
            frames >= MIN_FRAMES
            and actual_fps >= MIN_AVG_FPS
            and avg_speed >= 0.75
            and not stalled
            and net_feed_ratio >= MIN_NET_FEED_RATIO
            and elapsed < STABLE_FFMPEG_DURATION * 2.5
            and not has_errors
            and not has_stall_spikes
            and realtime_factor >= MIN_REALTIME_FACTOR
        )
        
        return {
            "ok": is_ok, "fps": round(actual_fps, 2), "frames": frames,
            "width": w, "height": h, "speed": round(avg_speed, 2),
            "elapsed": round(elapsed, 2), "realtime": round(realtime_factor, 3),
            "has_errors": has_errors
        }
    except asyncio.TimeoutError:
        try: proc.kill()
        except: pass
        return {"ok": False, "fps": 0, "frames": 0, "width": 0, "height": 0,
                "speed": 0, "elapsed": 0, "realtime": 0, "has_errors": False}
    except Exception as e:
        logger.debug(f"稳定测试异常 {url[:60]}: {e}")
        return {"ok": False, "fps": 0, "frames": 0, "width": 0, "height": 0,
                "speed": 0, "elapsed": 0, "realtime": 0, "has_errors": True}


def stream_quality_score(item: tuple) -> float:
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
    return fps_score * 0.35 + speed_score * 0.35 + time_score * 0.15 + res_score * 0.15


async def batch_test_pipeline(channel_map: Dict[Tuple[str, str], List[str]]
) -> Dict[Tuple[str, str], List[str]]:
    if not channel_map:
        return {}
    
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
        logger.info(f"全部来自缓存: {cached_ok} 条")
        return _finalize_result(result_map)
    
    logger.info(f"=== 第一层：连通性预检 ({len(all_urls)} 个) ===")
    alive_urls = await quick_connectivity_test(all_urls)
    dead_urls = set(all_urls) - set(alive_urls)
    for u in dead_urls:
        new_cache[u] = {"ok": False, "ts": time.time()}
    
    logger.info(f"=== 第二层：快速 FFmpeg 探测 ({len(alive_urls)} 个, 并发:{FAST_FFMPEG_CONCURRENCY}) ===")
    fast_sem = asyncio.Semaphore(FAST_FFMPEG_CONCURRENCY)
    fast_passed = []
    
    async def _fast_test(url: str):
        async with fast_sem:
            res = await fast_ffmpeg_probe(url)
            return url, res
    
    fast_tasks = [asyncio.ensure_future(_fast_test(u)) for u in alive_urls]
    fast_ok = 0
    for coro in asyncio.as_completed(fast_tasks):
        url, res = await coro
        if res and res["pass"]:
            fast_passed.append((url, res))
            fast_ok += 1
        else:
            new_cache[url] = {"ok": False, "ts": time.time()}
    
    logger.info(f"快速探测通过: {fast_ok}/{len(alive_urls)}")
    
    logger.info(f"=== 第三层：20秒稳定测试 ({len(fast_passed)} 个, 并发:{STABLE_FFMPEG_CONCURRENCY}) ===")
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


def _finalize_result(result_map):
    final = {}
    for k, vs in result_map.items():
        vs.sort(key=stream_quality_score, reverse=True)
        final[k] = [u for u, _, _, _, _, _ in vs[:MAX_LINKS_PER_CHANNEL]]
    logger.info(f"最终有效频道: {len(final)} 个")
    return final

# ############################################################################
#                          GitHub 源下载与解析
# ############################################################################

async def download_github(url: str, session: aiohttp.ClientSession) -> str:
    for attempt in range(1, GITHUB_RETRIES + 1):
        try:
            async with session.get(url, headers={'User-Agent': 'Mozilla/5.0'}) as r:
                if r.status == 200:
                    text = await r.text()
                    if not text or len(text.strip()) < 50:
                        logger.warning(f"GitHub数据过短({len(text)}字符): {url[:80]}")
                        continue
                    if '<html' in text[:500].lower() and '#EXTINF' not in text and ',' not in text[:1000]:
                        logger.warning(f"GitHub返回HTML而非文本: {url[:80]}")
                        continue
                    logger.info(f"GitHub下载成功: {url[:80]} ({len(text)}字符)")
                    return text
                logger.warning(f"GitHub HTTP {r.status}: {url[:80]}")
        except Exception as e:
            logger.warning(f"GitHub下载失败 ({attempt}/{GITHUB_RETRIES}): {e}")
            if attempt < GITHUB_RETRIES:
                await asyncio.sleep(2)
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

async def fetch_github_sources() -> List[Tuple[str, str, str]]:
    if not ENABLE_GITHUB or not GITHUB_URLS:
        return []
    logger.info(f"--- GitHub源下载 ({len(GITHUB_URLS)} 个) ---")
    all_channels = []
    timeout = aiohttp.ClientTimeout(total=GITHUB_TIMEOUT)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = [download_github(url, session) for url in GITHUB_URLS]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for i, result in enumerate(results):
            if isinstance(result, Exception) or not result:
                logger.warning(f"GitHub源{i+1} 下载失败")
                continue
            content = result.strip()
            if content.startswith('#EXTM3U') or '#EXTINF' in content:
                channels = parse_m3u_content(content)
            else:
                channels = parse_txt_content(content)
            logger.info(f"GitHub源{i+1}: 解析出 {len(channels)} 个频道")
            all_channels.extend(channels)
    logger.info(f"GitHub源合计: {len(all_channels)} 个")
    return all_channels

# ############################################################################
#                          网页抓取逻辑 (Playwright)
# ############################################################################

async def _is_page_alive(page) -> bool:
    """检查页面对象是否仍然可用"""
    try:
        await page.evaluate("1")
        return True
    except Exception:
        return False


async def _recover_page(ctx, old_page):
    """关闭失效页面，从同一上下文创建新页面"""
    try:
        await old_page.close()
    except Exception:
        pass
    new_page = await ctx.new_page()
    await new_page.add_init_script(STEALTH_JS)
    return new_page


async def scrape_ips_playwright(page, ctx, filter_type: str, max_pages: int) -> tuple:
    """
    返回 (entries, page) — page 在恢复时可能被替换。
    """
    entries = []
    seen = set()

    if filter_type != "all":
        target_url = f"{TARGET_URL}?t={filter_type}&province=all&limit={IPS_PER_PAGE}"
    else:
        target_url = f"{TARGET_URL}?province=all&limit={IPS_PER_PAGE}"

    filter_applied = False
    for _attempt in range(5):
        if not await _is_page_alive(page):
            logger.warning("[PW] 页面已失效，正在重建...")
            page = await _recover_page(ctx, page)
        try:
            await page.goto(target_url, timeout=PAGE_TIMEOUT, wait_until="commit")
        except Exception as e:
            err_str = str(e).lower()
            if "closed" in err_str or "target" in err_str:
                logger.warning(f"[PW] 页面关闭异常，尝试恢复... ({_attempt+1}/5)")
                page = await _recover_page(ctx, page)
                await asyncio.sleep(3)
                continue
            logger.info(f"[PW] 页面加载超时，重试 {_attempt+1}/5...")
            await asyncio.sleep(3)
            continue
        await asyncio.sleep(random.uniform(3, 5))
        try:
            await page.wait_for_load_state("networkidle", timeout=IDLE_TIMEOUT)
        except:
            pass
        if filter_type != "all":
            try:
                current_filter = await page.evaluate("() => document.querySelector('#typeSelect')?.value")
            except Exception:
                page = await _recover_page(ctx, page)
                continue
            if current_filter == filter_type:
                filter_applied = True
                break
            else:
                logger.info(f"[PW] 筛选未生效(当前值={current_filter})，重试 {_attempt+1}/5...")
                await asyncio.sleep(random.uniform(2, 4))
        else:
            filter_applied = True
            break
    if not filter_applied:
        logger.warning(f"[PW] 筛选过滤未生效，使用全部数据+客户端过滤")

    current_page = 1
    while current_page <= max_pages:
        if not await _is_page_alive(page):
            logger.warning(f"[PW] 第{current_page}页前页面失效，正在恢复...")
            page = await _recover_page(ctx, page)
            try:
                await page.goto(target_url, timeout=PAGE_TIMEOUT, wait_until="commit")
                await asyncio.sleep(random.uniform(3, 5))
                try:
                    await page.wait_for_load_state("networkidle", timeout=IDLE_TIMEOUT)
                except:
                    pass
            except Exception as e:
                logger.warning(f"[PW] 恢复后重新加载失败: {e}")
                break

        logger.info(f"[PW] 正在抓取第 {current_page} 页...")
        await human_scroll(page)
        await random_mouse(page)

        try:
            page_entries = await page.evaluate(r"""
            () => {
                const rows = document.querySelectorAll('table.iptv-table tbody tr');
                return Array.from(rows).map(row => {
                    const cells = row.querySelectorAll('td');
                    if (cells.length < 6) return null;
                    const a = cells[0].querySelector('a');
                    if (!a) return null;
                    const onclick = a.getAttribute('onclick') || '';
                    const m = onclick.match(/gotoIP\('([^']+)',\s*'([^']+)'\)/);
                    return {
                        ip: a.innerText.trim(),
                        hash: m ? m[1] : '',
                        type: m ? m[2] : '',
                        channel_count: cells[1].innerText.trim(),
                        type_info: cells[2].innerText.trim(),
                        online_time: cells[3].innerText.trim(),
                        update_time: cells[4].innerText.trim(),
                        status: cells[5].innerText.trim()
                    };
                }).filter(x => x && x.ip && x.hash);
            }
        """)

        except Exception as e:
            err_str = str(e).lower()
            if "closed" in err_str or "target" in err_str:
                logger.warning(f"[PW] 第{current_page}页执行中页面关闭，尝试恢复...")
                page = await _recover_page(ctx, page)
                try:
                    await page.goto(target_url, timeout=PAGE_TIMEOUT, wait_until="commit")
                    await asyncio.sleep(random.uniform(3, 5))
                    try:
                        await page.wait_for_load_state("networkidle", timeout=IDLE_TIMEOUT)
                    except:
                        pass
                except:
                    pass
                continue
            else:
                logger.warning(f"[PW] 第{current_page}页执行异常: {e}")
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

        logger.info(f"[PW] 本页新增 {new_count} 个 (累计 {len(entries)} 个)")

        if new_count == 0 and current_page > 1:
            break

        try:
            nxt = await page.query_selector('a:has-text("下一页")')
        except Exception:
            logger.warning("[PW] 查找翻页按钮时页面异常")
            break
        if not nxt:
            break
        href = await nxt.get_attribute('href') or ''
        if 'page=' not in href:
            break

        delay = random.uniform(PAGE_DELAY_MIN, PAGE_DELAY_MAX)
        logger.info(f"[PW] 等待 {delay:.1f}s 翻页...")
        await asyncio.sleep(delay)
        try:
            await nxt.click()
        except Exception as e:
            err_str = str(e).lower()
            if "closed" in err_str or "target" in err_str:
                logger.warning("[PW] 翻页点击时页面关闭，尝试恢复...")
                page = await _recover_page(ctx, page)
                try:
                    await page.goto(target_url, timeout=PAGE_TIMEOUT, wait_until="commit")
                    await asyncio.sleep(random.uniform(3, 5))
                    try:
                        await page.wait_for_load_state("networkidle", timeout=IDLE_TIMEOUT)
                    except:
                        pass
                except:
                    pass
                continue
            else:
                logger.warning(f"[PW] 翻页点击异常: {e}")
                break
        try:
            await page.wait_for_load_state("networkidle", timeout=IDLE_TIMEOUT)
        except:
            pass
        await asyncio.sleep(random.uniform(PAGE_DELAY_MIN, PAGE_DELAY_MAX))
        current_page += 1

    return entries, page


async def extract_detail_channels_playwright(page, detail_url: str) -> list:
    channels = []
    start_time = time.perf_counter()

    def is_overtime():
        return time.perf_counter() - start_time > DETAIL_MAX_SECONDS

    try:
        await page.goto(detail_url, timeout=DETAIL_PAGE_TIMEOUT, wait_until="commit")
        await asyncio.sleep(random.uniform(DETAIL_WAIT_MIN, DETAIL_WAIT_MAX))
        try:
            await page.wait_for_load_state("networkidle", timeout=DETAIL_IDLE_TIMEOUT)
        except:
            pass
        await asyncio.sleep(random.uniform(1, 2))

        page_title = await page.title()
        page_text = ""
        try:
            page_text = (await page.inner_text("body"))[:200]
        except:
            pass
        if "安全验证" in page_title or "访问被拒绝" in page_text or "安全验证" in page_text:
            logger.debug(f"[PW] 详情页触发安全验证: {detail_url[:60]}")
            return channels

        for sel in ['a:has-text("查看频道列表")', 'a:has-text("频道")']:
            try:
                btn = await page.query_selector(sel)
                if btn:
                    await btn.click()
                    await asyncio.sleep(random.uniform(1.5, 2.5))
                    break
            except:
                pass

        last_url = ""
        last_count = 0
        same_count_times = 0

        for page_num in range(1, MAX_DETAIL_PAGES + 1):
            if is_overtime():
                logger.warning(f"详情页超时(>{DETAIL_MAX_SECONDS}s)强制结束: {detail_url[:60]}")
                break

            current_url = page.url
            if page_num > 1 and current_url == last_url:
                break
            last_url = current_url

            try:
                await page.wait_for_selector("table tbody tr", timeout=DETAIL_IDLE_TIMEOUT)
            except:
                pass

            rows = await page.query_selector_all("table tbody tr")
            page_channels = []
            for row in rows:
                cells = await row.query_selector_all("td")
                if len(cells) >= 3:
                    name = (await cells[1].inner_text()).strip()
                    url_el = await cells[2].query_selector("a")
                    if url_el:
                        url = await url_el.get_attribute("href") or (await cells[2].inner_text()).strip()
                    else:
                        url = (await cells[2].inner_text()).strip()
                    if name and url:
                        url = url.replace('&amp;', '&')
                        if not url.startswith(("http://", "https://")):
                            url = DEFAULT_PROTOCOL + url
                        page_channels.append((name, url))

            if len(page_channels) == 0:
                break
            if len(page_channels) == last_count:
                same_count_times += 1
                if same_count_times >= 2:
                    break
            else:
                same_count_times = 0
            last_count = len(page_channels)
            channels.extend(page_channels)

            if page_num >= MAX_DETAIL_PAGES:
                break

            nxt = await page.query_selector('a:has-text("下一页")')
            if not nxt:
                break
            disabled = await nxt.get_attribute("disabled") or ""
            cls = await nxt.get_attribute("class") or ""
            if disabled or "disabled" in cls:
                break
            href = await nxt.get_attribute("href") or ""
            if "page=" not in href:
                break

            await asyncio.sleep(random.uniform(DETAIL_PAGE_DELAY_MIN, DETAIL_PAGE_DELAY_MAX))
            try:
                await nxt.click()
                try:
                    await page.wait_for_load_state("networkidle", timeout=DETAIL_IDLE_TIMEOUT)
                except:
                    pass
            except Exception as e:
                logger.debug(f"翻页点击失败: {e}")
                break

    except Exception as e:
        logger.debug(f"详情页获取失败: {e}")

    return channels

# ############################################################################
#                          URL去重处理
# ############################################################################

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

# ############################################################################
#                          导出为 M3U/TXT 文件
# ############################################################################

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
                def cctv_sort_key(item):
                    ch_name = item[0]
                    return cctv_weight.get(ch_name, 9999)
                chs_sorted = sorted(chs, key=cctv_sort_key)
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
                def cctv_sort_key(item):
                    ch_name = item[0]
                    return cctv_weight.get(ch_name, 9999)
                chs_sorted = sorted(chs, key=cctv_sort_key)
            else:
                chs_sorted = sorted(chs, key=lambda x: x[0])

            for n, u in chs_sorted:
                if n.strip():
                    f.write(f"{n},{u}\n")
            f.write("\n")
        f.write(f"更新时间,#genre#\n{now},https://example.com\n")

    logger.info(f"导出完成: {len(ch_map)} 个频道")

# ############################################################################
#                          主入口
# ############################################################################

async def main():
    parser = argparse.ArgumentParser(description="IPTV源抓取器 v4 (GitHub Actions优化版)")
    parser.add_argument("--type", default="all", help="抓取类型: all/hotel/multicast/migu/other")
    parser.add_argument("--max-pages", type=int, default=MAX_PAGES, help="最大翻页数")
    parser.add_argument("--max-ips", type=int, default=MAX_IPS, help="最大处理IP数, 0=不限")
    parser.add_argument("--headless", default="true", help="无头模式: true/false")
    parser.add_argument("--skip-ffmpeg", action="store_true", help="跳过FFmpeg测速")
    parser.add_argument("--chrome-path", default="", help="Chrome/Chromium 可执行文件路径")
    parser.add_argument("--skip-scrape", action="store_true", help="跳过网页抓取")
    parser.add_argument("--skip-github", action="store_true", help="跳过GitHub源")
    args = parser.parse_args()

    config_raw_type = SCRAPE_SOURCE_FILTER
    cmd_raw_type = args.type
    if cmd_raw_type and cmd_raw_type.strip().lower() != "all":
        ft = norm_type(cmd_raw_type)
        logger.info(f"使用命令行指定类型: {ft} (覆盖配置 SCRAPE_SOURCE_FILTER={config_raw_type})")
    else:
        ft = norm_type(config_raw_type)
        logger.info(f"使用配置文件指定抓取类型: {ft}")

    max_pages = args.max_pages
    max_ips = args.max_ips
    headless = args.headless.lower() != "false" if args.headless else HEADLESS
    do_ffmpeg = ENABLE_FFMPEG and not args.skip_ffmpeg
    do_scrape = not args.skip_scrape

    start_time = time.time()
    logger.info("=" * 60)
    logger.info("IPTV源抓取器 v4 启动 (GitHub Actions 优化版)")
    logger.info(f"  类型: {ft}")
    logger.info(f"  每页IP: {IPS_PER_PAGE}")
    logger.info(f"  最大页数: {max_pages}")
    logger.info(f"  网页抓取: {'开' if do_scrape else '关'}")
    logger.info(f"  GitHub源: {'开' if ENABLE_GITHUB and not args.skip_github else '关'}")
    logger.info(f"  FFmpeg测速: {'开' if do_ffmpeg else '关'}")
    logger.info(f"  缓存有效期: {CACHE_EXPIRE_HOURS}h")
    logger.info("=" * 60)

    all_channels = []

    if ENABLE_GITHUB and not args.skip_github:
        github_chs = await fetch_github_sources()
        all_channels.extend(github_chs)

    if do_scrape:
        logger.info("--- 开始网页抓取 ---")

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
                logger.info(f"  Chrome路径: {chrome_path}")
            else:
                logger.info("  Chrome路径: Playwright默认")

            async with async_playwright() as p:
                launch_opts = {
                    "headless": headless,
                    "args": [
                        "--no-sandbox", "--disable-setuid-sandbox",
                        "--disable-dev-shm-usage", "--disable-gpu",
                        "--disable-blink-features=AutomationControlled"
                    ]
                }
                if chrome_path:
                    launch_opts["executable_path"] = chrome_path
                try:
                    browser = await p.chromium.launch(**launch_opts)
                except Exception as e:
                    logger.error(f"浏览器启动失败: {e}")
                    raise
                try:
                    ctx = await browser.new_context(
                        viewport={"width": 1920, "height": 1080},
                        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                    )
                    await ctx.add_init_script(STEALTH_JS)
                    page = await ctx.new_page()
                    logger.info("[PW] 浏览器初始化成功")
                except Exception as e:
                    logger.error(f"浏览器上下文/页面创建失败: {e}")
                    try: await browser.close()
                    except: pass
                    raise

                try:
                    entries, page = await scrape_ips_playwright(page, ctx, ft, max_pages)
                    logger.info(f"[PW] 共提取 {len(entries)} 个IP")
                except Exception as e:
                    logger.warning(f"Playwright IP列表获取失败: {e}")
                    entries = []

                if max_ips > 0:
                    entries = entries[:max_ips]
                    logger.info(f"限制为前 {max_ips} 个IP")

                if entries:
                    logger.info(f"开始获取 {len(entries)} 个IP的详情页频道...")
                    for i, entry in enumerate(entries):
                        try:
                            if not await _is_page_alive(page):
                                logger.warning("[PW] 详情页抓取前页面失效，正在恢复...")
                                page = await _recover_page(ctx, page)
                            detail_url = f"{TARGET_URL}?p={entry['hash']}&t={entry['type']}"
                            logger.info(f"[{i + 1}/{len(entries)}] {entry['ip']}")
                            chs = await extract_detail_channels_playwright(page, detail_url)
                            if not chs:
                                logger.debug(f"[PW] {entry['ip']} 详情页无频道数据")
                            for name, url in chs:
                                std_ch = unify_channel_name(name)
                                g = classify(std_ch)
                                if g:
                                    fn = std_ch if g == "央视频道" else clean_cn(std_ch)
                                    all_channels.append((g, fn, url))
                            await asyncio.sleep(random.uniform(IP_DELAY_MIN, IP_DELAY_MAX))
                        except Exception as e:
                            err_str = str(e).lower()
                            if "closed" in err_str or "target" in err_str:
                                logger.warning(f"[PW] IP {entry['ip']} 详情页抓取时页面关闭，正在恢复...")
                                page = await _recover_page(ctx, page)
                            else:
                                logger.warning(f"IP {entry['ip']} 失败: {e}")

                try: await page.close()
                except: pass
                try: await ctx.close()
                except: pass
                try: await browser.close()
                except: pass
        except Exception as e:
            logger.warning(f"Playwright启动失败: {e}")

        logger.info(f"网页抓取完成: {len(all_channels)} 条原始记录")

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

    logger.info(f"去重后: {len(ch_map)} 个频道, {sum(len(v) for v in ch_map.values())} 个链接")

    if do_ffmpeg and ch_map:
        logger.info("--- 三层筛选测速 (6s快速→20s稳定) ---")
        ff_start = time.time()
        ch_map = await batch_test_pipeline(ch_map)
        logger.info(f"测速总耗时: {time.time() - ff_start:.1f}s")

    export(ch_map)

    total_time = time.time() - start_time
    logger.info("=" * 60)
    logger.info("全部完成!")
    logger.info(f"  频道数量: {len(ch_map)}")
    logger.info(f"  链接总数: {sum(len(v) for v in ch_map.values())}")
    logger.info(f"  总耗时: {total_time:.1f}s")
    logger.info("=" * 60)

if __name__ == "__main__":
    asyncio.run(main())
