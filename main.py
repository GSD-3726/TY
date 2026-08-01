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
# 网页爬取 配置区域 (保持不变)
# ############################################################################
TARGET_URL = "https://iptv.cqshushu.com/index.php"
DEFAULT_PROTOCOL = "http://"
IPS_PER_PAGE = 10
MAX_PAGES = 10
MAX_LINKS_PER_CHANNEL = 8
MAX_IPS = 0
MAX_DETAIL_PAGES = 30
DETAIL_PAGE_TIMEOUT = 30000
DETAIL_IDLE_TIMEOUT = 5000
DETAIL_MAX_SECONDS = 60
DETAIL_PAGE_DELAY_MIN = 1.0
DETAIL_PAGE_DELAY_MAX = 2.0
IP_MAX_SECONDS = 10
PAGE_DELAY_MIN = 5.0
PAGE_DELAY_MAX = 8.0
IP_DELAY_MIN = 2.0
IP_DELAY_MAX = 4.0
DETAIL_WAIT_MIN = 2.0
DETAIL_WAIT_MAX = 4.0
HEADLESS = True
CHROME_PATH = ""
PAGE_TIMEOUT = 60000
IDLE_TIMEOUT = 15000
SCRAPE_SOURCE_FILTER = "hotel"

# ############################################################################
# FFmpeg测速 配置区域 (两阶段优化版 - 适配2核环境)
# ############################################################################
ENABLE_FFMPEG = True
FFMPEG_PATH = "ffmpeg"

# 两阶段测速参数
FAST_TEST_DURATION = 3          # 快速筛查时长（秒）
PRECISE_TEST_DURATION = 6       # 精准测速时长（秒），缩短至6秒
FAST_PROC_TIMEOUT = 6           # 快速阶段超时
PRECISE_PROC_TIMEOUT = 10       # 精准阶段超时，匹配6秒时长

# 并发控制（精准并发降至6，避免CPU抢占）
FFMPEG_FAST_CONCURRENCY = 25
FFMPEG_PRECISE_CONCURRENCY = 6
FFMPEG_CONCURRENCY_ADAPTIVE = False  # 关闭自适应，固定
FFMPEG_MAX_CONCURRENCY = 30

FFMPEG_RETRIES = 1  # 临界源重试一次

# 精准判定阈值（适当放宽，减少环境误杀）
MIN_AVG_FPS = 20
MIN_FRAMES_RATIO = 0.80
MIN_AVG_SPEED = 0.80
MIN_LAST_SPEED = 0.75
MIN_NET_FEED_RATIO = 0.80
MAX_ERROR_RATIO = 0.02
MAX_STARTUP_TIME = 4.0

# ############################################################################
# 连通性测试 (默认关闭)
# ############################################################################
ENABLE_CONNECTIVITY = False
CONN_CONCURRENCY = 15
CONN_TIMEOUT = 2

# ############################################################################
# 缓存与GitHub源 (保持不变)
# ############################################################################
ENABLE_CACHE = True
CACHE_FILE = Path(__file__).parent / "iptv_speed_cache.json"
CACHE_EXPIRE_HOURS = 6
CACHE_EXPIRE_SEC = CACHE_EXPIRE_HOURS * 3600
ENABLE_GITHUB = True
GITHUB_URLS = [
    "https://gh-proxy.com/https://github.com/vbskycn/iptv/blob/master/tv/iptv4.txt",
    "https://gh-proxy.com/https://github.com/GSD-3726/TY/blob/main/iptv_channels.txt",
    "https://gh.927223.xyz/https://raw.githubusercontent.com/develop202/migu_video/refs/heads/main/interface.txt",
    "https://gh-proxy.com/https://github.com/GSD-3726/MMM/blob/main/iptv_channels.txt",
]
GITHUB_TIMEOUT = 30
GITHUB_RETRIES = 3

# ############################################################################
# 输出与分类 (保持不变)
# ############################################################################
OUTPUT_DIR = Path(__file__).parent
OUTPUT_M3U = OUTPUT_DIR / "iptv_channels.m3u"
OUTPUT_TXT = OUTPUT_DIR / "iptv_channels.txt"

CATEGORY_RULES = [
    {"name": "央视频道", "keywords": ["cctv", "cetv", "央视"]},
    {"name": "卫视频道", "keywords": ["卫视"]},
    {"name": "影视频道", "keywords": ["影视", "影院", "chc", "剧场", "电影"]},
    {"name": "少儿频道", "keywords": ["少儿", "卡通", "动画", "动漫"]},
    {"name": "地方频道", "keywords": ["地方", "都市", "综合", "新闻", "公共"]},
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
CLEAR_SUFFIX_RE = re.compile(r'[\s\-_]*(高清|超清|4K|超高清|标清|HD|FHD|UHD|2K|蓝光|原画|流畅|720P|1080P|2160P)', re.IGNORECASE)

STEALTH_JS = """ // ... (同v5，内容不变，省略) ... """

# 日志系统 (同v5)
class BJFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.datetime.fromtimestamp(record.created, datetime.timezone(datetime.timedelta(hours=8)))
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
# 工具函数 (保持不变)
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
    bar = '█' * (pct // 5) + '-' * (20 - pct // 5)
    logger.info(f"({pct}%) {bar} ({cur}/{total}) 成功：{ok} 失败：{fail}")
    sys.stdout.flush()
    return pct

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
# 缓存管理 (保持不变)
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
# 两阶段测速核心逻辑 (新增，替换原有单次测速)
# ############################################################################
RE_FRAME = re.compile(r'frame=\s*(\d+)')
RE_SPEED = re.compile(r'speed=\s*([\d.]+)x')
RE_VIDEO_RES = re.compile(r'Video:.*?(\d{3,})x(\d{3,})', re.IGNORECASE)
RE_TIME = re.compile(r'time=(\d+):(\d+):([\d.]+)')
RE_ERROR = re.compile(r'(overrun|corrupt|missing|error while decoding|Invalid data found|PES packet size mismatch)', re.IGNORECASE)

def parse_ffmpeg_time(time_str_h: str, time_str_m: str, time_str_s: str) -> float:
    try:
        return int(time_str_h) * 3600 + int(time_str_m) * 60 + float(time_str_s)
    except (ValueError, TypeError):
        return 0.0

async def _run_ffmpeg_test(url: str, duration: int, timeout: int) -> dict:
    """单次FFmpeg测速，返回详细指标"""
    if not shutil.which(FFMPEG_PATH):
        return {"ok": False, "fps": 0.0, "frames": 0, "width": 0, "height": 0,
                "speed": 0.0, "avg_speed": 0.0, "startup_time": 999.0,
                "error_count": 999, "net_feed_ratio": 0.0, "msg": "ffmpeg not found"}

    cmd = [
        FFMPEG_PATH, "-y",
        "-timeout", str(int(timeout * 1_000_000)),
        "-rw_timeout", str(int(timeout * 1_000_000)),
        "-i", url,
        "-t", str(duration),
        "-an", "-sn",
        "-f", "null", "-"
    ]

    start_time = time.time()
    frames = 0
    last_speed = 0.0
    speed_list = []
    error_count = 0
    width = height = 0
    startup_time = 999.0
    last_play_time = 0.0
    got_first_frame = False

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
            limit=1024 * 1024
        )
        try:
            async with asyncio.timeout(timeout):
                stderr_data = await proc.stderr.read()
                await proc.wait()
        except asyncio.TimeoutError:
            try:
                proc.terminate()
                await asyncio.sleep(0.5)
                if proc.returncode is None:
                    proc.kill()
                    await proc.wait()
            except:
                pass
            return {"ok": False, "fps": 0.0, "frames": frames, "width": width, "height": height,
                    "speed": 0.0, "avg_speed": 0.0, "startup_time": startup_time,
                    "error_count": error_count, "net_feed_ratio": 0.0, "msg": "process timeout"}

        stderr_text = stderr_data.decode("utf-8", errors="ignore")
        elapsed = time.time() - start_time

        res_match = RE_VIDEO_RES.search(stderr_text)
        if res_match:
            width, height = int(res_match.group(1)), int(res_match.group(2))

        lines = stderr_text.splitlines()
        for line in lines:
            if RE_ERROR.search(line):
                error_count += 1
                continue
            frame_match = RE_FRAME.search(line)
            if frame_match:
                frames = int(frame_match.group(1))
                if not got_first_frame and frames > 0:
                    startup_time = time.time() - start_time
                    got_first_frame = True
            speed_match = RE_SPEED.search(line)
            if speed_match:
                last_speed = float(speed_match.group(1))
                speed_list.append(last_speed)
            time_match = RE_TIME.search(line)
            if time_match:
                last_play_time = parse_ffmpeg_time(
                    time_match.group(1), time_match.group(2), time_match.group(3)
                )

        avg_speed = sum(speed_list) / len(speed_list) if speed_list else 0.0
        avg_fps = frames / duration if duration > 0 else 0.0
        net_feed_ratio = last_play_time / elapsed if elapsed > 0 else 0.0
        frames_ratio = frames / (duration * 25) if duration > 0 else 0.0
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
        return {"ok": False, "fps": 0.0, "frames": 0, "width": 0, "height": 0,
                "speed": 0.0, "avg_speed": 0.0, "startup_time": 999.0,
                "error_count": 999, "net_feed_ratio": 0.0, "msg": str(e)}

def is_quality_pass(result: dict) -> bool:
    """精准阶段质量合格判定（7维度）"""
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
    """批量两阶段测速，适配2核环境"""
    if not ENABLE_FFMPEG or not urls:
        return {}

    cache = load_cache()
    total = len(urls)
    logger.info(f"开始FFmpeg两阶段测速，共 {total} 条链接")

    # 第一阶段：快速筛查
    logger.info(f"【第一阶段】快速筛查，并发 {FFMPEG_FAST_CONCURRENCY}，单条最长 {FAST_PROC_TIMEOUT}s")
    fast_sem = asyncio.Semaphore(FFMPEG_FAST_CONCURRENCY)
    fast_results = {}
    ok_count = fail_count = 0
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
    logger.info(f"快速筛查完成：通过 {len(passed_urls)} 条，淘汰 {total - len(passed_urls)} 条")

    if not passed_urls:
        return {}

    # 第二阶段：精准测速（低并发）
    logger.info(f"【第二阶段】精准测速，并发 {FFMPEG_PRECISE_CONCURRENCY}，单条最长 {PRECISE_PROC_TIMEOUT}s")
    precise_sem = asyncio.Semaphore(FFMPEG_PRECISE_CONCURRENCY)
    final_results = {}
    ok_count = fail_count = 0
    last_pct = -1
    total_precise = len(passed_urls)

    async def _precise_worker(url):
        nonlocal ok_count, fail_count, last_pct
        async with precise_sem:
            cached = cache.get(url, {})
            if cached.get("frames", 0) >= PRECISE_TEST_DURATION * 20:
                res = cached
            else:
                res = await _run_ffmpeg_test(url, PRECISE_TEST_DURATION, PRECISE_PROC_TIMEOUT)
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
# GitHub源下载与解析 (保持不变)
# ############################################################################
async def download_github(url: str, session: aiohttp.ClientSession) -> str:
    for attempt in range(1, GITHUB_RETRIES + 1):
        try:
            async with session.get(url, headers={'User-Agent': 'Mozilla/5.0'}) as r:
                if r.status == 200:
                    text = await r.text()
                    if not text or len(text.strip()) < 50:
                        logger.warning(f"GitHub内容过短({len(text)}字符): {url[:80]}")
                        continue
                    if '<html' in text[:500].lower() and '#EXTINF' not in text and ',' not in text[:1000]:
                        logger.warning(f"GitHub返回HTML而非文本数据: {url[:80]}")
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
    all_channels = []
    timeout = aiohttp.ClientTimeout(total=GITHUB_TIMEOUT)
    urls = list(GITHUB_URLS)
    if len(urls) >= 3:
        urls[2] = urls[2].replace('jk=m3u', 'jk=txt')
        logger.info("GitHub-3: 已自动解析为txt格式")
    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = [download_github(url, session) for url in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for i, result in enumerate(results):
            source_name = f"GitHub-{i+1}"
            if isinstance(result, Exception) or not result:
                logger.warning(f"{source_name}: 下载失败")
                continue
            content = result.strip()
            if content.startswith('#EXTM3U') or '#EXTINF' in content:
                channels = parse_m3u_content(content)
            else:
                channels = parse_txt_content(content)
            logger.info(f"{source_name}: 获取 {len(channels)} 个频道")
            all_channels.extend(channels)
    logger.info(f"GitHub 源合计: {len(all_channels)} 条原始链接")
    return all_channels

# ############################################################################
# 网页爬取逻辑 (v5完整版，保持不变)
# ############################################################################
async def scrape_ips_playwright(ctx, filter_type: str, max_pages: int) -> list:
    """使用 Playwright 爬取IP列表（增强容错版）"""
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
            logger.warning(f"[PW] 页面初始化失败，重试 {attempt+1}/5")
            page = None
            await asyncio.sleep(3)
    if page is None or page.is_closed():
        logger.error("[PW] 浏览器页面无法保持打开，放弃爬取")
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
            logger.warning(f"[PW] 查找下一页按钮失败: {e}")
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
            logger.warning(f"[PW] 翻页点击失败: {e}")
            break
        current_page += 1

    logger.info(f"[PW] 共抓取 {len(entries)} 个IP")
    return entries

async def extract_detail_channels_playwright(ctx, detail_url: str) -> list:
    """从详情页获取频道列表（完整流程）"""
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
        if "安全验证" in page_title or "暂时被拒绝" in page_text or "安全验证" in page_text:
            logger.debug(f"[PW] 详情页触发安全验证: {detail_url[:60]}")
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
                logger.debug(f"[PW] 获取频道列表URL: {channel_list_url[:80]}")

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
                logger.debug(f"详情页超时(>{DETAIL_MAX_SECONDS}s)，强制结束: {detail_url[:60]}")
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
                    url = url.replace('&amp;', '&')
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
                logger.debug(f"翻页点击失败: {e}")
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

# ############################################################################
# URL去重 (保持不变)
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
# 导出 (保持不变)
# ############################################################################
def export(ch_map: Dict[Tuple[str, str], List[str]]):
    now = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")
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
# 主流程 (修改测速调用部分)
# ############################################################################
async def main():
    parser = argparse.ArgumentParser(description="IPTV源抓取器 v5 (两阶段测速整合版)")
    parser.add_argument("--type", default="all", help="抓取类型: all/hotel/multicast/migu/other")
    parser.add_argument("--max-pages", type=int, default=MAX_PAGES, help="最大翻页数")
    parser.add_argument("--max-ips", type=int, default=MAX_IPS, help="最大IP数, 0=不限")
    parser.add_argument("--headless", default="true", help="无头模式: true/false")
    parser.add_argument("--skip-ffmpeg", action="store_true", help="跳过FFmpeg测速")
    parser.add_argument("--chrome-path", default="", help="Chrome路径")
    parser.add_argument("--skip-scrape", action="store_true", help="跳过网页抓取")
    parser.add_argument("--skip-github", action="store_true", help="跳过GitHub源")
    args = parser.parse_args()

    config_raw_type = SCRAPE_SOURCE_FILTER
    cmd_raw_type = args.type
    if cmd_raw_type and cmd_raw_type.strip().lower() != "all":
        ft = norm_type(cmd_raw_type)
        logger.info(f"使用命令行指定类型: {ft}")
    else:
        ft = norm_type(config_raw_type)
        logger.info(f"使用配置文件指定类型: {ft}")

    max_pages = args.max_pages
    max_ips = args.max_ips
    headless = args.headless.lower() != "false" if args.headless else HEADLESS
    do_ffmpeg = ENABLE_FFMPEG and not args.skip_ffmpeg
    do_scrape = not args.skip_scrape

    start_time = time.time()
    logger.info("=" * 60)
    logger.info("IPTV 源抓取器 v5 (两阶段测速) 启动")
    logger.info(f" 类型: {ft} | 网页抓取: {'开' if do_scrape else '关'} | GitHub: {'开' if ENABLE_GITHUB and not args.skip_github else '关'} | FFmpeg: {'开' if do_ffmpeg else '关'}")
    logger.info("=" * 60)

    all_channels = []  # (group, name, url)
    github_count = 0
    site_count = 0

    # GitHub源
    if ENABLE_GITHUB and not args.skip_github:
        github_chs = await fetch_github_sources()
        github_count = len(github_chs)
        all_channels.extend(github_chs)

    # 网页抓取 (Playwright)
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
                logger.info(f"Chrome路径: {chrome_path}")
            else:
                logger.info("Chrome路径: Playwright默认")

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
                    logger.info(f"[PW] 共抓取 {len(entries)} 个IP")
                except Exception as e:
                    logger.warning(f"Playwright IP列表抓取失败: {e}")

                if max_ips > 0:
                    entries = entries[:max_ips]

                if entries:
                    logger.info(f"开始获取 {len(entries)} 个IP的详情页频道...")
                    for i, entry in enumerate(entries):
                        try:
                            detail_url = f"{TARGET_URL}?p={entry['hash']}&t={entry['type']}"
                            chs = await extract_detail_channels_playwright(ctx, detail_url)
                            if chs:
                                logger.info(f"[{i+1}/{len(entries)}] {entry['ip']}: {len(chs)} 个频道")
                            for name, url in chs:
                                std_ch = unify_channel_name(name)
                                g = classify(std_ch)
                                if g:
                                    fn = std_ch if g == "央视频道" else clean_cn(std_ch)
                                    all_channels.append((g, fn, url))
                                    site_count += 1
                            await asyncio.sleep(random.uniform(IP_DELAY_MIN, IP_DELAY_MAX))
                        except Exception as e:
                            logger.warning(f"IP {entry['ip']} 处理失败")

                try: await ctx.close()
                except: pass
                try: await browser.close()
                except: pass

        except Exception as e:
            logger.warning(f"Playwright启动失败: {e}")

    # 汇总统计
    logger.info("=" * 60)
    logger.info(f"爬取汇总: GitHub={github_count} 条, 网站={site_count} 条, 合计={github_count + site_count} 条")
    logger.info("=" * 60)

    # 构建频道映射
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

    total_raw = sum(len(v) for v in ch_map.values())
    logger.info(f"去重后: {len(ch_map)} 个频道, {total_raw} 条链接")

    # ---------- FFmpeg测速（使用两阶段测速） ----------
    if do_ffmpeg and ch_map:
        logger.info("--- FFmpeg 两阶段测速 ---")
        ff_start = time.time()
        # 提取所有待测URL
        all_urls = []
        for urls in ch_map.values():
            all_urls.extend(urls)
        all_urls = list(dict.fromkeys(all_urls))  # 全局去重

        # 执行两阶段测速，得到每个URL的结果字典
        speed_results = await batch_ffmpeg_test(all_urls)

        # 根据测速结果对每个频道的链接排序并截取前N条
        sorted_map = {}
        for (g, n), urls in ch_map.items():
            def sort_key(url):
                res = speed_results.get(url, {})
                is_pass = 1 if is_quality_pass(res) else 0
                fps = res.get("fps", 0)
                speed = res.get("avg_speed", 0)
                startup = -res.get("startup_time", 999)
                return (is_pass, fps, speed, startup)
            sorted_urls = sorted(urls, key=sort_key, reverse=True)
            sorted_map[(g, n)] = sorted_urls[:MAX_LINKS_PER_CHANNEL]
        ch_map = sorted_map
        logger.info(f"FFmpeg 测速+筛选完成，耗时: {time.time() - ff_start:.1f}s")

    export(ch_map)

    total_time = time.time() - start_time
    total_valid = sum(len(v) for v in ch_map.values())
    logger.info("=" * 60)
    logger.info("全部完成:")
    logger.info(f" 频道数: {len(ch_map)}")
    logger.info(f" 有效链接: {total_valid}")
    logger.info(f" 总耗时: {total_time:.1f}s")
    logger.info("=" * 60)

if __name__ == "__main__":
    asyncio.run(main())
