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
from urllib.parse import urlparse, urljoin

import aiohttp
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError


# ============================================================================
# ======================== 【中文配置区 - 极速优化版】=========================
# ============================================================================
# 所有可调参数均集中于此，分类整理，方便修改

# -------------------------- 1. 基础设置 ------------------------------------
TARGET_URL            = "https://iptv.809899.xyz"       # 目标网站地址
HEADLESS              = True                            # 无头模式（GitHub运行必须True）
BROWSER_TYPE          = "chromium"                      # 浏览器内核
OUTPUT_DIR            = Path(__file__).parent           # 输出目录（当前脚本目录）
OUTPUT_M3U_FILENAME   = OUTPUT_DIR / "iptv_channels.m3u" # M3U输出文件
OUTPUT_TXT_FILENAME   = OUTPUT_DIR / "iptv_channels.txt" # TXT输出文件
MAX_LINKS_PER_CHANNEL = 8                               # 每个频道最多保留几条链接
DEFAULT_PROTOCOL      = "http://"                        # 默认协议（用于补全链接）

# -------------------------- 2. 爬取控制 ------------------------------------
EXTRACT_MODE          = "酒店提取"                       # "酒店提取" 或 "组播提取"
MAX_IPS               = 100                               # 最多处理多少个IP
MAX_TOTAL_CHANNELS    = 0                                # 总频道上限（0=不限制）
MAX_CHANNELS_PER_IP   = 0                                # 单个IP最多提取频道数
DELAY_BETWEEN_IPS     = 0.05                             # 切换IP间隔（秒）→缩短
DELAY_AFTER_CLICK     = 0.05                             # 点击弹窗后等待（秒）→缩短
MODAL_WAIT_TIMEOUT    = 1                                # 等待模态框出现（秒）→缩短

# -------------------------- 3. 超时与等待 ----------------------------------
PAGE_LOAD_TIMEOUT      = 60                             # 页面加载超时（秒）→缩短
DATA_LOAD_TIMEOUT      = 30                              # 数据加载总超时（秒）→缩短
AFTER_START_WAIT       = 15                              # 点击【开始提取】后等待秒数→缩短
IP_ADDR_TIMEOUT        = 0.1                             # 读取IP地址超时（秒）
CHANNEL_NAME_TIMEOUT   = 0.1                             # 读取频道名称超时（秒）
CHANNEL_URL_TIMEOUT    = 0.1                             # 读取频道链接超时（秒）
SCROLL_TIMEOUT         = 0.1                             # 滚动到元素视野的超时（秒）
CLICK_TIMEOUT          = 0.1                             # 点击元素的超时（秒）
WAIT_FOR_ELEMENT_TIMEOUT = 15                             # wait_for_element默认超时（秒）→缩短
DATA_CHECK_INTERVAL    = 30                              # 数据加载检查间隔（秒）→缩短

# -------------------------- 4. GitHub源订阅 --------------------------------
ENABLE_GITHUB_SOURCES = True                            # 是否启用GitHub源
GITHUB_M3U_LINKS = [
    "https://gh-proxy.com/https://raw.githubusercontent.com/kakaxi-1/IPTV/main/ipv4.txt",
    "https://gh-proxy.com/https://raw.githubusercontent.com/3377/IPTV/master/output/ipv4/result.txt",
    "https://gh-proxy.com/https://raw.githubusercontent.com/vbskycn/iptv/master/tv/iptv4.txt",
    "https://gh-proxy.com/https://raw.githubusercontent.com/best-fan/iptv-sources/main/cn_cctv.m3u8"
]

# -------------------------- 5. FFmpeg测速设置【核心优化】---------------------
ENABLE_FFMPEG_TEST     = True                            # 是否启用FFmpeg测速
FFMPEG_PATH            = "ffmpeg"                        # FFmpeg 程序路径
FFMPEG_TEST_DURATION   = 3                               # 每个链接测试时长（秒）15→3
FFMPEG_CONCURRENCY     = 10                              # 固定高并发 0→10
MIN_AVG_FPS            = 24                              # 最低平均帧率（保证流畅）
MIN_FRAMES             = 60                              # 最低解码帧数 320→60（适配3秒测试）
QUICK_FFMPEG_TEST_DURATION = 0.5                         # 快速预检时长（秒）2→0.5

# -------------------------- 6. 缓存设置 ------------------------------------
ENABLE_CACHE           = True                            # 启用测速缓存
CACHE_FILE             = OUTPUT_DIR / "iptv_speed_cache.json"
CACHE_EXPIRE_HOURS     = 72                              # 缓存过期小时

# -------------------------- 7. 数据处理 ------------------------------------
ENABLE_CHINESE_CLEAN   = True                            # 清理非中文字符
ENABLE_DEDUPLICATION   = True                            # 全局去重
CCTV_USE_MAPPING       = True                            # CCTV映射中文名称
ENABLE_MIGU_FILTER     = True                            # 过滤包含"migu"的链接
SKIP_INTERNAL_IP       = True                            # 跳过内网IP
ENABLE_SATELLITE_CLEAN = True                            # 卫视名称清洗

# -------------------------- 8. 频道分类规则 -------------------------------
CATEGORY_RULES = [
    {"name": "4K专区",      "keywords": ["4k", "4K", "超高清", "2160p"]},
    {"name": "央视频道",    "keywords": ["cctv", "cetv", "央视", "中央", "CCTV", "CETV", "央视频道"]},
    {"name": "卫视频道",    "keywords": ["卫视", "卫视高清"]},
    {"name": "电影频道",    "keywords": ["电影", "影院", "chc", "动作", "剧场", "映画", "影视", "大片", "影视频道"]},
    {"name": "轮播频道",    "keywords": ["轮播", "滚动", "循环"]},
    {"name": "儿童频道",    "keywords": ["少儿", "动画", "动漫", "卡通", "亲子", "儿童", "宝贝"]},
]

GROUP_ORDER = ["央视频道", "卫视频道", "电影频道", "4K专区", "儿童频道", "轮播频道"]

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

# -------------------------- 9. 页面按钮匹配 --------------------------------
PAGE_CONFIG = {
    "engine_search": ["引索搜索", "引擎搜索", "关键词搜索"],
    "hotel":        ["酒店提取"],
    "multicast":    ["组播提取"],
    "start_button": ["开始播放", "开始搜索", "开始提取"],
}

# -------------------------- 10. 日志与更新 --------------------------------
TIME_DISPLAY_AT_TOP    = False
UPDATE_STREAM_URL      = "https://gitee.com/bmg369/tvtest/raw/master/cg/index.m3u8"
ENABLE_VERBOSE_LOGGING = False

# -------------------------- 11. 连通性/预检配置【极速优化】-----------------
CONNECTIVITY_CONCURRENCY = 300                         # 连通性并发 150→300
CONNECTIVITY_TIMEOUT     = 3                            # 超时 5→3

# 全局HTTP会话配置
HTTP_MAX_CONNECTIONS = 500                              # 最大连接数提升
HTTP_MAX_PER_HOST = 20                                  # 单主机并发提升
HTTP_DNS_CACHE_TTL = 300
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"

# ============================================================================
# ============================= 日志配置（北京时间） ===========================
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
    if "cctv5+" in name_lower or "cctv-5+" in name_lower:
        return "CCTV-5+体育赛事" if CCTV_USE_MAPPING else "CCTV-5+"
    
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

# ============================================================================
# ======================== 卫视名称清洗函数 ============================
# ============================================================================
def clean_satellite_name(name: str) -> str:
    if not ENABLE_SATELLITE_CLEAN:
        return name
    
    prefix_pattern = re.compile(
        r'^(移动|高清|HD|超高清|4K|标清|测试)\s*',
        re.IGNORECASE
    )
    name = prefix_pattern.sub('', name)
    
    suffix_pattern = re.compile(
        r'\s*(移动|高清|HD|超高清|4K|标清|测试)\s*$',
        re.IGNORECASE
    )
    name = suffix_pattern.sub('', name)
    
    bracket_pattern = re.compile(
        r'[（(]\s*(移动|高清|HD|超高清|4K|标清|测试)\s*[）)]',
        re.IGNORECASE
    )
    name = bracket_pattern.sub('', name)
    
    return name.strip()

# ============================================================================
# ========================= 缓存管理 ==================================
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
                    if "width" not in data:
                        data["width"] = 0
                        data["height"] = 0
                    if "precheck_ok" not in data:
                        data["precheck_ok"] = data.get("ok", False)
                    valid_cache[url] = data
            elif isinstance(data, (int, float)):
                valid_cache[url] = {
                    "ok": data > 0,
                    "fps": 0.0,
                    "frames": 0,
                    "width": 0,
                    "height": 0,
                    "precheck_ok": data > 0,
                    "timestamp": now
                }
        return valid_cache
    except:
        return {}

def save_cache(cache):
    if not ENABLE_CACHE:
        return
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except:
        pass

def is_cache_valid(timestamp):
    return time.time() - timestamp < CACHE_EXPIRE_SECONDS

# ============================================================================
# ========================= 连通性测试函数 ============================
# ============================================================================
async def check_url_connectivity(url: str, session: aiohttp.ClientSession, timeout: int) -> bool:
    if not url:
        return False
    
    parsed = urlparse(url)
    scheme = parsed.scheme.lower()
    
    if scheme in ('http', 'https'):
        try:
            async with session.head(
                url,
                timeout=aiohttp.ClientTimeout(total=timeout),
                allow_redirects=True,
                headers={"User-Agent": USER_AGENT}
            ) as response:
                return 200 <= response.status < 400
        except:
            try:
                async with session.get(
                    url,
                    timeout=aiohttp.ClientTimeout(total=timeout),
                    allow_redirects=True,
                    headers={"User-Agent": USER_AGENT}
                ) as response:
                    return 200 <= response.status < 400
            except:
                return False
    
    elif scheme in ('rtsp', 'rtmp'):
        try:
            host = parsed.hostname
            port = parsed.port or (554 if scheme == 'rtsp' else 1935)
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port),
                timeout=timeout
            )
            writer.close()
            await writer.wait_closed()
            return True
        except:
            return False
    
    return True

# ============================================================================
# ========================= 重试、进度、FFmpeg测速 =============================
# ============================================================================
def retry_async(max_retries=1, delay=0.5, exceptions=(Exception,)):
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            for attempt in range(1, max_retries+1):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_retries: break
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

@retry_async(max_retries=1, delay=0.3)
async def test_stream_with_ffmpeg(url: str, duration: Optional[int] = None) -> Dict[str, Any]:
    if not shutil.which(FFMPEG_PATH):
        return {"ok": False, "fps": 0.0, "frames": 0, "width": 0, "height": 0, "message": "FFmpeg未安装"}

    headers = f"User-Agent: {USER_AGENT}\r\n"
    if 'migu' in url.lower():
        headers += "Referer: https://www.miguvideo.com/\r\n"

    if duration is None:
        duration = FFMPEG_TEST_DURATION

    # 【极致优化】FFmpeg参数：最小化探测时间，加快校验
    cmd = [
        FFMPEG_PATH, "-hide_banner", "-nostdin", "-y",
        "-headers", headers,
        "-fflags", "nobuffer+fastseek",
        "-rw_timeout", "5000000",
        "-probesize", "500000",       # 缩小探针大小
        "-analyzeduration", "1000000",# 缩短分析时间
        "-i", url,
        "-t", str(duration),
        "-f", "null", "-"
    ]

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE
        )

        try:
            _, stderr = await asyncio.wait_for(proc.communicate(), timeout=duration + 3)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            return {"ok": False, "fps": 0.0, "frames": 0, "width": 0, "height": 0, "message": "超时"}

        output = stderr.decode('utf-8', errors='ignore')
        frame_matches = re.findall(r'frame=\s*(\d+)', output)
        fps_matches = re.findall(r'fps=\s*([\d.]+)', output)
        frames = int(frame_matches[-1]) if frame_matches else 0
        avg_fps = float(fps_matches[-1]) if fps_matches else 0.0

        width, height = 0, 0
        res_match = re.search(r'(\d+)x(\d+)', output)
        if res_match:
            width, height = int(res_match.group(1)), int(res_match.group(2))

        # 适配缩短后的测试时长
        min_frames = MIN_FRAMES if duration > 1 else 15
        is_smooth = frames >= min_frames and avg_fps >= MIN_AVG_FPS and width > 0 and height > 0

        return {
            "ok": is_smooth,
            "fps": avg_fps,
            "frames": frames,
            "width": width,
            "height": height,
        }
    except:
        return {"ok": False, "fps": 0.0, "frames": 0, "width": 0, "height": 0}

async def test_ts_preview(url: str, session: aiohttp.ClientSession, timeout: int = 2) -> Dict[str, Any]:
    if not url:
        return {"precheck_ok": False}

    parsed = urlparse(url)
    if parsed.scheme.lower() not in ('http', 'https'):
        return {"precheck_ok": True}

    try:
        async with session.get(url, headers={"User-Agent": USER_AGENT}, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
            if resp.status != 200:
                return {"precheck_ok": False}
            return {"precheck_ok": True}
    except:
        return {"precheck_ok": False}

async def run_ffmpeg_test(channel_map: Dict[Tuple[str, str], List[str]]) -> Dict[Tuple[str, str], List[str]]:
    if not channel_map:
        return {}

    cache = load_cache() if ENABLE_CACHE else {}
    new_cache = {}
    result_map = defaultdict(list)
    success_counts = defaultdict(int)
    total = sum(len(urls) for urls in channel_map.values())
    pending = []

    # 第一步：加载缓存，过滤已达标频道
    for (g, n), urls in channel_map.items():
        for u in urls:
            # 提前过滤：频道已达标，直接跳过
            if MAX_LINKS_PER_CHANNEL > 0 and success_counts[(g, n)] >= MAX_LINKS_PER_CHANNEL:
                continue
            if SKIP_INTERNAL_IP and is_internal_ip(u):
                continue

            # 缓存命中
            cache_item = cache.get(u)
            if cache_item and is_cache_valid(cache_item.get("timestamp", 0)):
                if cache_item.get("ok"):
                    result_map[(g, n)].append((u, cache_item.get("fps",0), cache_item.get("width",0), cache_item.get("height",0)))
                    success_counts[(g, n)] += 1
                continue

            pending.append((g, n, u))

    logger.info(f"需测速链接: {len(pending)} | 缓存复用: {sum(success_counts.values())}")
    if not pending:
        final = {k: [u for u,_,_,_ in vs[:MAX_LINKS_PER_CHANNEL]] for k, vs in result_map.items()}
        return final

    # 第二步：并发测速（核心：达标即停）
    sem = asyncio.Semaphore(FFMPEG_CONCURRENCY)
    locks = defaultdict(asyncio.Lock)
    connector = aiohttp.TCPConnector(limit=HTTP_MAX_CONNECTIONS, limit_per_host=HTTP_MAX_PER_HOST)
    session = aiohttp.ClientSession(connector=connector)

    async def test_one(item):
        g, n, u = item
        # 双重校验：测速中达标，立即跳过
        if MAX_LINKS_PER_CHANNEL > 0 and success_counts[(g, n)] >= MAX_LINKS_PER_CHANNEL:
            return g, n, u, {"ok": False, "skipped": True}

        async with sem:
            # 快速预检
            pre = await test_ts_preview(u, session)
            if not pre.get("precheck_ok"):
                return g, n, u, {"ok": False}

            # 快速测速
            quick_res = await test_stream_with_ffmpeg(u, QUICK_FFMPEG_TEST_DURATION)
            if quick_res.get("ok"):
                async with locks[(g, n)]:
                    if success_counts[(g, n)] < MAX_LINKS_PER_CHANNEL:
                        success_counts[(g, n)] += 1
                return g, n, u, quick_res

            # 正式测速
            full_res = await test_stream_with_ffmpeg(u, FFMPEG_TEST_DURATION)
            if full_res.get("ok"):
                async with locks[(g, n)]:
                    if success_counts[(g, n)] < MAX_LINKS_PER_CHANNEL:
                        success_counts[(g, n)] += 1
            return g, n, u, full_res

    # 执行任务
    tasks = [test_one(item) for item in pending]
    c, ok, ng, lp = 0, 0, 0, -100
    print_progress_bar(0, len(tasks), ok, ng, lp)

    for coro in asyncio.as_completed(tasks):
        g, n, u, res = await coro
        c += 1
        if res.get("ok") and not res.get("skipped"):
            ok += 1
            result_map[(g, n)].append((u, res.get("fps",0), res.get("width",0), res.get("height",0)))
        else:
            ng += 1

        # 更新缓存
        if ENABLE_CACHE:
            new_cache[u] = {**res, "timestamp": time.time()}

        lp = print_progress_bar(c, len(tasks), ok, ng, lp)

    await session.close()

    # 保存缓存
    if ENABLE_CACHE:
        cache.update(new_cache)
        save_cache(cache)

    # 排序+截断到最大链接数
    final = {}
    for k, vs in result_map.items():
        vs.sort(key=lambda x: (-x[2]*x[3], -x[1]))
        final[k] = [u for u,_,_,_ in vs[:MAX_LINKS_PER_CHANNEL]]

    logger.info(f"测速完成：{len(final)} 个有效频道")
    return final

# ============================================================================
# ========================= GitHub M3U 解析 ===========================
# ============================================================================
@retry_async(max_retries=2, delay=1)
async def download_github_m3u(url, session: aiohttp.ClientSession = None):
    close_session = False
    if session is None:
        connector = aiohttp.TCPConnector(limit=HTTP_MAX_CONNECTIONS, limit_per_host=HTTP_MAX_PER_HOST)
        session = aiohttp.ClientSession(connector=connector, timeout=aiohttp.ClientTimeout(total=10))
        close_session = True
    try:
        async with session.get(url, headers={"User-Agent": USER_AGENT}) as r:
            if r.status == 200:
                return await r.text()
    except:
        pass
    finally:
        if close_session:
            await session.close()
    return ""

def parse_m3u_file(content):
    channels = []
    name = ""
    for line in content.splitlines():
        line = line.strip()
        if line.startswith("#EXTINF"):
            parts = line.split(',', 1)
            if len(parts) == 2:
                name = parts[1].strip()
        elif line.startswith(("http","rtsp","rtmp")):
            url = line.strip()
            if name and url:
                name_cleaned = clean_satellite_name(name)
                normalized = normalize_cctv(name_cleaned)
                group = classify_channel(normalized)
                if group:
                    final_name = normalized if group == "央视频道" else (clean_chinese_only(name_cleaned) if ENABLE_CHINESE_CLEAN else name_cleaned)
                    channels.append((group, final_name, url))
            name = ""
    return channels

def parse_txt_content(content: str, default_group: str = "未分类") -> List[Tuple[str, str, str]]:
    channels = []
    for line in content.splitlines():
        line = line.strip()
        if not line or line.startswith('#') or line.endswith('#genre#'):
            continue
        if ',' in line:
            name, url_part = line.split(',', 1)
            url = url_part.split('$', 1)[0].strip()
            if name and url:
                name_cleaned = clean_satellite_name(name)
                normalized = normalize_cctv(name_cleaned)
                group = classify_channel(normalized)
                if group:
                    final_name = normalized if group == "央视频道" else (clean_chinese_only(name_cleaned) if ENABLE_CHINESE_CLEAN else name_cleaned)
                    channels.append((group, final_name, url))
    return channels

def parse_iptv_txt_file(filepath: Path) -> List[Tuple[str, str, str]]:
    if not filepath.exists():
        return []
    channels = []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.endswith('#genre#'):
                    continue
                if ',' in line:
                    name, url = line.split(',', 1)
                    url = url.split('$', 1)[0].strip()
                    if name and url:
                        name_cleaned = clean_satellite_name(name)
                        normalized = normalize_cctv(name_cleaned)
                        group = classify_channel(normalized)
                        if group:
                            final_name = normalized if group == "央视频道" else (clean_chinese_only(name_cleaned) if ENABLE_CHINESE_CLEAN else name_cleaned)
                            channels.append((group, final_name, url))
    except:
        pass
    return channels

async def robust_click(loc, timeout=CLICK_TIMEOUT):
    try:
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

@retry_async(max_retries=1, delay=0.5)
async def extract_one_ip(page, row, idx):
    entries = []
    try:
        addr = await row.locator("div.item-title").first.inner_text(timeout=IP_ADDR_TIMEOUT*1000)
        addr = addr.strip()
        if not addr:
            return []
    except:
        return []
    try:
        await row.click(timeout=1000)
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
                name_cleaned = clean_satellite_name(n)
                normalized = normalize_cctv(name_cleaned)
                group = classify_channel(normalized)
                if group:
                    final_name = normalized if group == "央视频道" else (clean_chinese_only(name_cleaned) if ENABLE_CHINESE_CLEAN else name_cleaned)
                    entries.append((group, final_name, u))
            except:
                continue
    except:
        pass
    finally:
        try:
            await page.locator(".modal-dialog button.close").first.click(timeout=1000)
        except:
            pass
    return entries

async def wait_data(page):
    logger.info("等待数据加载...")
    async def data_ready():
        return await page.evaluate('''()=>{
            return document.querySelectorAll('div.ios-list-item div.item-subtitle:contains("频道:")').length > 0
        }''')
    if await data_ready():
        return True
    for _ in range(DATA_LOAD_TIMEOUT // DATA_CHECK_INTERVAL + 1):
        await asyncio.sleep(DATA_CHECK_INTERVAL)
        if await data_ready():
            return True
    logger.error("数据加载超时")
    return False

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
            plus_channels = [ch for ch in channels if '5+' in ch[1].lower()]
            chosen = plus_channels[0] if plus_channels else max(channels, key=lambda ch: len(ch[1]))
            url_to_chosen[url] = chosen

    new_map = defaultdict(list)
    for (group, name), urls in channel_map.items():
        for url in urls:
            if url_to_chosen[url] == (group, name):
                new_map[(group, name)].append(url)
    return dict(new_map)

def export_results_with_timestamp(channel_map):
    now = (datetime.datetime.utcnow() + datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")
    gu = UPDATE_STREAM_URL
    g = defaultdict(list)
    for (gr, n), us in channel_map.items():
        for u in us:
            g[gr].append((n, u))
    
    with open(OUTPUT_M3U_FILENAME, 'w', encoding='utf-8') as f:
        f.write("#EXTM3U\n")
        for gro in GROUP_ORDER:
            if gro not in g: continue
            chs = sorted(g[gro], key=lambda x: x[0]) if gro != "央视频道" else g[gro]
            for n, u in chs:
                f.write(f'#EXTINF:-1 group-title="{gro}",{n}\n{u}\n')
        f.write(f'#EXTINF:-1 group-title="更新时间",{now}\n{gu}\n')

    with open(OUTPUT_TXT_FILENAME, 'w', encoding='utf-8') as f:
        for gro in GROUP_ORDER:
            if gro not in g: continue
            f.write(f"{gro},#genre#\n")
            chs = sorted(g[gro], key=lambda x: x[0]) if gro != "央视频道" else g[gro]
            for n, u in chs:
                f.write(f"{n},{u}\n")
        f.write("更新时间,#genre#\n")
        f.write(f"{now},{gu}\n")
    logger.info(f"导出完成：{len(channel_map)} 个频道")

def print_source_statistics(stats):
    logger.info("="*60)
    logger.info("📊 数据源统计")
    logger.info("="*60)
    total_raw = sum(s['raw'] for s in stats["github"]) + stats["web"]["raw"] + stats["local"]["raw"]
    total_output = sum(s['output'] for s in stats["github"]) + stats["web"]["output"] + stats["local"]["output"]
    logger.info(f"总原始链接：{total_raw} | 最终有效：{total_output}")
    logger.info("="*60)

async def main():
    overall_start_time = time.time()
    stats = {"github": [{"raw":0,"valid":0,"output":0} for _ in GITHUB_M3U_LINKS], "web": {"raw":0,"valid":0,"output":0}, "local": {"raw":0,"valid":0,"output":0}}
    all_entries_with_source = []

    if ENABLE_GITHUB_SOURCES:
        logger.info("--- 下载GitHub源 ---")
        connector = aiohttp.TCPConnector(limit=HTTP_MAX_CONNECTIONS, limit_per_host=HTTP_MAX_PER_HOST)
        async with aiohttp.ClientSession(connector=connector, timeout=aiohttp.ClientTimeout(total=10)) as session:
            tasks = [download_github_m3u(url, session) for url in GITHUB_M3U_LINKS]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for idx, res in enumerate(results):
                if isinstance(res, str) and res:
                    channels = parse_m3u_file(res) if '#EXTM3U' in res else parse_txt_content(res)
                    stats["github"][idx]["raw"] = len(channels)
                    all_entries_with_source.extend((g,n,u,"github",idx) for g,n,u in channels)

    # 网站爬取
    web_entries = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS, args=["--no-sandbox"])
        ctx = await browser.new_context(viewport={"width": 1920, "height": 1080})
        page = await ctx.new_page()
        try:
            await page.goto(TARGET_URL, timeout=PAGE_LOAD_TIMEOUT*1000, wait_until="domcontentloaded")
            # 选择模式
            tab_sel = build_selector(PAGE_CONFIG["hotel"] if EXTRACT_MODE=="酒店提取" else PAGE_CONFIG["multicast"], "div.segment-item")
            await robust_click(page.locator(tab_sel).first)
            await asyncio.sleep(0.5)
            # 开始提取
            start_btn = page.locator(build_selector(PAGE_CONFIG["start_button"])).first
            await robust_click(start_btn)
            await asyncio.sleep(AFTER_START_WAIT)

            if await wait_data(page):
                rows = page.locator("div.ios-list-item").filter(has=page.locator("div.item-subtitle:has-text('频道:')"))
                process_count = min(await rows.count(), MAX_IPS)
                for i in range(process_count):
                    entries = await extract_one_ip(page, rows.nth(i), i+1)
                    web_entries.extend(entries)
                    if MAX_TOTAL_CHANNELS and len(web_entries)>=MAX_TOTAL_CHANNELS: break
                    await asyncio.sleep(DELAY_BETWEEN_IPS)
            stats["web"]["raw"] = len(web_entries)
        except:
            logger.error("爬取失败")
        finally:
            await browser.close()

    all_entries_with_source.extend((g,n,u,"web",0) for g,n,u in web_entries)
    # 本地文件
    txt_entries = parse_iptv_txt_file(OUTPUT_DIR/"iptv_channels.txt")
    stats["local"]["raw"] = len(txt_entries)
    all_entries_with_source.extend((g,n,u,"local",0) for g,n,u in txt_entries)

    # 过滤
    if ENABLE_MIGU_FILTER:
        all_entries_with_source = [x for x in all_entries_with_source if 'migu' not in x[2].lower()]
    if SKIP_INTERNAL_IP:
        all_entries_with_source = [x for x in all_entries_with_source if not is_internal_ip(x[2])]

    # 合并去重
    temp_channel_map = defaultdict(list)
    url_source_map = {}
    for g,n,u,t,i in all_entries_with_source:
        temp_channel_map[(g,n)].append(u)
        url_source_map[u] = (t,i)
    if ENABLE_DEDUPLICATION:
        temp_channel_map = deduplicate_urls_per_channel(temp_channel_map)
    # 筛选分类
    temp_channel_map = {k:v for k,v in temp_channel_map.items() if k[0] in GROUP_ORDER}

    # 连通性测试
    unique_urls = list(url_source_map.keys())
    logger.info(f"唯一链接：{len(unique_urls)}")
    connector = aiohttp.TCPConnector(limit=HTTP_MAX_CONNECTIONS, limit_per_host=HTTP_MAX_PER_HOST)
    session = aiohttp.ClientSession(connector=connector)
    sem = asyncio.Semaphore(CONNECTIVITY_CONCURRENCY)
    async def check(u):
        async with sem:
            return u, await check_url_connectivity(u,session,CONNECTIVITY_TIMEOUT)
    tasks = [check(u) for u in unique_urls]
    connected_urls = [u for u,ok in await asyncio.gather(*tasks) if ok]
    await session.close()

    # 构建测速map
    channel_map_for_test = defaultdict(list)
    for url in connected_urls:
        pair = next(((g,n) for (g,n),urls in temp_channel_map.items() if url in urls), None)
        if pair: channel_map_for_test[pair].append(url)

    # FFmpeg测速
    final_channel_map = await run_ffmpeg_test(channel_map_for_test) if ENABLE_FFMPEG_TEST else channel_map_for_test

    # 导出
    export_results_with_timestamp(final_channel_map)
    print_source_statistics(stats)
    logger.info(f"总耗时：{time.time()-overall_start_time:.2f}秒")
    logger.info("🎉 完成！")

if __name__ == "__main__":
    if sys.platform == 'linux':
        asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())
    elif sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
