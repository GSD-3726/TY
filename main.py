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
# 所有可调参数均集中于此，分类整理，方便修改

# -------------------------- 1. 基础设置 ------------------------------------
TARGET_URL            = "https://iptv.809899.xyz"       # 目标网站地址
HEADLESS              = True                            # 无头模式（GitHub运行必须True）
BROWSER_TYPE          = "chromium"                      # 浏览器内核
OUTPUT_DIR            = Path(__file__).parent           # 输出目录（当前脚本目录）
OUTPUT_M3U_FILENAME   = OUTPUT_DIR / "iptv_channels.m3u" # M3U输出文件
OUTPUT_TXT_FILENAME   = OUTPUT_DIR / "iptv_channels.txt" # TXT输出文件
MAX_LINKS_PER_CHANNEL = 10                               # 每个频道最多保留几条链接
DEFAULT_PROTOCOL      = "http://"                        # 默认协议（用于补全链接）

# -------------------------- 2. 爬取控制 ------------------------------------
EXTRACT_MODE          = "组播提取"                       # "酒店提取" 或 "组播提取"
ENABLE_WEB_SCRAPING   = False                            # 是否启用网站爬取
MAX_IPS               = 5                               # 最多处理多少个IP
MAX_TOTAL_CHANNELS    = 0                                # 总频道上限（0=不限制）
MAX_CHANNELS_PER_IP   = 0                                # 单个IP最多提取频道数
DELAY_BETWEEN_IPS     = 0.1                              # 切换IP间隔（秒）
DELAY_AFTER_CLICK     = 0.3                              # 点击弹窗后等待（秒）
MODAL_WAIT_TIMEOUT    = 1                                # 等待模态框出现（秒）

# -------------------------- 3. 超时与等待 ----------------------------------
PAGE_LOAD_TIMEOUT      = 120                             # 页面加载超时（秒）
DATA_LOAD_TIMEOUT      = 60                              # 数据加载总超时（秒）
AFTER_START_WAIT       = 30                              # 点击【开始提取】后等待秒数
IP_ADDR_TIMEOUT        = 0.1                             # 读取IP地址超时（秒）
CHANNEL_NAME_TIMEOUT   = 0.1                             # 读取频道名称超时（秒）
CHANNEL_URL_TIMEOUT    = 0.1                             # 读取频道链接超时（秒）
SCROLL_TIMEOUT         = 0.1                             # 滚动到元素视野的超时（秒）
CLICK_TIMEOUT          = 0.1                             # 点击元素的超时（秒）
WAIT_FOR_ELEMENT_TIMEOUT = 30                             # wait_for_element默认超时（秒）
DATA_CHECK_INTERVAL    = 30                              # 数据加载检查间隔（秒）

# -------------------------- 4. GitHub源订阅 --------------------------------
ENABLE_GITHUB_SOURCES = True                            # 是否启用GitHub源
GITHUB_M3U_LINKS = [
    "https://gh-proxy.com/https://raw.githubusercontent.com/mzky/checklist/refs/heads/master/itvlist.m3u",
    "https://gh-proxy.com/https://raw.githubusercontent.com/ajqubbs/zhiboyuan/main/%E9%A6%99%E9%9B%A8%E7%9B%B4%E6%92%AD.txt",
   # "https://gh-proxy.com/https://raw.githubusercontent.com/fafa002/yf2025/refs/heads/main/yiyifafa.txt",
    "https://gh-proxy.com/https://raw.githubusercontent.com/vinkerq/iptv-api/refs/heads/master/iptv.txt"
]

# -------------------------- 5. FFmpeg测速设置 -------------------------------
ENABLE_FFMPEG_TEST     = True                            # 是否启用FFmpeg测速
FFMPEG_PATH            = "ffmpeg"                        # FFmpeg 程序路径
FFMPEG_TEST_DURATION   = 20                              # 每个链接测试时长（秒）
FFMPEG_CONCURRENCY     = 6                               # 并发测速数量
MIN_AVG_FPS            = 24                              # 最低平均帧率
MIN_FRAMES             = 420                             # 最低解码帧数

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

# -------------------------- 8. 频道分类规则 --------------------------------
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

# -------------------------- 11. 连通性/预检配置 -----------------------------
CONNECTIVITY_CONCURRENCY = 15
CONNECTIVITY_TIMEOUT     = 2

# -------------------------- 12. 增量更新配置 --------------------------------
ENABLE_INCREMENTAL_UPDATE = True
QUALITY_THRESHOLD         = 4
REQUIRED_CHANNELS_FILE    = OUTPUT_DIR / "频道.txt"      # 定义必须包含的频道列表

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
        if not host: return False
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
                    if "width" not in data:
                        data["width"] = 0
                        data["height"] = 0
                    valid_cache[url] = data
            else:
                if isinstance(data, (int, float)):
                    valid_cache[url] = {
                        "ok": data > 0,
                        "fps": 0.0,
                        "frames": 0,
                        "width": 0,
                        "height": 0,
                        "timestamp": now
                    }
        logger.info(f"缓存加载完成，有效条目数: {len(valid_cache)}")
        return valid_cache
    except Exception as e:
        logger.debug(f"加载缓存异常: {e}，将重新创建")
        return {}

def save_cache(cache):
    if not ENABLE_CACHE: return
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning(f"保存缓存失败: {e}")

def is_cache_valid(timestamp):
    return time.time() - timestamp < CACHE_EXPIRE_SECONDS

# ============================================================================
# ========================= 连通性测试函数 ====================================
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
                    if attempt == max_retries: raise
                    logger.warning(f"尝试 {attempt}/{max_retries} 失败: {e}，重试")
                    await asyncio.sleep(delay)
            return None
        return wrapper
    return decorator

def print_progress_bar(current: int, total: int, success: int, failed: int, last_percent: int) -> int:
    if total == 0: return 0
    percent_int = int((current / total) * 100)
    if not ((percent_int % 5 == 0 and percent_int > last_percent) or current == total or current == 0):
        return last_percent
    if percent_int == last_percent and current != total:
        return last_percent
    bar = '█' * int(20 * current / total) + '░' * (20 - int(20 * current / total))
    logger.info(f"[{percent_int:3d}%] {bar} ({current}/{total}) | 成功:{success} | 失败:{failed}")
    sys.stdout.flush()
    return percent_int

async def test_stream_with_ffmpeg(url: str) -> Dict[str, Any]:
    if not shutil.which(FFMPEG_PATH):
        logger.error(f"未找到FFmpeg: {FFMPEG_PATH}")
        return {"ok": False, "fps": 0.0, "frames": 0, "width": 0, "height": 0, "message": "FFmpeg未安装"}

    headers = (
        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36\r\n"
        "Referer: https://www.miguvideo.com/\r\n"
    )
    cmd = [
        FFMPEG_PATH, "-hide_banner", "-y",
        "-headers", headers,
        "-fflags", "nobuffer",
        "-rw_timeout", "5000000",
        "-i", url,
        "-t", str(FFMPEG_TEST_DURATION),
        "-f", "null", "-"
    ]
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE
        )
        try:
            _, stderr = await asyncio.wait_for(proc.communicate(), timeout=FFMPEG_TEST_DURATION + 5)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            return {"ok": False, "fps": 0.0, "frames": 0, "width": 0, "height": 0, "message": "连接超时"}
        output = stderr.decode('utf-8', errors='ignore')
        frame_matches = re.findall(r'frame=\s*(\d+)', output)
        fps_matches = re.findall(r'fps=\s*([\d.]+)', output)
        frames = int(frame_matches[-1]) if frame_matches else 0
        avg_fps = float(fps_matches[-1]) if fps_matches else 0.0
        width, height = 0, 0
        video_matches = re.finditer(r'Stream #0:(\d+).*Video:.*? (\d+)x(\d+)', output, re.IGNORECASE)
        for match in video_matches:
            w = int(match.group(2))
            h = int(match.group(3))
            if w > 0 and h > 0:
                width, height = w, h
                break
        if width == 0 or height == 0:
            generic_match = re.search(r'Video:.*? (\d+)x(\d+)', output, re.IGNORECASE)
            if generic_match:
                width = int(generic_match.group(1))
                height = int(generic_match.group(2))
        is_smooth = frames >= MIN_FRAMES and avg_fps >= MIN_AVG_FPS
        return {
            "ok": is_smooth,
            "fps": avg_fps,
            "frames": frames,
            "width": width,
            "height": height
        }
    except Exception as e:
        return {"ok": False, "fps": 0.0, "frames": 0, "width": 0, "height": 0, "message": f"异常: {str(e)[:50]}"}

async def run_ffmpeg_test(channel_map: Dict[Tuple[str, str], List[str]], 
                          required_map: Optional[Dict[Tuple[str, str], int]] = None) -> Dict[Tuple[str, str], List[str]]:
    if not channel_map:
        return {}
    cache = load_cache() if ENABLE_CACHE else {}
    new_cache = {}
    result_map = defaultdict(list)
    total = sum(len(us) for us in channel_map.values())
    cached_ok = 0
    cached_failed_skipped = 0
    pending = []
    if required_map is None:
        required_map = {k: MAX_LINKS_PER_CHANNEL for k in channel_map}

    for (g, n), us in channel_map.items():
        needed = required_map.get((g, n), 0)
        if needed <= 0:
            continue
        for u in us:
            cache_item = cache.get(u)
            if cache_item and isinstance(cache_item, dict) and "ok" in cache_item:
                if is_cache_valid(cache_item.get("timestamp", 0)):
                    if cache_item["ok"]:
                        result_map[(g, n)].append((
                            u,
                            cache_item.get("fps", 0.0),
                            cache_item.get("width", 0),
                            cache_item.get("height", 0),
                            False
                        ))
                        cached_ok += 1
                        if len(result_map[(g, n)]) >= needed:
                            continue
                    else:
                        cached_failed_skipped += 1
                    continue
            if SKIP_INTERNAL_IP and is_internal_ip(u):
                continue
            pending.append((g, n, u))

    logger.info(f"需要测速的频道数: {sum(1 for v in required_map.values() if v>0)}，总链接: {total} 缓存有效且成功: {cached_ok} 需处理: {len(pending)}")
    if not pending:
        logger.info("没有需要测速的链接，直接返回结果")
        final = {}
        for k, vs in result_map.items():
            vs.sort(key=lambda x: (-x[2]*x[3], -x[1]))
            final[k] = [u for u, _, _, _, _ in vs[:required_map.get(k, MAX_LINKS_PER_CHANNEL)]]
        return final

    sem = asyncio.Semaphore(FFMPEG_CONCURRENCY)
    channel_tasks = defaultdict(list)

    async def test_one(item):
        g, n, u = item
        async with sem:
            try:
                res = await test_stream_with_ffmpeg(u)
                return g, n, u, res
            except asyncio.CancelledError:
                raise

    tasks = []
    for g, n, u in pending:
        t = asyncio.ensure_future(test_one((g, n, u)))
        tasks.append(t)
        channel_tasks[(g, n)].append(t)

    completed_counts = defaultdict(int)
    for k, vs in result_map.items():
        completed_counts[k] = len(vs)

    c, ok, ng, lp = 0, 0, 0, -100
    print_progress_bar(0, len(tasks), ok, ng, lp)

    for coro in asyncio.as_completed(tasks):
        try:
            g, n, u, res = await coro
        except asyncio.CancelledError:
            continue
        except Exception as e:
            logger.error(f"测速任务异常: {e}")
            continue
        c += 1
        if res["ok"]:
            ok += 1
            result_map[(g, n)].append((u, res["fps"], res["width"], res["height"], True))
            completed_counts[(g, n)] += 1
            if completed_counts[(g, n)] >= required_map.get((g, n), MAX_LINKS_PER_CHANNEL):
                for t in channel_tasks[(g, n)]:
                    if not t.done():
                        t.cancel()
                logger.debug(f"频道 {g}-{n} 已满足需求，取消剩余测速任务")
        else:
            ng += 1
        if ENABLE_CACHE:
            new_cache[u] = {
                "ok": res["ok"],
                "fps": res["fps"],
                "frames": res.get("frames", 0),
                "width": res.get("width", 0),
                "height": res.get("height", 0),
                "timestamp": time.time()
            }
        lp = print_progress_bar(c, len(tasks), ok, ng, lp)

    if ENABLE_CACHE and new_cache:
        cache.update(new_cache)
        save_cache(cache)

    final = {}
    for k, vs in result_map.items():
        vs.sort(key=lambda x: (-x[2]*x[3], -x[1]))
        final[k] = [u for u, _, _, _, _ in vs[:required_map.get(k, MAX_LINKS_PER_CHANNEL)]]
    logger.info(f"测速完成，共 {len(final)} 个频道通过筛选")
    return final

# ============================================================================
# ===================== 解析现有M3U文件（用于增量更新） ========================
# ============================================================================
def parse_existing_m3u(filepath: Path) -> Tuple[Dict[Tuple[str, str], List[str]], Dict[str, int]]:
    channels = defaultdict(list)
    url_count = defaultdict(int)
    group = ""
    name = ""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line.startswith('#EXTINF'):
                    m = re.search(r'group-title="([^"]*)",(.+)', line)
                    if m:
                        group = m.group(1).strip()
                        name = m.group(2).strip()
                elif line.startswith('http') and name:
                    url = line
                    channels[(group, name)].append(url)
                    url_count[url] += 1
        logger.info(f"从现有M3U解析到 {len(channels)} 个频道，{sum(len(v) for v in channels.values())} 条链接")
    except Exception as e:
        logger.warning(f"解析现有M3U失败: {e}")
    return channels, url_count

# ============================================================================
# ===================== 解析 频道.txt 文件 ====================================
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
                if not line: continue
                if line.endswith('#genre#'):
                    current_group = line.split(',')[0].strip()
                    continue
                if current_group:
                    required[current_group].add(line)
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
    ch = []
    n = u = ""
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
            n = u = ""
    return ch

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
                        final_name = normalized if group == "央视频道" else (
                            clean_chinese_only(name_cleaned) if ENABLE_CHINESE_CLEAN else name_cleaned
                        )
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
                if not line: continue
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
                            fn = nn if gr == "央视频道" else (clean_chinese_only(name_cleaned) if ENABLE_CHINESE_CLEAN else n_cleaned)
                            channels.append((gr, fn, url))
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
                if not n or not u: continue
                if not u.startswith(('http://', 'https://', 'rtsp://', 'rtmp://')):
                    u = DEFAULT_PROTOCOL + u
                n_cleaned = clean_satellite_name(n)
                nn = normalize_cctv(n_cleaned)
                g = classify_channel(nn)
                if not g: continue
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
                for name, url in chs:
                    name_to_urls[name].append(url)
                name_to_std = {}
                for name in name_to_urls.keys():
                    std = None
                    if name in CCTV_ORDER:
                        std = name
                    else:
                        cctv_match = CCTV_PATTERN.search(name)
                        if cctv_match:
                            num = cctv_match.group(2)
                            for std_candidate in CCTV_ORDER:
                                std_num_match = CCTV_PATTERN.search(std_candidate)
                                if std_num_match:
                                    std_num = std_num_match.group(2)
                                    if num == std_num:
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
                chs = ordered_chs
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
            if gro not in g: continue
            f.write(f"{gro},#genre#\n")
            chs = g[gro]
            if gro == "央视频道":
                name_to_urls = defaultdict(list)
                for name, url in chs:
                    name_to_urls[name].append(url)
                name_to_std = {}
                for name in name_to_urls.keys():
                    std = None
                    if name in CCTV_ORDER:
                        std = name
                    else:
                        cctv_match = CCTV_PATTERN.search(name)
                        if cctv_match:
                            num = cctv_match.group(2)
                            for std_candidate in CCTV_ORDER:
                                std_num_match = CCTV_PATTERN.search(std_candidate)
                                if std_num_match:
                                    std_num = std_num_match.group(2)
                                    if num == std_num:
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
                chs = ordered_chs
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
def print_source_statistics(stats):
    logger.info("="*60)
    logger.info("📊 各数据源统计结果（有效率=连通有效数/原始获取数）")
    logger.info("="*60)
    for i, gh in enumerate(stats["github"]):
        raw = gh["raw"]
        valid = gh["valid"]
        output = gh["output"]
        rate = (valid / raw * 100) if raw > 0 else 0.0
        logger.info(f"GitHub源{i+1} | 原始获取:{raw:4d} | 有效:{valid:4d} | 有效率:{rate:6.1f}% | 最终输出:{output:4d}")
    web = stats["web"]
    raw = web["raw"]
    valid = web["valid"]
    output = web["output"]
    rate = (valid / raw * 100) if raw > 0 else 0.0
    logger.info(f"网站爬取源 | 原始获取:{raw:4d} | 有效:{valid:4d} | 有效率:{rate:6.1f}% | 最终输出:{output:4d}")
    local = stats["local"]
    raw = local["raw"]
    valid = local["valid"]
    output = local["output"]
    rate = (valid / raw * 100) if raw > 0 else 0.0
    logger.info(f"本地TXT源 | 原始获取:{raw:4d} | 有效:{valid:4d} | 有效率:{rate:6.1f}% | 最终输出:{output:4d}")
    if "reused" in stats:
        logger.info(f"🔁 复用旧链接数: {stats['reused']} (来自上次输出文件)")
    logger.info("="*60)

# ============================================================================
# ========================= 主流程 ===========================================
# ============================================================================
async def main():
    overall_start_time = time.time()
    inc_update = ENABLE_INCREMENTAL_UPDATE

    stats = {
        "github": [{"url": url, "raw": 0, "valid": 0, "output": 0} for url in GITHUB_M3U_LINKS],
        "web": {"raw": 0, "valid": 0, "output": 0},
        "local": {"raw": 0, "valid": 0, "output": 0},
        "reused": 0
    }
    all_entries_with_source = []

    if EXTRACT_MODE not in ["酒店提取", "组播提取"]:
        logger.error("配置错误！EXTRACT_MODE 只能填写：酒店提取 或 组播提取")
        return

    logger.info(f"✅ 当前运行模式：【{EXTRACT_MODE}】(网站爬取{'开启' if ENABLE_WEB_SCRAPING else '关闭'}，增量更新{'开启' if inc_update else '关闭'})")

    # ===================== 增量更新：解析现有输出文件 =====================
    old_valid_map = {}
    url_quality = {}
    required_map = {}
    old_all_urls = set()

    if inc_update and OUTPUT_M3U_FILENAME.exists():
        logger.info("--- 启用增量更新，解析现有输出文件 ---")
        existing_channels, url_count = parse_existing_m3u(OUTPUT_M3U_FILENAME)
        if existing_channels:
            # 读取频道.txt 要求的频道列表
            required_channels = parse_required_channels(REQUIRED_CHANNELS_FILE)

            # 检查分组完整性（仅警告，不整体回退）
            existing_groups = {g for (g, _) in existing_channels.keys()}
            required_groups = set(GROUP_ORDER)
            missing_groups = required_groups - existing_groups
            if missing_groups:
                logger.warning(f"旧输出文件缺少分组: {missing_groups}，将对这些分组的频道进行全量更新")
                # 对缺失分组中在频道.txt里定义的频道，全部标记为全量需求
                for group in missing_groups:
                    if group in required_channels:
                        for name in required_channels[group]:
                            required_map[(group, name)] = MAX_LINKS_PER_CHANNEL

            # 检查频道.txt中定义的频道是否缺失（即使分组存在）
            existing_set = {(g, n) for (g, n) in existing_channels}
            missing_channels = []
            for group, names in required_channels.items():
                for name in names:
                    if (group, name) not in existing_set and (group, name) not in required_map:
                        required_map[(group, name)] = MAX_LINKS_PER_CHANNEL
                        missing_channels.append(f"{group}-{name}")
            if missing_channels:
                logger.info(f"频道.txt中要求但旧文件缺失的频道: {missing_channels[:10]}...，将全量更新")

            # 对旧文件中存在的频道进行连通性测试和需求计算
            all_old_urls_list = []
            for urls in existing_channels.values():
                all_old_urls_list.extend(urls)
            unique_old_urls = list(set(all_old_urls_list))
            logger.info(f"旧文件中共 {len(unique_old_urls)} 个唯一链接，开始连通性测试...")

            sem_old = asyncio.Semaphore(CONNECTIVITY_CONCURRENCY)
            async def check_old(url):
                async with sem_old:
                    ok = await check_url_connectivity(url, CONNECTIVITY_TIMEOUT)
                    return url, ok
            tasks_old = [check_old(u) for u in unique_old_urls]
            old_connected = set()
            c, ok, ng, lp = 0, 0, 0, -100
            print_progress_bar(0, len(tasks_old), ok, ng, lp)
            for coro in asyncio.as_completed(tasks_old):
                url, is_ok = await coro
                c += 1
                if is_ok:
                    ok += 1
                    old_connected.add(url)
                else:
                    ng += 1
                lp = print_progress_bar(c, len(tasks_old), ok, ng, lp)
            logger.info(f"旧链接连通性测试完成，有效: {len(old_connected)}")

            for (g, n), urls in existing_channels.items():
                # 如果该频道已因缺失被标记为全量更新，则跳过旧链接分析
                if (g, n) in required_map:
                    continue
                valid_urls = []
                for u in urls:
                    if u in old_connected:
                        is_quality = url_count[u] >= QUALITY_THRESHOLD
                        valid_urls.append((u, is_quality))
                        url_quality[u] = is_quality
                        old_all_urls.add(u)
                if valid_urls:
                    valid_urls.sort(key=lambda x: (not x[1]))  # 优质在前
                    old_valid_map[(g, n)] = valid_urls
                    needed = MAX_LINKS_PER_CHANNEL - len(valid_urls)
                    if needed < 0:
                        needed = 0
                    required_map[(g, n)] = needed
                    if needed > 0:
                        logger.debug(f"频道 {g}-{n} 已有 {len(valid_urls)} 个旧链接，还需 {needed} 个新链接")
                else:
                    # 该频道在旧文件中存在但所有链接失效，需全量补充
                    required_map[(g, n)] = MAX_LINKS_PER_CHANNEL

            logger.info(f"增量分析完成：{len(old_valid_map)} 个频道有旧有效链接，总复用 {sum(len(v) for v in old_valid_map.values())} 条")
        else:
            logger.info("现有输出文件无有效频道，退回全量模式")
            inc_update = False

    if not inc_update:
        logger.info("模式切换为全量更新，所有频道将重新爬取测速")

    # ===================== GitHub源 =====================
    if ENABLE_GITHUB_SOURCES:
        logger.info("--- 正在并发获取 GitHub 源 ---")
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
            tasks = [download_github_m3u(url, session) for url in GITHUB_M3U_LINKS]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for idx, result in enumerate(results):
                link_no = idx + 1
                if isinstance(result, Exception):
                    logger.warning(f"下载 {GITHUB_M3U_LINKS[idx]} 异常: {result}")
                    continue
                if result and isinstance(result, str):
                    content = result.strip()
                    if content.startswith('#EXTM3U') or '#EXTINF' in content:
                        channels = parse_m3u_file(content)
                    else:
                        channels = parse_txt_content(content, default_group="GitHub源")
                    stats["github"][idx]["raw"] = len(channels)
                    logger.info(f"✅ GitHub链接 {link_no} 获取到 {len(channels)} 条频道")
                    added = 0
                    for g, n, u in channels:
                        if inc_update and u in old_all_urls:
                            continue
                        all_entries_with_source.append((g, n, u, "github", idx))
                        added += 1
                    if added < len(channels):
                        logger.debug(f"GitHub源{link_no} 过滤旧链接 {len(channels)-added} 条")
                else:
                    logger.info(f"✅ GitHub链接 {link_no} 获取到 0 条频道")
            logger.info(f"GitHub源累计获取: {sum(s['raw'] for s in stats['github'])} 条，去重后新增: {len([e for e in all_entries_with_source if e[3]=='github'])} 条")

    # ===================== 网站爬取 =====================
    web_entries = []
    if ENABLE_WEB_SCRAPING:
        logger.info("--- 网站爬取已启用，正在启动浏览器 ---")
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=HEADLESS,
                args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage", "--disable-gpu", "--single-process"]
            )
            ctx = await browser.new_context(viewport={"width": 1920, "height": 1080})
            page = await ctx.new_page()
            try:
                logger.info(f"--- 正在爬取网站: {TARGET_URL} ---")
                await page.goto(TARGET_URL, timeout=PAGE_LOAD_TIMEOUT*1000, wait_until="domcontentloaded")
                eng_sel = build_selector(PAGE_CONFIG["engine_search"], "a.sidebar-link,button,div.segment-item")
                if eng_sel:
                    eng = page.locator(eng_sel).first
                    if await eng.count() > 0:
                        logger.info("点击引擎搜索")
                        await robust_click(eng)
                if EXTRACT_MODE == "酒店提取":
                    tab_sel = build_selector(PAGE_CONFIG["hotel"], "div.segment-item")
                else:
                    tab_sel = build_selector(PAGE_CONFIG["multicast"], "div.segment-item")
                tab = page.locator(tab_sel).first
                await robust_click(tab)
                start_sel = build_selector(PAGE_CONFIG["start_button"], "button")
                start_btn = page.locator(start_sel).first
                logger.info("点击【开始提取】")
                await robust_click(start_btn)
                logger.info(f"⏳ 等待 {AFTER_START_WAIT} 秒后开始提取数据...")
                await asyncio.sleep(AFTER_START_WAIT)
                if await wait_data(page):
                    rows = page.locator("div.ios-list-item").filter(
                        has=page.locator("div.item-subtitle:has-text('频道:')")
                    )
                    total_rows = await rows.count()
                    process_count = min(total_rows, MAX_IPS) if MAX_IPS > 0 else total_rows
                    logger.info(f"找到 {total_rows} 个IP，准备处理前 {process_count} 个")
                    for i in range(process_count):
                        entries = await extract_one_ip(page, rows.nth(i), i + 1)
                        if entries:
                            web_entries.extend(entries)
                        if MAX_TOTAL_CHANNELS > 0 and len(web_entries) >= MAX_TOTAL_CHANNELS:
                            web_entries = web_entries[:MAX_TOTAL_CHANNELS]
                            break
                        await asyncio.sleep(DELAY_BETWEEN_IPS)
                    stats["web"]["raw"] = len(web_entries)
                    logger.info(f"网站爬取完成: {len(web_entries)} 条")
            except Exception as e:
                logger.exception("❌ 爬取过程异常")
            finally:
                await page.close()
                await ctx.close()
                await browser.close()
    else:
        logger.info("--- 网站爬取已关闭，跳过此步骤 ---")

    web_added = 0
    for g, n, u in web_entries:
        if inc_update and u in old_all_urls:
            continue
        all_entries_with_source.append((g, n, u, "web", 0))
        web_added += 1
    if inc_update and ENABLE_WEB_SCRAPING:
        logger.debug(f"网站爬取源过滤旧链接 {len(web_entries)-web_added} 条")

    # ===================== 本地TXT =====================
    logger.info("--- 正在读取本地 iptv_channels.txt ---")
    iptv_txt_path = OUTPUT_DIR / "iptv_channels.txt"
    txt_entries = parse_iptv_txt_file(iptv_txt_path)
    stats["local"]["raw"] = len(txt_entries)
    local_added = 0
    for g, n, u in txt_entries:
        if inc_update and u in old_all_urls:
            continue
        all_entries_with_source.append((g, n, u, "local", 0))
        local_added += 1
    logger.info(f"三源合并后总新增条目数: {len(all_entries_with_source)}")

    # ===================== 过滤 =====================
    if ENABLE_MIGU_FILTER:
        original = len(all_entries_with_source)
        all_entries_with_source = [(g, n, u, t, i) for g, n, u, t, i in all_entries_with_source if 'migu' not in u.lower()]
        logger.info(f"过滤Migu: 移除 {original - len(all_entries_with_source)} 条")
    if SKIP_INTERNAL_IP:
        original = len(all_entries_with_source)
        all_entries_with_source = [(g, n, u, t, i) for g, n, u, t, i in all_entries_with_source if not is_internal_ip(u)]
        logger.info(f"过滤内网IP: 移除 {original - len(all_entries_with_source)} 条")

    # ===================== 合并去重 =====================
    logger.info("--- 正在合并去重 ---")
    temp_channel_map = defaultdict(list)
    url_source_map = {}
    for g, n, u, source_type, source_idx in all_entries_with_source:
        temp_channel_map[(g, n)].append(u)
        url_source_map[u] = (source_type, source_idx)

    if ENABLE_DEDUPLICATION:
        temp_channel_map = deduplicate_urls_per_channel(temp_channel_map)

    allowed_groups = set(GROUP_ORDER)
    original_count = sum(len(urls) for urls in temp_channel_map.values())
    filtered_map = {}
    for (group, name), urls in temp_channel_map.items():
        if group in allowed_groups:
            filtered_map[(group, name)] = urls
    temp_channel_map = filtered_map
    filtered_count = sum(len(urls) for urls in temp_channel_map.values())
    logger.info(f"频道筛选: 过滤前 {original_count} 条链接，过滤后 {filtered_count} 条链接")

    unique_urls = list(url_source_map.keys())
    logger.info(f"✅ 合并去重并筛选后共 {len(unique_urls)} 个唯一链接（仅新增部分）")

    # ===================== 新链接连通性测试 =====================
    logger.info("--- 正在进行新链接连通性测试 (前置筛选) ---")
    connectivity_start = time.time()
    sem = asyncio.Semaphore(CONNECTIVITY_CONCURRENCY)
    async def check_one(url):
        async with sem:
            ok = await check_url_connectivity(url, CONNECTIVITY_TIMEOUT)
            return url, ok
    tasks = [check_one(url) for url in unique_urls]
    connected_urls = []
    c, ok, ng, lp = 0, 0, 0, -100
    print_progress_bar(0, len(tasks), ok, ng, lp)
    for coro in asyncio.as_completed(tasks):
        url, is_ok = await coro
        c += 1
        if is_ok:
            ok += 1
            connected_urls.append(url)
            source_type, source_idx = url_source_map[url]
            if source_type == "github":
                stats["github"][source_idx]["valid"] += 1
            elif source_type == "web":
                stats["web"]["valid"] += 1
            elif source_type == "local":
                stats["local"]["valid"] += 1
        else:
            ng += 1
        lp = print_progress_bar(c, len(tasks), ok, ng, lp)
    connectivity_time = time.time() - connectivity_start
    logger.info(f"新链接连通性测试耗时: {connectivity_time:.2f}s")

    # ===================== 构建测速映射 =====================
    channel_map_for_test = defaultdict(list)
    for url in connected_urls:
        found = None
        for k, urls in temp_channel_map.items():
            if url in urls:
                found = k
                break
        if found:
            g, n = found
            if inc_update and (g, n) in required_map:
                if required_map[(g, n)] <= 0:
                    continue
            channel_map_for_test[found].append(url)

    if inc_update:
        logger.info(f"增量模式下，需测速的频道数: {len(channel_map_for_test)}")
        for (g, n), urls in channel_map_for_test.items():
            need = required_map.get((g, n), MAX_LINKS_PER_CHANNEL)
            if need == MAX_LINKS_PER_CHANNEL:
                logger.debug(f"频道 {g}-{n} 全量测速，需 {MAX_LINKS_PER_CHANNEL} 个链接")
            else:
                logger.debug(f"频道 {g}-{n} 增量补充，已有 {MAX_LINKS_PER_CHANNEL - need} 个，需 {need} 个新链接")
    else:
        required_map = None

    # ===================== FFmpeg测速 =====================
    logger.info("--- 正在进行 FFmpeg 测速 ---")
    ffmpeg_start = time.time()
    final_new_map = {}
    if ENABLE_FFMPEG_TEST:
        final_new_map = await run_ffmpeg_test(channel_map_for_test, required_map)
    else:
        final_new_map = channel_map_for_test
    ffmpeg_time = time.time() - ffmpeg_start
    logger.info(f"FFmpeg 测速耗时: {ffmpeg_time:.2f}s")

    # ===================== 合并旧新链接 =====================
    final_channel_map = defaultdict(list)
    if inc_update:
        for (g, n), url_quality_list in old_valid_map.items():
            for url, is_quality in url_quality_list:
                final_channel_map[(g, n)].append(url)
                stats["reused"] += 1
        for (g, n), urls in final_new_map.items():
            current = len(final_channel_map[(g, n)])
            allowed = MAX_LINKS_PER_CHANNEL - current
            if allowed > 0:
                for url in urls[:allowed]:
                    final_channel_map[(g, n)].append(url)
        logger.info(f"增量合并完成：复用 {stats['reused']} 个旧链接，新增 {sum(len(v) for v in final_new_map.values())} 个新链接")
    else:
        final_channel_map = final_new_map
        for urls in final_channel_map.values():
            for url in urls:
                if url in url_source_map:
                    source_type, source_idx = url_source_map[url]
                    if source_type == "github":
                        stats["github"][source_idx]["output"] += 1
                    elif source_type == "web":
                        stats["web"]["output"] += 1
                    elif source_type == "local":
                        stats["local"]["output"] += 1

    # ===================== 导出 + 统计 =====================
    export_results_with_timestamp(final_channel_map)
    print_source_statistics(stats)

    total_time = time.time() - overall_start_time
    logger.info("="*30)
    logger.info(f"⏱️  阶段耗时统计:")
    logger.info(f"  - 新链接连通性测试: {connectivity_time:.2f}s")
    logger.info(f"  - FFmpeg 测速: {ffmpeg_time:.2f}s")
    logger.info(f"  - 总运行时间: {total_time:.2f}s")
    logger.info("="*30)
    logger.info("🎉 任务全部完成！")

if __name__ == "__main__":
    if sys.platform == 'linux':
        asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())
    elif sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
