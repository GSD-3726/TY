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
MAX_LINKS_PER_CHANNEL = 8                               # 每个频道最多保留几条链接
DEFAULT_PROTOCOL      = "http://"                        # 默认协议（用于补全链接）

# -------------------------- 2. 爬取控制 ------------------------------------
EXTRACT_MODE          = "酒店提取"                       # "酒店提取" 或 "组播提取"
MAX_IPS               = 100                               # 最多处理多少个IP
MAX_TOTAL_CHANNELS    = 0                                # 总频道上限（0=不限制）
MAX_CHANNELS_PER_IP   = 0                                # 单个IP最多提取频道数
DELAY_BETWEEN_IPS     = 0.1                              # 切换IP间隔（秒）
DELAY_AFTER_CLICK     = 0.5                              # 点击弹窗后等待（秒）
MODAL_WAIT_TIMEOUT    = 2                                # 等待模态框出现（秒）

# -------------------------- 3. 超时与等待 ----------------------------------
PAGE_LOAD_TIMEOUT      = 120                             # 页面加载超时（秒）
DATA_LOAD_TIMEOUT      = 60                              # 数据加载总超时（秒）
AFTER_START_WAIT       = 30                              # 点击【开始提取】后等待秒数
IP_ADDR_TIMEOUT        = 0.5                             # 读取IP地址超时（秒）
CHANNEL_NAME_TIMEOUT   = 0.5                             # 读取频道名称超时（秒）
CHANNEL_URL_TIMEOUT    = 0.5                             # 读取频道链接超时（秒）
SCROLL_TIMEOUT         = 0.5                             # 滚动到元素视野的超时（秒）
CLICK_TIMEOUT          = 0.5                             # 点击元素的超时（秒）
WAIT_FOR_ELEMENT_TIMEOUT = 30                             # wait_for_element默认超时（秒）
DATA_CHECK_INTERVAL    = 5                               # 数据加载检查间隔（秒）【优化：从30秒降到5秒】

# -------------------------- 4. GitHub源订阅 --------------------------------
ENABLE_GITHUB_SOURCES = True                            # 是否启用GitHub源
GITHUB_M3U_LINKS = [
    "https://gh-proxy.com/https://raw.githubusercontent.com/kakaxi-1/IPTV/main/ipv4.txt",
    "https://gh-proxy.com/https://raw.githubusercontent.com/3377/IPTV/master/output/ipv4/result.txt",
    "https://gh-proxy.com/https://raw.githubusercontent.com/vbskycn/iptv/master/tv/iptv4.txt",
    "https://gh-proxy.com/https://raw.githubusercontent.com/best-fan/iptv-sources/main/cn_cctv.m3u8"
]

# -------------------------- 5. FFmpeg测速设置 -------------------------------
ENABLE_FFMPEG_TEST     = True                            # 是否启用FFmpeg测速
FFMPEG_PATH            = "ffmpeg"                        # FFmpeg 程序路径（如果不在PATH中需写完整）
FFMPEG_TEST_DURATION   = 15                              # 每个链接测试时长（秒）
FFMPEG_CONCURRENCY     = 0                               # 并发测速数量（0=自动适配CPU，一般建议≤2）
MIN_AVG_FPS            = 24                              # 最低平均帧率
MIN_FRAMES             = 210                             # 最低解码帧数（防止只有几秒数据）
QUICK_FFMPEG_TEST_DURATION = 3                            # 快速预检时长（秒）

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
ENABLE_SATELLITE_CLEAN = True                            # 卫视名称清洗（如“XX卫视移动”->“XX卫视”）

# -------------------------- 8. 频道分类规则（已扩展关键词） ------------------
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
TIME_DISPLAY_AT_TOP    = False                           # 更新时间是否放顶部
UPDATE_STREAM_URL      = "https://gitee.com/bmg369/tvtest/raw/master/cg/index.m3u8"
ENABLE_VERBOSE_LOGGING = False                           # 详细日志已关闭（保留配置，未使用）

# -------------------------- 11. 连通性/预检配置 -----------------------------
# 【核心优化】并发数从15提升到150，超时从2秒提升到5秒
CONNECTIVITY_CONCURRENCY = 150                         # 连通性测试并发数【优化：15→150】
CONNECTIVITY_TIMEOUT     = 5                            # 连通性测试超时（秒）【优化：2→5】

# 全局HTTP会话配置（连接池复用）
HTTP_MAX_CONNECTIONS = 200                              # 最大连接数
HTTP_MAX_PER_HOST = 10                                  # 单主机最大并发
HTTP_DNS_CACHE_TTL = 300                                # DNS缓存时间（秒）
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
# ======================== 【优化】卫视名称清洗函数 ============================
# ============================================================================
def clean_satellite_name(name: str) -> str:
    if not ENABLE_SATELLITE_CLEAN:
        return name
    
    # 处理前缀修饰词（如"高清XX卫视"）
    prefix_pattern = re.compile(
        r'^(移动|高清|HD|超高清|4K|标清|测试)\s*',
        re.IGNORECASE
    )
    name = prefix_pattern.sub('', name)
    
    # 处理后缀修饰词（如"XX卫视高清"）
    suffix_pattern = re.compile(
        r'\s*(移动|高清|HD|超高清|4K|标清|测试)\s*$',
        re.IGNORECASE
    )
    name = suffix_pattern.sub('', name)
    
    # 处理带括号的修饰词（如"XX卫视(高清)"）
    bracket_pattern = re.compile(
        r'[（(]\s*(移动|高清|HD|超高清|4K|标清|测试)\s*[）)]',
        re.IGNORECASE
    )
    name = bracket_pattern.sub('', name)
    
    return name.strip()

# ============================================================================
# ========================= 【优化】缓存管理 ==================================
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
                # 兼容旧格式缓存
                valid_cache[url] = {
                    "ok": data > 0,
                    "fps": 0.0,
                    "frames": 0,
                    "width": 0,
                    "height": 0,
                    "precheck_ok": data > 0,
                    "timestamp": now
                }
        logger.info(f"缓存加载完成，有效条目数: {len(valid_cache)}")
        return valid_cache
    except Exception as e:
        logger.warning(f"加载缓存异常: {e}，将重新创建")
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
# ========================= 【优化】连通性测试函数 ============================
# ============================================================================
async def check_url_connectivity(url: str, session: aiohttp.ClientSession, timeout: int) -> bool:
    """
    优化版连通性检测：
    1. 全局session复用，连接池复用
    2. 优先HEAD请求，最小化数据传输
    3. 支持RTSP/RTMP端口检测
    4. 智能错误处理
    """
    if not url:
        return False
    
    parsed = urlparse(url)
    scheme = parsed.scheme.lower()
    
    if scheme in ('http', 'https'):
        # HTTP/HTTPS：优先HEAD，失败回退GET
        try:
            async with session.head(
                url,
                timeout=aiohttp.ClientTimeout(total=timeout),
                allow_redirects=True,
                headers={"User-Agent": USER_AGENT}
            ) as response:
                return 200 <= response.status < 400
                
        except Exception as e:
            # HEAD失败，回退到GET（仅检查状态，不下载内容）
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
        # RTSP/RTMP：简单端口检测
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
    
    # 其他协议暂时认为有效
    return True

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

async def test_stream_with_ffmpeg(url: str, duration: Optional[int] = None) -> Dict[str, Any]:
    if not shutil.which(FFMPEG_PATH):
        logger.error(f"未找到FFmpeg: {FFMPEG_PATH}")
        return {"ok": False, "fps": 0.0, "frames": 0, "width": 0, "height": 0, "message": "FFmpeg未安装"}

    # 优化：仅对咪咕链接添加Referer
    headers = f"User-Agent: {USER_AGENT}\r\n"
    if 'migu' in url.lower():
        headers += "Referer: https://www.miguvideo.com/\r\n"

    if duration is None:
        duration = FFMPEG_TEST_DURATION

    cmd = [
        FFMPEG_PATH, "-hide_banner", "-nostdin", "-y",
        "-headers", headers,
        "-fflags", "nobuffer",
        "-rw_timeout", "10000000",  # 优化：超时从5秒升到10秒
        "-probesize", "1000000",
        "-analyzeduration", "2000000",
        "-i", url,
        "-t", str(duration),
        "-f", "null", "-"
    ]

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE
        )

        try:
            _, stderr = await asyncio.wait_for(proc.communicate(), timeout=duration + 10)
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
        for match in re.finditer(r'Stream #0:(\d+).*Video:.*?(\d+)x(\d+)', output, re.IGNORECASE):
            w = int(match.group(2))
            h = int(match.group(3))
            if w > 0 and h > 0:
                width, height = w, h
                break

        if width == 0 or height == 0:
            generic_match = re.search(r'Video:.*?(\d+)x(\d+)', output, re.IGNORECASE)
            if generic_match:
                width = int(generic_match.group(1))
                height = int(generic_match.group(2))

        if duration == QUICK_FFMPEG_TEST_DURATION:
            min_frames = max(30, int(MIN_FRAMES * duration / max(1, FFMPEG_TEST_DURATION)))
            min_avg_fps = max(8.0, MIN_AVG_FPS * 0.5)
        else:
            min_frames = MIN_FRAMES
            min_avg_fps = MIN_AVG_FPS

        is_smooth = frames >= min_frames and avg_fps >= min_avg_fps and width > 0 and height > 0

        return {
            "ok": is_smooth,
            "fps": avg_fps,
            "frames": frames,
            "width": width,
            "height": height,
            "min_frames": min_frames,
            "min_avg_fps": min_avg_fps
        }
    except Exception as e:
        return {"ok": False, "fps": 0.0, "frames": 0, "width": 0, "height": 0, "message": f"异常: {str(e)[:50]}"}

async def test_ts_preview(url: str, session: aiohttp.ClientSession, timeout: int = 5) -> Dict[str, Any]:
    """优化版TS预检：复用全局session"""
    if not url:
        return {"precheck_ok": False, "message": "URL 为空"}

    parsed = urlparse(url)
    scheme = parsed.scheme.lower()
    
    if scheme not in ('http', 'https'):
        return {"precheck_ok": True, "message": "非HTTP协议，跳过预检"}

    timeout_obj = aiohttp.ClientTimeout(total=timeout)
    headers = {"User-Agent": USER_AGENT}

    try:
        if '.m3u8' in url.lower():
            async with session.get(url, headers=headers, allow_redirects=True, timeout=timeout_obj) as resp:
                if resp.status != 200:
                    return {"precheck_ok": False, "message": f"M3U8 获取失败({resp.status})"}
                playlist = await resp.text()

            for line in playlist.splitlines():
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                seg_url = urljoin(url, line)
                try:
                    async with session.get(seg_url, headers=headers, allow_redirects=True, timeout=aiohttp.ClientTimeout(total=3)) as seg_resp:
                        if seg_resp.status != 200:
                            continue
                        chunk = await seg_resp.content.read(256)
                        return {"precheck_ok": len(chunk) > 0, "segment": seg_url}
                except:
                    continue

            return {"precheck_ok": False, "message": "未找到可用 TS 分片"}

        async with session.get(url, headers=headers, allow_redirects=True, timeout=timeout_obj) as resp:
            if resp.status != 200:
                return {"precheck_ok": False, "message": f"URL 获取失败({resp.status})"}
            chunk = await resp.content.read(256)
            return {"precheck_ok": len(chunk) > 0}
    except asyncio.TimeoutError:
        return {"precheck_ok": False, "message": "预检超时"}
    except Exception as e:
        return {"precheck_ok": False, "message": f"预检异常: {str(e)[:80]}"}

async def run_ffmpeg_test(channel_map: Dict[Tuple[str, str], List[str]]) -> Dict[Tuple[str, str], List[str]]:
    if not channel_map:
        return {}

    cache = load_cache() if ENABLE_CACHE else {}
    new_cache = {}
    result_map = defaultdict(list)
    success_counts = defaultdict(int)
    total = sum(len(urls) for urls in channel_map.values())
    cached_ok = 0
    cached_failed_skipped = 0
    pending = []

    for (g, n), urls in channel_map.items():
        for u in urls:
            if SKIP_INTERNAL_IP and is_internal_ip(u):
                continue

            if MAX_LINKS_PER_CHANNEL > 0 and success_counts[(g, n)] >= MAX_LINKS_PER_CHANNEL:
                continue

            cache_item = cache.get(u)
            if cache_item and isinstance(cache_item, dict) and is_cache_valid(cache_item.get("timestamp", 0)):
                if cache_item.get("ok"):
                    result_map[(g, n)].append((
                        u,
                        cache_item.get("fps", 0.0),
                        cache_item.get("width", 0),
                        cache_item.get("height", 0),
                        False
                    ))
                    cached_ok += 1
                    if MAX_LINKS_PER_CHANNEL == 0 or success_counts[(g, n)] < MAX_LINKS_PER_CHANNEL:
                        success_counts[(g, n)] += 1
                    continue
                cached_failed_skipped += 1
                continue

            pending.append((g, n, u))

    logger.info(f"总链接: {total} 缓存有效且成功: {cached_ok} 缓存有效但失败(跳过): {cached_failed_skipped} 需处理: {len(pending)}")

    if not pending:
        logger.info("没有需要测速的链接，直接返回缓存结果")
        final = {}
        for k, vs in result_map.items():
            vs.sort(key=lambda x: (-x[2] * x[3], -x[1]))
            final[k] = [u for u, _, _, _, _ in vs] if MAX_LINKS_PER_CHANNEL == 0 else [u for u, _, _, _, _ in vs[:MAX_LINKS_PER_CHANNEL]]
        return final

    if FFMPEG_CONCURRENCY > 0:
        concurrency = FFMPEG_CONCURRENCY
    else:
        cpu_count = os.cpu_count() or 1
        concurrency = max(1, min(cpu_count // 2, len(pending)))

    logger.info(f"FFmpeg 并发测速: {concurrency} (CPU核: {os.cpu_count()})")
    sem = asyncio.Semaphore(concurrency)
    locks = defaultdict(asyncio.Lock)
    
    # 初始化全局HTTP会话
    connector = aiohttp.TCPConnector(
        limit=HTTP_MAX_CONNECTIONS,
        limit_per_host=HTTP_MAX_PER_HOST,
        ttl_dns_cache=HTTP_DNS_CACHE_TTL,
        use_dns_cache=True
    )
    session = aiohttp.ClientSession(
        connector=connector,
        trust_env=True
    )

    async def test_one(item):
        g, n, u = item
        if MAX_LINKS_PER_CHANNEL > 0 and success_counts[(g, n)] >= MAX_LINKS_PER_CHANNEL:
            return g, n, u, {"ok": False, "skipped": True, "message": "频道已达上限"}

        async with sem:
            pre = await test_ts_preview(u, session)
            pre_ok = bool(pre.get("precheck_ok", False))
            if not pre_ok:
                return g, n, u, {"ok": False, "fps": 0.0, "frames": 0, "width": 0, "height": 0, "precheck_ok": False, "message": pre.get("message", "TS 预检失败")}

            if MAX_LINKS_PER_CHANNEL > 0 and success_counts[(g, n)] >= MAX_LINKS_PER_CHANNEL:
                return g, n, u, {"ok": False, "skipped": True, "message": "频道已达上限"}

            quick_res = await test_stream_with_ffmpeg(u, duration=QUICK_FFMPEG_TEST_DURATION)
            quick_res["precheck_ok"] = True
            if quick_res.get("ok"):
                async with locks[(g, n)]:
                    if MAX_LINKS_PER_CHANNEL == 0 or success_counts[(g, n)] < MAX_LINKS_PER_CHANNEL:
                        success_counts[(g, n)] += 1
                return g, n, u, quick_res

            if MAX_LINKS_PER_CHANNEL > 0 and success_counts[(g, n)] >= MAX_LINKS_PER_CHANNEL:
                return g, n, u, {"ok": False, "skipped": True, "message": "频道已达上限"}

            full_res = await test_stream_with_ffmpeg(u, duration=FFMPEG_TEST_DURATION)
            full_res["precheck_ok"] = True
            if full_res.get("ok"):
                async with locks[(g, n)]:
                    if MAX_LINKS_PER_CHANNEL == 0 or success_counts[(g, n)] < MAX_LINKS_PER_CHANNEL:
                        success_counts[(g, n)] += 1
            return g, n, u, full_res

    tasks = [test_one(item) for item in pending]
    c, ok, ng, lp = 0, 0, 0, -100
    print_progress_bar(0, len(tasks), ok, ng, lp)

    for coro in asyncio.as_completed(tasks):
        g, n, u, res = await coro
        c += 1
        if res.get("ok"):
            ok += 1
            result_map[(g, n)].append((u, res.get("fps", 0.0), res.get("width", 0), res.get("height", 0), True))
        else:
            ng += 1

        if ENABLE_CACHE:
            new_cache[u] = {
                "ok": res.get("ok", False),
                "fps": res.get("fps", 0.0),
                "frames": res.get("frames", 0),
                "width": res.get("width", 0),
                "height": res.get("height", 0),
                "precheck_ok": res.get("precheck_ok", res.get("ok", False)),
                "timestamp": time.time()
            }

        lp = print_progress_bar(c, len(tasks), ok, ng, lp)

    await session.close()

    if ENABLE_CACHE and new_cache:
        cache.update(new_cache)
        save_cache(cache)

    final = {}
    for k, vs in result_map.items():
        vs.sort(key=lambda x: (-x[2] * x[3], -x[1]))
        final[k] = [u for u, _, _, _, _ in vs] if MAX_LINKS_PER_CHANNEL == 0 else [u for u, _, _, _, _ in vs[:MAX_LINKS_PER_CHANNEL]]

    logger.info(f"测速完成，共 {len(final)} 个频道通过筛选")
    return final

# ============================================================================
# ========================= GitHub M3U 解析（优化版）===========================
# ============================================================================
@retry_async(max_retries=3, delay=2)
async def download_github_m3u(url, session: Optional[aiohttp.ClientSession] = None):
    close_session = False
    if session is None:
        connector = aiohttp.TCPConnector(
            limit=HTTP_MAX_CONNECTIONS,
            limit_per_host=HTTP_MAX_PER_HOST,
            ttl_dns_cache=HTTP_DNS_CACHE_TTL,
            use_dns_cache=True
        )
        session = aiohttp.ClientSession(
            connector=connector,
            timeout=aiohttp.ClientTimeout(total=30),
            trust_env=True
        )
        close_session = True
    try:
        async with session.get(url, headers={"User-Agent": USER_AGENT}) as r:
            if r.status == 200:
                t = await r.text()
                logger.info(f"下载成功 {url}")
                return t
    except Exception as e:
        logger.warning(f"下载失败 {url}: {str(e)[:60]}")
    finally:
        if close_session:
            await session.close()
    return ""

def parse_m3u_file(content):
    """
    优化版M3U解析：更健壮的频道名提取
    """
    channels = []
    name = ""
    for line in content.splitlines():
        line = line.strip()
        if line.startswith("#EXTINF"):
            # 匹配最后一个逗号后面的内容，不受属性顺序影响
            parts = line.split(',', 1)
            if len(parts) == 2:
                name = parts[1].strip()
        elif line.startswith("http") or line.startswith("rtsp://") or line.startswith("rtmp://"):
            url = line.strip()
            if name and url:
                name_cleaned = clean_satellite_name(name)
                normalized = normalize_cctv(name_cleaned)
                group = classify_channel(normalized)
                if group:
                    final_name = normalized if group == "央视频道" else (
                        clean_chinese_only(name_cleaned) if ENABLE_CHINESE_CLEAN else name_cleaned
                    )
                    channels.append((group, final_name, url))
            # 重置
            name = ""
    return channels

def parse_txt_content(content: str, default_group: str = "未分类") -> List[Tuple[str, str, str]]:
    """
    优化版TXT解析：处理多个$注释
    """
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
                # 处理多个$注释，只保留第一个$前面的部分
                if '$' in url_part:
                    url = url_part.split('$', 1)[0].strip()
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
        logger.info(f"本地文件不存在: {filepath}")
        return []
    channels = []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                if line.endswith('#genre#'):
                    continue
                if ',' in line:
                    parts = line.split(',', 1)
                    if len(parts) == 2:
                        name, url = parts[0].strip(), parts[1].strip()
                        if '$' in url:
                            url = url.split('$', 1)[0].strip()
                        if name and url:
                            name_cleaned = clean_satellite_name(name)
                            normalized = normalize_cctv(name_cleaned)
                            group = classify_channel(normalized)
                            if group:
                                final_name = normalized if group == "央视频道" else (
                                    clean_chinese_only(name_cleaned) if ENABLE_CHINESE_CLEAN else name_cleaned
                                )
                                channels.append((group, final_name, url))
        logger.info(f"从本地文件解析到 {len(channels)} 个链接")
    except Exception as e:
        logger.error(f"解析本地文件失败: {e}")
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
                name_cleaned = clean_satellite_name(n)
                normalized = normalize_cctv(name_cleaned)
                group = classify_channel(normalized)
                if not group:
                    continue
                final_name = normalized if group == "央视频道" else (
                    clean_chinese_only(name_cleaned) if ENABLE_CHINESE_CLEAN else name_cleaned
                )
                entries.append((group, final_name, u))
            except:
                continue
    except Exception as e:
        logger.debug(f"提取IP {idx} 异常: {e}")
    finally:
        # 自动关闭模态框
        try:
            close_btn = page.locator(".modal-dialog button.close").first
            if await close_btn.count() > 0:
                await robust_click(close_btn)
            await asyncio.sleep(0.1)
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
            # 优先选择CCTV5+而不是CCTV5
            plus_channels = [ch for ch in channels if '5+' in ch[1].lower() or 'plus' in ch[1].lower()]
            if plus_channels:
                chosen = plus_channels[0]
            else:
                # 优先选择名称更长的（更完整）
                chosen = max(channels, key=lambda ch: len(ch[1]))
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
        if TIME_DISPLAY_AT_TOP:
            f.write(f'#EXTINF:-1 tvg-name="更新" group-title="更新时间",{now}\n{gu}\n\n')
        for gro in GROUP_ORDER:
            if gro not in g:
                continue
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
            if gro not in g:
                continue
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
    logger.info(f"导出完成：{len(channel_map)} 个频道，{sum(len(urls) for urls in channel_map.values())} 条链接")

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
    
    logger.info("="*60)

async def main():
    overall_start_time = time.time()
    
    stats = {
        "github": [{"url": url, "raw": 0, "valid": 0, "output": 0} for url in GITHUB_M3U_LINKS],
        "web": {"raw": 0, "valid": 0, "output": 0},
        "local": {"raw": 0, "valid": 0, "output": 0}
    }
    all_entries_with_source = []
    
    if EXTRACT_MODE not in ["酒店提取", "组播提取"]:
        logger.error("配置错误！EXTRACT_MODE 只能填写：酒店提取 或 组播提取")
        return

    logger.info(f"✅ 当前运行模式：【{EXTRACT_MODE}】(极速优化版)")

    if ENABLE_GITHUB_SOURCES:
        logger.info("--- 正在并发获取 GitHub 源 ---")
        # 初始化全局HTTP会话
        connector = aiohttp.TCPConnector(
            limit=HTTP_MAX_CONNECTIONS,
            limit_per_host=HTTP_MAX_PER_HOST,
            ttl_dns_cache=HTTP_DNS_CACHE_TTL,
            use_dns_cache=True
        )
        async with aiohttp.ClientSession(
            connector=connector,
            timeout=aiohttp.ClientTimeout(total=30),
            trust_env=True
        ) as session:
            tasks = [download_github_m3u(url, session) for url in GITHUB_M3U_LINKS]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for idx, result in enumerate(results):
                link_no = idx + 1
                if isinstance(result, Exception):
                    logger.warning(f"下载 GitHub源{link_no} 异常: {str(result)[:60]}")
                    continue
                if result and isinstance(result, str):
                    content = result.strip()
                    if content.startswith('#EXTM3U') or '#EXTINF' in content:
                        channels = parse_m3u_file(content)
                    else:
                        channels = parse_txt_content(content, default_group="GitHub源")
                    stats["github"][idx]["raw"] = len(channels)
                    logger.info(f"✅ GitHub源{link_no} 获取到 {len(channels)} 条频道")
                    all_entries_with_source.extend((g, n, u, "github", idx) for g, n, u in channels)
                else:
                    logger.info(f"GitHub源{link_no} 获取到 0 条频道")
            logger.info(f"GitHub源累计获取: {sum(s['raw'] for s in stats['github'])} 条")

    web_entries = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=HEADLESS,
            args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage", "--disable-gpu"]
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
                    await asyncio.sleep(0.5)

            if EXTRACT_MODE == "酒店提取":
                tab_sel = build_selector(PAGE_CONFIG["hotel"], "div.segment-item")
            else:
                tab_sel = build_selector(PAGE_CONFIG["multicast"], "div.segment-item")
            
            tab = page.locator(tab_sel).first
            if await tab.count() > 0:
                await robust_click(tab)
                await asyncio.sleep(0.5)

            start_sel = build_selector(PAGE_CONFIG["start_button"], "button")
            start_btn = page.locator(start_sel).first
            if await start_btn.count() > 0:
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
                    try:
                        entries = await extract_one_ip(page, rows.nth(i), i + 1)
                        if entries:
                            web_entries.extend(entries)
                        if MAX_TOTAL_CHANNELS > 0 and len(web_entries) >= MAX_TOTAL_CHANNELS:
                            web_entries = web_entries[:MAX_TOTAL_CHANNELS]
                            logger.info(f"已达到总频道上限 {MAX_TOTAL_CHANNELS}，停止爬取")
                            break
                        await asyncio.sleep(DELAY_BETWEEN_IPS)
                    except Exception as e:
                        logger.warning(f"处理IP {i+1} 异常: {e}")
                        continue
                stats["web"]["raw"] = len(web_entries)
                logger.info(f"网站爬取完成: {len(web_entries)} 条")

        except Exception as e:
            logger.exception("❌ 爬取过程异常")
        finally:
            await page.close()
            await ctx.close()
            await browser.close()
    
    all_entries_with_source.extend((g, n, u, "web", 0) for g, n, u in web_entries)

    logger.info("--- 正在读取本地 iptv_channels.txt ---")
    iptv_txt_path = OUTPUT_DIR / "iptv_channels.txt"
    txt_entries = parse_iptv_txt_file(iptv_txt_path)
    stats["local"]["raw"] = len(txt_entries)
    all_entries_with_source.extend((g, n, u, "local", 0) for g, n, u in txt_entries)
    
    logger.info(f"三源合并后总条目数: {len(all_entries_with_source)}")

    if ENABLE_MIGU_FILTER:
        original_count = len(all_entries_with_source)
        all_entries_with_source = [(g, n, u, t, i) for g, n, u, t, i in all_entries_with_source if 'migu' not in u.lower()]
        logger.info(f"过滤Migu: 移除 {original_count - len(all_entries_with_source)} 条")

    if SKIP_INTERNAL_IP:
        original_count = len(all_entries_with_source)
        all_entries_with_source = [(g, n, u, t, i) for g, n, u, t, i in all_entries_with_source if not is_internal_ip(u)]
        logger.info(f"过滤内网IP: 移除 {original_count - len(all_entries_with_source)} 条")

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
    logger.info(f"✅ 合并去重并筛选后共 {len(unique_urls)} 个唯一链接")

    logger.info("--- 正在进行连通性测试 (极速版) ---")
    connectivity_start = time.time()
    
    # 初始化全局HTTP会话（连接池复用）
    connector = aiohttp.TCPConnector(
        limit=HTTP_MAX_CONNECTIONS,
        limit_per_host=HTTP_MAX_PER_HOST,
        ttl_dns_cache=HTTP_DNS_CACHE_TTL,
        use_dns_cache=True
    )
    session = aiohttp.ClientSession(
        connector=connector,
        trust_env=True
    )
    
    sem = asyncio.Semaphore(CONNECTIVITY_CONCURRENCY)
    async def check_one(url):
        async with sem:
            ok = await check_url_connectivity(url, session, CONNECTIVITY_TIMEOUT)
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

    await session.close()

    connectivity_time = time.time() - connectivity_start
    logger.info(f"连通性测试耗时: {connectivity_time:.2f}s")

    channel_map_for_test = defaultdict(list)
    for url in connected_urls:
        pair = next(((g, n) for (g, n), urls in temp_channel_map.items() if url in urls), None)
        if pair:
            channel_map_for_test[pair].append(url)

    logger.info("--- 正在进行 FFmpeg 测速 ---")
    ffmpeg_start = time.time()
    
    final_channel_map = {}
    if ENABLE_FFMPEG_TEST:
        final_channel_map = await run_ffmpeg_test(channel_map_for_test)
    else:
        final_channel_map = channel_map_for_test
        
    ffmpeg_time = time.time() - ffmpeg_start
    logger.info(f"FFmpeg 测速耗时: {ffmpeg_time:.2f}s")

    for urls in final_channel_map.values():
        for url in urls:
            source_type, source_idx = url_source_map[url]
            if source_type == "github":
                stats["github"][source_idx]["output"] += 1
            elif source_type == "web":
                stats["web"]["output"] += 1
            elif source_type == "local":
                stats["local"]["output"] += 1

    export_results_with_timestamp(final_channel_map)
    print_source_statistics(stats)
    
    total_time = time.time() - overall_start_time
    logger.info("="*30)
    logger.info(f"⏱️  阶段耗时统计:")
    logger.info(f"  - 连通性测试: {connectivity_time:.2f}s")
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
