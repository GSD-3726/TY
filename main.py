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
EXTRACT_MODE          = "酒店提取"                       # "酒店提取" 或 "组播提取"
MAX_IPS               = 100                              # 最多处理多少个IP
MAX_TOTAL_CHANNELS    = 0                                # 总频道上限（0=不限制）
MAX_CHANNELS_PER_IP   = 0                                # 单个IP最多提取频道数
DELAY_BETWEEN_IPS     = 0.1                              # 切换IP间隔（秒）
DELAY_AFTER_CLICK     = 0.3                              # 点击弹窗后等待（秒）
MODAL_WAIT_TIMEOUT    = 1000                             # 等待模态框出现（毫秒）

# -------------------------- 3. 超时与等待 ----------------------------------
PAGE_LOAD_TIMEOUT      = 120000                          # 页面加载超时（毫秒）
DATA_LOAD_TIMEOUT      = 60                              # 数据加载总超时（秒）
AFTER_START_WAIT       = 30                              # 点击【开始提取】后等待秒数
IP_ADDR_TIMEOUT        = 1000                            # 读取IP地址超时（毫秒）
CHANNEL_NAME_TIMEOUT   = 1000                            # 读取频道名称超时（毫秒）
CHANNEL_URL_TIMEOUT    = 1000                            # 读取频道链接超时（毫秒）
SCROLL_TIMEOUT         = 1000                            # 滚动到元素视野的超时（毫秒）
CLICK_TIMEOUT          = 10000                           # 点击元素的超时（毫秒）
WAIT_FOR_ELEMENT_TIMEOUT = 30000                         # wait_for_element默认超时（毫秒）
DATA_CHECK_INTERVAL    = 30                              # 数据加载检查间隔（秒）

# -------------------------- 4. GitHub源订阅 --------------------------------
ENABLE_GITHUB_SOURCES = True                            # 是否启用GitHub源
GITHUB_M3U_LINKS = [
    "https://gh-proxy.com/https://raw.githubusercontent.com/hujingguang/ChinaIPTV/main/cnTV_AutoUpdate.m3u8",
    "https://gh-proxy.com/https://raw.githubusercontent.com/ioptu/IPTV.txt2m3u.player/main/migu.m3u",
    "https://gh-proxy.com/https://raw.githubusercontent.com/ioptu/IPTV.txt2m3u.player/main/httop_merged.m3u",
    "https://gh-proxy.com/https://raw.githubusercontent.com/ioptu/IPTV.txt2m3u.player/main/httop.m3u",
    "https://gh-proxy.com/https://raw.githubusercontent.com/ioptu/IPTV.txt2m3u.player/main/htldx_merged.m3u8",
    "https://gh-proxy.com/https://raw.githubusercontent.com/3377/IPTV/master/output/result.txt",
    "https://gh-proxy.com/https://raw.githubusercontent.com/Guovin/iptv-database/master/result.txt",
    "https://gh-proxy.com/https://raw.githubusercontent.com/iptv-org/iptv/gh-pages/countries/cn.m3u8",
    "https://gh-proxy.com/https://raw.githubusercontent.com/iptv-org/iptv/master/streams/cn.m3u8",
    "https://gh-proxy.com/https://raw.githubusercontent.com/suxuang/myIPTV/main/ipv6.m3u8",
    "https://gh-proxy.com/https://raw.githubusercontent.com/kimwang1978/collect-tv-txt/main/merged_output.txt",
    "https://gh-proxy.com/https://raw.githubusercontent.com/xzw832/cmys/main/S_CCTV.txt",
    "https://gh-proxy.com/https://raw.githubusercontent.com/xzw832/cmys/main/S_weishi.txt",
    "https://gh-proxy.com/https://raw.githubusercontent.com/asdjkl6/tv/tv/.m3u/整套直播源/测试/整套直播源/l.txt",
    "https://gh-proxy.com/https://raw.githubusercontent.com/asdjkl6/tv/tv/.m3u/整套直播源/测试/整套直播源/kk.txt",
    "https://gh-proxy.com/https://raw.githubusercontent.com/yuanzl77/IPTV/master/live.txt",
    "https://gh-proxy.com/https://raw.githubusercontent.com/fanmingming/live/main/tv/m3u/ipv6.m3u8",
    "https://gh-proxy.com/https://raw.githubusercontent.com/vbskycn/iptv/master/tv/iptv6.txt",
    "https://gh-proxy.com/https://raw.githubusercontent.com/vbskycn/iptv/master/tv/iptv4.txt",
    "https://gh-proxy.com/https://raw.githubusercontent.com/YueChan/Live/main/APTV.m3u8",
    "https://gh-proxy.com/https://raw.githubusercontent.com/YanG-1989/m3u/main/Gather.m3u8",
    "https://gh-proxy.com/https://raw.githubusercontent.com/suxuang/myIPTV/main/ipv4.m3u8",
    "https://gh-proxy.com/https://raw.githubusercontent.com/kimwang1978/collect-tv-txt/main/others_output.txt",
]

# -------------------------- 5. FFmpeg测速设置 -------------------------------
ENABLE_FFMPEG_TEST     = True                            # 是否启用FFmpeg测速
FFMPEG_PATH            = "ffmpeg"                        # FFmpeg 程序路径（如果不在PATH中需写完整）
FFMPEG_TEST_DURATION   = 10                              # 每个链接测试时长（秒）
FFMPEG_CONCURRENCY     = 6                               # 并发测速数量（GitHub Actions建议≤2）
MIN_AVG_FPS            = 24                              # 最低平均帧率
MIN_FRAMES             = 210                             # 最低解码帧数（防止只有几秒数据）

# -------------------------- 6. 缓存设置 ------------------------------------
ENABLE_CACHE           = True                            # 启用测速缓存
CACHE_FILE             = OUTPUT_DIR / "iptv_speed_cache.json"
CACHE_EXPIRE_HOURS     = 72                              # 缓存过期小时

# -------------------------- 7. 数据处理 ------------------------------------
ENABLE_CHINESE_CLEAN   = True                            # 清理非中文字符
ENABLE_DEDUPLICATION   = True                            # 全局去重
CCTV_USE_MAPPING       = True                            # CCTV映射中文名称
ENABLE_HISTORY_CHECK   = True                            # 检查历史输出文件中的链接
HISTORY_FILE           = OUTPUT_TXT_FILENAME             # 历史文件路径
HISTORY_CHECK_CONCURRENCY = 10                           # 并发检查数量
HISTORY_CHECK_TIMEOUT  = 10                              # 每个链接检查超时（秒）
ENABLE_MIGU_FILTER     = True                            # 过滤包含"migu"的链接
SKIP_INTERNAL_IP       = True                            # 跳过内网IP
ENABLE_URL_PRE_CHECK   = False                           # 预检已禁用，直接测速

# -------------------------- 8. 频道分类规则 --------------------------------
CATEGORY_RULES = [
    {"name": "4K专区",      "keywords": ["4k"]},
    {"name": "央视频道",    "keywords": ["cctv", "cetv", "央视","中央"]},
    {"name": "卫视频道",    "keywords": ["卫视", "凤凰", "tvb", "湖南", "浙江", "江苏", "东方"]},
    {"name": "电影频道",    "keywords": ["电影", "影院", "chc", "动作", "剧场", "映画"]},
    {"name": "轮播频道",    "keywords": ["轮播"]},
    {"name": "儿童频道",    "keywords": ["少儿", "动画", "动漫","卡通"]},
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
ENABLE_VERBOSE_LOGGING = False                           # 详细日志已关闭

# ============================================================================
# ============================= 日志配置（北京时间） ===========================
# ============================================================================
log_level = logging.INFO  # 固定INFO级别，不输出DEBUG

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
file_h = logging.FileHandler(OUTPUT_DIR / 'iptv_extractor.log', encoding='utf-8')
formatter = BeijingFormatter("%(asctime)s - %(levelname)s - %(message)s")
stdout_h.setFormatter(formatter)
file_h.setFormatter(formatter)
logger.addHandler(stdout_h)
logger.addHandler(file_h)

# ============================================================================
# ========================= 工具函数 =========================================
# ============================================================================
CCTV_PATTERN = re.compile(r'(cctv)[-\s]?(\d{1,3}|5\+)', re.IGNORECASE)
CHINESE_ONLY_PATTERN = re.compile(r'[^\u4e00-\u9fff]')
INTERNAL_IP_PATTERN = re.compile(r'^(192\.168\.|10\.|172\.(1[6-9]|2[0-9]|3[0-1])\.|127\.0\.0\.1)')

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
    # 处理 CCTV4K / CCTV-4K / CCTV 4K 等变体，确保归类到 4K专区
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
    """判断URL是否指向内网IP"""
    try:
        host = urlparse(url).hostname
        if not host:
            return False
        return bool(INTERNAL_IP_PATTERN.match(host))
    except:
        return False

# ============================================================================
# ========================= 缓存管理（兼容新格式）==============================
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
                    # 兼容旧版本：如果没有width/height字段，则设为0
                    if "width" not in data:
                        data["width"] = 0
                        data["height"] = 0
                    valid_cache[url] = data
            else:
                # 兼容旧格式：如果只存了速度，转换为新格式
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

async def test_stream_with_ffmpeg(url: str) -> Dict[str, Any]:
    """调用 FFmpeg 测试流媒体质量，返回解码帧数、平均帧率和视频分辨率"""
    if not shutil.which(FFMPEG_PATH):
        logger.error(f"未找到FFmpeg: {FFMPEG_PATH}")
        return {"ok": False, "fps": 0.0, "frames": 0, "width": 0, "height": 0, "message": "FFmpeg未安装"}

    # 构造请求头，模拟浏览器，提高对平台源的兼容性
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

        # 提取帧率和帧数
        frame_matches = re.findall(r'frame=\s*(\d+)', output)
        fps_matches = re.findall(r'fps=\s*([\d.]+)', output)
        frames = int(frame_matches[-1]) if frame_matches else 0
        avg_fps = float(fps_matches[-1]) if fps_matches else 0.0

        # ========== 改进的分辨率提取 ==========
        width, height = 0, 0

        # 方法1：精确匹配视频流行（允许任意索引 #0:0, #0:1, ...）
        # 匹配 "Stream #0:1(und): Video: h264 (High), yuv420p, 1920x1080 ..."
        video_matches = re.finditer(r'Stream #0:(\d+).*Video:.*? (\d+)x(\d+)', output, re.IGNORECASE)
        for match in video_matches:
            w = int(match.group(2))
            h = int(match.group(3))
            if w > 0 and h > 0:
                width, height = w, h
                break

        # 方法2：如果上面没找到，尝试更通用的 "Video: ... 1920x1080"
        if width == 0 or height == 0:
            generic_match = re.search(r'Video:.*? (\d+)x(\d+)', output, re.IGNORECASE)
            if generic_match:
                width = int(generic_match.group(1))
                height = int(generic_match.group(2))
        # ======================================

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

async def run_ffmpeg_test(channel_map: Dict[Tuple[str, str], List[str]]) -> Dict[Tuple[str, str], List[str]]:
    """
    对频道映射中的链接进行FFmpeg测速筛选，返回每个频道有效链接列表。
    测速结果包含分辨率，排序时先按分辨率面积降序，再按帧率降序（不考虑是否本次新测速）。
    """
    if not channel_map:
        return {}
    cache = load_cache() if ENABLE_CACHE else {}
    new_cache = {}
    # 数据结构变更: (url, fps, width, height, is_new_test)
    result_map = defaultdict(list)  # key: (group, name) -> list of tuple
    total = sum(len(us) for us in channel_map.values())
    cached_ok = 0
    pending = []

    # 1. 检查缓存，跳过内网IP
    for (g, n), us in channel_map.items():
        for u in us:
            cache_item = cache.get(u)
            if cache_item and isinstance(cache_item, dict) and "ok" in cache_item:
                if is_cache_valid(cache_item.get("timestamp", 0)):
                    if cache_item["ok"]:
                        # 从缓存中获取分辨率和帧率，标记为【非本次测速】(False)
                        result_map[(g, n)].append((
                            u,
                            cache_item.get("fps", 0.0),
                            cache_item.get("width", 0),
                            cache_item.get("height", 0),
                            False  # <--- 标记：缓存命中
                        ))
                    cached_ok += 1
                    continue
            if SKIP_INTERNAL_IP and is_internal_ip(u):
                continue
            pending.append((g, n, u))

    logger.info(f"总链接:{total} 缓存有效:{cached_ok} 需测速:{len(pending)}")
    if not pending:
        final = {}
        for k, vs in result_map.items():
            # 排序：先按分辨率面积降序，再按帧率降序（不考虑新旧）
            vs.sort(key=lambda x: (-x[2]*x[3], -x[1]))
            final[k] = [u for u, _, _, _, _ in vs[:MAX_LINKS_PER_CHANNEL]]
        return final

    # 2. 并发测速（FFmpeg）
    sem = asyncio.Semaphore(FFMPEG_CONCURRENCY)

    async def test_one(item):
        g, n, u = item
        async with sem:
            res = await test_stream_with_ffmpeg(u)
            return g, n, u, res

    tasks = [test_one(i) for i in pending]
    c, ok, ng, lp = 0, 0, 0, -100
    print_progress_bar(0, len(tasks), ok, ng, lp)

    for coro in asyncio.as_completed(tasks):
        g, n, u, res = await coro
        c += 1
        if res["ok"]:
            ok += 1
            # 标记为【本次测速】(True)
            result_map[(g, n)].append((u, res["fps"], res["width"], res["height"], True))
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

    # 3. 更新缓存
    if ENABLE_CACHE and new_cache:
        cache.update(new_cache)
        save_cache(cache)

    # 4. 排序并截取
    final = {}
    for k, vs in result_map.items():
        # ========== 修改后的排序逻辑 ==========
        # 先按分辨率面积降序，再按帧率降序，不考虑新旧标记（即移除原先优先最新测速的规则）
        vs.sort(key=lambda x: (-x[2]*x[3], -x[1]))
        # 提取URL时注意元组长度变化 (现在是5个元素)
        final[k] = [u for u, _, _, _, _ in vs[:MAX_LINKS_PER_CHANNEL]]

    logger.info(f"测速完成，共 {len(final)} 个频道通过筛选")
    return final

# ============================================================================
# ========================= GitHub M3U 解析 ==================================
# ============================================================================
@retry_async(max_retries=3, delay=2)
async def download_github_m3u(url):
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as s:
            async with s.get(url, headers={'User-Agent': 'Mozilla/5.0'}) as r:
                if r.status == 200:
                    t = await r.text()
                    logger.info(f"下载成功 {url}")
                    return t
    except Exception as e:
        logger.debug(f"下载失败 {url}: {e}")
    return ""

def parse_m3u_file(content):
    ch = []
    g = n = u = ""
    for l in content.splitlines():
        l = l.strip()
        if l.startswith("#EXTINF"):
            m = re.search(r'#EXTINF:-1.*?group-title="([^"]*)",(.+)', l)
            if m:
                g = m.group(1).strip()
                n = m.group(2).strip()
            else:
                m = re.search(r'#EXTINF:-1.*?,(.+)', l)
                if m:
                    n = m.group(1).strip()
        elif l.startswith("http"):
            u = l.strip()  # 保留完整URL（含参数）
            if n and u:
                nn = normalize_cctv(n)
                gr = classify_channel(nn) or g
                fn = nn if gr == "央视频道" else (clean_chinese_only(n) if ENABLE_CHINESE_CLEAN else n)
                ch.append((gr, fn, u))
            g = n = u = ""
    return ch

# ============================================================================
# ========================= 页面点击与提取 ===================================
# ============================================================================
async def robust_click(loc, timeout=CLICK_TIMEOUT):
    try:
        await loc.scroll_into_view_if_needed(timeout=SCROLL_TIMEOUT)
        await asyncio.sleep(0.2)
        await loc.click(force=True, timeout=timeout)
        return True
    except:
        try:
            await loc.evaluate("el=>el.click()")
            return True
        except:
            return False

async def wait_for_element(page, sel, timeout=WAIT_FOR_ELEMENT_TIMEOUT):
    try:
        await page.wait_for_selector(sel, timeout=timeout)
        return True
    except:
        return False

@retry_async(max_retries=2, delay=1)
async def extract_one_ip(page, row, idx):
    e = []
    try:
        addr = await row.locator("div.item-title").first.inner_text(timeout=IP_ADDR_TIMEOUT)
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
        await asyncio.sleep(DELAY_AFTER_CLICK)  # 使用配置的点击后等待时间
        if not await wait_for_element(page, ".modal-dialog", MODAL_WAIT_TIMEOUT):  # 使用配置的模态框超时
            return []
        items = page.locator(".modal-dialog .item-content")
        total = await items.count()
        if total == 0:
            return []
        if MAX_CHANNELS_PER_IP > 0:
            total = min(total, MAX_CHANNELS_PER_IP)
        for i in range(total):
            try:
                n = await items.nth(i).locator(".item-title").inner_text(timeout=CHANNEL_NAME_TIMEOUT)
                u = await items.nth(i).locator(".item-subtitle").inner_text(timeout=CHANNEL_URL_TIMEOUT)
                n, u = n.strip(), u.strip()
                if not n or not u:
                    continue
                if not u.startswith(('http://', 'https://', 'rtsp://', 'rtmp://')):
                    u = DEFAULT_PROTOCOL + u
                nn = normalize_cctv(n)
                g = classify_channel(nn)
                if not g:
                    continue
                fn = nn if g == "央视频道" else (clean_chinese_only(n) if ENABLE_CHINESE_CLEAN else n)
                e.append((g, fn, u))
            except:
                continue
    except:
        pass
    return e

async def wait_data(page):
    logger.info("等待数据加载...")
    for _ in range(DATA_LOAD_TIMEOUT // DATA_CHECK_INTERVAL + 1):
        await asyncio.sleep(DATA_CHECK_INTERVAL)
        ok = await page.evaluate('''()=>{
            for(let i of document.querySelectorAll('div.ios-list-item')){
                let s=i.querySelector('.item-subtitle')?.innerText||'';
                if(s.includes('频道:'))return true;
            }return false;
        }''')
        if ok:
            logger.info("数据加载完成")
            return True
    logger.error("数据加载超时，爬取失败")
    return False

# ============================================================================
# ================= 新增：历史链接检查函数 ===================================
# ============================================================================
def parse_txt_file(filepath: Path) -> List[Tuple[str, str]]:
    """解析 iptv_channels.txt 文件，返回 (频道名, URL) 列表，忽略注释行和空行"""
    if not filepath.exists():
        logger.info(f"历史文件不存在: {filepath}")
        return []
    channels = []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('更新时间') or line.endswith('#genre#'):
                    continue
                if ',' in line:
                    parts = line.split(',', 1)
                    if len(parts) == 2:
                        name, url = parts[0].strip(), parts[1].strip()
                        if name and url:
                            channels.append((name, url))
        logger.info(f"从历史文件解析到 {len(channels)} 个链接")
    except Exception as e:
        logger.error(f"解析历史文件失败: {e}")
    return channels

async def check_url_connectivity(url: str, timeout: int) -> bool:
    """检查URL是否可连通（读取前1024字节）"""
    try:
        timeout_obj = aiohttp.ClientTimeout(total=timeout)
        async with aiohttp.ClientSession(timeout=timeout_obj) as session:
            async with session.get(url, headers={'User-Agent': 'Mozilla/5.0'}, allow_redirects=True) as resp:
                if resp.status != 200:
                    return False
                # 尝试读取一点数据，确保不是空响应
                try:
                    await resp.content.readexactly(1024)
                except asyncio.IncompleteReadError as e:
                    # 如果不足1024，但读到了部分，也算成功
                    return len(e.partial) > 0
                except Exception:
                    return False
                return True
    except Exception:
        return False

# ============================================================================
# ========================= 结果导出（已修复央视频道输出逻辑）==================
# ============================================================================
def export_results_with_timestamp(channel_map):
    # 使用北京时间（UTC+8）生成当前时间
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
                # ----- 修复后的央视频道处理：先按标准顺序，无法映射的保留原名称并排序后附加 -----
                # 构建原始名称到URL列表的映射
                name_to_urls = defaultdict(list)
                for name, url in chs:
                    name_to_urls[name].append(url)

                # 建立原始名称到标准名称的映射（尽可能匹配）
                name_to_std = {}
                for name in name_to_urls.keys():
                    std = None
                    # 先尝试完全匹配
                    if name in CCTV_ORDER:
                        std = name
                    else:
                        # 尝试按数字匹配（如CCTV-1匹配到CCTV-1综合）
                        cctv_match = CCTV_PATTERN.search(name)
                        if cctv_match:
                            num = cctv_match.group(2)  # 数字或"5+"
                            for std_candidate in CCTV_ORDER:
                                if num in std_candidate:
                                    std = std_candidate
                                    break
                    name_to_std[name] = std

                # 构建标准名称到URL列表的映射（合并同一标准名称的所有URL）
                std_to_urls = defaultdict(list)
                remaining = []  # 存放无法映射的 (原始名称, url)
                for name, urls in name_to_urls.items():
                    std = name_to_std.get(name)
                    if std:
                        std_to_urls[std].extend(urls)
                    else:
                        for url in urls:
                            remaining.append((name, url))

                # 按CCTV_ORDER顺序输出
                ordered_chs = []
                for std_name in CCTV_ORDER:
                    if std_name in std_to_urls:
                        for url in std_to_urls[std_name]:
                            ordered_chs.append((std_name, url))

                # 剩余无法映射的按名称排序后附加
                remaining.sort(key=lambda x: x[0])
                ordered_chs.extend(remaining)
                chs = ordered_chs
                # ----- 修复结束 -----
            else:
                chs = sorted(chs, key=lambda x: x[0])
            # 过滤空名称频道
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
                # ----- 同样处理央视频道（复用上述逻辑） -----
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
                                if num in std_candidate:
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
                # ----- 结束 -----
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
# ========================= 主流程 ===========================================
# ============================================================================
async def main():
    if EXTRACT_MODE not in ["酒店提取", "组播提取"]:
        logger.error("配置错误！EXTRACT_MODE 只能填写：酒店提取 或 组播提取")
        return

    logger.info(f"✅ 当前运行模式：【{EXTRACT_MODE}】")
    all_channels = []

    # 1. 从 GitHub 源获取
    if ENABLE_GITHUB_SOURCES:
        for url in GITHUB_M3U_LINKS:
            txt = await download_github_m3u(url)
            if txt:
                channels = parse_m3u_file(txt)
                all_channels.extend(channels)

    # 2. 从网站爬取
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=HEADLESS,
            args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage", "--disable-gpu", "--single-process"]
        )
        ctx = await browser.new_context(viewport={"width": 1920, "height": 1080})
        page = await ctx.new_page()

        try:
            logger.info(f"正在访问：{TARGET_URL}")
            await page.goto(TARGET_URL, timeout=PAGE_LOAD_TIMEOUT, wait_until="networkidle")

            eng_sel = build_selector(PAGE_CONFIG["engine_search"], "a.sidebar-link,button,div.segment-item")
            if eng_sel:
                eng = page.locator(eng_sel).first
                if await eng.count() > 0:
                    logger.info("点击引擎搜索")
                    await robust_click(eng)

            if EXTRACT_MODE == "酒店提取":
                tab_sel = build_selector(PAGE_CONFIG["hotel"], "div.segment-item")
                logger.info("点击网页按钮：【酒店提取】")
            else:
                tab_sel = build_selector(PAGE_CONFIG["multicast"], "div.segment-item")
                logger.info("点击网页按钮：【组播提取】")

            tab = page.locator(tab_sel).first
            await robust_click(tab)

            start_sel = build_selector(PAGE_CONFIG["start_button"], "button")
            start_btn = page.locator(start_sel).first
            logger.info("点击【开始提取】")
            await robust_click(start_btn)

            logger.info(f"⏳ 等待 {AFTER_START_WAIT} 秒后开始提取数据...")
            await asyncio.sleep(AFTER_START_WAIT)

            if not await wait_data(page):
                logger.error("❌ 网站爬取失败")
            else:
                rows = page.locator("div.ios-list-item").filter(
                    has=page.locator("div.item-subtitle:has-text('频道:')")
                )
                total_rows = await rows.count()
                process_count = min(total_rows, MAX_IPS) if MAX_IPS > 0 else total_rows
                if MAX_IPS > 0 and process_count < total_rows:
                    logger.info(f"总共找到 {total_rows} 个IP，准备处理前 {process_count} 个IP")
                else:
                    logger.info(f"总共找到 {total_rows} 个IP，准备处理全部 {process_count} 个IP")

                web_channels = []
                for i in range(process_count):
                    entries = await extract_one_ip(page, rows.nth(i), i + 1)
                    if entries:
                        web_channels.extend(entries)
                    if MAX_TOTAL_CHANNELS > 0 and len(web_channels) >= MAX_TOTAL_CHANNELS:
                        web_channels = web_channels[:MAX_TOTAL_CHANNELS]
                        logger.info("已达频道上限，停止提取")
                        break
                    await asyncio.sleep(DELAY_BETWEEN_IPS)

                all_channels.extend(web_channels)
                logger.info(f"网站提取完成：{len(web_channels)} 条")

        except Exception as e:
            logger.exception("❌ 爬取过程异常")
        finally:
            await page.close()
            await ctx.close()
            await browser.close()

    # ========== 检查历史链接 ==========
    if ENABLE_HISTORY_CHECK:
        history_items = parse_txt_file(HISTORY_FILE)
        if history_items:
            logger.info(f"开始对 {len(history_items)} 个历史链接进行连通性测试...")
            sem = asyncio.Semaphore(HISTORY_CHECK_CONCURRENCY)

            async def check_one(name, url):
                async with sem:
                    ok = await check_url_connectivity(url, HISTORY_CHECK_TIMEOUT)
                    return name, url, ok

            tasks = [check_one(name, url) for name, url in history_items]
            passed = 0
            for coro in asyncio.as_completed(tasks):
                name, url, ok = await coro
                if ok:
                    passed += 1
                    # 对频道名进行标准化和分类
                    nn = normalize_cctv(name)
                    gr = classify_channel(nn)
                    if gr:  # 只有能分类的才加入
                        fn = nn if gr == "央视频道" else (clean_chinese_only(name) if ENABLE_CHINESE_CLEAN else name)
                        all_channels.append((gr, fn, url))
            logger.info(f"历史链接连通性测试完成，通过 {passed}/{len(history_items)} 条")
        else:
            logger.info("历史文件为空或不存在，跳过")

    # ========== 过滤migu链接 ==========
    if ENABLE_MIGU_FILTER:
        original_count = len(all_channels)
        all_channels = [(g, n, u) for (g, n, u) in all_channels if 'migu' not in u.lower()]
        filtered_count = original_count - len(all_channels)
        logger.info(f"已过滤 {filtered_count} 条包含 'migu' 的链接，剩余 {len(all_channels)} 条")

    # 3. 去重
    if not all_channels:
        logger.error("❌ 未获取到任何频道")
        return

    channel_map = defaultdict(list)
    seen = set()
    for g, n, u in all_channels:
        key = (g, n, u)
        if key in seen:
            continue
        seen.add(key)
        channel_map[(g, n)].append(u)

    # 4. FFmpeg测速筛选（包含分辨率排序，已修改排序规则）
    if ENABLE_FFMPEG_TEST:
        logger.info("开始FFmpeg测速筛选（链接将按分辨率优先、帧率其次排序）")
        channel_map = await run_ffmpeg_test(channel_map)

    # 5. 导出结果
    export_results_with_timestamp(channel_map)
    logger.info("🎉 任务全部完成！")

if __name__ == "__main__":
    if sys.platform == 'linux':
        asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())
    elif sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
