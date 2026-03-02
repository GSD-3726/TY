#!/usr/bin/env python3
"""
IPTV 组播提取工具（测速优化版）
- 网站爬取逻辑完全不变
- 测速模块冗余清理，简化核心逻辑
- 取消预过滤 + 放宽测速要求 + 优化日志
- 适配 GitHub Actions 低并发、超时保护
"""

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

import aiohttp
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError


# ============================================================================
# ======================== 【硬编码配置区】 ==================================
# ============================================================================

# -------------------------- 1. 基础爬取设置 --------------------------
TARGET_URL = "https://iptv.809899.xyz"          # 目标网站地址
HEADLESS = True                                  # 是否无头模式
BROWSER_TYPE = "chromium"                        # 浏览器内核
MAX_IPS = 10                                      # 最多处理IP行数
MAX_TOTAL_CHANNELS = 0                            # 总频道数上限（0不限）
PAGE_LOAD_TIMEOUT = 120000                        # 页面加载超时（毫秒）

# -------------------------- 2. 文件输出设置 --------------------------
OUTPUT_DIR = Path(__file__).parent                # 输出目录（脚本所在目录）
OUTPUT_M3U_FILENAME = OUTPUT_DIR / "iptv_channels.m3u"
OUTPUT_TXT_FILENAME = OUTPUT_DIR / "iptv_channels.txt"
MAX_LINKS_PER_CHANNEL = 5                          # 每个频道保留最多链接数

# -------------------------- 3. FFmpeg 测速设置 --------------------------
ENABLE_FFMPEG_TEST = True                          # 是否启用FFmpeg测速
FFMPEG_PATH = "ffmpeg"                             # FFmpeg路径（假设在PATH中）
FFMPEG_TEST_DURATION = 10                           # 单链接测速时长（秒）
FFMPEG_CONCURRENCY = 2                             # 并发测速数
MIN_AVG_FPS = 20.0                                  # 最低平均帧率
MIN_FRAMES = 180                                    # 最低解码帧数

# -------------------------- 4. GitHub 自定义源设置 --------------------------
ENABLE_GITHUB_SOURCES = False                        # 是否启用GitHub源
GITHUB_M3U_LINKS = [                                 # GitHub源列表
    "https://gh-proxy.com/https://raw.githubusercontent.com/develop202/migu_video/main/interface.txt",
    "https://gh-proxy.com/https://raw.githubusercontent.com/9527xiao9527/iptv/main/iptv.txt",
    "https://gh.llkk.cc/https://raw.githubusercontent.com/Kimentanm/aptv/master/m3u/iptv.m3u",
    "https://gh-proxy.com/https://raw.githubusercontent.com/vbskycn/iptv/master/tv/iptv4.m3u8",
]

# -------------------------- 5. 网页操作延时 --------------------------
DELAY_BETWEEN_IPS = 1.0                             # IP处理间隔（秒）
DELAY_AFTER_CLICK = 1.0                             # 点击后等待（秒）
MAX_CHANNELS_PER_IP = 0                              # 单IP最大频道数（0不限）
DATA_LOAD_TIMEOUT = 120                              # 数据加载超时（秒）

# -------------------------- 6. 数据清洗 --------------------------
ENABLE_CHINESE_CLEAN = True                          # 清理非中文字符
ENABLE_DEDUPLICATION = True                          # 启用去重
ENABLE_SCREENSHOTS = False                            # 截图调试
CCTV_USE_MAPPING = True                               # CCTV映射中文名

# -------------------------- 7. 网络协议 --------------------------
DEFAULT_PROTOCOL = "http://"                          # 默认协议

# -------------------------- 8. 缓存设置 --------------------------
ENABLE_CACHE = True                                   # 启用缓存
CACHE_FILE = OUTPUT_DIR / "iptv_speed_cache.json"     # 缓存文件
CACHE_EXPIRE_HOURS = 48                                # 缓存过期小时

# -------------------------- 9. 更新时间显示 --------------------------
TIME_DISPLAY_AT_TOP = False                            # 更新时间在顶部
UPDATE_STREAM_URL = "https://gitee.com/bmg369/tvtest/raw/master/cg/index.m3u8"  # 占位流

# -------------------------- 10. 页面元素定位关键词 --------------------------
PAGE_CONFIG = {
    "engine_search": ["引索搜索", "引擎搜索", "关键词搜索"],
    "multicast_tab": ["组播提取"],
    "start_button": ["开始播放", "开始搜索", "开始提取"],
}

# ============================================================================
# ============================ 频道分类规则 ==================================
# ============================================================================

# 频道自动分类规则
CATEGORY_RULES = [
    {"name": "4K专区",      "keywords": ["4k"]},
    {"name": "央视频道",    "keywords": ["cctv", "cetv", "中央"]},
    {"name": "卫视频道",    "keywords": ["卫视", "凤凰", "tvb", "湖南", "浙江", "江苏", "东方"]},
    {"name": "电影频道",    "keywords": ["电影", "影院", "chc"]},
    {"name": "轮播频道",    "keywords": ["轮播"]},
    {"name": "儿童频道",    "keywords": ["少儿", "动画", "卡通"]},
]

GROUP_ORDER = ["央视频道", "卫视频道", "电影频道", "4K专区", "儿童频道", "轮播频道"]

# CCTV 台标映射表
CCTV_NAME_MAPPING = {
    "1": "综合", "2": "财经", "3": "综艺", "4": "国际", "5": "体育",
    "5+": "体育赛事", "6": "电影", "7": "国防军事", "8": "电视剧",
    "9": "纪录", "10": "科教", "11": "戏曲", "12": "社会与法",
    "13": "新闻", "14": "少儿", "15": "音乐", "16": "奥林匹克",
    "17": "农业农村",
}

# 央视严格排序
CCTV_ORDER = [
    "CCTV-1综合", "CCTV-2财经", "CCTV-3综艺", "CCTV-4国际",
    "CCTV-5体育", "CCTV-5+体育赛事", "CCTV-6电影", "CCTV-7国防军事",
    "CCTV-8电视剧", "CCTV-9纪录", "CCTV-10科教", "CCTV-11戏曲",
    "CCTV-12社会与法", "CCTV-13新闻", "CCTV-14少儿", "CCTV-15音乐",
    "CCTV-16奥林匹克", "CCTV-17农业农村", "CETV1", "CETV2", "CETV4", "CETV5"
]

# ============================================================================
# ============================= 日志配置 =====================================
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(OUTPUT_DIR / 'iptv_extractor.log', encoding='utf-8')
    ]
)
logger = logging.getLogger('IPTV-Extractor')

# ============================================================================
# ========================= 工具函数（分类、选择器等） ========================
# ============================================================================

CCTV_PATTERN = re.compile(r'(cctv)[-\s]?(\d{1,3}|5\+)', re.IGNORECASE)
CHINESE_ONLY_PATTERN = re.compile(r'[^\u4e00-\u9fff]')


def build_classifier():
    compiled = []
    for rule in CATEGORY_RULES:
        if not rule["keywords"]:
            continue
        pattern = re.compile("|".join(re.escape(kw.lower()) for kw in rule["keywords"]))
        compiled.append((rule["name"], pattern))
    return lambda name: next((group for group, pat in compiled if pat.search(name.lower())), None)


classify_channel = build_classifier()


def normalize_cctv(name: str) -> str:
    name_lower = name.lower()
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
    """构建精准的元素选择器，支持指定元素类型"""
    if not text_list:
        return ""
    if len(text_list) == 1:
        return f"{element_type}:has-text('{text_list[0]}')"
    pattern = "|".join(re.escape(t) for t in text_list)
    return f"{element_type}:text-matches('{pattern}')"


# 预先生成页面元素选择器
ENGINE_SELECTOR = build_selector(PAGE_CONFIG["engine_search"], "a.sidebar-link,button,div.segment-item")
MCAST_SELECTOR = build_selector(PAGE_CONFIG["multicast_tab"], "div.segment-item")
START_SELECTOR = build_selector(PAGE_CONFIG["start_button"], "button")

# ============================================================================
# ========================= 重试装饰器 =======================================
# ============================================================================

def retry_async(max_retries=2, delay=1.0, exceptions=(Exception,)):
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            for attempt in range(1, max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_retries:
                        raise
                    logger.warning(f"尝试 {attempt}/{max_retries} 失败: {e}，重试中...")
                    await asyncio.sleep(delay)
            return None
        return wrapper
    return decorator


# ============================================================================
# ========================= 进度条打印工具（5%步进，减少日志） =================
# ============================================================================

def print_progress_bar(current: int, total: int, success: int, failed: int, last_percent: int) -> int:
    """打印测速进度条，5%步进减少日志"""
    if total == 0:
        return 0
    percent_int = int((current / total) * 100)
    # 仅在5%步进、开始、结束时打印
    if not ((percent_int % 5 == 0 and percent_int > last_percent) or current == total or current == 0):
        return last_percent
    if percent_int == last_percent and current != total:
        return last_percent

    bar = '█' * int(20 * current / total) + '░' * (20 - int(20 * current / total))
    logger.info(f"[{percent_int:3d}%] {bar} ({current}/{total}) | 成功:{success} | 失败:{failed}")
    return percent_int


# ============================================================================
# ========================= FFmpeg 自动检测（适配Linux） ======================
# ============================================================================

def find_ffmpeg() -> str:
    """自动查找FFmpeg可执行文件，找不到则退出"""
    # 先检查配置路径，再检查常见位置
    for path in [FFMPEG_PATH, "ffmpeg", "/usr/local/bin/ffmpeg", "/usr/bin/ffmpeg"]:
        if shutil.which(path):
            logger.info(f"找到FFmpeg: {path}")
            return path
    logger.error("未找到FFmpeg！请安装FFmpeg或正确配置FFMPEG_PATH")
    sys.exit(1)

# 重新设置FFMPEG_PATH为有效路径
FFMPEG_PATH = find_ffmpeg()


# ============================================================================
# ========================= FFmpeg 输出解析（核心精简版） =====================
# ============================================================================

def parse_ffmpeg_output(output: str) -> Tuple[int, float]:
    """解析FFmpeg输出，仅提取核心的帧数和平均帧率"""
    frame_pattern = re.compile(r'frame\s*=\s*(\d+)', re.IGNORECASE)
    fps_pattern = re.compile(r'(?:fps|avg_fps)\s*=\s*([\d.]+)', re.IGNORECASE)

    frame_matches = frame_pattern.findall(output)
    fps_matches = fps_pattern.findall(output)

    frames = int(frame_matches[-1]) if frame_matches else 0
    avg_fps = float(fps_matches[-1]) if fps_matches else (frames / FFMPEG_TEST_DURATION if frames > 0 else 0.0)

    return frames, avg_fps


# ============================================================================
# ========================= 【核心】FFmpeg测速（精简优化版） ==================
# ============================================================================

async def test_stream_with_ffmpeg(url: str) -> Dict[str, Any]:
    """FFmpeg流测速，仅基于帧数和帧率判定有效性，提前终止无效流"""
    cmd = [
        FFMPEG_PATH, "-hide_banner", "-y",
        "-fflags", "nobuffer+flush_packets",
        "-flags", "low_delay",
        "-rw_timeout", "3000000",
        "-i", url,
        "-t", str(FFMPEG_TEST_DURATION),
        "-vf", "fps=1",
        "-f", "null", "-"
    ]

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE
        )

        kill_trigger = False
        # 5秒无有效帧提前终止
        async def monitor_proc():
            nonlocal kill_trigger
            await asyncio.sleep(5)
            if proc.returncode is None:
                try:
                    stderr_part = await proc.stderr.readline()
                    if b"frame=0" in stderr_part or b"Invalid data" in stderr_part:
                        kill_trigger = True
                        proc.kill()
                except:
                    pass

        monitor_task = asyncio.create_task(monitor_proc())
        try:
            _, stderr = await asyncio.wait_for(proc.communicate(), timeout=FFMPEG_TEST_DURATION + 5)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            return {"ok": False, "fps": 0.0, "frames": 0}
        finally:
            monitor_task.cancel()
            if proc.returncode is None:
                proc.kill()
                await proc.wait()

        if kill_trigger:
            return {"ok": False, "fps": 0.0, "frames": 0}

        output = stderr.decode('utf-8', errors='ignore')
        frames, avg_fps = parse_ffmpeg_output(output)
        is_smooth = frames >= MIN_FRAMES and avg_fps >= MIN_AVG_FPS

        logger.debug(f"测速结果：{url[:50]} | 帧数={frames} | 帧率={avg_fps:.2f} | 有效={is_smooth}")
        return {"ok": is_smooth, "fps": avg_fps, "frames": frames}

    except Exception as e:
        logger.debug(f"测速失败：{url[:50]} | 原因：{str(e)[:50]}")
        return {"ok": False, "fps": 0.0, "frames": 0}


# ============================================================================
# ========================= 批量测速（精简版，取消预过滤） ====================
# ============================================================================

async def run_ffmpeg_test(channel_map: Dict[Tuple[str, str], List[str]]) -> Dict[Tuple[str, str], List[str]]:
    if not channel_map:
        return {}

    cache = load_cache() if ENABLE_CACHE else {}
    new_cache = {}
    result_map = defaultdict(list)
    pending_tasks = []

    # 分流缓存与待测速链接
    total_urls = sum(len(urls) for urls in channel_map.values())
    cached_valid = 0
    for (group, name), urls in channel_map.items():
        for url in urls:
            if url in cache and cache[url]["ok"]:
                result_map[(group, name)].append((url, cache[url]["fps"]))
                cached_valid += 1
            else:
                pending_tasks.append((group, name, url))

    logger.info(f"总待处理链接：{total_urls} | 缓存有效：{cached_valid} | 需测速：{len(pending_tasks)}")
    if not pending_tasks:
        return {k: [u for u, _ in sorted(v, key=lambda x: -x[1])[:MAX_LINKS_PER_CHANNEL]] for k, v in result_map.items()}

    # 并发测速控制
    sem = asyncio.Semaphore(FFMPEG_CONCURRENCY)
    async def bound_test(item):
        group, name, url = item
        async with sem:
            return group, name, url, await test_stream_with_ffmpeg(url)

    tasks = [bound_test(item) for item in pending_tasks]
    total_pending = len(tasks)
    completed, success, failed = 0, 0, 0
    last_percent = -100

    print_progress_bar(0, total_pending, success, failed, last_percent)
    for coro in asyncio.as_completed(tasks):
        group, name, url, res = await coro
        completed += 1

        if ENABLE_CACHE:
            new_cache[url] = {"ok": res["ok"], "fps": res["fps"], "frames": res["frames"], "timestamp": time.time()}

        if res["ok"]:
            success += 1
            result_map[(group, name)].append((url, res["fps"]))
        else:
            failed += 1

        last_percent = print_progress_bar(completed, total_pending, success, failed, last_percent)

    # 更新缓存
    if ENABLE_CACHE and new_cache:
        cache.update(new_cache)
        save_cache(cache)

    # 最终排序与截断
    final_map = {}
    for key, items in result_map.items():
        items.sort(key=lambda x: -x[1])
        final_map[key] = [url for url, _ in items[:MAX_LINKS_PER_CHANNEL]]

    logger.info(f"测速筛选完成，最终保留 {sum(len(v) for v in final_map.values())} 条优质链接")
    return final_map


# ============================================================================
# ========================= 缓存工具函数（精简版） ============================
# ============================================================================

def load_cache() -> Dict[str, Dict[str, Any]]:
    """加载测速缓存，自动过滤过期条目"""
    if not ENABLE_CACHE or not CACHE_FILE.exists():
        return {}
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            cache = json.load(f)
        now = time.time()
        expire = CACHE_EXPIRE_HOURS * 3600
        valid_cache = {u: d for u, d in cache.items() if expire == 0 or (now - d.get("timestamp", 0)) < expire}
        logger.info(f"缓存加载完成，有效条目：{len(valid_cache)}")
        return valid_cache
    except Exception as e:
        logger.warning(f"缓存加载失败: {e}")
        return {}


def save_cache(cache: Dict[str, Dict[str, Any]]) -> None:
    """保存测速缓存"""
    if not ENABLE_CACHE:
        return
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning(f"缓存保存失败: {e}")


# ============================================================================
# ========================= GitHub M3U 处理 ==================================
# ============================================================================

@retry_async(max_retries=3, delay=2.0)
async def download_github_m3u(url: str) -> str:
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
            headers = {'User-Agent': 'Mozilla/5.0'}
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    content = await response.text()
                    logger.info(f"成功下载 {url}，长度: {len(content)} 字符")
                    return content
                else:
                    logger.error(f"下载失败，状态码: {response.status}")
                    return ""
    except Exception as e:
        logger.error(f"下载失败: {e}")
        return ""


def parse_m3u_file(content: str) -> List[Tuple[str, str, str]]:
    channels = []
    lines = content.splitlines()
    current_group = ""
    current_name = ""
    current_url = ""

    for line in lines:
        line = line.strip()
        if line.startswith("#EXTINF"):
            match = re.search(r'#EXTINF:-1.*?group-title="([^"]+)",(.+)', line)
            if match:
                current_group = match.group(1)
                current_name = match.group(2)
            else:
                match = re.search(r'#EXTINF:-1.*?,(.+)', line)
                if match:
                    current_name = match.group(1)
        elif line.startswith("http"):
            current_url = line.split("?")[0]
            if current_name and current_url:
                norm_name = normalize_cctv(current_name)
                group = classify_channel(norm_name) or current_group
                final_name = norm_name if group == "央视频道" else (clean_chinese_only(current_name) if ENABLE_CHINESE_CLEAN else current_name)
                channels.append((group, final_name, current_url))
                current_name = ""
                current_url = ""
    return channels


# ============================================================================
# ============================ 页面交互函数（完全未修改） =====================
# ============================================================================

async def robust_click(locator, timeout=10000):
    try:
        await locator.scroll_into_view_if_needed(timeout=3000)
        await asyncio.sleep(0.2)
        await locator.click(force=True, timeout=timeout)
        return True
    except Exception:
        try:
            await locator.evaluate("el => el.click()")
            return True
        except Exception:
            return False


async def wait_for_element(page, selector, timeout=30000):
    try:
        await page.wait_for_selector(selector, timeout=timeout)
        return True
    except PlaywrightTimeoutError:
        return False


@retry_async(max_retries=2, delay=1.0)
async def extract_one_ip(page, row, ip_index):
    entries = []
    addr = None
    try:
        addr_elem = row.locator("div.item-title").first
        addr = await addr_elem.inner_text(timeout=3000)
        addr = addr.strip()
        if not addr:
            logger.warning(f"第 {ip_index} 行地址为空，跳过")
            return []
        logger.info(f"处理地址 [{ip_index}]: {addr}")
    except Exception as e:
        logger.warning(f"提取第 {ip_index} 行地址失败: {e}")
        return []

    try:
        list_btn = row.locator("button:has(i.fa-list)").first
        if await list_btn.count() > 0:
            if not await robust_click(list_btn):
                await row.click(timeout=3000)
        else:
            await row.click(timeout=3000)
        await asyncio.sleep(DELAY_AFTER_CLICK)

        # 弹窗选择器
        modal = page.locator(".modal-dialog").first
        if not await wait_for_element(page, ".modal-dialog", timeout=5000):
            logger.warning(f"第 {ip_index} 行地址 {addr} 未弹出频道弹窗，跳过")
            return []

        # 频道项选择器
        items = modal.locator(".item-content")
        total = await items.count()
        if total == 0:
            logger.warning(f"第 {ip_index} 行地址 {addr} 无频道数据，跳过")
            return []
            
        if MAX_CHANNELS_PER_IP > 0:
            total = min(total, MAX_CHANNELS_PER_IP)

        logger.info(f"第 {ip_index} 行地址 {addr} 共提取到 {total} 个频道")
        for i in range(total):
            try:
                name_elem = items.nth(i).locator(".item-title").first
                link_elem = items.nth(i).locator(".item-subtitle").first
                name = await name_elem.inner_text(timeout=2000)
                link = await link_elem.inner_text(timeout=2000)
                name, link = name.strip(), link.strip()
                if not name or not link:
                    continue

                if not link.startswith(('http://', 'https://', 'rtsp://', 'rtmp://')):
                    link = DEFAULT_PROTOCOL + link

                norm = normalize_cctv(name)
                group = classify_channel(norm)
                if not group:
                    continue
                final_name = norm if group == "央视频道" else (clean_chinese_only(name) if ENABLE_CHINESE_CLEAN else name)
                if final_name:
                    entries.append((group, final_name, link))
            except Exception as e:
                logger.warning(f"提取第 {ip_index} 行第 {i+1} 个频道失败: {e}")
                continue
    except Exception as e:
        logger.warning(f"提取第 {ip_index} 行地址 {addr} 出错: {e}")
    return entries


async def wait_data(page):
    """等待数据加载，超时从配置读取，每次30秒轮询"""
    timeout = DATA_LOAD_TIMEOUT
    logger.info(f"等待数据加载，超时设置：{timeout}秒")
    start_time = time.time()
    attempts = 0
    max_attempts = timeout // 30 + 1

    while attempts < max_attempts:
        attempts += 1
        logger.info(f"等待30秒加载数据... (尝试 {attempts}/{max_attempts})")
        await asyncio.sleep(30)

        has_data = await page.evaluate('''() => {
            const items = document.querySelectorAll('div.ios-list-item');
            for(let item of items) {
                const subtitle = item.querySelector('.item-subtitle')?.innerText?.trim();
                if(subtitle && subtitle.includes('频道:')) {
                    return true;
                }
            }
            return false;
        }''')
        if has_data:
            logger.info("数据加载完成")
            return True

    logger.error(f"数据加载超时（{timeout}秒）")
    if ENABLE_SCREENSHOTS:
        screenshot_path = OUTPUT_DIR / f"timeout_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
        await page.screenshot(path=screenshot_path, full_page=True)
        logger.info(f"超时截图已保存：{screenshot_path}")
    return False


# ============================================================================
# ============================ 文件导出函数（完全未修改） =====================
# ============================================================================

def export_results_with_timestamp(channel_map: Dict[Tuple[str, str], List[str]]):
    now = datetime.datetime.now()
    time_str = now.strftime("%Y-%m-%d %H:%M:%S")
    update_url = UPDATE_STREAM_URL

    grouped = defaultdict(list)
    for (group, name), urls in channel_map.items():
        for url in urls:
            grouped[group].append((name, url))

    # 导出 M3U
    with open(OUTPUT_M3U_FILENAME, "w", encoding="utf-8") as f:
        f.write("#EXTM3U\n")
        if TIME_DISPLAY_AT_TOP:
            f.write(f'#EXTINF:-1 tvg-name="{time_str}" tvg-id="更新时间" tvg-logo="" group-title="更新时间", {time_str}\n{update_url}\n\n')

        for group in GROUP_ORDER:
            if group not in grouped:
                continue
            if group == "央视频道":
                name_url_dict = {name: url for name, url in grouped[group]}
                sorted_channels = [(name, name_url_dict[name]) for name in CCTV_ORDER if name in name_url_dict]
            else:
                sorted_channels = sorted(grouped[group], key=lambda x: x[0])
            for name, url in sorted_channels:
                f.write(f'#EXTINF:-1 group-title="{group}",{name}\n{url}\n')
            f.write("\n")

        if not TIME_DISPLAY_AT_TOP:
            f.write(f'#EXTINF:-1 tvg-name="{time_str}" tvg-id="更新时间" tvg-logo="" group-title="更新时间", {time_str}\n{update_url}\n\n')

    # 导出 TXT
    with open(OUTPUT_TXT_FILENAME, "w", encoding="utf-8") as f:
        if TIME_DISPLAY_AT_TOP:
            f.write("更新时间,#genre#\n")
            f.write(f"{time_str},{update_url}\n\n")
        for group in GROUP_ORDER:
            if group not in grouped:
                continue
            f.write(f"{group},#genre#\n")
            if group == "央视频道":
                name_url_dict = {name: url for name, url in grouped[group]}
                sorted_channels = [(name, name_url_dict[name]) for name in CCTV_ORDER if name in name_url_dict]
            else:
                sorted_channels = sorted(grouped[group], key=lambda x: x[0])
            for name, url in sorted_channels:
                f.write(f"{name},{url}\n")
            f.write("\n")
        if not TIME_DISPLAY_AT_TOP:
            f.write("更新时间,#genre#\n")
            f.write(f"{time_str},{update_url}\n\n")

    total_links = sum(len(v) for v in grouped.values()) + 1
    logger.info(f"导出完成！共 {total_links} 条链接（含更新时间）")


# ============================================================================
# ============================= 主流程（爬取逻辑完全未修改） ==================
# ============================================================================

async def main():
    if ENABLE_SCREENSHOTS:
        (OUTPUT_DIR / "screenshots").mkdir(exist_ok=True)

    # 存储所有来源的频道数据
    all_channels = []

    # ========== 1. 从GitHub下载M3U文件并解析（受开关控制） ==========
    logger.info("=== 开始处理GitHub M3U链接 ===")
    github_total = 0
    if ENABLE_GITHUB_SOURCES and GITHUB_M3U_LINKS:
        for url in GITHUB_M3U_LINKS:
            logger.info(f"正在下载: {url}")
            content = await download_github_m3u(url)
            if content:
                channels = parse_m3u_file(content)
                github_channels = []
                for group, name, link in channels:
                    if not link.startswith(('http://', 'https://', 'rtsp://', 'rtmp://')):
                        link = DEFAULT_PROTOCOL + link
                    github_channels.append((group, name, link))
                all_channels.extend(github_channels)
                count = len(github_channels)
                github_total += count
                logger.info(f"从 {url} 提取 {count} 个频道")
        logger.info(f"从GitHub链接总共获取到 {github_total} 个频道（去重前）")
    else:
        logger.info("GitHub源已禁用或未配置，跳过")

    # ========== 2. 从目标网站爬取频道 ==========
    logger.info("\n=== 开始从目标网站爬取频道 ===")
    website_total = 0
    async with async_playwright() as p:
        browser = await getattr(p, BROWSER_TYPE).launch(
            headless=HEADLESS,
            args=[
                "--no-sandbox", "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",  # 解决内存不足
                "--disable-gpu",            # 无GPU环境优化
                "--single-process"           # 单进程模式（Actions稳定）
            ]
        )
        ctx = await browser.new_context(viewport={"width": 1920, "height": 1080})
        page = await ctx.new_page()

        try:
            logger.info(f"正在访问: {TARGET_URL}")
            await page.goto(TARGET_URL, timeout=PAGE_LOAD_TIMEOUT, wait_until="networkidle")

            if ENGINE_SELECTOR:
                eng = page.locator(ENGINE_SELECTOR).first
                if await eng.count() > 0:
                    logger.info("点击引擎搜索按钮")
                    await robust_click(eng)

            if MCAST_SELECTOR:
                mcast = page.locator(MCAST_SELECTOR).first
                logger.info("点击酒店提取标签")
                await robust_click(mcast)

            if START_SELECTOR:
                start = page.locator(START_SELECTOR).first
                logger.info("点击开始提取按钮")
                await robust_click(start)

            # 等待数据加载
            if not await wait_data(page):
                logger.error("网站数据加载失败，跳过网站爬取")
            else:
                # 精准筛选IP行
                rows = page.locator("div.ios-list-item").filter(
                    has=page.locator("div.item-subtitle:has-text('频道:')")
                )
                total_rows = await rows.count()
                logger.info(f"【精准筛选】找到包含频道信息的IP行总数：{total_rows}")
                
                if total_rows == 0:
                    logger.error("未找到任何包含频道信息的IP行，跳过网站爬取")
                    return

                process_count = min(total_rows, MAX_IPS) if MAX_IPS > 0 else total_rows
                logger.info(f"配置MAX_IPS={MAX_IPS}，实际将处理前 {process_count} 个IP行")

                website_channels = []
                processed_ip_count = 0
                for i in range(process_count):
                    row = rows.nth(i)
                    entries = await extract_one_ip(page, row, i+1)
                    if entries:
                        website_channels.extend(entries)
                        processed_ip_count += 1
                    
                    if MAX_TOTAL_CHANNELS > 0 and len(website_channels) >= MAX_TOTAL_CHANNELS:
                        website_channels = website_channels[:MAX_TOTAL_CHANNELS]
                        logger.info(f"已达到总频道数上限 {MAX_TOTAL_CHANNELS}，停止提取IP行")
                        break
                    
                    if i < process_count - 1:
                        await asyncio.sleep(DELAY_BETWEEN_IPS)

                website_total = len(website_channels)
                logger.info(f"从网站提取频道总数：{website_total} 条")
                all_channels.extend(website_channels)

        except Exception as e:
            logger.exception("网站爬取过程异常")
        finally:
            # 确保浏览器彻底关闭
            await page.close()
            await ctx.close()
            await browser.close()

    # ========== 3. 合并并处理所有频道 ==========
    logger.info(f"\n=== 开始合并处理所有频道 ===")
    logger.info(f"GitHub来源：{github_total} 条 | 网站来源：{website_total} 条 | 合并后总频道数（含重复）：{len(all_channels)} 条")

    if not all_channels:
        logger.error("未从任何来源提取到频道，程序结束")
        return

    # 全局去重
    channel_map = defaultdict(list)
    seen = set()
    duplicate_count = 0
    for group, name, url in all_channels:
        if ENABLE_DEDUPLICATION:
            key = (group, name, url)
            if key in seen:
                duplicate_count += 1
                continue
            seen.add(key)
        channel_map[(group, name)].append(url)

    logger.info(f"去重完成，重复频道数：{duplicate_count} 条")
    logger.info(f"去重后独立频道数：{len(channel_map)} 个")

    # FFmpeg测速
    if ENABLE_FFMPEG_TEST and channel_map:
        logger.info("开始FFmpeg测速筛选")
        channel_map = await run_ffmpeg_test(channel_map)
    else:
        logger.info("跳过FFmpeg测速")

    # 导出最终结果
    export_results_with_timestamp(channel_map)

    logger.info("\n=== 任务全部完成！===")
    logger.info(f"最终导出的频道数：{len(channel_map)} 个")
    logger.info(f"最终导出的链接总数：{sum(len(v) for v in channel_map.values())} 条")


if __name__ == "__main__":
    # 事件循环策略适配
    if sys.platform == 'linux':
        asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())
    elif sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
