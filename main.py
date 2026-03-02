#!/usr/bin/env python3
"""
IPTV 组播提取工具（Vue SPA适配版）
仅从config.ini读取配置，无硬编码默认值
核心功能：
1. 适配Vue SPA单页应用的segment-item选项卡切换
2. 同时从GitHub M3U链接和目标网站爬取频道（双来源）
3. FFmpeg测速筛选优质源
4. 自动分类、去重、导出M3U/TXT格式
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
import aiohttp
import configparser
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any
from urllib.parse import urljoin
import functools
from urllib.parse import urlparse

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError


# ============================================================================
# ========================= 配置加载函数 =====================================
# ============================================================================

def load_config(config_file: str = "config.ini") -> configparser.ConfigParser:
    """仅从INI文件加载配置，无任何默认值"""
    config = configparser.ConfigParser()
    
    # 读取配置文件（必须存在且包含所有必要配置）
    if not os.path.exists(config_file):
        raise FileNotFoundError(f"配置文件 {config_file} 不存在，请创建并配置所有必要项")
    
    config.read(config_file, encoding='utf-8')
    
    # 验证必要的配置节是否存在
    required_sections = [
        "General", "Output", "FFmpeg", "GitHub", "Delay", 
        "Cleaning", "Network", "Cache", "UpdateTime", "PageElements"
    ]
    missing_sections = [sec for sec in required_sections if sec not in config.sections()]
    if missing_sections:
        raise ValueError(f"配置文件缺少必要节：{', '.join(missing_sections)}")
    
    return config


def parse_list(value: str) -> List[str]:
    """解析多行/逗号分隔的列表，支持空行和注释，过滤无效字符"""
    if not value:
        return []
    
    # 步骤1：按换行分割，处理每行
    lines = [line.strip() for line in value.split('\n') if line.strip()]
    result = []
    
    for line in lines:
        # 忽略注释行
        if line.startswith('#'):
            continue
        # 处理逗号分隔的情况（兼容原有格式）
        if ',' in line:
            items = [item.strip() for item in line.split(',') if item.strip()]
            result.extend(items)
        else:
            # 过滤无效URL（仅对GitHub链接生效）
            if line.startswith(('http://', 'https://')) or not any(char in line for char in ['http', '://']):
                result.append(line)
    
    # 去重并返回
    return list(dict.fromkeys(result))


# ============================================================================
# ======================== 全局配置变量 ======================================
# ============================================================================

# 加载配置（无默认值，全部从INI读取）
config = load_config()

# -------------------------- 1. 基础爬取设置 --------------------------
TARGET_URL = config.get("General", "TARGET_URL")
HEADLESS = config.getboolean("General", "HEADLESS")
BROWSER_TYPE = config.get("General", "BROWSER_TYPE")
MAX_IPS = config.getint("General", "MAX_IPS")
MAX_TOTAL_CHANNELS = config.getint("General", "MAX_TOTAL_CHANNELS")
PAGE_LOAD_TIMEOUT = config.getint("General", "PAGE_LOAD_TIMEOUT")

# -------------------------- 2. 文件输出设置 --------------------------
output_dir_str = config.get("Output", "OUTPUT_DIR")
if output_dir_str:
    OUTPUT_DIR = Path(output_dir_str)
else:
    OUTPUT_DIR = Path(__file__).parent
OUTPUT_M3U_FILENAME = OUTPUT_DIR / config.get("Output", "OUTPUT_M3U_FILENAME")
OUTPUT_TXT_FILENAME = OUTPUT_DIR / config.get("Output", "OUTPUT_TXT_FILENAME")
MAX_LINKS_PER_CHANNEL = config.getint("Output", "MAX_LINKS_PER_CHANNEL")

# -------------------------- 3. FFmpeg 测速设置 --------------------------
ENABLE_FFMPEG_TEST = config.getboolean("FFmpeg", "ENABLE_FFMPEG_TEST")
FFMPEG_PATH = config.get("FFmpeg", "FFMPEG_PATH")
FFMPEG_TEST_DURATION = config.getint("FFmpeg", "FFMPEG_TEST_DURATION")
FFMPEG_CONCURRENCY = config.getint("FFmpeg", "FFMPEG_CONCURRENCY")
MIN_AVG_FPS = config.getfloat("FFmpeg", "MIN_AVG_FPS")
MIN_FRAMES = config.getint("FFmpeg", "MIN_FRAMES")
MIN_STABILITY_PERCENT = config.getfloat("FFmpeg", "MIN_STABILITY_PERCENT")

# 从GitHub获取的M3U链接（支持每行一个）
GITHUB_M3U_LINKS = parse_list(config.get("GitHub", "GITHUB_M3U_LINKS"))

# -------------------------- 4. 网页操作延时 --------------------------
DELAY_BETWEEN_IPS = config.getfloat("Delay", "DELAY_BETWEEN_IPS")
DELAY_AFTER_CLICK = config.getfloat("Delay", "DELAY_AFTER_CLICK")
MAX_CHANNELS_PER_IP = config.getint("Delay", "MAX_CHANNELS_PER_IP")

# -------------------------- 5. 数据清洗 --------------------------
ENABLE_CHINESE_CLEAN = config.getboolean("Cleaning", "ENABLE_CHINESE_CLEAN")
ENABLE_DEDUPLICATION = config.getboolean("Cleaning", "ENABLE_DEDUPLICATION")
ENABLE_SCREENSHOTS = config.getboolean("Cleaning", "ENABLE_SCREENSHOTS")
CCTV_USE_MAPPING = config.getboolean("Cleaning", "CCTV_USE_MAPPING")

# -------------------------- 6. 网络协议 --------------------------
DEFAULT_PROTOCOL = config.get("Network", "DEFAULT_PROTOCOL")

# -------------------------- 7. 缓存设置 --------------------------
ENABLE_CACHE = config.getboolean("Cache", "ENABLE_CACHE")
CACHE_FILE = OUTPUT_DIR / config.get("Cache", "CACHE_FILE")
CACHE_EXPIRE_HOURS = config.getint("Cache", "CACHE_EXPIRE_HOURS")

# -------------------------- 8. 更新时间显示 --------------------------
TIME_DISPLAY_AT_TOP = config.getboolean("UpdateTime", "TIME_DISPLAY_AT_TOP")

# -------------------------- 9. 更新时间条目占位流 --------------------------
UPDATE_STREAM_URL = config.get("UpdateTime", "UPDATE_STREAM_URL")

# ============================================================================
# ============================ 频道分类规则 ==================================
# ============================================================================

# 页面元素定位关键词（支持每行一个，全部从INI读取）
PAGE_CONFIG = {
    "search_tab": parse_list(config.get("PageElements", "search_tab")),
    "engine_search": parse_list(config.get("PageElements", "engine_search")),
    "multicast_tab": parse_list(config.get("PageElements", "multicast_tab")),
    "start_button": parse_list(config.get("PageElements", "start_button")),
}

# 频道自动分类规则 (按关键词匹配)
CATEGORY_RULES = [
    {"name": "4K专区",      "keywords": ["4k"]},
    {"name": "央视频道",    "keywords": ["cctv", "cetv", "中央"]},
    {"name": "卫视频道",    "keywords": ["卫视", "凤凰", "tvb", "湖南", "浙江", "江苏", "东方"]},
    {"name": "电影频道",    "keywords": ["电影", "影院", "chc"]},
    {"name": "轮播频道",    "keywords": ["轮播"]},
    {"name": "儿童频道",    "keywords": ["少儿", "动画", "卡通"]},
]

# 导出文件时的分组排序
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
    "CCTV-1综合",
    "CCTV-2财经",
    "CCTV-3综艺",
    "CCTV-4国际",
    "CCTV-5体育",
    "CCTV-5+体育赛事",
    "CCTV-6电影",
    "CCTV-7国防军事",
    "CCTV-8电视剧",
    "CCTV-9纪录",
    "CCTV-10科教",
    "CCTV-11戏曲",
    "CCTV-12社会与法",
    "CCTV-13新闻",
    "CCTV-14少儿",
    "CCTV-15音乐",
    "CCTV-16奥林匹克",
    "CCTV-17农业农村",
    "CETV1",
    "CETV2",
    "CETV4",
    "CETV5"
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
# ============================= 缓存工具函数 ==================================
# ============================================================================

def load_cache() -> Dict[str, Dict[str, Any]]:
    if not ENABLE_CACHE:
        return {}
    if not CACHE_FILE.exists():
        return {}
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            cache = json.load(f)
        now = time.time()
        expire_seconds = CACHE_EXPIRE_HOURS * 3600
        valid_cache = {}
        for url, data in cache.items():
            if expire_seconds == 0 or (now - data.get("timestamp", 0)) < expire_seconds:
                valid_cache[url] = data
        logger.info(f"缓存加载完成，有效缓存共 {len(valid_cache)} 条")
        return valid_cache
    except Exception as e:
        logger.warning(f"加载缓存失败: {e}")
        return {}


def save_cache(cache: Dict[str, Dict[str, Any]]) -> None:
    if not ENABLE_CACHE:
        return
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning(f"保存缓存失败: {e}")


# ============================================================================
# ============================= 工具函数 =====================================
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


def build_selector(text_list, element_type="div"):
    """
    适配Vue SPA页面的选择器构建
    元素类型从调用处指定，无默认值（实际使用时从业务逻辑确定）
    """
    if not text_list:
        return ""
    # SPA页面核心元素是div.segment-item（模式选项卡）
    base_selector = f"{element_type}.segment-item,div.ios-tab-item,button,a.sidebar-link"
    if len(text_list) == 1:
        return f"{base_selector}:has-text('{text_list[0]}')"
    pattern = "|".join(re.escape(t) for t in text_list)
    return f"{base_selector}:text-matches('{pattern}')"


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
# ========================= 进度条打印工具 ===================================
# ============================================================================

def print_progress_bar(current: int, total: int, success: int, failed: int, last_percent: int) -> int:
    """打印进度条，每2%刷新一次"""
    if total == 0:
        return 0

    percent = current / total
    percent_int = int(percent * 100)

    should_print = (
        (percent_int % 2 == 0 and percent_int > last_percent) or
        current == total or
        current == 0
    )

    if should_print:
        if percent_int == last_percent and current != total:
            return last_percent

        bar_length = 20
        filled_length = int(bar_length * percent)
        bar = '█' * filled_length + '░' * (bar_length - filled_length)

        logger.info(f"[{percent_int:3d}%] {bar} ({current}/{total}) | 成功:{success} | 失败:{failed}")
        return percent_int

    return last_percent


# ============================================================================
# ========================= FFmpeg测速代码 ===================================
# ============================================================================

async def test_stream_with_ffmpeg(url: str) -> Dict[str, Any]:
    if not shutil.which(FFMPEG_PATH):
        logger.error(f"未找到FFmpeg: {FFMPEG_PATH}")
        return {"ok": False, "fps": 0.0, "message": "FFmpeg未安装"}

    # FFmpeg命令
    cmd = [
        FFMPEG_PATH, "-hide_banner", "-y",
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
            return {"ok": False, "fps": 0.0, "message": "连接超时"}

        output = stderr.decode('utf-8', errors='ignore')

        frame_matches = re.findall(r'frame=\s*(\d+)', output)
        fps_matches = re.findall(r'fps=\s*([\d.]+)', output)

        frames = int(frame_matches[-1]) if frame_matches else 0
        avg_fps = float(fps_matches[-1]) if fps_matches else 0.0

        # 帧率稳定性检查
        is_stable = True
        if len(fps_matches) > 5:
            fps_values = list(map(float, fps_matches))
            mean_fps = sum(fps_values) / len(fps_values)
            std_dev = (sum((x - mean_fps) ** 2 for x in fps_values) / len(fps_values)) ** 0.5
            stability_percent = (std_dev / mean_fps) * 100
            
            if stability_percent > MIN_STABILITY_PERCENT:
                is_stable = False

        is_smooth = frames >= MIN_FRAMES and avg_fps >= MIN_AVG_FPS and is_stable

        return {"ok": is_smooth, "fps": avg_fps, "frames": frames, "stability": is_stable}
    except Exception as e:
        return {"ok": False, "fps": 0.0, "message": f"异常: {str(e)[:50]}"}


async def run_ffmpeg_test(channel_map: Dict[Tuple[str, str], List[str]]) -> Dict[Tuple[str, str], List[str]]:
    if not channel_map:
        return {}

    cache = load_cache()
    new_cache_entries = {}
    result_map = defaultdict(list)
    pending_tasks_data = []

    # 统计总链接数
    total_urls = 0
    for urls in channel_map.values():
        total_urls += len(urls)

    # 分流：使用缓存
    cached_valid_count = 0
    for (group, name), urls in channel_map.items():
        for url in urls:
            if url in cache:
                if cache[url].get("ok"):
                    result_map[(group, name)].append((url, cache[url].get("fps", 0)))
                    cached_valid_count += 1
            else:
                pending_tasks_data.append((group, name, url))

    total_pending = len(pending_tasks_data)

    logger.info(f"总待处理链接：{total_urls} 条")
    logger.info(f"缓存有效（无需测速）：{cached_valid_count} 条")
    logger.info(f"需要重新测速：{total_pending} 条")

    if total_pending == 0:
        return finalize_results(result_map)

    # 并发测速
    sem = asyncio.Semaphore(FFMPEG_CONCURRENCY)

    async def bound_test(item):
        group, name, url = item
        async with sem:
            result = await test_stream_with_ffmpeg(url)
            return (group, name, url, result)

    tasks = [bound_test(item) for item in pending_tasks_data]

    # 进度统计
    completed = 0
    success_count = 0
    failed_count = 0
    last_printed_percent = -100

    print_progress_bar(0, total_pending, 0, 0, last_printed_percent)

    for coro in asyncio.as_completed(tasks):
        group, name, url, res = await coro
        completed += 1

        new_cache_entries[url] = {
            "ok": res["ok"], "fps": res["fps"],
            "frames": res.get("frames", 0), "timestamp": time.time(),
            "stability": res.get("stability", False)
        }

        if res["ok"]:
            success_count += 1
            result_map[(group, name)].append((url, res["fps"]))
        else:
            failed_count += 1

        last_printed_percent = print_progress_bar(completed, total_pending, success_count, failed_count, last_printed_percent)

    if new_cache_entries:
        cache.update(new_cache_entries)
        save_cache(cache)
        logger.info(f"缓存更新：新增 {len(new_cache_entries)} 条记录")

    return finalize_results(result_map)


def finalize_results(result_map):
    final_map = {}
    for key, items in result_map.items():
        items.sort(key=lambda x: -x[1])
        final_map[key] = [url for url, _ in items[:MAX_LINKS_PER_CHANNEL]]

    total_final = sum(len(v) for v in final_map.values())
    logger.info(f"测速筛选完成，最终保留 {total_final} 条优质链接")
    return final_map


# ============================================================================
# ============================ GitHub M3U处理 ================================
# ============================================================================

@retry_async(max_retries=3, delay=2.0)
async def download_github_m3u(url: str) -> str:
    """从GitHub下载M3U文件内容（带重试）"""
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            }
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    content = await response.text()
                    logger.info(f"成功下载 {url}，内容长度: {len(content)} 字符")
                    return content
                else:
                    logger.error(f"下载失败，状态码: {response.status}，URL: {url}")
                    return ""
    except asyncio.TimeoutError:
        logger.error(f"下载超时，URL: {url}")
        return ""
    except Exception as e:
        logger.error(f"下载GitHub M3U文件失败: {e}，URL: {url}")
        return ""


def parse_m3u_file(content: str) -> List[Tuple[str, str, str]]:
    """解析M3U文件，提取频道名称、组名和URL"""
    channels = []
    lines = content.splitlines()
    
    current_group = ""
    current_name = ""
    current_url = ""
    
    for line in lines:
        line = line.strip()
        if line.startswith("#EXTINF"):
            # 提取频道信息
            match = re.search(r'#EXTINF:-1.*?group-title="([^"]+)",(.+)', line)
            if match:
                current_group = match.group(1)
                current_name = match.group(2)
            else:
                match = re.search(r'#EXTINF:-1.*?,(.+)', line)
                if match:
                    current_name = match.group(1)
        elif line.startswith("http"):
            current_url = line
            if current_name and current_url:
                # 清理URL中的多余参数
                if "?" in current_url:
                    current_url = current_url.split("?")[0]
                # 自动分类
                norm_name = normalize_cctv(current_name)
                group = classify_channel(norm_name) or current_group
                final_name = norm_name if group == "央视频道" else (clean_chinese_only(current_name) if ENABLE_CHINESE_CLEAN else current_name)
                channels.append((group, final_name, current_url))
                current_name = ""
                current_url = ""
    
    return channels


# ============================================================================
# ============================ 页面交互函数 ==================================
# ============================================================================

async def robust_click(locator, timeout=10000):
    """
    增强版点击函数，适配Vue SPA的div元素点击
    """
    try:
        await locator.scroll_into_view_if_needed(timeout=3000)
        await asyncio.sleep(0.2)
        # 优先使用playwright点击
        await locator.click(force=True, timeout=timeout)
        return True
    except Exception:
        try:
            # 备用方案：JS原生点击（适配SPA）
            await locator.evaluate("el => { el.click(); el.classList.add('active'); }")
            return True
        except Exception as e:
            logger.error(f"点击失败: {e}")
            return False


async def wait_for_element(page, selector, timeout=30000):
    try:
        await page.wait_for_selector(selector, timeout=timeout)
        return True
    except PlaywrightTimeoutError:
        return False


async def wait_for_spa_state(page, state_check_js: str, timeout=30000):
    """
    等待Vue SPA状态变更（如currentTab切换）
    """
    start_time = time.time()
    while time.time() - start_time < timeout / 1000:
        try:
            is_ready = await page.evaluate(state_check_js)
            if is_ready:
                return True
            await asyncio.sleep(0.5)
        except Exception:
            await asyncio.sleep(0.5)
    return False


@retry_async(max_retries=2, delay=1.0)
async def extract_one_ip(page, row, ip_index):
    entries = []
    addr = None
    try:
        # SPA页面：IP地址在item-title中
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
        # SPA页面：查看频道的按钮是带fa-list图标的按钮
        list_btn = row.locator("button:has(i.fa-list)").first
        if await list_btn.count() > 0:
            if not await robust_click(list_btn): 
                await row.click(timeout=3000)
        else:
            # SPA页面：直接点击行触发弹窗
            await row.click(timeout=3000)
        await asyncio.sleep(DELAY_AFTER_CLICK)

        # SPA页面：频道弹窗是 #channelListModal 下的 .modal-content
        modal = page.locator("#channelListModal .modal-content").first
        if not await wait_for_element(page, "#channelListModal", timeout=5000):
            logger.warning(f"第 {ip_index} 行地址 {addr} 未弹出频道弹窗，跳过")
            return []

        # SPA页面：频道列表在弹窗内的ios-list-item中
        items = modal.locator(".ios-list-item")
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
    """适配Vue SPA页面的数据加载等待逻辑"""
    for retry in range(3):
        logger.info(f"等待SPA数据加载... (重试 {retry+1}/3)")
        await asyncio.sleep(20)  # SPA加载稍慢，延长等待
        # SPA页面：检测是否有包含频道数的搜索结果
        has_data = await page.evaluate('''()=>{
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
            logger.info("SPA数据加载完成")
            return True
    logger.error("SPA数据加载超时（60秒）")
    return False


# ============================================================================
# ============================ 文件导出函数 ==================================
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
            f.write(f'#EXTINF:-1 tvg-name="{time_str}" tvg-id="更新时间" tvg-logo="" group-title="更新时间", {time_str}\n')
            f.write(f"{update_url}\n\n")

        for group in GROUP_ORDER:
            if group not in grouped:
                continue

            # 央视频道严格排序
            if group == "央视频道":
                name_url_dict = {name: url for name, url in grouped[group]}
                sorted_channels = [(name, name_url_dict[name]) for name in CCTV_ORDER if name in name_url_dict]
            else:
                sorted_channels = sorted(grouped[group], key=lambda x: x[0])

            for name, url in sorted_channels:
                f.write(f'#EXTINF:-1 group-title="{group}",{name}\n{url}\n')
            f.write("\n")

        if not TIME_DISPLAY_AT_TOP:
            f.write(f'#EXTINF:-1 tvg-name="{time_str}" tvg-id="更新时间" tvg-logo="" group-title="更新时间", {time_str}\n')
            f.write(f"{update_url}\n\n")

    # 导出 TXT
    with open(OUTPUT_TXT_FILENAME, "w", encoding="utf-8") as f:
        if TIME_DISPLAY_AT_TOP:
            f.write("更新时间,#genre#\n")
            f.write(f"{time_str},{update_url}\n\n")

        for group in GROUP_ORDER:
            if group not in grouped:
                continue

            if group == "央视频道":
                name_url_dict = {name: url for name, url in grouped[group]}
                sorted_channels = [(name, name_url_dict[name]) for name in CCTV_ORDER if name in name_url_dict]
            else:
                sorted_channels = sorted(grouped[group], key=lambda x: x[0])

            f.write(f"{group},#genre#\n")
            for name, url in sorted_channels:
                f.write(f"{name},{url}\n")
            f.write("\n")

        if not TIME_DISPLAY_AT_TOP:
            f.write("更新时间,#genre#\n")
            f.write(f"{time_str},{update_url}\n\n")

    total_links = sum(len(v) for v in grouped.values()) + 1
    position_text = "顶部" if TIME_DISPLAY_AT_TOP else "底部"
    logger.info(f"导出完成！共 {total_links} 条链接（含更新时间），更新时间已放在{position_text}")


# ============================================================================
# ============================= 主流程 =======================================
# ============================================================================

async def main():
    if ENABLE_SCREENSHOTS:
        (OUTPUT_DIR / "screenshots").mkdir(exist_ok=True)

    # 构建选择器（全部从INI读取配置，无硬编码默认值）
    SEARCH_TAB_SELECTOR = build_selector(PAGE_CONFIG["search_tab"], element_type="div")
    ENGINE_SELECTOR = build_selector(PAGE_CONFIG["engine_search"], element_type="div")
    MCAST_SELECTOR = build_selector(PAGE_CONFIG["multicast_tab"], element_type="div")
    START_SELECTOR = build_selector(PAGE_CONFIG["start_button"], element_type="button")

    # 存储所有来源的频道数据
    all_channels = []

    # ========== 1. 从GitHub下载M3U文件并解析 ==========
    logger.info("=== 开始处理GitHub M3U链接 ===")
    if GITHUB_M3U_LINKS:
        for url in GITHUB_M3U_LINKS:
            logger.info(f"正在从GitHub下载M3U文件: {url}")
            content = await download_github_m3u(url)
            if content:
                channels = parse_m3u_file(content)
                github_channels = []
                for group, name, link in channels:
                    if not link.startswith(('http://', 'https://', 'rtsp://', 'rtmp://')):
                        link = DEFAULT_PROTOCOL + link
                    github_channels.append((group, name, link))
                all_channels.extend(github_channels)
                logger.info(f"成功从 {url} 提取 {len(github_channels)} 个频道")
            else:
                logger.warning(f"从 {url} 未提取到任何频道")
    else:
        logger.info("未配置GitHub M3U链接，跳过该来源")

    # ========== 2. 从Vue SPA网站爬取频道 ==========
    logger.info("\n=== 开始从Vue SPA网站爬取频道 ===")
    async with async_playwright() as p:
        browser = await getattr(p, BROWSER_TYPE).launch(
            headless=HEADLESS, args=["--no-sandbox", "--disable-setuid-sandbox"]
        )
        ctx = await browser.new_context(viewport={"width": 1920, "height": 1080})
        page = await ctx.new_page()

        try:
            logger.info(f"正在访问Vue SPA网站: {TARGET_URL}")
            await page.goto(TARGET_URL, timeout=PAGE_LOAD_TIMEOUT, wait_until="networkidle")

            # ========== SPA标签页切换（全部从INI配置读取） ==========
            # 第一步：切换到"搜索"标签页
            if SEARCH_TAB_SELECTOR:
                search_tab = page.locator(SEARCH_TAB_SELECTOR).first
                if await search_tab.count() > 0:
                    logger.info("切换到SPA的搜索标签页")
                    if await robust_click(search_tab):
                        # 等待SPA状态变更
                        await wait_for_spa_state(page, "() => window.currentTab === 'search' || document.querySelector('.segment-item.active')?.textContent?.includes('搜索')")
                    else:
                        logger.warning("搜索标签页点击失败，尝试自动检测")

            # 第二步：选择引擎搜索/关键词搜索
            if ENGINE_SELECTOR:
                eng = page.locator(ENGINE_SELECTOR).first
                if await eng.count() > 0:
                    logger.info("选择关键词搜索模式")
                    await robust_click(eng)
                    await asyncio.sleep(1.0)

            # 第三步：选择组播提取/酒店提取模式
            if MCAST_SELECTOR:
                mcast = page.locator(MCAST_SELECTOR).first
                if await mcast.count() > 0:
                    logger.info("选择组播/酒店提取模式")
                    await robust_click(mcast)
                    await asyncio.sleep(1.0)

            # 第四步：点击开始按钮
            if START_SELECTOR:
                start = page.locator(START_SELECTOR).first
                if await start.count() > 0:
                    logger.info("点击开始搜索按钮")
                    await robust_click(start)
                else:
                    # 备用方案：查找包含开始文本的div
                    start_div = page.locator(build_selector(PAGE_CONFIG["start_button"], element_type="div")).first
                    if await start_div.count() > 0:
                        logger.info("点击SPA的开始搜索div元素")
                        await robust_click(start_div)

            # 等待SPA加载数据
            if not await wait_data(page):
                logger.error("SPA网站数据加载失败，跳过网站爬取")
            else:
                # SPA页面：精准匹配搜索结果中的IP行（包含频道数信息）
                rows = page.locator("div.ios-list-item").filter(
                    has=page.locator("div.item-subtitle:has-text('频道:')")
                )
                total_rows = await rows.count()
                logger.info(f"【SPA精准筛选】找到包含频道信息的IP行总数：{total_rows}")
                
                if total_rows > 0:
                    # 计算实际要处理的IP行数
                    process_count = min(total_rows, MAX_IPS) if MAX_IPS > 0 else total_rows
                    logger.info(f"配置MAX_IPS={MAX_IPS}，实际将处理前 {process_count} 个IP行")

                    website_channels = []
                    processed_ip_count = 0
                    for i in range(process_count):
                        row = rows.nth(i)
                        row_text = await row.inner_text(timeout=3000)
                        logger.debug(f"第 {i+1} 行原始文本：{row_text[:100]}...")
                        
                        entries = await extract_one_ip(page, row, i+1)
                        if entries:
                            website_channels.extend(entries)
                            processed_ip_count += 1
                        
                        # 限制总频道数
                        if MAX_TOTAL_CHANNELS > 0 and len(website_channels) >= MAX_TOTAL_CHANNELS:
                            website_channels = website_channels[:MAX_TOTAL_CHANNELS]
                            logger.info(f"已达到总频道数上限 {MAX_TOTAL_CHANNELS}，停止提取IP行")
                            break
                        
                        if i < process_count - 1:
                            await asyncio.sleep(DELAY_BETWEEN_IPS)

                    logger.info(f"从SPA网站实际处理IP行数：{processed_ip_count} / {process_count}")
                    logger.info(f"从SPA网站提取频道总数：{len(website_channels)} 条")
                    all_channels.extend(website_channels)

        except Exception as e:
            logger.exception("SPA网站爬取过程异常，跳过网站爬取")
        finally:
            await browser.close()

    # ========== 3. 合并并处理所有频道 ==========
    logger.info(f"\n=== 开始合并处理所有频道 ===")
    logger.info(f"合并后总频道数（含重复）：{len(all_channels)} 条")

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
    asyncio.run(main())
