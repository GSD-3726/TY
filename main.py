#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
IPTV 组播提取工具（网页阅读优化版）
核心功能：自动爬取IPTV直播源 + 多层测速筛选 + 自动生成M3U/TXT播放列表
适配场景：本地Windows/macOS/Linux运行 + GitHub Actions自动定时更新
"""

# ============================================================================
# 【必填前置】提前导入路径工具，禁止删除/移动！
# ============================================================================
from pathlib import Path

# ============================================================================
# ======================== 【配置区 · 网页友好优化版】 =========================
# 📌 格式说明：参数 = 值  # 🎯 功能说明 | ⚠️ 注意事项 | 💡 推荐值
# 📌 关键参数已用 emoji 高亮，无特殊需求保持默认值即可直接运行
# ============================================================================

# ==================================
# 🔴 1. 基础爬取核心设置
# ==================================
TARGET_URL = "https://iptv.809899.xyz"          # 🎯 爬取目标地址 | ⚠️ 无需修改
HEADLESS = True                                   # 🎯 浏览器窗口开关 | ⚠️ GitHub Actions必须为True | 💡 True=后台运行 False=显示窗口
BROWSER_TYPE = "chromium"                         # 🎯 浏览器内核 | 💡 可选firefox/webkit，推荐默认
MAX_IPS = 20                                       # 🎯 最大处理IP数量 | ⚠️ 数值越大运行越久 | 💡 GitHub Actions=20-50 本地=0(不限制)
PAGE_LOAD_TIMEOUT = 120000                        # 🎯 页面加载超时时间(毫秒) | 💡 网络慢可调大

# ==================================
# 🟠 2. 输出文件设置
# ==================================
OUTPUT_DIR = Path(__file__).parent                # 🎯 文件保存目录 | 💡 默认=脚本所在文件夹
# 修复点：显式转换为Path对象，避免字符串拼接错误
OUTPUT_M3U_FILENAME = Path(OUTPUT_DIR) / "iptv_channels.m3u"  # 🎯 M3U播放列表文件名 | ✅ 支持电视/盒子直接打开
OUTPUT_TXT_FILENAME = Path(OUTPUT_DIR) / "iptv_channels.txt"  # 🎯 TXT格式文件名 | ✅ 通用文本格式
MAX_LINKS_PER_CHANNEL = 10                         # 🎯 单个频道最多保留源数 | 💡 避免文件过大

# ==================================
# 🟡 3. FFmpeg测速核心设置
# ==================================
ENABLE_FFMPEG_TEST = True                          # 🎯 测速总开关 | 💡 True=只保留流畅源 False=保存所有链接
FFMPEG_PATH = "ffmpeg"                             # 🎯 FFmpeg程序路径 | ⚠️ Windows必填完整路径 | 💡 Windows示例: r"C:\ffmpeg\bin\ffmpeg.exe"
FFPROBE_PATH = "ffprobe"                           # 🎯 FFprobe程序路径 | ⚠️ 填写规则同上
FFMPEG_TEST_DURATION = 10                           # 🎯 单个链接测速时长(秒) | ⚠️ 越长越准但越慢 | 💡 GitHub Actions=5-10 本地=10-20
FFMPEG_CONCURRENCY = 2                              # 🎯 同时测速的链接数 | ⚠️ 越大越快但对CPU/网络要求高 | 💡 GitHub Actions=1-2 本地=CPU核心数
MIN_AVG_FPS = 24.0                                  # 🎯 最低平均帧率 | 💡 24是流畅播放最低标准
MIN_FRAMES = 210                                     # 🎯 最低解码帧数 | 💡 防止误判，无需修改
MIN_FRAMES_RATIO = 0.75                              # 🎯 动态帧率阈值 | 💡 无需修改

# ==================================
# 🟢 4. 预检测设置（减少无效测速）
# ==================================
ENABLE_FFPROBE_CHECK = True                         # 🎯 ffprobe预检测开关 | 💡 True=先检测有无视频流
ENABLE_HLS_QUICK_CHECK = True                       # 🎯 HLS快速检测开关 | 💡 True=先检测m3u8是否有效
HLS_CHECK_TIMEOUT = 6                                # 🎯 HLS检测超时时间(秒) | 💡 无需修改

# ==================================
# 🔵 5. 网页操作延时设置
# ==================================
DELAY_BETWEEN_IPS = 1.0                              # 🎯 处理完一个IP后的等待时间(秒) | 💡 防止被拦截
DELAY_AFTER_CLICK = 0.5                              # 🎯 点击按钮后等待弹窗时间(秒) | 💡 网站慢可调大
MAX_CHANNELS_PER_IP = 0                               # 🎯 单个IP最多提取频道数 | 💡 0=不限制

# ==================================
# 🟣 6. 频道名称清洗设置
# ==================================
ENABLE_CHINESE_CLEAN = True                          # 🎯 中文清洗开关 | 💡 True=移除乱码/特殊字符
ENABLE_DEDUPLICATION = True                           # 🎯 去重开关 | 💡 True=相同频道名+链接只保留一个
ENABLE_SCREENSHOTS = False                            # 🎯 调试截图开关 | 💡 仅本地调试用
CCTV_USE_MAPPING = True                               # 🎯 CCTV名称标准化 | 💡 True=自动适配EPG节目单

# ==================================
# 🟤 7. 网络协议设置
# ==================================
DEFAULT_PROTOCOL = "http://"                          # 🎯 默认协议头 | 💡 无需修改

# ==================================
# ⚫ 8. 测速缓存设置（GitHub Actions必开）
# ==================================
ENABLE_CACHE = True                                    # 🎯 缓存开关 | 💡 True=24小时内测过的链接不再重测
# 修复点：显式转换为Path对象
CACHE_FILE = Path(OUTPUT_DIR) / "iptv_speed_cache.json"    # 🎯 缓存文件保存路径 | 💡 无需修改
CACHE_EXPIRE_HOURS = 24                                # 🎯 缓存过期时间(小时) | 💡 0=永不过期

# ==================================
# ⚪ 9. 更新时间显示设置
# ==================================
TIME_DISPLAY_AT_TOP = False                            # 🎯 更新时间位置 | 💡 True=顶部 False=底部
UPDATE_STREAM_URL = "https://gitee.com/bmg369/test/blob/main/175081947304562457.webp"  # 🎯 更新时间占位流 | 💡 无需修改

# ============================================================================
# ============================= 【代码区 · 无需修改】 =========================
# ============================================================================

import asyncio
import json
import logging
import os
import re
import sys
import time
import shutil
import datetime
import requests
from collections import defaultdict
from typing import Dict, List, Tuple, Optional, Any
from urllib.parse import urljoin
import functools

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# ============================================================================
# ============================ 频道分类规则 ==================================
# ============================================================================

PAGE_CONFIG = {
    "engine_search": ["引索搜索", "引擎搜索", "关键词搜索"],
    "multicast_tab": ["酒店提取"],
    "start_button": ["开始播放", "开始搜索", "开始提取"],
}

CATEGORY_RULES = [
    {"name": "4K专区",      "keywords": ["4k"]},
    {"name": "央视频道",    "keywords": ["cctv", "cetv", "中央"]},
    {"name": "卫视频道",    "keywords": ["卫视", "凤凰", "tvb", "湖南", "浙江", "江苏", "东方"]},
    {"name": "电影频道",    "keywords": ["电影", "影院", "chc"]},
    {"name": "轮播频道",    "keywords": ["轮播"]},
    {"name": "儿童频道",    "keywords": ["少儿", "动画", "卡通"]},
]

GROUP_ORDER = ["央视频道", "卫视频道", "电影频道", "4K专区", "儿童频道", "轮播频道"]

CCTV_NAME_MAPPING = {
    "1": "综合", "2": "财经", "3": "综艺", "4": "国际", "5": "体育",
    "5+": "体育赛事", "6": "电影", "7": "国防军事", "8": "电视剧",
    "9": "纪录", "10": "科教", "11": "戏曲", "12": "社会与法",
    "13": "新闻", "14": "少儿", "15": "音乐", "16": "奥林匹克",
    "17": "农业农村",
}

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
    if not ENABLE_CACHE: return {}
    if not CACHE_FILE.exists(): return {}
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            cache = json.load(f)
        now = time.time()
        expire_seconds = CACHE_EXPIRE_HOURS * 3600
        valid_cache = {}
        for url, data in cache.items():
            if expire_seconds == 0 or (now - data.get("timestamp", 0)) < expire_seconds:
                valid_cache[url] = data
        return valid_cache
    except Exception as e:
        logger.warning(f"加载缓存失败: {e}")
        return {}

def save_cache(cache: Dict[str, Dict[str, Any]]) -> None:
    if not ENABLE_CACHE: return
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning(f"保存缓存失败: {e}")

# ============================================================================
# ============================= 工具函数 =====================================
# ============================================================================

async def ffprobe_check(url: str) -> bool:
    if not ENABLE_FFPROBE_CHECK or not shutil.which(FFPROBE_PATH):
        return True

    cmd = [
        FFPROBE_PATH,
        "-v", "error",
        "-show_streams",
        "-print_format", "json",
        url
    ]

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL
        )

        out, _ = await proc.communicate()
        if not out:
            return False

        data = json.loads(out.decode())
        streams = data.get("streams", [])
        return any(s.get("codec_type") == "video" for s in streams)
    except Exception:
        return True

def hls_quick_check(url: str) -> bool:
    if not ENABLE_HLS_QUICK_CHECK or not url.endswith(".m3u8"):
        return True

    try:
        r = requests.get(url, timeout=HLS_CHECK_TIMEOUT)
        if r.status_code != 200:
            return False
        txt = r.text
        if "#EXTINF" not in txt:
            return False
        return True
    except Exception:
        return False

CCTV_PATTERN = re.compile(r'(cctv)[-\s]?(\d{1,3})', re.IGNORECASE)
CHINESE_ONLY_PATTERN = re.compile(r'[^\u4e00-\u9fff]')

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
    if "cctv5+" in name_lower: return "CCTV-5+体育赛事" if CCTV_USE_MAPPING else "CCTV5+"
    cctv_match = CCTV_PATTERN.search(name_lower)
    if cctv_match:
        num = cctv_match.group(2)
        if CCTV_USE_MAPPING and num in CCTV_NAME_MAPPING: return f"CCTV-{num}{CCTV_NAME_MAPPING[num]}"
        return f"CCTV-{num}"
    return name

def clean_chinese_only(name: str) -> str:
    return CHINESE_ONLY_PATTERN.sub('', name)

def build_selector(text_list, element_type="button"):
    if not text_list: return ""
    if len(text_list) == 1: return f"{element_type}:has-text('{text_list[0]}')"
    pattern = "|".join(re.escape(t) for t in text_list)
    return f"{element_type}:text-matches('{pattern}')"

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
                    if attempt == max_retries: raise
                    logger.warning(f"尝试 {attempt}/{max_retries} 失败: {e}，重试中...")
                    await asyncio.sleep(delay)
            return None
        return wrapper
    return decorator

# ============================================================================
# ========================= 进度条打印工具 (2%步进) ==========================
# ============================================================================

def print_progress_bar(current: int, total: int, success: int, failed: int, last_percent: int) -> int:
    if total == 0: return 0
    
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
# ========================= 【核心】FFmpeg测速代码 ============================
# ============================================================================

async def test_stream_with_ffmpeg(url: str) -> Dict[str, Any]:
    """增强版测速：添加单行读取超时，避免卡死"""
    if not shutil.which(FFMPEG_PATH):
        logger.error(f"未找到FFmpeg: {FFMPEG_PATH}")
        return {"ok": False, "fps": 0.0, "frames": 0, "message": "FFmpeg未安装"}

    if not await ffprobe_check(url):
        return {"ok": False, "fps": 0.0, "frames": 0, "message": "ffprobe检测无视频流"}

    if not hls_quick_check(url):
        return {"ok": False, "fps": 0.0, "frames": 0, "message": "HLS检测失败"}

    cmd = [
        FFMPEG_PATH, "-hide_banner", "-y",
        "-fflags", "nobuffer",
        "-rw_timeout", "5000000",
        "-i", url,
        "-t", str(FFMPEG_TEST_DURATION),
        "-f", "null", "-progress", "pipe:1", "-"
    ]

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL
        )
        
        frames = 0
        fps = 0.0
        start = time.time()

        # 单行读取超时（秒）
        LINE_READ_TIMEOUT = 10

        while True:
            # 如果总运行时间超过允许值，强制退出
            elapsed = time.time() - start
            if elapsed > FFMPEG_TEST_DURATION + 8:
                break

            try:
                # 使用 wait_for 避免 readline 永久阻塞
                line = await asyncio.wait_for(proc.stdout.readline(), timeout=LINE_READ_TIMEOUT)
            except asyncio.TimeoutError:
                # 超时后尝试终止进程
                logger.warning(f"读取 ffmpeg 输出超时，可能链接卡死: {url}")
                try:
                    proc.kill()
                except:
                    pass
                break

            if not line:
                # 进程结束
                break

            text = line.decode(errors="ignore").strip()
            if text.startswith("frame="):
                try:
                    frames = int(text.split("=")[1].strip())
                except:
                    pass
            if text.startswith("fps="):
                try:
                    fps = float(text.split("=")[1].strip())
                except:
                    pass

        # 等待进程结束（最多再等 2 秒）
        try:
            await asyncio.wait_for(proc.wait(), timeout=2)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()

        min_frames_dynamic = int(FFMPEG_TEST_DURATION * 25 * MIN_FRAMES_RATIO)
        min_frames_final = max(min_frames_dynamic, MIN_FRAMES)
        is_smooth = frames >= min_frames_final and fps >= MIN_AVG_FPS
        
        return {
            "ok": is_smooth, 
            "fps": fps, 
            "frames": frames,
            "message": "成功" if is_smooth else f"帧数不足({frames}/{min_frames_final})或帧率低({fps}/{MIN_AVG_FPS})"
        }
    except asyncio.TimeoutError:
        return {"ok": False, "fps": 0.0, "frames": 0, "message": "测速超时"}
    except Exception as e:
        return {"ok": False, "fps": 0.0, "frames": 0, "message": f"异常: {str(e)[:50]}"}

async def run_ffmpeg_test(channel_map: Dict[Tuple[str, str], List[str]]) -> Dict[Tuple[str, str], List[str]]:
    if not channel_map: return {}

    cache = load_cache()
    new_cache_entries = {}
    result_map = defaultdict(list)
    pending_tasks_data = [] 

    for (group, name), urls in channel_map.items():
        for url in urls:
            if url in cache:
                if cache[url].get("ok"):
                    result_map[(group, name)].append((url, cache[url].get("fps", 0)))
            else:
                pending_tasks_data.append((group, name, url))

    total_pending = len(pending_tasks_data)
    cached_count = len([item for sublist in result_map.values() for item in sublist])
    logger.info(f"缓存命中 {cached_count} 条，需测速 {total_pending} 条")

    if total_pending == 0:
        return finalize_results(result_map)

    sem = asyncio.Semaphore(FFMPEG_CONCURRENCY)
    
    async def bound_test(item):
        group, name, url = item
        async with sem:
            result = await test_stream_with_ffmpeg(url)
            return (group, name, url, result)

    tasks = [bound_test(item) for item in pending_tasks_data]
    
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
            "message": res["message"]
        }

        if res["ok"]:
            success_count += 1
            result_map[(group, name)].append((url, res["fps"]))
        else:
            failed_count += 1
            logger.debug(f"链接 {url} 测速失败: {res['message']}")

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
# ============================ 页面交互函数 ==================================
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
        if not addr: return []
        logger.info(f"处理地址 [{ip_index}]: {addr}")
    except Exception as e:
        logger.warning(f"提取地址失败: {e}")
        return []

    try:
        list_btn = row.locator("button:has(i.fa-list)").first
        if await list_btn.count() > 0:
            if not await robust_click(list_btn): await row.click(timeout=3000)
        else:
            await row.click(timeout=3000)
        await asyncio.sleep(DELAY_AFTER_CLICK)

        modal = page.locator(".modal-dialog").first
        if not await wait_for_element(page, ".modal-dialog", timeout=5000): return []

        items = modal.locator(".item-content")
        total = await items.count()
        if MAX_CHANNELS_PER_IP > 0: total = min(total, MAX_CHANNELS_PER_IP)

        for i in range(total):
            try:
                name_elem = items.nth(i).locator(".item-title").first
                link_elem = items.nth(i).locator(".item-subtitle").first
                name = await name_elem.inner_text(timeout=2000)
                link = await link_elem.inner_text(timeout=2000)
                name, link = name.strip(), link.strip()
                if not name or not link: continue

                if not link.startswith(('http://', 'https://', 'rtsp://', 'rtmp://')):
                    link = DEFAULT_PROTOCOL + link

                norm = normalize_cctv(name)
                group = classify_channel(norm)
                if not group: continue
                final_name = norm if group == "央视频道" else (clean_chinese_only(name) if ENABLE_CHINESE_CLEAN else name)
                if final_name: entries.append((group, final_name, link))
            except Exception:
                continue
    except Exception as e:
        logger.warning(f"提取出错 {addr}: {e}")
    return entries

async def wait_data(page):
    for _ in range(2):
        logger.info("等待30秒加载数据...")
        await asyncio.sleep(30)
        has = await page.evaluate('''()=>{
            for(let e of document.querySelectorAll('div.item-title')){
                if(e.innerText.trim()) return true;
            }
            return false;
        }''')
        if has:
            logger.info("数据加载完成")
            return True
    logger.warning("数据加载超时")
    return False

# ============================================================================
# ===================== 【导出】带时间戳的文件 ================================
# ============================================================================

def export_results_with_timestamp(channel_map: Dict[Tuple[str, str], List[str]]):
    now = datetime.datetime.now()
    time_str = now.strftime("%Y-%m-%d %H:%M:%S")
    update_url = UPDATE_STREAM_URL

    grouped = defaultdict(list)
    for (group, name), urls in channel_map.items():
        for url in urls:
            grouped[group].append((name, url))

    with open(OUTPUT_M3U_FILENAME, "w", encoding="utf-8") as f:
        f.write("#EXTM3U\n")
        if TIME_DISPLAY_AT_TOP:
            f.write(f'#EXTINF:-1 tvg-name="{time_str}" tvg-id="更新时间" tvg-logo="" group-title="更新时间", {time_str}\n')
            f.write(f"{update_url}\n\n")
        for group in GROUP_ORDER:
            if group in grouped:
                for name, url in grouped[group]:
                    f.write(f'#EXTINF:-1 group-title="{group}",{name}\n{url}\n')
                f.write("\n")
        if not TIME_DISPLAY_AT_TOP:
            f.write(f'#EXTINF:-1 tvg-name="{time_str}" tvg-id="更新时间" tvg-logo="" group-title="更新时间", {time_str}\n')
            f.write(f"{update_url}\n\n")

    with open(OUTPUT_TXT_FILENAME, "w", encoding="utf-8") as f:
        if TIME_DISPLAY_AT_TOP:
            f.write("更新时间,#genre#\n")
            f.write(f"{time_str},{update_url}\n\n")
        for group in GROUP_ORDER:
            if group not in grouped:
                continue
            f.write(f"{group},#genre#\n")
            for name, url in grouped[group]:
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

    async with async_playwright() as p:
        browser = await getattr(p, BROWSER_TYPE).launch(
            headless=HEADLESS, args=["--no-sandbox", "--disable-setuid-sandbox"]
        )
        ctx = await browser.new_context(viewport={"width": 1920, "height": 1080})
        page = await ctx.new_page()

        try:
            logger.info(f"正在访问 {TARGET_URL}")
            await page.goto(TARGET_URL, timeout=PAGE_LOAD_TIMEOUT, wait_until="networkidle")

            if ENGINE_SELECTOR:
                eng = page.locator(ENGINE_SELECTOR).first
                if await eng.count() > 0: await robust_click(eng)
            
            if MCAST_SELECTOR:
                mcast = page.locator(MCAST_SELECTOR).first
                await robust_click(mcast)
            
            if START_SELECTOR:
                start = page.locator(START_SELECTOR).first
                await robust_click(start)

            if not await wait_data(page):
                logger.error("数据加载失败")
                return

            rows = page.locator("div.ios-list-item").filter(has_text="频道:")
            total_rows = await rows.count()
            if total_rows == 0:
                logger.error("未找到任何地址")
                return

            process_count = min(total_rows, MAX_IPS) if MAX_IPS > 0 else total_rows
            logger.info(f"找到 {total_rows} 个地址，处理前 {process_count} 个")

            raw_entries = []
            for i in range(process_count):
                entries = await extract_one_ip(page, rows.nth(i), i+1)
                raw_entries.extend(entries)
                if i < process_count - 1: await asyncio.sleep(DELAY_BETWEEN_IPS)

            logger.info(f"原始提取：{len(raw_entries)} 条")

            channel_map = defaultdict(list)
            seen = set()
            for group, name, url in raw_entries:
                if ENABLE_DEDUPLICATION:
                    key = (group, name, url)
                    if key in seen: continue
                    seen.add(key)
                channel_map[(group, name)].append(url)

            if ENABLE_FFMPEG_TEST and channel_map:
                channel_map = await run_ffmpeg_test(channel_map)

            export_results_with_timestamp(channel_map)

        except Exception as e:
            logger.exception("主流程异常")
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
