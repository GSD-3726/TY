# ============================================================================
# 【必填前置】提前导入路径工具，禁止删除/移动！
# ============================================================================
from pathlib import Path

# ============================================================================
# ======================== 【配置区 · 全中文填写指南】 =========================
# 👉 所有参数按模块分类，#后面是填写说明，直接修改=后面的值即可
# 👉 无特殊需求，保持默认值就能直接运行，无需修改
# ============================================================================

# ==================================
# 🔴 1. 基础爬取核心设置
# ==================================
# 【爬取目标地址】要提取直播源的网站地址，无需修改
TARGET_URL = "https://iptv.809899.xyz"

# 【浏览器窗口开关】True=后台静默运行(无窗口)，False=显示浏览器窗口(本地调试用)
# ⚠️ GitHub Actions必须设为True，否则会运行失败
HEADLESS = True

# 【浏览器内核】默认chromium即可，可选firefox/webkit，无需修改
BROWSER_TYPE = "chromium"

# 【最大处理IP数量】最多提取前N个IP里的频道，0=不限制全部提取
# ⚠️ 数值越大，运行时间越长，GitHub Actions建议设20-50，本地可设0
MAX_IPS = 20

# 【页面加载超时时间】单位：毫秒，120000=2分钟，网络慢可以调大
PAGE_LOAD_TIMEOUT = 120000

# ==================================
# 🟠 2. 输出文件设置
# ==================================
# 【文件保存目录】默认=脚本所在的文件夹，无需修改
OUTPUT_DIR = Path(__file__).parent

# 【M3U播放列表文件名】支持电视/盒子/播放器直接打开的格式，无需修改
OUTPUT_M3U_FILENAME = OUTPUT_DIR / "iptv_channels.m3u"

# 【TXT格式文件名】通用文本格式，无需修改
OUTPUT_TXT_FILENAME = OUTPUT_DIR / "iptv_channels.txt"

# 【单个频道最多保留源数】每个频道只保留测速最快的前N个源，避免文件过大
MAX_LINKS_PER_CHANNEL = 10

# ==================================
# 🟡 3. FFmpeg测速核心设置
# ==================================
# 【测速总开关】True=开启测速筛选(只保留流畅源)，False=直接保存所有爬取到的链接(无测速)
ENABLE_FFMPEG_TEST = True

# 【FFmpeg程序路径】
# ✅ Windows：必须写完整绝对路径，比如 r"C:\ffmpeg\bin\ffmpeg.exe"
# ✅ Linux/macOS/GitHub Actions：直接填 "ffmpeg" 即可
FFMPEG_PATH = "ffmpeg"

# 【FFprobe程序路径】和FFmpeg同目录，填写规则同上
FFPROBE_PATH = "ffprobe"

# 【单个链接测速时长】单位：秒，数值越大测速越准，运行时间也越长
# ⚠️ GitHub Actions建议设5-10秒，本地可设10-20秒
FFMPEG_TEST_DURATION = 10

# 【同时测速的链接数】数值越大测速越快，对CPU/网络要求越高
# ⚠️ GitHub Actions建议设1-2，本地4核CPU可设4，8核可设8
FFMPEG_CONCURRENCY = 2

# 【最低平均帧率】低于这个值的卡顿源会被丢弃，24是流畅播放的最低标准
MIN_AVG_FPS = 24.0

# 【最低解码帧数】防止只有几秒数据就误判为成功，默认210=10秒×25帧×0.85，无需修改
MIN_FRAMES = 210

# 【动态帧率阈值】和上面的固定帧数取最大值，无需修改
MIN_FRAMES_RATIO = 0.75

# ==================================
# 🟢 4. 预检测设置（减少无效测速，节省时间）
# ==================================
# 【ffprobe预检测开关】True=先检测链接有没有视频流，无视频流直接跳过测速
ENABLE_FFPROBE_CHECK = True

# 【HLS快速检测开关】True=先检测m3u8链接是否有效，无效直接跳过测速
ENABLE_HLS_QUICK_CHECK = True

# 【HLS检测超时时间】单位：秒，无需修改
HLS_CHECK_TIMEOUT = 6

# ==================================
# 🔵 5. 网页操作延时设置（网站卡顿可调大）
# ==================================
# 【处理完一个IP后的等待时间】单位：秒，防止操作太快被网站拦截
DELAY_BETWEEN_IPS = 1.0

# 【点击按钮后等待弹窗的时间】单位：秒，网站加载慢可以调大
DELAY_AFTER_CLICK = 0.5

# 【单个IP最多提取频道数】0=不限制，防止单个IP频道太多拖慢速度
MAX_CHANNELS_PER_IP = 0

# ==================================
# 🟣 6. 频道名称清洗设置
# ==================================
# 【中文清洗开关】True=移除非央视频道名称里的乱码/特殊字符，只保留中文
ENABLE_CHINESE_CLEAN = True

# 【去重开关】True=相同频道名+相同链接只保留一个，避免重复
ENABLE_DEDUPLICATION = True

# 【调试截图开关】True=关键步骤自动截图，保存在screenshots文件夹，仅本地调试用
ENABLE_SCREENSHOTS = False

# 【CCTV名称标准化】True=自动把CCTV-1转为CCTV-1综合，适配EPG节目单
CCTV_USE_MAPPING = True

# ==================================
# 🟤 7. 网络协议设置
# ==================================
# 【默认协议头】当爬取的链接没有http/https开头时，自动补全的协议，无需修改
DEFAULT_PROTOCOL = "http://"

# ==================================
# ⚫ 8. 测速缓存设置（GitHub Actions必开，大幅节省时间）
# ==================================
# 【缓存开关】True=测过的链接24小时内不再重复测速，False=每次都重新测所有链接
ENABLE_CACHE = True

# 【缓存文件保存路径】无需修改
CACHE_FILE = OUTPUT_DIR / "iptv_speed_cache.json"

# 【缓存过期时间】单位：小时，24=一天内测过的链接不再重测，0=永不过期
CACHE_EXPIRE_HOURS = 24

# ==================================
# ⚪ 9. 更新时间显示设置
# ==================================
# 【更新时间位置】True=更新时间放在播放列表最顶部，False=放在最底部
TIME_DISPLAY_AT_TOP = False

# 【更新时间占位流地址】无需修改
UPDATE_STREAM_URL = "https://gitee.com/bmg369/test/blob/main/175081947304562457.webp"

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

        while True:
            if time.time() - start > FFMPEG_TEST_DURATION + 8:
                break

            line = await proc.stdout.readline()
            if not line:
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
