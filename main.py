#!/usr/bin/env python3
"""
IPTV 组播提取工具 - FFmpeg专业测速版
修改说明：
1. 移除HTTP下载测速，改为使用FFmpeg进行真实解码测试
2. 增加详细的中文注释说明
3. 优化缓存逻辑，适配GitHub Actions环境
"""

import asyncio
import json
import logging
import os
import re
import sys
import time
import shutil
import subprocess
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any
from urllib.parse import urljoin
import functools

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# ============================================================================
# ======================== 【配置区 · 全中文说明】 =============================
# ============================================================================

# 1. 网站与浏览器设置 --------------------------------------------------------
TARGET_URL = "https://iptv.809899.xyz"          # 目标网站地址
HEADLESS = True                                  # 是否无头模式（True不显示浏览器窗口）
BROWSER_TYPE = "chromium"                        # 浏览器类型
MAX_IPS = 15                                     # 最多提取前N个IP地址的频道
PAGE_LOAD_TIMEOUT = 120000                       # 页面加载超时（毫秒）

# 2. 输出文件设置 ------------------------------------------------------------
OUTPUT_DIR = Path(__file__).parent               # 输出目录（默认为脚本所在目录）
OUTPUT_M3U_FILENAME = OUTPUT_DIR / "iptv_channels.m3u"
OUTPUT_TXT_FILENAME = OUTPUT_DIR / "iptv_channels.txt"
MAX_LINKS_PER_CHANNEL = 10                       # 每个频道保留的最优链接数量

# 3. FFmpeg测速设置 (核心修改) ------------------------------------------------
ENABLE_FFMPEG_TEST = True                         # 总开关：是否使用FFmpeg测试
FFMPEG_PATH = "ffmpeg"                            # FFmpeg路径 (Windows建议写绝对路径，如 r"C:\ffmpeg\bin\ffmpeg.exe")
FFMPEG_TEST_DURATION = 8                          # 单个流的测试时长（秒，建议5-15秒）
FFMPEG_CONCURRENCY = 5                            # FFmpeg并发数 (非常消耗CPU/内存，GitHub Actions建议<=2)
MIN_AVG_FPS = 15.0                                # 最低平均帧率 (低于此值认为卡顿)
MIN_FRAMES = 50                                    # 最低解码帧数 (防止只有几秒数据)

# 4. 页面操作延迟 ------------------------------------------------------------
DELAY_BETWEEN_IPS = 1.0
DELAY_AFTER_CLICK = 0.5
MAX_CHANNELS_PER_IP = 0

# 5. 数据清洗与去重 ----------------------------------------------------------
ENABLE_CHINESE_CLEAN = True
ENABLE_DEDUPLICATION = True
ENABLE_SCREENSHOTS = False
CCTV_USE_MAPPING = True

# 6. 网络与安全设置 ---------------------------------------------------------
DEFAULT_PROTOCOL = "http://"

# 7. 缓存设置 (重点说明) -----------------------------------------------------
ENABLE_CACHE = True                                  # 是否启用缓存
CACHE_FILE = OUTPUT_DIR / "iptv_speed_cache.json"  # 【缓存位置】
#  本地运行：保存在脚本旁边，文件名为 iptv_speed_cache.json
#  GitHub：必须在Workflow中配置 actions/cache 保存此文件，否则每次运行都会丢失！
CACHE_EXPIRE_HOURS = 24                              # 缓存过期时间（小时）

# ============================================================================
# ============================ 频道分类规则 ==================================
# ============================================================================

# 页面元素选择器配置
PAGE_CONFIG = {
    "engine_search": ["引索搜索", "引擎搜索", "关键词搜索"],
    "multicast_tab": ["酒店提取"],
    "start_button": ["开始播放", "开始搜索", "开始提取"],
}

# 频道分类规则
CATEGORY_RULES = [
    {"name": "4K专区",      "keywords": ["4k"]},
    {"name": "央视频道",    "keywords": ["cctv", "cetv", "中央"]},
    {"name": "卫视频道",    "keywords": ["卫视", "凤凰", "tvb", "湖南", "浙江", "江苏", "东方"]},
    {"name": "电影频道",    "keywords": ["电影", "影院", "chc"]},
    {"name": "轮播频道",    "keywords": ["轮播"]},
    {"name": "儿童频道",    "keywords": ["少儿", "动画", "卡通"]},
]

GROUP_ORDER = ["央视频道", "卫视频道", "电影频道", "4K专区", "儿童频道", "轮播频道"]

# CCTV频道号映射
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
    """加载测速缓存"""
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
        return valid_cache
    except Exception as e:
        logger.warning(f"加载缓存失败: {e}")
        return {}

def save_cache(cache: Dict[str, Dict[str, Any]]):
    """保存测速缓存"""
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
# ========================= 【核心】FFmpeg测速代码 ============================
# ============================================================================

async def test_stream_with_ffmpeg(url: str) -> Dict[str, Any]:
    """
    使用FFmpeg测试直播源
    返回: {"ok": bool, "fps": float, "message": str}
    """
    # 1. 检查FFmpeg是否存在
    if not shutil.which(FFMPEG_PATH):
        logger.error(f"未找到FFmpeg，请检查路径配置: {FFMPEG_PATH}")
        return {"ok": False, "fps": 0.0, "message": "FFmpeg未安装"}

    # 2. 构建FFmpeg命令
    # -t: 测试时长
    # -f null: 只解码不输出文件
    cmd = [
        FFMPEG_PATH,
        "-hide_banner",
        "-y",
        "-fflags", "nobuffer",
        "-rw_timeout", "5000000",  # 5秒超时 (微秒)
        "-i", url,
        "-t", str(FFMPEG_TEST_DURATION),
        "-f", "null",
        "-"
    ]

    start_time = time.monotonic()
    try:
        # 3. 异步运行FFmpeg
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE
        )
        
        # 等待完成或超时 (额外加2秒缓冲)
        try:
            _, stderr = await asyncio.wait_for(
                proc.communicate(),
                timeout=FFMPEG_TEST_DURATION + 5
            )
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            return {"ok": False, "fps": 0.0, "message": "连接超时"}

        output = stderr.decode('utf-8', errors='ignore')
        
        # 4. 解析FFmpeg输出
        # 寻找 frame=123 fps=25.0 ...
        frames = 0
        fps_sum = 0.0
        fps_count = 0
        
        # 正则匹配最后出现的 frame 和 fps
        frame_matches = re.findall(r'frame=\s*(\d+)', output)
        fps_matches = re.findall(r'fps=\s*([\d.]+)', output)
        
        if frame_matches:
            frames = int(frame_matches[-1])
        if fps_matches:
            # 简单取最后一个fps值，或者求平均
            avg_fps = float(fps_matches[-1])
        else:
            avg_fps = 0.0

        # 5. 判断是否流畅
        is_smooth = frames >= MIN_FRAMES and avg_fps >= MIN_AVG_FPS
        
        logger.debug(f"FFmpeg测试 [{url[:50]}...]: 帧={frames}, FPS={avg_fps:.1f}, 结果={'通过' if is_smooth else '失败'}")

        return {
            "ok": is_smooth,
            "fps": avg_fps,
            "frames": frames,
            "message": "正常" if is_smooth else f"卡顿 (帧:{frames}/FPS:{avg_fps:.1f})"
        }

    except Exception as e:
        return {"ok": False, "fps": 0.0, "message": f"异常: {str(e)[:50]}"}

async def run_ffmpeg_test(channel_map: Dict[Tuple[str, str], List[str]]) -> Dict[Tuple[str, str], List[str]]:
    """
    并发运行FFmpeg测试，并处理缓存
    """
    if not channel_map: return {}

    cache = load_cache()
    new_cache_entries = {}
    result_map = defaultdict(list)
    pending_tasks = []

    # 1. 分流：查缓存 vs 待测速
    for (group, name), urls in channel_map.items():
        for url in urls:
            if url in cache:
                # 缓存命中
                data = cache[url]
                if data.get("ok"):
                    result_map[(group, name)].append((url, data.get("fps", 0)))
            else:
                # 缓存未命中
                pending_tasks.append((group, name, url))

    logger.info(f"缓存命中 {len([item for sublist in result_map.values() for item in sublist])} 条，需测试 {len(pending_tasks)} 条")

    # 2. 并发执行FFmpeg测试
    if pending_tasks:
        sem = asyncio.Semaphore(FFMPEG_CONCURRENCY) # 控制并发数

        async def bound_test(url):
            async with sem:
                logger.info(f"正在测速: {url[:60]}...")
                return await test_stream_with_ffmpeg(url)

        # 创建任务
        tasks = [bound_test(url) for (_, _, url) in pending_tasks]
        results = await asyncio.gather(*tasks)

        # 3. 处理结果
        for i, res in enumerate(results):
            group, name, url = pending_tasks[i]
            
            # 存入缓存（无论好坏，避免下次重测坏链）
            new_cache_entries[url] = {
                "ok": res["ok"],
                "fps": res["fps"],
                "frames": res.get("frames", 0),
                "timestamp": time.time()
            }

            if res["ok"]:
                result_map[(group, name)].append((url, res["fps"]))

    # 4. 保存缓存
    if new_cache_entries:
        cache.update(new_cache_entries)
        save_cache(cache)
        logger.info(f"缓存已更新，新增 {len(new_cache_entries)} 条记录")

    # 5. 按FPS排序并截断
    final_map = {}
    for key, items in result_map.items():
        items.sort(key=lambda x: -x[1]) # 按帧率从高到低
        final_map[key] = [url for url, _ in items[:MAX_LINKS_PER_CHANNEL]]

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
    except Exception as e:
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
            if not await robust_click(list_btn):
                await row.click(timeout=3000)
        else:
            await row.click(timeout=3000)
        await asyncio.sleep(DELAY_AFTER_CLICK)

        modal = page.locator(".modal-dialog").first
        if not await wait_for_element(page, ".modal-dialog", timeout=5000):
            return []

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
# ============================= 主流程 =======================================
# ============================================================================

async def main():
    if ENABLE_SCREENSHOTS:
        (OUTPUT_DIR / "screenshots").mkdir(exist_ok=True)

    async with async_playwright() as p:
        browser = await getattr(p, BROWSER_TYPE).launch(
            headless=HEADLESS,
            args=["--no-sandbox", "--disable-setuid-sandbox"]
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

            # 去重
            channel_map = defaultdict(list)
            seen = set()
            for group, name, url in raw_entries:
                if ENABLE_DEDUPLICATION:
                    key = (group, name, url)
                    if key in seen: continue
                    seen.add(key)
                channel_map[(group, name)].append(url)

            # FFmpeg测速
            if ENABLE_FFMPEG_TEST and channel_map:
                channel_map = await run_ffmpeg_test(channel_map)

            # 导出
            grouped = defaultdict(list)
            for (group, name), urls in channel_map.items():
                for url in urls:
                    grouped[group].append((name, url))

            with open(OUTPUT_M3U_FILENAME, "w", encoding="utf-8") as f:
                f.write("#EXTM3U\n")
                for group in GROUP_ORDER:
                    for name, url in grouped.get(group, []):
                        f.write(f'#EXTINF:-1 group-title="{group}",{name}\n{url}\n')

            with open(OUTPUT_TXT_FILENAME, "w", encoding="utf-8") as f:
                for group in GROUP_ORDER:
                    if group not in grouped: continue
                    f.write(f"{group},#genre#\n")
                    for name, url in grouped[group]:
                        f.write(f"{name},{url}\n")
                    f.write("\n")

            logger.info(f"完成！结果保存在 {OUTPUT_DIR}")

        except Exception as e:
            logger.exception("主流程异常")
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
