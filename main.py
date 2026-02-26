#!/usr/bin/env python3
"""
IPTV 组播/域名源提取工具 - 修复点击弹窗版
修改说明：
1. 移除了对列表图标的依赖，直接点击整行
2. 强化了模态框(弹窗)的等待和提取逻辑
3. 增加了关闭弹窗的步骤，防止遮挡
4. 保留所有中文说明和实时日志功能
"""

import asyncio
import logging
import os
import re
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from urllib.parse import urljoin
import functools

import aiohttp
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# ============================================================================
# ======================== 【配置区 · 全中文说明】 =============================
# ============================================================================

# 1. 网站与浏览器设置 --------------------------------------------------------
TARGET_URL = "https://iptv.809899.xyz"          # 目标网站地址
HEADLESS = False                                  # 【建议】先设为 False，看浏览器是否正常点击
BROWSER_TYPE = "chromium"                        # 浏览器类型
MAX_SOURCES = 1                                  # 最多处理前N个源（避免耗时太长）
PAGE_LOAD_TIMEOUT = 180000                       # 页面加载超时(毫秒)

# 2. 输出文件设置 ------------------------------------------------------------
OUTPUT_M3U_FILENAME = "iptv_channels.m3u"        # 生成的M3U文件名
OUTPUT_TXT_FILENAME = "iptv_channels.txt"        # 生成的TXT文件名
MAX_LINKS_PER_CHANNEL = 5                         # 每个频道保留几个链接

# 3. 测速总开关 --------------------------------------------------------------
ENABLE_SPEED_TEST = True                          # 是否进行速度测试 (False会快很多)
SPEED_TEST_CONCURRENCY = 5                        # 同时测速的协程数
MIN_STABLE_SPEED = 0.5                             # 最低要求速度(Mbps)

# 4. 页面操作延迟 ------------------------------------------------------------
DELAY_BETWEEN_SOURCES = 0.8                       # 处理完一个源后等待秒数
DELAY_AFTER_CLICK = 0.5                           # 点击后等待弹窗加载

# 5. 数据清洗 ----------------------------------------------------------------
ENABLE_CHINESE_CLEAN = False                       # 是否只保留中文名 (建议False，防止误删)
ENABLE_DEDUPLICATION = True                         # 是否去重
CCTV_USE_MAPPING = True                             # 是否格式化CCTV名称

# ============================================================================
# ============================ 频道分类规则 ==================================
# ============================================================================

OUTPUT_DIR = Path(__file__).parent

# 页面元素点击关键词
PAGE_CONFIG = {
    "engine_search": ["引索搜索", "引擎搜索", "关键词搜索", "直播搜索"],
    "multicast_tab": ["组播提取", "酒店提取"],
    "start_button": ["开始播放", "开始搜索", "开始提取"],
}

# 频道分类规则
CATEGORY_RULES = [
    {"name": "4K专区",      "keywords": ["4k"]},
    {"name": "央视频道",    "keywords": ["cctv", "cetv", "中央"]},
    {"name": "卫视频道",    "keywords": ["卫视", "凤凰", "tvb", "湖南", "浙江", "江苏", "东方"]},
    {"name": "电影频道",    "keywords": ["电影", "影院", "chc"]},
    {"name": "其他频道",    "keywords": []}, # 兜底
]

GROUP_ORDER = ["央视频道", "卫视频道", "电影频道", "4K专区", "其他频道"]

# CCTV名称映射
CCTV_NAME_MAPPING = {
    "1": "综合", "2": "财经", "3": "综艺", "4": "国际", "5": "体育",
    "5+": "体育赛事", "6": "电影", "7": "国防军事", "8": "电视剧",
    "9": "纪录", "10": "科教", "11": "戏曲", "12": "社会与法",
    "13": "新闻", "14": "少儿", "15": "音乐", "16": "奥林匹克",
    "17": "农业农村",
}

# ============================================================================
# ============================= 日志配置 (实时版) =============================
# ============================================================================

class UnbufferedStreamHandler(logging.StreamHandler):
    """强制每次输出后立即刷新，解决日志延迟"""
    def emit(self, record):
        super().emit(record)
        self.flush()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        UnbufferedStreamHandler(sys.stdout),
        logging.FileHandler('iptv_extractor.log', encoding='utf-8')
    ]
)
logger = logging.getLogger('IPTV-Extractor')

# ============================================================================
# ============================= 工具函数 =====================================
# ============================================================================

CCTV_PATTERN = re.compile(r'(cctv)[-\s]?(\d{1,3})', re.IGNORECASE)
RESOLUTION_PATTERN = re.compile(r'(\d+)x(\d+)')

def build_classifier():
    compiled = []
    default_group = None
    for rule in CATEGORY_RULES:
        if not rule["keywords"]:
            default_group = rule["name"]
            continue
        pattern = re.compile("|".join(re.escape(kw.lower()) for kw in rule["keywords"]))
        compiled.append((rule["name"], pattern))
    
    def classifier(name):
        for group, pat in compiled:
            if pat.search(name.lower()):
                return group
        return default_group
    return classifier

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

def build_selector(text_list, element_type="button"):
    if not text_list: return ""
    if len(text_list) == 1: return f"{element_type}:has-text('{text_list[0]}')"
    pattern = "|".join(re.escape(t) for t in text_list)
    return f"{element_type}:text-matches('{pattern}')"

ENGINE_SELECTOR = build_selector(PAGE_CONFIG["engine_search"], "a,button,div")
MCAST_SELECTOR = build_selector(PAGE_CONFIG["multicast_tab"], "div,button")
START_SELECTOR = build_selector(PAGE_CONFIG["start_button"], "button")

# ============================================================================
# ========================= 核心交互逻辑 (修复区) =============================
# ============================================================================

async def robust_click(locator, timeout=5000):
    """强制点击，多种方法尝试"""
    try:
        await locator.scroll_into_view_if_needed(timeout=2000)
        await asyncio.sleep(0.1)
        await locator.click(force=True, timeout=timeout)
        return True
    except:
        try:
            await locator.evaluate("el => el.click()")
            return True
        except:
            return False

async def close_modal(page):
    """尝试关闭弹窗，防止遮挡下一个点击"""
    try:
        # 尝试按 ESC 键，这是最通用的方法
        await page.keyboard.press("Escape")
        await asyncio.sleep(0.2)
        # 如果有明显的关闭按钮也可以点一下
        close_btn = page.locator(".modal-header button, .btn-close, [aria-label='Close']").first
        if await close_btn.count() > 0:
            await close_btn.click(timeout=1000)
    except:
        pass

async def extract_from_modal(page):
    """
    从弹窗中提取真正的链接
    假设结构：.modal-dialog -> .item-content -> .item-title (频道名) + .item-subtitle (链接)
    """
    entries = []
    try:
        # 1. 等待弹窗出现
        modal = page.locator(".modal-dialog").first
        await modal.wait_for(state="visible", timeout=8000)
        await asyncio.sleep(0.5) # 稍作等待，让内容渲染
        
        # 2. 获取所有频道项
        items = modal.locator(".item-content")
        total = await items.count()
        
        if total == 0:
            # 备选：如果没有 .item-content，试试找所有带 title/subtitle 的 div
            items = modal.locator("div[class*='item']")
            total = await items.count()

        logger.info(f"  -> 弹窗加载成功，发现 {total} 个频道")

        for i in range(total):
            try:
                item = items.nth(i)
                
                # 提取名称
                name_el = item.locator(".item-title").first
                # 提取链接 (核心：链接在弹窗的 subtitle 里)
                link_el = item.locator(".item-subtitle").first
                
                # 容错：如果取不到，尝试直接取所有文本然后正则找链接
                name = await name_el.inner_text(timeout=1000)
                link = await link_el.inner_text(timeout=1000)
                
                name = name.strip()
                link = link.strip()
                
                # 简单校验：必须包含链接协议
                if not name or not link or not ("://" in link):
                    continue
                
                # 分类
                group = classify_channel(name)
                if not group:
                    group = "其他频道"
                
                # 名称美化
                final_name = normalize_cctv(name) if group == "央视频道" else name
                
                entries.append((group, final_name, link))
            except Exception as e:
                # 单个频道失败不影响整体
                continue
                
    except Exception as e:
        logger.debug(f"  -> 弹窗提取出错: {e}")
    
    return entries

# ============================================================================
# ========================= 测速逻辑 (简化版) =================================
# ============================================================================

async def fetch_url(session: aiohttp.ClientSession, url: str, timeout: int):
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
            if resp.status == 200:
                return await resp.read()
    except:
        pass
    return None

async def test_simple_speed(session, url):
    """简单测速：只看能不能连接，不测太细"""
    try:
        content = await fetch_url(session, url, 5)
        if content:
            # 简单通过
            return True, 1.0
    except:
        pass
    return False, 0.0

async def run_speed_test_simple(channel_map):
    if not ENABLE_SPEED_TEST:
        return channel_map

    conn = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=conn) as session:
        sem = asyncio.Semaphore(SPEED_TEST_CONCURRENCY)
        
        async def task(url):
            async with sem:
                ok, _ = await test_simple_speed(session, url)
                return url if ok else None

        final_map = {}
        total = sum(len(v) for v in channel_map.values())
        logger.info(f"开始简易测速，共 {total} 条...")
        
        for (group, name), urls in channel_map.items():
            results = await asyncio.gather(*[task(u) for u in urls])
            valid_urls = [u for u in results if u]
            if valid_urls:
                final_map[(group, name)] = valid_urls[:MAX_LINKS_PER_CHANNEL]
        
        logger.info(f"测速完成，保留 {sum(len(v) for v in final_map.values())} 条")
        return final_map

# ============================================================================
# ============================= 主流程 =======================================
# ============================================================================

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=HEADLESS,
            args=["--no-sandbox", "--disable-dev-shm-usage"]
        )
        ctx = await browser.new_context(viewport={"width": 1920, "height": 1080})
        page = await ctx.new_page()

        try:
            logger.info(f"正在访问 {TARGET_URL} ...")
            await page.goto(TARGET_URL, timeout=PAGE_LOAD_TIMEOUT, wait_until="networkidle")
            
            # 稍微等待一下页面渲染，或者你可以手动点击一下Tab
            await asyncio.sleep(2)

            # 尝试自动点击切换 Tab (如果需要)
            if MCAST_SELECTOR:
                t = page.locator(MCAST_SELECTOR).first
                if await t.count() > 0:
                    logger.info("自动切换到目标标签页...")
                    await robust_click(t)
                    await asyncio.sleep(1)

            # 【关键】定位所有列表项
            # 只认 class="ios-list-item"，这是你提供的HTML里的准确类名
            rows = page.locator("div.ios-list-item")
            total_rows = await rows.count()
            
            logger.info(f"页面扫描完成，共发现 {total_rows} 个源")

            if total_rows == 0:
                logger.error("未找到任何源！请检查：1. 是否在正确的页面？ 2. 是否需要手动点击'开始'？")
                # 如果是无头模式，这里保持浏览器打开方便看
                if not HEADLESS:
                    await asyncio.sleep(1000)
                return

            process_count = min(total_rows, MAX_SOURCES)
            all_entries = []

            for i in range(process_count):
                row = rows.nth(i)
                
                # 1. 获取源的名字 (用于显示进度)
                source_name = f"源{i+1}"
                try:
                    title_el = row.locator("div.item-title").first
                    source_name = await title_el.inner_text(timeout=1000)
                except:
                    pass
                
                logger.info(f"[{i+1}/{process_count}] 正在处理: {source_name}")

                # 2. 【核心修复】直接点击这一行
                # 不再找按钮，直接点击整行
                click_ok = await robust_click(row)
                
                if not click_ok:
                    logger.warning("  -> 点击失败，跳过")
                    continue

                # 3. 等待弹窗并提取数据
                entries = await extract_from_modal(page)
                
                if entries:
                    logger.info(f"  -> 成功获取 {len(entries)} 条链接")
                    all_entries.extend(entries)
                else:
                    logger.info(f"  -> 未获取到链接")

                # 4. 关闭弹窗，准备下一个
                await close_modal(page)
                
                # 间隔
                await asyncio.sleep(DELAY_BETWEEN_SOURCES)

            # 数据整理
            logger.info(f"抓取结束，开始整理数据...")
            
            # 去重
            channel_map = defaultdict(list)
            seen = set()
            for g, n, u in all_entries:
                if ENABLE_DEDUPLICATION:
                    key = (g, n, u)
                    if key in seen: continue
                    seen.add(key)
                channel_map[(g,n)].append(u)

            # 测速 (可选)
            # channel_map = await run_speed_test_simple(channel_map)

            # 生成文件
            grouped = defaultdict(list)
            for (g, n), urls in channel_map.items():
                for u in urls[:MAX_LINKS_PER_CHANNEL]:
                    grouped[g].append((n, u))

            # 写 M3U
            with open(OUTPUT_M3U_FILENAME, "w", encoding="utf-8") as f:
                f.write("#EXTM3U\n")
                for g in GROUP_ORDER:
                    for n, u in grouped.get(g, []):
                        f.write(f'#EXTINF:-1 group-title="{g}",{n}\n{u}\n')

            # 写 TXT
            with open(OUTPUT_TXT_FILENAME, "w", encoding="utf-8") as f:
                for g in GROUP_ORDER:
                    if g not in grouped: continue
                    f.write(f"{g},#genre#\n")
                    for n, u in grouped[g]:
                        f.write(f"{n},{u}\n")
                    f.write("\n")

            total_final = sum(len(v) for v in grouped.values())
            logger.info(f"===== 全部完成 =====")
            logger.info(f"共导出 {total_final} 条链接")
            logger.info(f"请查看文件: {OUTPUT_M3U_FILENAME}")

        except Exception as e:
            logger.exception("运行出错")
        finally:
            if HEADLESS:
                await browser.close()
            else:
                logger.info("脚本结束，浏览器保持打开状态... (按 Ctrl+C 退出)")
                await asyncio.sleep(10000) # 保持打开方便调试

if __name__ == "__main__":
    asyncio.run(main())
