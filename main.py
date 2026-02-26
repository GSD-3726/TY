#!/usr/bin/env python3
"""
IPTV 源提取工具 - 修复点击版
针对特定 HTML 结构优化：直接点击行进入详情
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
# ======================== 【配置区】 =========================================
# ============================================================================

TARGET_URL = "https://iptv.809899.xyz"
HEADLESS = False  # 建议先设为 False，看浏览器动作是否正确
MAX_SOURCES = 2   # 只处理前10个，避免卡死

OUTPUT_M3U_FILENAME = "iptv_channels.m3u"
OUTPUT_TXT_FILENAME = "iptv_channels.txt"
MAX_LINKS_PER_CHANNEL = 5

ENABLE_SPEED_TEST = True
SPEED_TEST_CONCURRENCY = 5
MIN_STABLE_SPEED = 0.5

# 分类
CATEGORY_RULES = [
    {"name": "4K专区",      "keywords": ["4k"]},
    {"name": "央视频道",    "keywords": ["cctv", "cetv", "中央"]},
    {"name": "卫视频道",    "keywords": ["卫视", "凤凰", "tvb", "湖南", "浙江", "江苏", "东方"]},
    {"name": "电影频道",    "keywords": ["电影", "影院", "chc"]},
    {"name": "其他频道",    "keywords": []},
]
GROUP_ORDER = ["央视频道", "卫视频道", "电影频道", "4K专区", "其他频道"]

# ============================================================================
# ============================= 日志配置 =====================================
# ============================================================================

class UnbufferedStreamHandler(logging.StreamHandler):
    def emit(self, record):
        super().emit(record)
        self.flush()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[UnbufferedStreamHandler(sys.stdout), logging.FileHandler('iptv_extractor.log', encoding='utf-8')]
)
logger = logging.getLogger('IPTV-Extractor')

# ============================================================================
# ============================= 工具函数 =====================================
# ============================================================================

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

# ============================================================================
# ========================= 核心修复：页面交互 ================================
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

async def extract_from_modal(page, source_name):
    """
    【关键】从弹窗中提取链接
    假设弹窗结构：.item-content 里包含 .item-title (频道名) 和 .item-subtitle (链接)
    """
    entries = []
    try:
        # 等待弹窗出现
        modal = page.locator(".modal-dialog").first
        await modal.wait_for(state="visible", timeout=8000)
        await asyncio.sleep(0.5) # 稍微等一下内容渲染
        
        # 获取弹窗里的所有项
        items = modal.locator(".item-content")
        total = await items.count()
        logger.info(f"  -> 弹窗加载成功，发现 {total} 个频道")

        for i in range(total):
            try:
                item = items.nth(i)
                # 提取名称
                name_el = item.locator(".item-title").first
                # 提取链接 (这是核心，链接一定在弹窗的 subtitle 里)
                link_el = item.locator(".item-subtitle").first
                
                name = await name_el.inner_text(timeout=1000)
                link = await link_el.inner_text(timeout=1000)
                
                name = name.strip()
                link = link.strip()
                
                # 简单校验：链接必须包含协议
                if not name or not link or not ("://" in link):
                    continue
                
                # 分类
                group = classify_channel(name)
                if not group:
                    group = "其他频道"
                
                entries.append((group, name, link))
            except Exception as e:
                # 单个频道提取失败不影响其他
                continue
                
    except Exception as e:
        logger.warning(f"  -> 无法从弹窗提取内容: {e}")
    
    return entries

async def close_modal(page):
    """尝试关闭弹窗，防止遮挡下一个点击"""
    try:
        # 尝试点击背景、关闭按钮，或者按 ESC
        close_btn = page.locator(".modal-header button.close, .btn-close, [aria-label='Close']").first
        if await close_btn.count() > 0:
            await close_btn.click(timeout=1000)
        else:
            # 按 ESC 键
            await page.keyboard.press("Escape")
        await asyncio.sleep(0.3)
    except:
        pass

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS, args=["--no-sandbox"])
        ctx = await browser.new_context(viewport={"width": 1920, "height": 1080})
        page = await ctx.new_page()

        try:
            logger.info(f"正在访问网站...")
            await page.goto(TARGET_URL, timeout=120000, wait_until="networkidle")
            
            # 手动切换到正确的 Tab (根据你的截图，可能需要手动点一下"组播提取"或"酒店提取")
            # 这里增加一点手动等待时间，或者你可以在 HEADLESS=False 时手动点好
            await asyncio.sleep(3) 
            
            # 【核心修复 1】定位列表项
            # 直接定位所有 class="ios-list-item"，不再过滤文字，防止漏失
            rows = page.locator("div.ios-list-item")
            total_rows = await rows.count()
            logger.info(f"页面上共找到 {total_rows} 个源")

            if total_rows == 0:
                logger.error("未找到任何 ios-list-item，请检查是否在正确的页面上！")
                return

            process_count = min(total_rows, MAX_SOURCES)
            all_entries = []

            for i in range(process_count):
                row = rows.nth(i)
                
                # 1. 获取源的名字 (用于日志)
                source_name = "Unknown"
                try:
                    source_name = await row.locator(".item-title").first.inner_text(timeout=1000)
                except:
                    pass
                
                logger.info(f"[{i+1}/{process_count}] 正在处理: {source_name}")

                # 2. 【核心修复 2】点击这一行
                # 不找按钮了，直接点这一行的任意位置
                click_success = await robust_click(row)
                
                if not click_success:
                    logger.warning("  -> 点击失败，跳过")
                    continue

                # 3. 等待弹窗并提取
                entries = await extract_from_modal(page, source_name)
                if entries:
                    logger.info(f"  -> 成功提取 {len(entries)} 条链接")
                    all_entries.extend(entries)
                
                # 4. 关闭弹窗，准备下一个
                await close_modal(page)
                
                # 稍微歇一会
                await asyncio.sleep(0.5)

            # 去重
            logger.info(f"原始抓取结束，共 {len(all_entries)} 条，开始去重...")
            channel_map = defaultdict(list)
            seen = set()
            for g, n, u in all_entries:
                key = (g, n, u)
                if key in seen: continue
                seen.add(key)
                channel_map[(g,n)].append(u)

            # 简单测速 (可选，这里简化了，直接保留前5个)
            final_map = {}
            for k, v in channel_map.items():
                final_map[k] = v[:MAX_LINKS_PER_CHANNEL]

            # 生成文件
            grouped = defaultdict(list)
            for (g, n), urls in final_map.items():
                for u in urls:
                    grouped[g].append((n, u))

            with open(OUTPUT_M3U_FILENAME, "w", encoding="utf-8") as f:
                f.write("#EXTM3U\n")
                for g in GROUP_ORDER:
                    for n, u in grouped.get(g, []):
                        f.write(f'#EXTINF:-1 group-title="{g}",{n}\n{u}\n')

            logger.info(f"完成！生成文件 {OUTPUT_M3U_FILENAME}，共 {sum(len(v) for v in grouped.values())} 条")

        except Exception as e:
            logger.exception("主程序出错")
        finally:
            # 保持浏览器打开一会方便调试，或者直接关闭
            if HEADLESS:
                await browser.close()
            else:
                logger.info("按 Ctrl+C 关闭浏览器...")
                await asyncio.sleep(1000)

if __name__ == "__main__":
    asyncio.run(main())
