#!/usr/bin/env python3
"""
IPTV 组播/域名源提取工具 - 终极调试版
新增：强制点击开始、智能等待、自动截图、HTML 日志
"""

import asyncio
import logging
import os
import re
import sys
import time
from collections import defaultdict
from pathlib import Path

import aiohttp
from playwright.async_api import async_playwright

# ============================================================================
# ======================== 【配置区】 =========================================
# ============================================================================

TARGET_URL = "https://iptv.809899.xyz"
HEADLESS = True  # 服务器保持 True
MAX_WAIT_TIME = 120  # 最多等待数据加载多少秒
MAX_SOURCES = 10

OUTPUT_M3U = "iptv_channels.m3u"
OUTPUT_TXT = "iptv_channels.txt"

# 页面文字配置 (尽量多列几个同义词)
PAGE_CONFIG = {
    "tab_names": ["组播提取", "酒店提取", "直播源", "组播源扫描"],
    "start_names": ["开始播放", "开始搜索", "开始提取", "开始", "扫描", "一键获取"],
}

CATEGORY_RULES = [
    {"name": "央视频道", "keywords": ["cctv", "cetv", "中央"]},
    {"name": "卫视频道", "keywords": ["卫视", "凤凰", "tvb", "湖南", "浙江", "江苏", "东方"]},
    {"name": "其他频道", "keywords": []},
]
GROUP_ORDER = ["央视频道", "卫视频道", "其他频道"]

# ============================================================================
# ============================= 日志与初始化 =================================
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
logger = logging.getLogger('IPTV')

# 确保截图目录存在
Path("debug").mkdir(exist_ok=True)

# ============================================================================
# ============================= 核心工具 =====================================
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

async def take_screenshot(page, name):
    """保存截图用于调试"""
    try:
        path = f"debug/{name}.png"
        await page.screenshot(path=path, full_page=True)
        logger.info(f"  [调试] 截图已保存: {path}")
    except Exception as e:
        logger.debug(f"截图失败: {e}")

async def robust_click(page, text_list, element_type="*", name="按钮"):
    """尝试点击包含任意指定文本的元素"""
    for text in text_list:
        try:
            # 构造选择器：尝试多种可能
            selector = f"{element_type}:has-text('{text}')"
            locator = page.locator(selector).first
            if await locator.count() > 0:
                logger.info(f"  尝试点击: {name} ({text})")
                await locator.scroll_into_view_if_needed(timeout=5000)
                await locator.click(force=True, timeout=5000)
                return True
        except Exception as e:
            logger.debug(f"点击 {text} 失败: {e}")
            continue
    return False

async def wait_and_find_rows(page):
    """
    【核心】智能等待数据加载
    循环检查页面，直到找到列表或超时
    """
    logger.info("等待数据加载中 (最多120秒)...")
    
    start_time = time.time()
    
    while time.time() - start_time < MAX_WAIT_TIME:
        # 1. 先截图看看当前状态
        await take_screenshot(page, "current_state")
        
        # 2. 尝试多种选择器查找列表项
        # 优先级 1: 特定的 class
        rows = page.locator("div.ios-list-item")
        count = await rows.count()
        if count > 0:
            logger.info(f"成功！通过 ios-list-item 找到 {count} 个源")
            return rows

        # 优先级 2: 找包含 IP 格式的文本行 (通用)
        # 这里我们用 evaluate 检查页面 HTML 里有没有东西
        has_content = await page.evaluate('''()=>{
            const bodyText = document.body.innerText;
            // 检查是否有 IP 或 "频道" 字样
            return /\\d+\\.\\d+\\.\\d+\\.\\d+|频道:|\\.blog/.test(bodyText);
        }''')
        
        if has_content:
            logger.info("检测到页面有数据，但选择器没匹配到。尝试通用选择器...")
            # 尝试所有看起来像列表项的 div
            all_divs = page.locator("div[class*='item'], div[class*='list']")
            if await all_divs.count() > 5: # 只要有超过5个类似div，就认为是它了
                return all_divs

        # 3. 如果还没找到，尝试再点一次“开始”
        logger.info("  数据未出现，尝试点击开始按钮...")
        await robust_click(page, PAGE_CONFIG["start_names"], name="开始")
        
        await asyncio.sleep(5) # 每轮等5秒

    logger.error("等待超时，页面上依然没有找到数据")
    return None

# ============================================================================
# ============================= 提取逻辑 =====================================
# ============================================================================

async def extract_from_modal(page):
    """从弹窗提取链接"""
    entries = []
    try:
        # 等待弹窗
        modal = page.locator(".modal-dialog, div[role='dialog']").first
        await modal.wait_for(state="visible", timeout=10000)
        await asyncio.sleep(1)
        
        # 获取所有项
        items = modal.locator(".item-content, div[class*='item']")
        total = await items.count()
        logger.info(f"  -> 弹窗打开，发现 {total} 项")

        for i in range(total):
            try:
                item = items.nth(i)
                # 简单粗暴：获取这个区域的所有文本，然后正则找链接
                text_content = await item.inner_text(timeout=2000)
                
                # 正则提取 URL
                # 匹配 http:// 或 https:// 开头的非空白字符
                url_match = re.search(r'(https?://[^\s]+)', text_content)
                
                if url_match:
                    link = url_match.group(1)
                    # 尝试提取名字（取链接前面的文本）
                    # 简单处理：用换行符分割，第一行非链接文本当作名字
                    name = f"频道{i+1}"
                    lines = text_content.splitlines()
                    for line in lines:
                        line = line.strip()
                        if line and not line.startswith("http") and len(line) < 50:
                            name = line
                            break
                
                    group = classify_channel(name)
                    entries.append((group, name, link))
            except Exception as e:
                continue
    except Exception as e:
        logger.debug(f"弹窗提取失败: {e}")
    return entries

async def close_modal(page):
    try:
        await page.keyboard.press("Escape")
        await asyncio.sleep(0.5)
    except:
        pass

# ============================================================================
# ============================= 主流程 =======================================
# ============================================================================

async def main():
    async with async_playwright() as p:
        logger.info("启动浏览器...")
        browser = await p.chromium.launch(
            headless=HEADLESS,
            args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"]
        )
        ctx = await browser.new_context(viewport={"width": 1920, "height": 1080})
        page = await ctx.new_page()

        all_entries = []

        try:
            # 1. 访问页面
            logger.info(f"访问: {TARGET_URL}")
            await page.goto(TARGET_URL, timeout=60000, wait_until="domcontentloaded")
            await asyncio.sleep(3)
            await take_screenshot(page, "01_after_load")

            # 2. 尝试点击所有可能的 Tab
            logger.info("尝试切换标签页...")
            clicked_tab = await robust_click(page, PAGE_CONFIG["tab_names"], element_type="div,button,a", name="标签页")
            await asyncio.sleep(2)
            await take_screenshot(page, "02_after_tab_click")

            # 3. 【关键】智能等待并查找列表
            rows = await wait_and_find_rows(page)
            
            if not rows:
                logger.error("彻底失败，未找到任何列表。请查看 debug/ 文件夹下的截图！")
                # 保存页面 HTML 源码供分析
                html_content = await page.content()
                with open("debug/page_source.html", "w", encoding="utf-8") as f:
                    f.write(html_content)
                logger.info("页面源码已保存至 debug/page_source.html")
                return

            # 4. 开始遍历点击
            total_count = await rows.count()
            process_count = min(total_count, MAX_SOURCES)
            logger.info(f"准备处理前 {process_count} 个源...")

            for i in range(process_count):
                row = rows.nth(i)
                
                # 获取源名称
                source_name = f"源{i+1}"
                try:
                    source_name = await row.locator("div.item-title").first.inner_text(timeout=1000)
                except:
                    pass
                
                logger.info(f"[{i+1}/{process_count}] 处理: {source_name}")

                # 点击行
                try:
                    await row.scroll_into_view_if_needed(timeout=3000)
                    await row.click(force=True, timeout=5000)
                except:
                    try:
                        await row.evaluate("el => el.click()")
                    except:
                        logger.warning("  点击失败，跳过")
                        continue

                await asyncio.sleep(1)
                
                # 提取
                entries = await extract_from_modal(page)
                if entries:
                    logger.info(f"  -> 收获 {len(entries)} 条")
                    all_entries.extend(entries)
                
                # 关闭
                await close_modal(page)
                await asyncio.sleep(0.5)

        except Exception as e:
            logger.exception("发生严重错误")
        finally:
            await browser.close()

        # 5. 生成文件
        if not all_entries:
            logger.warning("没有抓取到任何数据")
            return

        logger.info("整理数据并生成文件...")
        
        # 简单去重
        seen = set()
        unique = []
        for g, n, u in all_entries:
            key = (g, n, u)
            if key not in seen:
                seen.add(key)
                unique.append((g, n, u))

        # 分组
        grouped = defaultdict(list)
        for g, n, u in unique:
            grouped[g].append((n, u))

        # 写 M3U
        with open(OUTPUT_M3U, "w", encoding="utf-8") as f:
            f.write("#EXTM3U\n")
            for g in GROUP_ORDER:
                for n, u in grouped.get(g, []):
                    f.write(f'#EXTINF:-1 group-title="{g}",{n}\n{u}\n')

        logger.info(f"完成！共导出 {len(unique)} 条链接到 {OUTPUT_M3U}")

if __name__ == "__main__":
    asyncio.run(main())
