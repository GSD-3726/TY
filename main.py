#!/usr/bin/env python3
"""
IPTV 组播提取工具 —— GitHub Actions 全自动版
所有配置项均已集中管理，一键运行，无需人工干预。
"""

import asyncio
import re
import subprocess
import sys
from collections import defaultdict
from pathlib import Path
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# ============================================================================
# 用户可配置区域（请根据需求修改）
# ============================================================================

# ---------------------------- 基础设置 ------------------------------------
TARGET_URL = "https://iptv.809899.xyz"          # 目标网页
OUTPUT_DIR = Path(__file__).parent              # 输出目录（仓库根目录）
MAX_IPS = 10                                    # 只处理前 N 个 IP（0=全部）
HEADLESS = True                                 # 无头模式（CI 必须为 True）
BROWSER_TYPE = "chromium"                       # 可选 chromium / firefox / webkit

# ------------------------ 播放列表生成设置 --------------------------------
# 分组输出顺序（严格按照此顺序）
GROUP_ORDER = [
    "央视频道",
    "卫视频道",
    "电影频道",
    "4K专区",
    "儿童频道",
    "轮播频道"
]

# 每个频道名最多保留的链接数量（自动去重，取前 N 个）
MAX_LINKS_PER_CHANNEL = 10

# 输出文件名（可自定义）
OUTPUT_M3U_FILENAME = "iptv.m3u"
OUTPUT_TXT_FILENAME = "iptv.txt"

# -------------------------- 功能开关 -------------------------------------
# 是否启用汉字清洗（非央视频道）
ENABLE_CHINESE_CLEAN = True

# 是否启用去重（同一分组内频道名+链接完全一致则去重）
ENABLE_DEDUPLICATION = True

# 调试截图开关（CI 中建议关闭以节省时间）
ENABLE_SCREENSHOTS = False

# ============================================================================
# 以下为核心代码，非必要请勿修改
# ============================================================================

SCREENSHOT_DIR = OUTPUT_DIR / "debug_screenshots"
if ENABLE_SCREENSHOTS:
    SCREENSHOT_DIR.mkdir(exist_ok=True)

# 浏览器启动参数
LAUNCH_ARGS = {
    "headless": HEADLESS,
    "args": ["--no-sandbox"]
}

def ensure_browser_installed():
    """确保 Playwright 浏览器驱动已安装（GitHub Actions 专用）"""
    try:
        import playwright
    except ImportError:
        print("❌ Playwright 未安装，请先执行: pip install playwright")
        sys.exit(1)
    
    result = subprocess.run(
        [sys.executable, "-m", "playwright", "install", "--dry-run"],
        capture_output=True, text=True
    )
    if BROWSER_TYPE not in result.stdout:
        print(f"📦 正在安装 {BROWSER_TYPE} 浏览器驱动...")
        subprocess.run(
            [sys.executable, "-m", "playwright", "install", BROWSER_TYPE],
            check=True
        )
        print("✅ 浏览器驱动安装完成")

# ---------- 频道标准化与分类 ----------
def normalize_cctv(name: str) -> str:
    """将央视相关频道统一为 CCTV数字 或 CCTV5+ 格式"""
    name_lower = name.lower()
    if "cctv5+" in name_lower or "cctv5＋" in name_lower or "cctv5加" in name_lower:
        return "CCTV5+"
    match = re.search(r'cctv(\d{1,3})', name_lower)
    if match:
        return f"CCTV{match.group(1)}"
    match = re.search(r'cetv(\d)', name_lower)
    if match:
        return f"CETV{match.group(1)}"
    return name

def clean_chinese_only(name: str) -> str:
    """只保留汉字字符"""
    return re.sub(r'[^\u4e00-\u9fff]', '', name)

def classify_channel(name: str) -> str | None:
    """返回分组名称，不属于允许分类则返回 None"""
    name_lower = name.lower()
    if "4k" in name_lower:
        return "4K专区"
    if re.search(r'cctv|cetv|中央', name_lower):
        return "央视频道"
    if re.search(r'卫视|凤凰|tvb|湖南|浙江|江苏|东方|北京|深圳|山东|天津|'
                 r'贵州|四川|黑龙江|安徽|江西|湖北|东南|辽宁|广东|河北|'
                 r'甘肃|新疆|西藏|兵团|重庆|云南|广西|山西|陕西|吉林|'
                 r'内蒙古|河南|宁夏|青海', name_lower):
        return "卫视频道"
    if re.search(r'电影|影迷|家庭影院|动作电影|光影|动作影院|喜剧影院|'
                 r'经典电影|爱电影|chc', name_lower):
        return "电影频道"
    if "轮播频道" in name or "轮播" in name:
        return "轮播频道"
    if re.search(r'少儿|动画|卡通|kids|金鹰卡通|嘉佳卡通|卡酷少儿|动漫秀场|优优宝贝', name_lower):
        return "儿童频道"
    return None

# ---------- 主流程 ----------
async def main():
    ensure_browser_installed()

    async with async_playwright() as p:
        browser = await getattr(p, BROWSER_TYPE).launch(**LAUNCH_ARGS)
        context = await browser.new_context(
            accept_downloads=True,
            viewport={"width": 1920, "height": 1080}
        )
        page = await context.new_page()

        print("🌐 正在打开页面...")
        await page.goto(TARGET_URL, timeout=60000)
        await page.wait_for_load_state("networkidle", timeout=10000)
        if ENABLE_SCREENSHOTS:
            await page.screenshot(path=SCREENSHOT_DIR / "01_initial.png")
            print("📸 已保存初始页面截图")

        # ----- 1. 点击「引索搜索」-----
        engine_selectors = [
            "a.sidebar-link:text-matches('引索搜索|引擎搜索')",
            "button:has-text('引擎搜索')",
            "div.segment-item:has-text('关键词搜索')"
        ]
        for selector in engine_selectors:
            element = page.locator(selector).first
            if await element.count() > 0:
                await element.click(timeout=10000)
                print(f"✅ 点击「{selector}」")
                break
        else:
            print("⚠️ 未找到引擎搜索按钮，继续后续步骤")
        await page.wait_for_timeout(1000)

        # ----- 2. 点击「组播提取」-----
        mcast_tab = page.locator("div.segment-item:has-text('组播提取')").first
        await mcast_tab.wait_for(state="attached", timeout=15000)
        await mcast_tab.click(timeout=10000)
        print("✅ 点击「组播提取」")
        await page.wait_for_timeout(500)

        # ----- 3. 点击「开始播放」-----
        start_selectors = [
            "button:has-text('开始播放')",
            "button:has-text('开始搜索')",
            "button:has-text('开始提取')"
        ]
        for selector in start_selectors:
            btn = page.locator(selector).first
            if await btn.count() > 0:
                await btn.click(timeout=10000)
                print(f"✅ 点击「{selector}」")
                break
        else:
            if ENABLE_SCREENSHOTS:
                await page.screenshot(path=SCREENSHOT_DIR / "02_start_button_missing.png")
            raise Exception("❌ 未找到开始播放/搜索/提取按钮")

        # ----- 4. 等待扫描结果 -----
        print("⏳ 等待扫描结果（最多60秒）...")
        ip_locator = page.locator("div.item-title:text-matches('\\d+\\.\\d+\\.\\d+\\.\\d+')").first
        try:
            await ip_locator.wait_for(state="attached", timeout=60000)
            print("✅ 扫描完成")
        except PlaywrightTimeoutError:
            if ENABLE_SCREENSHOTS:
                await page.screenshot(path=SCREENSHOT_DIR / "03_scan_timeout.png")
            print("⚠️ 扫描超时，但可能已有历史结果")
        if ENABLE_SCREENSHOTS:
            await page.screenshot(path=SCREENSHOT_DIR / "04_results_page.png")

        # ----- 5. 获取IP列表并限制数量 -----
        result_rows = page.locator("div.ios-list-item").filter(has_text="频道:")
        total = await result_rows.count()
        process_count = total if MAX_IPS <= 0 else min(total, MAX_IPS)
        print(f"📋 共 {total} 个IP，本次处理前 {process_count} 个")

        # 存储所有原始条目 (group, channel_name, url)
        raw_entries = []

        for i in range(process_count):
            row = result_rows.nth(i)
            ip_text = await row.locator("div.item-title").first.inner_text()
            ip_text = ip_text.strip()
            if not re.match(r'^\d+\.\d+\.\d+\.\d+$', ip_text):
                print(f"\n📌 [{i+1}/{process_count}] {ip_text} (非IP，跳过)")
                continue
            print(f"\n📌 [{i+1}/{process_count}] {ip_text}")

            # 点击菜单按钮
            menu_btn = row.locator("button:has(i.fas.fa-list), button:has-text('≡'), button:has(i.fa-list)").first
            if await menu_btn.count() > 0:
                await menu_btn.click(timeout=5000)
                print("   🖱️ 点击菜单按钮")
            else:
                await row.locator("div.item-title").first.click(timeout=5000)
                print("   ⚠️ 点击IP地址")

            # 等待模态框
            modal = page.locator(".modal-dialog").first
            try:
                await modal.wait_for(state="visible", timeout=8000)
                print("   ✅ 模态框已打开")
            except PlaywrightTimeoutError:
                subtitle = row.locator("div.item-subtitle:has-text('频道:')").first
                if await subtitle.count() > 0:
                    print("   ⚠️ 尝试点击频道文本")
                    await subtitle.click(timeout=5000)
                    try:
                        await modal.wait_for(state="visible", timeout=5000)
                    except PlaywrightTimeoutError:
                        print("   ❌ 模态框未出现，跳过")
                        await page.keyboard.press("Escape")
                        continue
                else:
                    print("   ❌ 无法打开模态框，跳过")
                    await page.keyboard.press("Escape")
                    continue

            # 提取频道
            items = modal.locator(".item-content")
            count = await items.count()
            print(f"   📺 共 {count} 个频道")

            for j in range(count):
                item = items.nth(j)
                raw_name = await item.locator(".item-title").first.inner_text()
                link = await item.locator(".item-subtitle").first.inner_text()
                raw_name = raw_name.strip()
                link = link.strip()
                if not raw_name or not link:
                    continue

                # 标准化央视
                norm_name = normalize_cctv(raw_name)
                group = classify_channel(norm_name) or classify_channel(raw_name)
                if not group:
                    continue

                # 名称清洗
                if group == "央视频道":
                    final_name = norm_name
                elif ENABLE_CHINESE_CLEAN:
                    final_name = clean_chinese_only(raw_name)
                    if not final_name:
                        continue
                else:
                    final_name = raw_name

                raw_entries.append((group, final_name, link))

                if j < 3 or count <= 5:
                    print(f"      {j+1}. {final_name} -> {link[:60]}...")

            await page.keyboard.press("Escape")
            await page.wait_for_timeout(500)

        print(f"\n📊 原始条目数：{len(raw_entries)}")

        # ----- 6. 分组、去重、限制每个频道名的链接数量 -----
        # 按 (group, name) 聚合所有链接
        channel_urls = defaultdict(list)
        seen_set = set() if ENABLE_DEDUPLICATION else None

        for group, name, url in raw_entries:
            # 去重：同一 (group, name, url) 只保留一次
            if ENABLE_DEDUPLICATION:
                key = (group, name, url)
                if key in seen_set:
                    continue
                seen_set.add(key)

            # 收集该频道的所有链接（保留发现顺序）
            channel_urls[(group, name)].append(url)

        # 对每个频道，只保留前 MAX_LINKS_PER_CHANNEL 个链接
        limited_entries = []
        for (group, name), urls in channel_urls.items():
            for url in urls[:MAX_LINKS_PER_CHANNEL] if MAX_LINKS_PER_CHANNEL > 0 else urls:
                limited_entries.append((group, name, url))

        print(f"✅ 每个频道最多保留 {MAX_LINKS_PER_CHANNEL} 个链接，剩余 {len(limited_entries)} 条")

        # 按分组整理
        grouped = defaultdict(list)
        for group, name, url in limited_entries:
            grouped[group].append((name, url))

        # ----- 7. 各组内排序 -----
        # 央视频道按数字排序
        if "央视频道" in grouped:
            def cctv_sort_key(item):
                name = item[0]
                if name == "CCTV5+":
                    return (5, 1)
                m = re.search(r'CCTV(\d+)', name)
                if m:
                    return (int(m.group(1)), 0)
                m = re.search(r'CETV(\d+)', name)
                if m:
                    return (int(m.group(1)) + 100, 0)
                return (999, 0)
            grouped["央视频道"].sort(key=cctv_sort_key)

        # 其他分组按频道名称排序
        for g in grouped:
            if g != "央视频道":
                grouped[g].sort(key=lambda x: x[0])

        # ----- 8. 生成播放列表（按 GROUP_ORDER 顺序）-----
        m3u_path = OUTPUT_DIR / OUTPUT_M3U_FILENAME
        with open(m3u_path, "w", encoding="utf-8") as f:
            f.write("#EXTM3U\n")
            for group in GROUP_ORDER:
                if group not in grouped:
                    continue
                for name, url in grouped[group]:
                    f.write(f'#EXTINF:-1 group-title="{group}",{name}\n')
                    f.write(f"{url}\n")
        print(f"📀 M3U: {m3u_path}")

        txt_path = OUTPUT_DIR / OUTPUT_TXT_FILENAME
        with open(txt_path, "w", encoding="utf-8") as f:
            for group in GROUP_ORDER:
                if group not in grouped:
                    continue
                f.write(f"{group},#genre#\n")
                for name, url in grouped[group]:
                    f.write(f"{name},{url}\n")
                f.write("\n")
        print(f"📄 TXT: {txt_path}")

        total_channels = sum(len(v) for v in grouped.values())
        print(f"\n🎉 完成！共输出 {total_channels} 个频道条目（每个频道名 ≤ {MAX_LINKS_PER_CHANNEL} 链接）")
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
