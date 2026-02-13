#!/usr/bin/env python3
"""
IPTV 组播提取工具 —— 全配置化自动版（GitHub Actions 兼容）
所有配置项均已在文件顶部集中管理，修改配置即可适配任何网站或命名习惯。
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

# ------------------------ 页面交互配置 ------------------------------------
# 所有按钮文字均在此配置，支持多个备选（按顺序尝试）
PAGE_CONFIG = {
    # 侧边栏「引擎搜索」按钮（可配置多个文字变体）
    "engine_search": ["引索搜索", "引擎搜索", "关键词搜索"],
    # 顶部「组播提取」标签（一般固定）
    "multicast_tab": ["组播提取"],
    # 开始扫描按钮（根据网站实际文字填写）
    "start_button": ["开始播放", "开始搜索", "开始提取"],
}

# ------------------------ 分类规则配置 ------------------------------------
# 自定义分组名称及匹配关键词（支持正则）
# 每个分组定义：{"name": "显示名称", "keywords": ["关键词1", "关键词2", ...]}
# 匹配时不区分大小写，只要频道名包含任意关键词即归入该分组
CATEGORY_RULES = [
    {"name": "4K专区",      "keywords": ["4k"]},
    {"name": "央视频道",    "keywords": ["cctv", "cetv", "中央"]},
    {"name": "卫视频道",    "keywords": ["卫视", "凤凰", "tvb", "湖南", "浙江", "江苏", "东方", 
                                      "北京", "深圳", "山东", "天津", "贵州", "四川", "黑龙江",
                                      "安徽", "江西", "湖北", "东南", "辽宁", "广东", "河北",
                                      "甘肃", "新疆", "西藏", "兵团", "重庆", "云南", "广西",
                                      "山西", "陕西", "吉林", "内蒙古", "河南", "宁夏", "青海"]},
    {"name": "电影频道",    "keywords": ["电影", "影迷", "家庭影院", "动作电影", "光影", 
                                      "动作影院", "喜剧影院", "经典电影", "爱电影", "chc"]},
    {"name": "轮播频道",    "keywords": ["轮播频道", "轮播"]},
    {"name": "儿童频道",    "keywords": ["少儿", "动画", "卡通", "kids", "金鹰卡通", 
                                      "嘉佳卡通", "卡酷少儿", "动漫秀场", "优优宝贝"]},
]

# 播放列表分组输出顺序（使用上面定义的 name，不存在的分组自动跳过）
GROUP_ORDER = [
    "央视频道",
    "卫视频道",
    "电影频道",
    "4K专区",
    "儿童频道",
    "轮播频道",
]

# ------------------------ 播放列表生成设置 --------------------------------
# 每个频道名最多保留的链接数量（自动去重，取前 N 个）
MAX_LINKS_PER_CHANNEL = 10

# 输出文件名（可自定义）
OUTPUT_M3U_FILENAME = "iptv_channels.m3u"
OUTPUT_TXT_FILENAME = "iptv_channels.txt"

# -------------------------- 功能开关 -------------------------------------
# 是否启用汉字清洗（非央视频道）
ENABLE_CHINESE_CLEAN = True

# 是否启用去重（同一分组内频道名+链接完全一致则去重）
ENABLE_DEDUPLICATION = True

# 调试截图开关（CI 中建议关闭以节省时间）
ENABLE_SCREENSHOTS = False

# ============================================================================
# 以下为核心代码，非必要请勿修改（基于上方配置自动适配）
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

# ---------- 从配置动态生成分类函数 ----------
def build_classifier():
    """根据 CATEGORY_RULES 生成分类函数"""
    # 编译所有关键词为正则模式（不区分大小写）
    patterns = []
    for rule in CATEGORY_RULES:
        if not rule["keywords"]:
            continue
        # 将所有关键词用 | 连接，并整体作为正则
        pattern = "|".join(re.escape(kw.lower()) for kw in rule["keywords"])
        patterns.append((rule["name"], re.compile(pattern)))
    
    def classify(name: str) -> str | None:
        name_lower = name.lower()
        for group_name, pattern in patterns:
            if pattern.search(name_lower):
                return group_name
        return None
    return classify

classify_channel = build_classifier()

# ---------- 频道标准化（央视专用）----------
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

# ---------- 构建页面选择器（基于配置）----------
def build_selector(text_list: list, element_type: str = "button") -> str:
    """根据多个备选文本构建选择器"""
    if not text_list:
        return ""
    if len(text_list) == 1:
        return f"{element_type}:has-text('{text_list[0]}')"
    # 多个文本：用 text-matches 正则匹配
    pattern = "|".join(re.escape(t) for t in text_list)
    return f"{element_type}:text-matches('{pattern}')"

ENGINE_SELECTOR = build_selector(PAGE_CONFIG["engine_search"], "a.sidebar-link,button,div.segment-item")
MCAST_SELECTOR = build_selector(PAGE_CONFIG["multicast_tab"], "div.segment-item")
START_SELECTOR = build_selector(PAGE_CONFIG["start_button"], "button")

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

        # ----- 1. 点击「引擎搜索」（使用配置）-----
        if ENGINE_SELECTOR:
            element = page.locator(ENGINE_SELECTOR).first
            if await element.count() > 0:
                await element.click(timeout=10000)
                print(f"✅ 点击引擎搜索（配置：{PAGE_CONFIG['engine_search']}）")
            else:
                print("⚠️ 未找到引擎搜索按钮，继续后续步骤")
        await page.wait_for_timeout(1000)

        # ----- 2. 点击「组播提取」标签-----
        if MCAST_SELECTOR:
            mcast_tab = page.locator(MCAST_SELECTOR).first
            await mcast_tab.wait_for(state="attached", timeout=15000)
            await mcast_tab.click(timeout=10000)
            print(f"✅ 点击组播提取（配置：{PAGE_CONFIG['multicast_tab']}）")
        await page.wait_for_timeout(500)

        # ----- 3. 点击「开始播放」按钮-----
        if START_SELECTOR:
            start_btn = page.locator(START_SELECTOR).first
            if await start_btn.count() > 0:
                await start_btn.click(timeout=10000)
                print(f"✅ 点击开始按钮（配置：{PAGE_CONFIG['start_button']}）")
            else:
                if ENABLE_SCREENSHOTS:
                    await page.screenshot(path=SCREENSHOT_DIR / "02_start_button_missing.png")
                raise Exception("❌ 未找到开始按钮，请检查 PAGE_CONFIG['start_button'] 配置")
        else:
            raise Exception("❌ 开始按钮未配置")

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

        raw_entries = []  # (group, channel_name, url)

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
                # 分类（使用配置生成的分类器）
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
        channel_urls = defaultdict(list)
        seen_set = set() if ENABLE_DEDUPLICATION else None

        for group, name, url in raw_entries:
            if ENABLE_DEDUPLICATION:
                key = (group, name, url)
                if key in seen_set:
                    continue
                seen_set.add(key)

            channel_urls[(group, name)].append(url)

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
        # 央视频道按数字排序（分组名称必须包含“央视频道”）
        CCTV_GROUP = next((g for g in grouped.keys() if "央视" in g or "cctv" in g.lower()), None)
        if CCTV_GROUP:
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
            grouped[CCTV_GROUP].sort(key=cctv_sort_key)

        # 其他分组按频道名称排序
        for g in grouped:
            if g != CCTV_GROUP:
                grouped[g].sort(key=lambda x: x[0])

        # ----- 8. 生成播放列表（按 GROUP_ORDER 顺序）-----
        m3u_path = OUTPUT_DIR / OUTPUT_M3U_FILENAME
        with open(m3u_path, "w", encoding="utf-8") as f:
            f.write("#EXTM3U\n")
            for group_name in GROUP_ORDER:
                if group_name not in grouped:
                    continue
                for name, url in grouped[group_name]:
                    f.write(f'#EXTINF:-1 group-title="{group_name}",{name}\n')
                    f.write(f"{url}\n")
        print(f"📀 M3U: {m3u_path}")

        txt_path = OUTPUT_DIR / OUTPUT_TXT_FILENAME
        with open(txt_path, "w", encoding="utf-8") as f:
            for group_name in GROUP_ORDER:
                if group_name not in grouped:
                    continue
                f.write(f"{group_name},#genre#\n")
                for name, url in grouped[group_name]:
                    f.write(f"{name},{url}\n")
                f.write("\n")
        print(f"📄 TXT: {txt_path}")

        total_channels = sum(len(v) for v in grouped.values())
        print(f"\n🎉 完成！共输出 {total_channels} 个频道条目（每个频道名 ≤ {MAX_LINKS_PER_CHANNEL} 链接）")
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
