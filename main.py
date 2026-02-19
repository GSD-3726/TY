#!/usr/bin/env python3
"""
IPTV 组播提取工具 —— 全配置自动化版（GitHub Actions 优化 + 负载控制 + 央视名称统一映射）
所有配置项均在文件顶部集中管理，修改配置即可适配任何网站或命名习惯。
优化版：更稳、边界更安全、无逻辑变更
"""

import asyncio
import re
import subprocess
import sys
import shutil
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
BROWSER_TYPE = "chromium"                      # 可选 chromium / firefox / webkit

# ------------------------ 页面交互配置 ------------------------------------
PAGE_CONFIG = {
    "engine_search": ["引索搜索", "引擎搜索", "关键词搜索"],
    "multicast_tab": ["组播提取"],
    "start_button": ["开始播放", "开始搜索", "开始提取"],
}

# ------------------------ 分类规则配置 ------------------------------------
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

# 播放列表分组输出顺序
GROUP_ORDER = [
    "央视频道",
    "卫视频道",
    "电影频道",
    "4K专区",
    "儿童频道",
    "轮播频道",
]

# ------------------------ 播放列表生成设置 --------------------------------
MAX_LINKS_PER_CHANNEL = 10                     # 每个频道名最多保留链接数
OUTPUT_M3U_FILENAME = "iptv_channels.m3u"
OUTPUT_TXT_FILENAME = "iptv_channels.txt"

# -------------------------- 功能开关 -------------------------------------
ENABLE_CHINESE_CLEAN = True                   # 非央视频道清洗为纯汉字
ENABLE_DEDUPLICATION = True                  # 链接去重
ENABLE_SCREENSHOTS = False                   # 调试截图（CI 建议关闭）

# -------------------------- 央视频道名称映射（⚠️ 核心配置）----------------
CCTV_USE_MAPPING = True                      # 是否启用映射（True=使用下方映射表，False=保留原始名称）
CCTV_NAME_MAPPING = {
    "1": "综合",
    "2": "财经",
    "3": "综艺",
    "4": "国际",
    "5": "体育",
    "5+": "体育赛事",
    "6": "电影",
    "7": "国防军事",
    "8": "电视剧",
    "9": "纪录",
    "10": "科教",
    "11": "戏曲",
    "12": "社会与法",
    "13": "新闻",
    "14": "少儿",
    "15": "音乐",
    "16": "奥林匹克",
    "17": "农业农村",
}

# -------------------------- 测速设置（GitHub Actions 优化版）----------------
ENABLE_SPEED_TEST = True                      # 是否启用 ffmpeg 测速
SPEED_TEST_CONCURRENCY = 5                    # 并发测速数（可调）
SPEED_TEST_DURATION = 1                       # 每个链接测速时长（秒）
KEEP_ON_SPEED_FAIL = False                     # 测速失败时是否保留链接（False=丢弃）
SPEED_TEST_VERBOSE = False                     # 是否输出每个链接的详细日志（默认关闭）

# -------------------------- 分辨率筛选设置（新增）--------------------------
ENABLE_RESOLUTION_FILTER = True                # 是否启用分辨率筛选
MIN_RESOLUTION_WIDTH = 1920                     # 最小宽度
MIN_RESOLUTION_HEIGHT = 1080                    # 最小高度

# -------------------------- 负载控制（减轻服务器压力）----------------------
DELAY_BETWEEN_IPS = 3.0                      # 处理完一个 IP 后等待秒数
DELAY_AFTER_CLICK = 0.5                       # 每次点击后等待秒数
MAX_CHANNELS_PER_IP = 0                        # 每个 IP 最多提取频道数（0=不限制）

# ============================================================================
# 以下为核心代码，非必要请勿修改
# ============================================================================

SCREENSHOT_DIR = OUTPUT_DIR / "debug_screenshots"
if ENABLE_SCREENSHOTS:
    SCREENSHOT_DIR.mkdir(exist_ok=True)

LAUNCH_ARGS = {
    "headless": HEADLESS,
    "args": ["--no-sandbox"]
}

def ensure_browser_installed():
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

# ====================== 优化：分类器预编译 ======================
def build_classifier():
    compiled = []
    for rule in CATEGORY_RULES:
        if not rule["keywords"]:
            continue
        pattern = re.compile("|".join(re.escape(kw.lower()) for kw in rule["keywords"]))
        compiled.append((rule["name"], pattern))

    def classify(name: str) -> str | None:
        name_lower = name.lower()
        for group_name, pat in compiled:
            if pat.search(name_lower):
                return group_name
        return None
    return classify

classify_channel = build_classifier()

# ---------- 央视名称标准化（增强版，支持映射表）----------
def normalize_cctv(name: str) -> str:
    name_lower = name.lower()

    if "cctv5+" in name_lower or "cctv5＋" in name_lower or "cctv5加" in name_lower:
        if CCTV_USE_MAPPING and "5+" in CCTV_NAME_MAPPING:
            return f"CCTV-5+{CCTV_NAME_MAPPING['5+']}"
        else:
            return "CCTV5+"

    cctv_match = re.search(r'(cctv)[-\s]?(\d{1,3})', name_lower)
    if cctv_match:
        number = cctv_match.group(2)
        if CCTV_USE_MAPPING:
            if number in CCTV_NAME_MAPPING:
                return f"CCTV-{number}{CCTV_NAME_MAPPING[number]}"
            else:
                return f"CCTV-{number}"
        else:
            rest = name[cctv_match.end():].strip()
            redundant = re.sub(r'(?i)(HD|SD|高清|标清|超清|\s*-?\s*)?$', '', rest).strip()
            if redundant:
                if '-' in name[cctv_match.start():cctv_match.end()]:
                    return f"CCTV-{number} {redundant}"
                else:
                    return f"CCTV{number} {redundant}"
            else:
                if '-' in name[cctv_match.start():cctv_match.end()]:
                    return f"CCTV-{number}"
                else:
                    return f"CCTV{number}"

    cetv_match = re.search(r'(cetv)[-\s]?(\d)', name_lower)
    if cetv_match:
        prefix = cetv_match.group(1).upper()
        number = cetv_match.group(2)
        if CCTV_USE_MAPPING:
            return f"CETV-{number}"
        else:
            if '-' in name[cetv_match.start():cetv_match.end()]:
                return f"CETV-{number}"
            else:
                return f"CETV{number}"

    return name

def clean_chinese_only(name: str) -> str:
    """只保留汉字字符"""
    return re.sub(r'[^\u4e00-\u9fff]', '', name)

# ---------- 构建页面选择器 ----------
def build_selector(text_list: list, element_type: str = "button") -> str:
    if not text_list:
        return ""
    if len(text_list) == 1:
        return f"{element_type}:has-text('{text_list[0]}')"
    pattern = "|".join(re.escape(t) for t in text_list)
    return f"{element_type}:text-matches('{pattern}')"

ENGINE_SELECTOR = build_selector(PAGE_CONFIG["engine_search"], "a.sidebar-link,button,div.segment-item")
MCAST_SELECTOR = build_selector(PAGE_CONFIG["multicast_tab"], "div.segment-item")
START_SELECTOR = build_selector(PAGE_CONFIG["start_button"], "button")

# ---------- 增强点击函数 ----------
async def robust_click(locator, timeout=10000, description="元素"):
    try:
        await locator.scroll_into_view_if_needed(timeout=5000)
        await asyncio.sleep(0.2)
        await locator.click(force=True, timeout=timeout)
        print(f"✅ {description} 点击成功（强制点击）")
        return True
    except Exception as e:
        print(f"⚠️ {description} 强制点击失败: {e}")
        try:
            await locator.evaluate('el => el.scrollIntoViewIfNeeded()')
            await locator.evaluate('el => el.click()')
            print(f"✅ {description} 点击成功（JavaScript 回退）")
            return True
        except Exception as e2:
            print(f"❌ {description} 所有点击方式均失败: {e2}")
            return False

# ---------- 测速函数（支持分辨率解析）----------
async def test_speed(url: str, group: str, name: str, semaphore: asyncio.Semaphore):
    async with semaphore:
        if SPEED_TEST_VERBOSE:
            print(f"   ⏳ 测速: [{group}] {name[:30]}...")
        cmd = [
            'ffmpeg',
            '-i', url,
            '-t', str(SPEED_TEST_DURATION),
            '-f', 'null',
            '-',
            '-loglevel', 'error',
            '-stats'
        ]
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=SPEED_TEST_DURATION + 5)
        except asyncio.TimeoutError:
            process.kill()
            await process.wait()
            if SPEED_TEST_VERBOSE:
                print(f"   ❌ [{group}] {name[:30]} 超时")
            return None
        if process.returncode != 0:
            if SPEED_TEST_VERBOSE:
                print(f"   ❌ [{group}] {name[:30]} 失败 (ffmpeg 返回码 {process.returncode})")
            return None

        stderr_text = stderr.decode('utf-8', errors='ignore')
        lines = stderr_text.splitlines()

        speed = None
        for line in reversed(lines):
            match = re.search(r'speed=\s*([\d.]+)x', line)
            if match:
                speed = float(match.group(1))
                break
        if speed is None:
            if SPEED_TEST_VERBOSE:
                print(f"   ❌ [{group}] {name[:30]} 无法解析速度")
            return None

        width = height = None
        if ENABLE_RESOLUTION_FILTER:
            for line in lines:
                if 'Video:' in line:
                    res_match = re.search(r'(\d+)x(\d+)', line)
                    if res_match:
                        width = int(res_match.group(1))
                        height = int(res_match.group(2))
                        break
            if width is None or height is None:
                if SPEED_TEST_VERBOSE:
                    print(f"   ❌ [{group}] {name[:30]} 无法获取分辨率，丢弃")
                return None
            if width < MIN_RESOLUTION_WIDTH or height < MIN_RESOLUTION_HEIGHT:
                if SPEED_TEST_VERBOSE:
                    print(f"   ❌ [{group}] {name[:30]} 分辨率 {width}x{height} 低于要求，丢弃")
                return None

        if SPEED_TEST_VERBOSE:
            res_str = f"{width}x{height}" if width else "未知"
            print(f"   ✅ [{group}] {name[:30]} 速度: {speed:.2f}x, 分辨率: {res_str}")
        return (url, group, name, speed)

# ---------- 主流程 ----------
async def main():
    global ENABLE_SPEED_TEST
    ensure_browser_installed()

    if ENABLE_SPEED_TEST:
        if shutil.which('ffmpeg') is None:
            print("⚠️ 系统中未找到 ffmpeg，测速功能已自动禁用。")
            ENABLE_SPEED_TEST = False

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

        # 1. 点击引擎搜索
        if ENGINE_SELECTOR:
            element = page.locator(ENGINE_SELECTOR).first
            if await element.count() > 0:
                await robust_click(element, description="引擎搜索按钮")
                await asyncio.sleep(DELAY_AFTER_CLICK)
            else:
                print("⚠️ 未找到引擎搜索按钮，继续后续步骤")
        await page.wait_for_timeout(1000)

        # 2. 点击组播提取标签
        if MCAST_SELECTOR:
            mcast_tab = page.locator(MCAST_SELECTOR).first
            await mcast_tab.wait_for(state="attached", timeout=15000)
            await robust_click(mcast_tab, description="组播提取标签")
            await asyncio.sleep(DELAY_AFTER_CLICK)
        await page.wait_for_timeout(500)

        # 3. 点击开始按钮
        if START_SELECTOR:
            start_btn = page.locator(START_SELECTOR).first
            if await start_btn.count() > 0:
                await robust_click(start_btn, description="开始按钮")
                await asyncio.sleep(DELAY_AFTER_CLICK)
            else:
                if ENABLE_SCREENSHOTS:
                    await page.screenshot(path=SCREENSHOT_DIR / "02_start_button_missing.png")
                raise Exception("❌ 未找到开始按钮，请检查 PAGE_CONFIG['start_button'] 配置")
        else:
            raise Exception("❌ 开始按钮未配置")

        # 4. 等待扫描结果
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

        # 5. 获取IP列表
        result_rows = page.locator("div.ios-list-item").filter(has_text="频道:")
        total = await result_rows.count()
        process_count = total if MAX_IPS <= 0 else min(total, MAX_IPS)
        print(f"📋 共 {total} 个IP，本次处理前 {process_count} 个")

        raw_entries = []

        for i in range(process_count):
            row = result_rows.nth(i)
            ip_text = await row.locator("div.item-title").first.inner_text()
            ip_text = ip_text.strip()

            # ====================== 优化：标准 IPv4 正则 ======================
            if not re.match(r'^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$', ip_text):
                print(f"\n📌 [{i+1}/{process_count}] {ip_text} (非IP，跳过)")
                continue

            print(f"\n📌 [{i+1}/{process_count}] {ip_text}")

            # 点击菜单
            menu_btn = row.locator("button:has(i.fas.fa-list), button:has-text('≡'), button:has(i.fa-list)").first
            if await menu_btn.count() > 0:
                await robust_click(menu_btn, description="菜单按钮")
                await asyncio.sleep(DELAY_AFTER_CLICK)
            else:
                print("   ⚠️ 未找到菜单按钮，尝试点击IP地址")
                await row.locator("div.item-title").first.click(timeout=5000)
                await asyncio.sleep(DELAY_AFTER_CLICK)

            # 等待模态框
            modal = page.locator(".modal-dialog").first
            try:
                await modal.wait_for(state="visible", timeout=8000)
                print("   ✅ 模态框已打开")
            except PlaywrightTimeoutError:
                subtitle = row.locator("div.item-subtitle:has-text('频道:')").first
                if await subtitle.count() > 0:
                    print("   ⚠️ 模态框未出现，尝试点击频道文本")
                    await subtitle.click(timeout=5000)
                    await asyncio.sleep(DELAY_AFTER_CLICK)
                    try:
                        await modal.wait_for(state="visible", timeout=5000)
                        print("   ✅ 模态框已打开")
                    except PlaywrightTimeoutError:
                        print("   ❌ 模态框仍未出现，跳过此IP")
                        # ====================== 优化：安全关闭弹窗 ======================
                        if await modal.is_visible():
                            await page.keyboard.press("Escape")
                            await asyncio.sleep(0.2)
                        continue
                else:
                    print("   ❌ 无法打开模态框，跳过")
                    if await modal.is_visible():
                        await page.keyboard.press("Escape")
                        await asyncio.sleep(0.2)
                    continue

            # 提取频道
            items = modal.locator(".item-content")
            total_channels_in_modal = await items.count()
            extract_limit = total_channels_in_modal
            if MAX_CHANNELS_PER_IP > 0:
                extract_limit = min(total_channels_in_modal, MAX_CHANNELS_PER_IP)
            print(f"   📺 共 {total_channels_in_modal} 个频道，本次提取前 {extract_limit} 个")

            for j in range(extract_limit):
                item = items.nth(j)
                try:
                    raw_name = await item.locator(".item-title").first.inner_text(timeout=5000)
                    link = await item.locator(".item-subtitle").first.inner_text(timeout=5000)
                except Exception as e:
                    print(f"      ⚠️ 第 {j+1} 个频道获取失败（可能未渲染），跳过: {e}")
                    continue
                raw_name = raw_name.strip()
                link = link.strip()
                if not raw_name or not link:
                    continue

                norm_name = normalize_cctv(raw_name)
                group = classify_channel(norm_name) or classify_channel(raw_name)
                if not group:
                    continue

                # 名称处理
                if group == "央视频道":
                    final_name = norm_name
                elif ENABLE_CHINESE_CLEAN:
                    final_name = clean_chinese_only(raw_name)
                else:
                    final_name = raw_name

                # ====================== 优化：空名称直接跳过 ======================
                if not final_name:
                    continue

                raw_entries.append((group, final_name, link))

                if j < 3 or extract_limit <= 5:
                    print(f"      {j+1}. {final_name} -> {link[:60]}...")

            # 关闭模态框（安全版）
            if await modal.is_visible():
                await page.keyboard.press("Escape")
                await asyncio.sleep(0.2)

            if i < process_count - 1:
                print(f"⏳ 等待 {DELAY_BETWEEN_IPS} 秒后处理下一个 IP...")
                await asyncio.sleep(DELAY_BETWEEN_IPS)

        print(f"\n📊 原始条目数：{len(raw_entries)}")

        # 去重
        channel_urls = defaultdict(list)
        seen_set = set() if ENABLE_DEDUPLICATION else None

        for group, name, url in raw_entries:
            if ENABLE_DEDUPLICATION:
                key = (group, name, url)
                if key in seen_set:
                    continue
                seen_set.add(key)
            channel_urls[(group, name)].append(url)

        # 测速
        if ENABLE_SPEED_TEST:
            total_links = sum(len(v) for v in channel_urls.values())
            filter_info = ""
            if ENABLE_RESOLUTION_FILTER:
                filter_info = f"，分辨率≥{MIN_RESOLUTION_WIDTH}x{MIN_RESOLUTION_HEIGHT}"
            print(f"🚀 开始测速（并发 {SPEED_TEST_CONCURRENCY}，时长 {SPEED_TEST_DURATION}s{filter_info}，共 {total_links} 个链接）...")
            semaphore = asyncio.Semaphore(SPEED_TEST_CONCURRENCY)
            tasks = []
            for (group, name), urls in channel_urls.items():
                for url in urls:
                    tasks.append(test_speed(url, group, name, semaphore))

            # ====================== 优化：10% 一档更平滑 ======================
            completed = 0
            next_progress = 10
            results = []
            for coro in asyncio.as_completed(tasks):
                res = await coro
                results.append(res)
                completed += 1
                percent = completed * 100 // total_links
                if percent >= next_progress:
                    print(f"   📊 测速进度: {completed}/{total_links} ({percent}%)")
                    next_progress += 10

            speed_map = defaultdict(list)
            for res in results:
                if res is None:
                    continue
                url, group, name, speed = res
                speed_map[(group, name)].append((url, speed))

            new_channel_urls = defaultdict(list)
            for (group, name), items in speed_map.items():
                items.sort(key=lambda x: x[1], reverse=True)
                kept = items[:MAX_LINKS_PER_CHANNEL] if MAX_LINKS_PER_CHANNEL > 0 else items
                for url, speed in kept:
                    new_channel_urls[(group, name)].append(url)
            channel_urls = new_channel_urls
            print(f"✅ 测速完成，剩余 {sum(len(v) for v in channel_urls.values())} 个链接")
        else:
            new_channel_urls = defaultdict(list)
            for (group, name), urls in channel_urls.items():
                for url in urls[:MAX_LINKS_PER_CHANNEL] if MAX_LINKS_PER_CHANNEL > 0 else urls:
                    new_channel_urls[(group, name)].append(url)
            channel_urls = new_channel_urls

        limited_entries = []
        for (group, name), urls in channel_urls.items():
            for url in urls:
                limited_entries.append((group, name, url))

        print(f"✅ 每个频道最多保留 {MAX_LINKS_PER_CHANNEL} 个链接，剩余 {len(limited_entries)} 条")

        # 分组
        grouped = defaultdict(list)
        for group, name, url in limited_entries:
            grouped[group].append((name, url))

        # 央视排序
        CCTV_GROUP = next((g for g in grouped.keys() if "央视" in g or "cctv" in g.lower()), None)
        if CCTV_GROUP:
            def cctv_sort_key(item):
                name = item[0]
                m = re.search(r'CCTV-?(\d+)(?:\+|)', name, re.IGNORECASE)
                if m:
                    num = int(m.group(1))
                    if '5+' in name:
                        return (num, 1)
                    return (num, 0)
                m = re.search(r'CETV-?(\d+)', name, re.IGNORECASE)
                if m:
                    return (int(m.group(1)) + 100, 0)
                return (999, 0)
            grouped[CCTV_GROUP].sort(key=cctv_sort_key)

        # 其他排序
        for g in grouped:
            if g != CCTV_GROUP:
                grouped[g].sort(key=lambda x: x[0])

        # 输出文件（换行统一）
        m3u_path = OUTPUT_DIR / OUTPUT_M3U_FILENAME
        with open(m3u_path, "w", encoding="utf-8", newline="") as f:
            f.write("#EXTM3U\n")
            for group_name in GROUP_ORDER:
                if group_name not in grouped:
                    continue
                for name, url in grouped[group_name]:
                    f.write(f'#EXTINF:-1 group-title="{group_name}",{name}\n')
                    f.write(f"{url}\n")
        print(f"📀 M3U: {m3u_path}")

        txt_path = OUTPUT_DIR / OUTPUT_TXT_FILENAME
        with open(txt_path, "w", encoding="utf-8", newline="") as f:
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
