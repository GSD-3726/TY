#!/usr/bin/env python3
"""
IPTV 组播提取工具 —— 全配置置顶版（修复 ffmpeg 日志级别导致测速失败）
"""

# ==================== 必须的导入 ====================
import asyncio
import os
import re
import subprocess
import shutil
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Set

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# ============================================================================
# 全部配置区域（按需修改）
# ============================================================================

# ---------------------------- 基础设置 ------------------------------------
TARGET_URL = os.getenv("TARGET_URL", "https://iptv.809899.xyz")          # 目标网页
OUTPUT_DIR = Path(__file__).parent                                        # 输出目录
MAX_IPS = int(os.getenv("MAX_IPS", "1"))                                  # 只处理前 N 个 IP（0=全部）
HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"                # 无头模式（CI 必须为 True）
BROWSER_TYPE = os.getenv("BROWSER_TYPE", "chromium")                      # 可选 chromium / firefox / webkit

# ------------------------ 页面加载超时 ------------------------------------
PAGE_LOAD_TIMEOUT = int(os.getenv("PAGE_LOAD_TIMEOUT", "60000"))          # 页面加载超时（毫秒）

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

GROUP_ORDER = [
    "央视频道",
    "卫视频道",
    "电影频道",
    "4K专区",
    "儿童频道",
    "轮播频道",
]

# ------------------------ 播放列表生成设置 --------------------------------
MAX_LINKS_PER_CHANNEL = int(os.getenv("MAX_LINKS_PER_CHANNEL", "10"))
OUTPUT_M3U_FILENAME = os.getenv("OUTPUT_M3U", "iptv_channels.m3u")
OUTPUT_TXT_FILENAME = os.getenv("OUTPUT_TXT", "iptv_channels.txt")

# -------------------------- 功能开关 -------------------------------------
ENABLE_CHINESE_CLEAN = True
ENABLE_DEDUPLICATION = True
ENABLE_SCREENSHOTS = False

# -------------------------- 央视频道名称映射 -----------------------------
CCTV_USE_MAPPING = True
CCTV_NAME_MAPPING = {
    "1": "综合", "2": "财经", "3": "综艺", "4": "国际", "5": "体育",
    "5+": "体育赛事", "6": "电影", "7": "国防军事", "8": "电视剧",
    "9": "纪录", "10": "科教", "11": "戏曲", "12": "社会与法",
    "13": "新闻", "14": "少儿", "15": "音乐", "16": "奥林匹克",
    "17": "农业农村",
}

# -------------------------- 测速设置 --------------------------------------
ENABLE_SPEED_TEST = os.getenv("ENABLE_SPEED_TEST", "true").lower() == "true"
SPEED_TEST_CONCURRENCY = int(os.getenv("SPEED_TEST_CONCURRENCY", "10"))   # 并发测速数
SPEED_TEST_DURATION = int(os.getenv("SPEED_TEST_DURATION", "2"))          # 每个链接测速时长（秒）
SPEED_TEST_TIMEOUT = int(os.getenv("SPEED_TEST_TIMEOUT", "480"))          # 测速总超时（秒）
SPEED_TEST_VERBOSE = False

# -------------------------- 速度倍数过滤 ----------------------------------
ENABLE_SPEED_FACTOR_FILTER = True          # 是否启用速度倍数过滤
MIN_SPEED_FACTOR = 0.5                      # 最低速度倍数（低于此值丢弃）

# -------------------------- 分辨率筛选设置 --------------------------------
ENABLE_RESOLUTION_FILTER = True
MIN_RESOLUTION_WIDTH = 1280
MIN_RESOLUTION_HEIGHT = 720

# -------------------------- 负载控制 --------------------------------------
DELAY_BETWEEN_IPS = float(os.getenv("DELAY_BETWEEN_IPS", "3.0"))
DELAY_AFTER_CLICK = float(os.getenv("DELAY_AFTER_CLICK", "0.5"))
MAX_CHANNELS_PER_IP = int(os.getenv("MAX_CHANNELS_PER_IP", "0"))

# -------------------------- 脚本全局超时（30分钟）------------------------
SCRIPT_TIMEOUT = int(os.getenv("SCRIPT_TIMEOUT", "4800"))

# ============================================================================
# 以下为核心代码，非必要请勿修改
# ============================================================================

# 预编译正则表达式
IP_PATTERN = re.compile(r'^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$')
CCTV_PATTERN = re.compile(r'(cctv)[-\s]?(\d{1,3})', re.IGNORECASE)
CETV_PATTERN = re.compile(r'(cetv)[-\s]?(\d)', re.IGNORECASE)
SPEED_PATTERN = re.compile(r'speed=\s*([\d.]+)x')
RESOLUTION_PATTERN = re.compile(r'(\d+)x(\d+)')
CHINESE_ONLY_PATTERN = re.compile(r'[^\u4e00-\u9fff]')

SCREENSHOT_DIR = OUTPUT_DIR / "debug_screenshots"
if ENABLE_SCREENSHOTS:
    SCREENSHOT_DIR.mkdir(exist_ok=True)

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
    if "cctv5+" in name_lower or "cctv5＋" in name_lower or "cctv5加" in name_lower:
        if CCTV_USE_MAPPING and "5+" in CCTV_NAME_MAPPING:
            return f"CCTV-5+{CCTV_NAME_MAPPING['5+']}"
        return "CCTV5+"
    cctv_match = CCTV_PATTERN.search(name_lower)
    if cctv_match:
        number = cctv_match.group(2)
        if CCTV_USE_MAPPING:
            suffix = CCTV_NAME_MAPPING.get(number, "")
            return f"CCTV-{number}{suffix}"
        rest = name[cctv_match.end():].strip()
        rest = re.sub(r'(?i)(HD|SD|高清|标清|超清|\s*-?\s*)?$', '', rest).strip()
        return f"CCTV-{number} {rest}".strip() if rest else f"CCTV-{number}"
    cetv_match = CETV_PATTERN.search(name_lower)
    if cetv_match:
        number = cetv_match.group(2)
        return f"CETV-{number}" if CCTV_USE_MAPPING else f"CETV{number}"
    return name

def clean_chinese_only(name: str) -> str:
    return CHINESE_ONLY_PATTERN.sub('', name)

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

# ====================== 【已优化】精准真实测速函数 ======================
async def test_speed(url: str, group: str, name: str, semaphore: asyncio.Semaphore) -> Optional[Tuple[str, str, str, float]]:
    """单个链接测速，返回 (url, group, name, speed) 或 None（失败或速度低于阈值）"""
    async with semaphore:
        if SPEED_TEST_VERBOSE:
            print(f"   ⏳ 测速: [{group}] {name[:30]}...")

        cmd = [
            'ffmpeg',
            '-i', url,
            '-t', str(SPEED_TEST_DURATION),
            '-f', 'null',
            '-',
            '-loglevel', 'warning',
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
            if process.returncode is None:
                try:
                    process.kill()
                except ProcessLookupError:
                    pass
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

        # 真实速度 KB/s
        speed_kb = 0.0
        size_match = re.search(r'total size:\s*(\d+)', stderr_text.lower())
        if size_match:
            size_bytes = int(size_match.group(1))
            cost = max(SPEED_TEST_DURATION, 0.5)
            speed_kb = size_bytes / cost / 1024

        # 兜底 speed=x
        speed_x = None
        for line in reversed(lines):
            match = SPEED_PATTERN.search(line)
            if match:
                speed_x = float(match.group(1))
                break

        # 最终速度
        if speed_kb > 0:
            speed = speed_kb
        else:
            speed = (speed_x or 0) * 100

        if speed is None or speed <= 0:
            if SPEED_TEST_VERBOSE:
                print(f"   ❌ [{group}] {name[:30]} 无法解析速度")
            return None

        # 速度过滤
        if ENABLE_SPEED_FACTOR_FILTER and speed < 50:
            if SPEED_TEST_VERBOSE:
                print(f"   ❌ [{group}] {name[:30]} 速度过低 {speed:.0f} KB/s，丢弃")
            return None

        # 分辨率过滤
        if ENABLE_RESOLUTION_FILTER:
            width = height = None
            for line in lines:
                if 'Video:' in line:
                    match = RESOLUTION_PATTERN.search(line)
                    if match:
                        width, height = int(match.group(1)), int(match.group(2))
                        break
            if width is None or height is None or width < MIN_RESOLUTION_WIDTH or height < MIN_RESOLUTION_HEIGHT:
                if SPEED_TEST_VERBOSE:
                    res = f"{width}x{height}" if width else "未知"
                    print(f"   ❌ [{group}] {name[:30]} 分辨率 {res} 不符合要求")
                return None

        if SPEED_TEST_VERBOSE:
            print(f"   ✅ [{group}] {name[:30]} 速度: {speed:.0f} KB/s")

        return (url, group, name, speed)

async def run_speed_test(channel_urls: Dict[Tuple[str, str], List[str]]) -> Dict[Tuple[str, str], List[str]]:
    total_links = sum(len(v) for v in channel_urls.values())
    filter_info = []
    if ENABLE_RESOLUTION_FILTER:
        filter_info.append(f"分辨率≥{MIN_RESOLUTION_WIDTH}x{MIN_RESOLUTION_HEIGHT}")
    if ENABLE_SPEED_FACTOR_FILTER:
        filter_info.append(f"速度≥50KB/s")
    filter_str = "，".join(filter_info)
    print(f"🚀 开始测速（并发 {SPEED_TEST_CONCURRENCY}，时长 {SPEED_TEST_DURATION}s，{filter_str}，共 {total_links} 个链接）...")

    semaphore = asyncio.Semaphore(SPEED_TEST_CONCURRENCY)
    tasks = []
    for (group, name), urls in channel_urls.items():
        for url in urls:
            tasks.append(test_speed(url, group, name, semaphore))

    results: List[Optional[Tuple]] = []
    completed = 0
    next_progress = 10
    start_time = time.monotonic()

    pending = {asyncio.create_task(t) for t in tasks}
    while pending:
        done, pending = await asyncio.wait(pending, timeout=5.0, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            res = task.result()
            results.append(res)
            completed += 1
            percent = completed * 100 // total_links
            if percent >= next_progress:
                print(f"   📊 测速进度: {completed}/{total_links} ({percent}%)")
                next_progress += 10

        elapsed = time.monotonic() - start_time
        if elapsed > SPEED_TEST_TIMEOUT:
            print(f"⚠️ 测速整体超时（{SPEED_TEST_TIMEOUT}秒），强制结束，已测 {completed}/{total_links} 个链接")
            for task in pending:
                task.cancel()
            break

    speed_map = defaultdict(list)
    for res in results:
        if res is None:
            continue
        url, group, name, speed = res
        speed_map[(group, name)].append((url, speed))

    new_channel_urls = defaultdict(list)
    for key, items in speed_map.items():
        items.sort(key=lambda x: x[1], reverse=True)
        kept = items[:MAX_LINKS_PER_CHANNEL] if MAX_LINKS_PER_CHANNEL > 0 else items
        for url, _ in kept:
            new_channel_urls[key].append(url)

    print(f"✅ 测速完成，剩余 {sum(len(v) for v in new_channel_urls.values())} 个链接")
    return new_channel_urls

# ====================== IP 提取逻辑 ================================
async def extract_from_ip(page, row, ip_text: str) -> List[Tuple[str, str, str]]:
    entries = []
    print(f"\n📌 处理 IP: {ip_text}")

    menu_btn = row.locator("button:has(i.fas.fa-list), button:has-text('≡'), button:has(i.fa-list)").first
    if await menu_btn.count() > 0:
        await robust_click(menu_btn, description="菜单按钮")
    else:
        print("   ⚠️ 未找到菜单按钮，尝试点击IP地址")
        await row.locator("div.item-title").first.click(timeout=5000)
    await asyncio.sleep(DELAY_AFTER_CLICK)

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
                if await modal.is_visible():
                    await page.keyboard.press("Escape")
                return entries
        else:
            print("   ❌ 无法打开模态框，跳过")
            if await modal.is_visible():
                await page.keyboard.press("Escape")
            return entries

    items = modal.locator(".item-content")
    total_channels = await items.count()
    extract_limit = total_channels if MAX_CHANNELS_PER_IP <= 0 else min(total_channels, MAX_CHANNELS_PER_IP)
    print(f"   📺 共 {total_channels} 个频道，本次提取前 {extract_limit} 个")

    for j in range(extract_limit):
        item = items.nth(j)
        try:
            raw_name = await item.locator(".item-title").first.inner_text(timeout=5000)
            link = await item.locator(".item-subtitle").first.inner_text(timeout=5000)
        except Exception as e:
            print(f"      ⚠️ 第 {j+1} 个频道获取失败: {e}")
            continue
        raw_name = raw_name.strip()
        link = link.strip()
        if not raw_name or not link:
            continue

        norm_name = normalize_cctv(raw_name)
        group = classify_channel(norm_name) or classify_channel(raw_name)
        if not group:
            continue

        if group == "央视频道":
            final_name = norm_name
        elif ENABLE_CHINESE_CLEAN:
            final_name = clean_chinese_only(raw_name)
        else:
            final_name = raw_name

        if not final_name:
            continue

        entries.append((group, final_name, link))
        if j < 3 or extract_limit <= 5:
            print(f"      {j+1}. {final_name} -> {link[:60]}...")

    if await modal.is_visible():
        await page.keyboard.press("Escape")
        await asyncio.sleep(0.2)

    return entries

# ====================== 主流程 ================================
async def _main():
    global ENABLE_SPEED_TEST

    print(f"[{time.strftime('%H:%M:%S')}] 🚀 脚本开始运行")

    # 检查浏览器
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
    else:
        print(f"✅ {BROWSER_TYPE} 浏览器驱动已就绪")

    # 检查 ffmpeg
    if ENABLE_SPEED_TEST and shutil.which('ffmpeg') is None:
        print("⚠️ 系统中未找到 ffmpeg，测速功能已自动禁用。")
        ENABLE_SPEED_TEST = False

    print(f"[{time.strftime('%H:%M:%S')}] 启动 Playwright {BROWSER_TYPE} 浏览器...")
    async with async_playwright() as p:
        browser = await getattr(p, BROWSER_TYPE).launch(headless=HEADLESS, args=["--no-sandbox"])
        context = await browser.new_context(
            accept_downloads=True,
            viewport={"width": 1920, "height": 1080}
        )
        page = await context.new_page()
        print("✅ 浏览器启动完成")

        print(f"🌐 正在打开页面: {TARGET_URL}")
        await page.goto(TARGET_URL, timeout=PAGE_LOAD_TIMEOUT, wait_until="networkidle")
        print("✅ 页面加载完成")

        if ENABLE_SCREENSHOTS:
            await page.screenshot(path=SCREENSHOT_DIR / "01_initial.png")

        if ENGINE_SELECTOR:
            elem = page.locator(ENGINE_SELECTOR).first
            if await elem.count() > 0:
                await robust_click(elem, description="引擎搜索按钮")
                await asyncio.sleep(DELAY_AFTER_CLICK)

        if MCAST_SELECTOR:
            tab = page.locator(MCAST_SELECTOR).first
            await tab.wait_for(state="attached", timeout=15000)
            await robust_click(tab, description="组播提取标签")
            await asyncio.sleep(DELAY_AFTER_CLICK)

        if START_SELECTOR:
            start_btn = page.locator(START_SELECTOR).first
            if await start_btn.count() > 0:
                await robust_click(start_btn, description="开始按钮")
                await asyncio.sleep(DELAY_AFTER_CLICK)
            else:
                raise Exception("❌ 未找到开始按钮，请检查配置")

        print("⏳ 等待扫描结果（最多60秒）...")
        ip_locator = page.locator("div.item-title:text-matches('\\d+\\.\\d+\\.\\d+\\.\\d+')").first
        try:
            await ip_locator.wait_for(state="attached", timeout=60000)
            print("✅ 扫描完成")
        except PlaywrightTimeoutError:
            print("⚠️ 扫描超时，但可能已有历史结果")

        if ENABLE_SCREENSHOTS:
            await page.screenshot(path=SCREENSHOT_DIR / "04_results_page.png")

        result_rows = page.locator("div.ios-list-item").filter(has_text="频道:")
        total_ips = await result_rows.count()
        process_count = total_ips if MAX_IPS <= 0 else min(total_ips, MAX_IPS)
        print(f"📋 共 {total_ips} 个IP，本次处理前 {process_count} 个")

        raw_entries = []
        for i in range(process_count):
            row = result_rows.nth(i)
            ip_text = await row.locator("div.item-title").first.inner_text()
            ip_text = ip_text.strip()

            if not IP_PATTERN.match(ip_text):
                print(f"\n📌 [{i+1}/{process_count}] {ip_text} (非IP，跳过)")
                continue

            entries = await extract_from_ip(page, row, ip_text)
            raw_entries.extend(entries)

            if i < process_count - 1:
                print(f"⏳ 等待 {DELAY_BETWEEN_IPS} 秒后处理下一个 IP...")
                await asyncio.sleep(DELAY_BETWEEN_IPS)

        print(f"\n📊 原始条目数：{len(raw_entries)}")

        # 去重
        channel_urls = defaultdict(list)
        seen: Set[Tuple] = set() if ENABLE_DEDUPLICATION else None
        for group, name, url in raw_entries:
            if ENABLE_DEDUPLICATION:
                key = (group, name, url)
                if key in seen:
                    continue
                seen.add(key)
            channel_urls[(group, name)].append(url)

        # 测速
        if ENABLE_SPEED_TEST and channel_urls:
            channel_urls = await run_speed_test(channel_urls)
        else:
            # 直接截取
            new_urls = defaultdict(list)
            for key, urls in channel_urls.items():
                for url in (urls[:MAX_LINKS_PER_CHANNEL] if MAX_LINKS_PER_CHANNEL > 0 else urls):
                    new_urls[key].append(url)
            channel_urls = new_urls

        final_entries = []
        for (group, name), urls in channel_urls.items():
            for url in urls:
                final_entries.append((group, name, url))

        print(f"✅ 每个频道最多保留 {MAX_LINKS_PER_CHANNEL} 个链接，剩余 {len(final_entries)} 条")

        # 分组排序
        grouped = defaultdict(list)
        for group, name, url in final_entries:
            grouped[group].append((name, url))

        # 央视排序
        cctv_group = next((g for g in grouped if "央视" in g or "cctv" in g.lower()), None)
        if cctv_group:
            def cctv_key(item):
                name = item[0]
                m = re.search(r'CCTV-?(\d+)(?:\+|)', name, re.IGNORECASE)
                if m:
                    num = int(m.group(1))
                    return (num, 1 if '5+' in name else 0)
                m = re.search(r'CETV-?(\d+)', name, re.IGNORECASE)
                if m:
                    return (int(m.group(1)) + 100, 0)
                return (999, 0)
            grouped[cctv_group].sort(key=cctv_key)

        for g in grouped:
            if g != cctv_group:
                grouped[g].sort(key=lambda x: x[0])

        # 输出 M3U
        m3u_path = OUTPUT_DIR / OUTPUT_M3U_FILENAME
        with open(m3u_path, "w", encoding="utf-8", newline="") as f:
            f.write("#EXTM3U\n")
            for group_name in GROUP_ORDER:
                for name, url in grouped.get(group_name, []):
                    f.write(f'#EXTINF:-1 group-title="{group_name}",{name}\n{url}\n')
        print(f"📀 M3U: {m3u_path}")

        # 输出 TXT
        txt_path = OUTPUT_DIR / OUTPUT_TXT_FILENAME
        with open(txt_path, "w", encoding="utf-8", newline="") as f:
            for group_name in GROUP_ORDER:
                if group_name not in grouped:
                    continue
                f.write(f"{group_name},#genre#\n")
                for name, url in grouped.get(group_name, []):
                    f.write(f"{name},{url}\n")
                f.write("\n")
        print(f"📄 TXT: {txt_path}")

        total_channels = sum(len(v) for v in grouped.values())
        print(f"\n🎉 完成！共输出 {total_channels} 个频道条目")

        await browser.close()

async def main_with_timeout():
    try:
        await asyncio.wait_for(_main(), timeout=SCRIPT_TIMEOUT)
    except asyncio.TimeoutError:
        print(f"❌ 脚本运行超时（{SCRIPT_TIMEOUT}秒），强制退出")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main_with_timeout())
