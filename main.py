#!/usr/bin/env python3
"""
IPTV 组播提取工具 - 【打开速度优先版】
优先：首包延迟（秒开） → 分辨率1080P+ → 下载速度
"""

import asyncio
import os
import re
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Set
from urllib.parse import urljoin

import aiohttp
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# 尝试导入 tqdm 进度条库，若失败则使用简单回退
try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False
    class tqdm:
        def __init__(self, *args, **kwargs):
            self.total = kwargs.get('total', 0)
            self.desc = kwargs.get('desc', '')
            self.unit = kwargs.get('unit', 'it')
            self.n = 0
        def update(self, n=1):
            self.n += n
        def close(self):
            print()
        def __enter__(self):
            return self
        def __exit__(self, *args):
            self.close()

# ============================================================================
# 全部配置区域（只改这里）
# ============================================================================

# ---------------------------- 基础设置 ------------------------------------
TARGET_URL = os.getenv("TARGET_URL", "https://iptv.809899.xyz")
OUTPUT_DIR = Path(__file__).parent
MAX_IPS = int(os.getenv("MAX_IPS", "15"))
HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
BROWSER_TYPE = os.getenv("BROWSER_TYPE", "chromium")

# ------------------------ 页面加载超时 ------------------------------------
PAGE_LOAD_TIMEOUT = int(os.getenv("PAGE_LOAD_TIMEOUT", "60000"))

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
    "央视频道", "卫视频道", "电影频道", "4K专区", "儿童频道", "轮播频道"
]

# ------------------------ 播放列表生成设置 --------------------------------
MAX_LINKS_PER_CHANNEL = int(os.getenv("MAX_LINKS_PER_CHANNEL", "5"))  # 只留最快5条
OUTPUT_M3U_FILENAME = os.getenv("OUTPUT_M3U", "iptv_fast_channels.m3u")
OUTPUT_TXT_FILENAME = os.getenv("OUTPUT_TXT", "iptv_fast_channels.txt")

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

# ====================== 【打开速度优先】核心配置 =======================
ENABLE_SPEED_TEST = True
SPEED_TEST_CONCURRENCY = 20
SPEED_TEST_TIMEOUT = 240

# 首包延迟（打开速度核心）
FIRST_PACKET_TIMEOUT = 0.8  # 超过0.8秒直接丢

# 速度门槛
MIN_SPEED_FACTOR = 2.0

# 分辨率必须1080P+
ENABLE_RESOLUTION_FILTER = True
MIN_RESOLUTION_WIDTH = 1920
MIN_RESOLUTION_HEIGHT = 1080
FALLBACK_TO_SPEED_WHEN_NO_RESOLUTION = False

# ====================== 负载控制 ======================
DELAY_BETWEEN_IPS = 2.0
DELAY_AFTER_CLICK = 0.5
MAX_CHANNELS_PER_IP = 0

# ====================== 脚本全局超时 ======================
SCRIPT_TIMEOUT = 3600

# ============================================================================
# 工具函数
# ============================================================================

IP_PATTERN = re.compile(r'^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$')
CCTV_PATTERN = re.compile(r'(cctv)[-\s]?(\d{1,3})', re.IGNORECASE)
CETV_PATTERN = re.compile(r'(cetv)[-\s]?(\d)', re.IGNORECASE)
RESOLUTION_PATTERN = re.compile(r'(\d+)x(\d+)')
CHINESE_ONLY_PATTERN = re.compile(r'[^\u4e00-\u9fff]')

SCREENSHOT_DIR = OUTPUT_DIR / "debug_screenshots"
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
        return True
    except Exception:
        try:
            await locator.evaluate('el => el.scrollIntoViewIfNeeded()')
            await locator.evaluate('el => el.click()')
            return True
        except Exception:
            return False

# ====================== 测速核心：优先首包延迟 =======================

async def fetch_url(session: aiohttp.ClientSession, url: str, timeout: int) -> Optional[bytes]:
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
            if resp.status == 200:
                return await resp.read()
    except Exception:
        pass
    return None

async def resolve_m3u8_playlist(session: aiohttp.ClientSession, url: str, timeout: int) -> Tuple[Optional[int], Optional[int], List[str]]:
    content = await fetch_url(session, url, timeout)
    if not content:
        return None, None, []
    lines = content.decode('utf-8', errors='ignore').splitlines()
    base_url = url[:url.rfind('/')+1] if '/' in url else url

    best_w, best_h = 0, 0
    best_uri = None
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if line.startswith('#EXT-X-STREAM-INF:'):
            res_match = re.search(r'RESOLUTION=(\d+)x(\d+)', line)
            w, h = 0, 0
            if res_match:
                w, h = int(res_match.group(1)), int(res_match.group(2))
            if i+1 < len(lines):
                uri = lines[i+1].strip()
                if w * h > best_w * best_h:
                    best_w, best_h = w, h
                    best_uri = uri
            i += 2
        else:
            i += 1

    if best_uri:
        next_url = urljoin(base_url, best_uri)
        return await resolve_m3u8_playlist(session, next_url, timeout)

    ts_urls = []
    for line in lines:
        line = line.strip()
        if line and not line.startswith('#'):
            ts_urls.append(urljoin(base_url, line))
    return best_w, best_h, ts_urls

async def test_speed_ts(url: str) -> Tuple[Optional[float], Optional[int], Optional[int]]:
    try:
        async with aiohttp.ClientSession() as session:
            width, height, ts_urls = await resolve_m3u8_playlist(session, url, 1)
            if not ts_urls:
                return None, None, None

            sample_urls = ts_urls[:2]
            total_bytes = 0
            total_time = 0.0
            for u in sample_urls:
                t0 = time.monotonic()
                data = await fetch_url(session, u, 1)
                used = time.monotonic() - t0
                if data:
                    total_bytes += len(data)
                    total_time += used
            if total_time <= 0 or total_bytes == 0:
                return None, None, None

            speed_mbps = (total_bytes / total_time) * 8 / 1_000_000
            return speed_mbps, width, height
    except Exception:
        return None, None, None

async def test_speed_fast(url: str, group: str, name: str, sem: asyncio.Semaphore) -> Optional[Tuple[str, str, str, float, float, bool]]:
    async with sem:
        try:
            # 先测首包
            t0 = time.monotonic()
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=2)) as session:
                async with session.head(url, allow_redirects=True):
                    pass
            latency = time.monotonic() - t0
            if latency > FIRST_PACKET_TIMEOUT:
                return None

            # 再测分辨率+速度
            is_m3u8 = url.lower().endswith(".m3u8")
            if not is_m3u8:
                return None

            speed, w, h = await test_speed_ts(url)
            if speed is None or speed < MIN_SPEED_FACTOR:
                return None

            res_ok = (w is not None and h is not None and
                      w >= MIN_RESOLUTION_WIDTH and h >= MIN_RESOLUTION_HEIGHT)
            if not res_ok:
                return None

            return (url, group, name, speed, latency, res_ok)
        except Exception:
            return None

async def run_speed_test(channel_urls: Dict[Tuple[str, str], List[str]]) -> Dict[Tuple[str, str], List[str]]:
    total = sum(len(v) for v in channel_urls.values())
    print(f"🚀 开始测速（优先打开速度），共 {total} 条链接")

    sem = asyncio.Semaphore(SPEED_TEST_CONCURRENCY)
    tasks = []
    for (g, n), urls in channel_urls.items():
        for u in urls:
            tasks.append(test_speed_fast(u, g, n, sem))

    results = []
    finished = 0
    printed = {10,20,30,40,50,60,70,80,90,100}
    progress_printed = set()

    for task in asyncio.as_completed(tasks):
        res = await task
        if res:
            results.append(res)
        finished += 1
        pct = int((finished / len(tasks)) * 100)
        for step in sorted(printed):
            if pct >= step and step not in progress_printed:
                print(f"测速进度：{step}%")
                progress_printed.add(step)

    speed_map = defaultdict(list)
    for r in results:
        url, g, n, speed, lat, ok = r
        speed_map[(g, n)].append((url, speed, lat))

    out = defaultdict(list)
    for key, items in speed_map.items():
        # 排序：先按延迟升序，再按速度降序
        items.sort(key=lambda x: (x[2], -x[1]))
        final = [u for u, s, lt in items[:MAX_LINKS_PER_CHANNEL]]
        out[key] = final

    print(f"✅ 测速完成，保留 {sum(len(v) for v in out.values())} 条优质快链接")
    return out

# ====================== 页面提取逻辑 ===============================

async def extract_from_ip(page, row, ip_text: str) -> List[Tuple[str, str, str]]:
    entries = []
    print(f"\n📌 处理 IP: {ip_text}")

    menu_btn = row.locator("button:has(i.fas.fa-list), button:has-text('≡')").first
    if await menu_btn.count() > 0:
        await robust_click(menu_btn, description="菜单")
    else:
        await row.locator("div.item-title").first.click(timeout=5000)
    await asyncio.sleep(DELAY_AFTER_CLICK)

    modal = page.locator(".modal-dialog").first
    try:
        await modal.wait_for(state="visible", timeout=8000)
    except PlaywrightTimeoutError:
        return entries

    items = modal.locator(".item-content")
    total = await items.count()
    limit = total if MAX_CHANNELS_PER_IP <= 0 else min(total, MAX_CHANNELS_PER_IP)

    for j in range(limit):
        item = items.nth(j)
        try:
            raw_name = await item.locator(".item-title").first.inner_text(timeout=3000)
            link = await item.locator(".item-subtitle").first.inner_text(timeout=3000)
        except:
            continue

        raw_name = raw_name.strip()
        link = link.strip()
        if not raw_name or not link:
            continue

        norm_name = normalize_cctv(raw_name)
        group = classify_channel(norm_name) or classify_channel(raw_name)
        if not group:
            continue

        final_name = norm_name if group == "央视频道" else (clean_chinese_only(raw_name) if ENABLE_CHINESE_CLEAN else raw_name)
        if not final_name:
            continue

        entries.append((group, final_name, link))
    return entries

async def wait_for_ip_elements(page):
    for attempt in range(2):
        print(f"⏳ 等待IP数据 {attempt+1}/2")
        await asyncio.sleep(30)
        try:
            ok = await page.wait_for_function("""
                () => {
                    const es = document.querySelectorAll('div.item-title');
                    for(let e of es) if (e.innerText.match(/\\d+\\.\\d+\\.\\d+\\.\\d+/)) return true;
                    return false;
                }
            """, timeout=5000)
            if ok:
                print("✅ IP 数据已加载")
                return True
        except Exception:
            continue
    print("⚠️ 继续执行")
    return False

# ====================== 主流程 ===============================

async def _main():
    print(f"[{time.strftime('%H:%M:%S')}] 🚀 启动【打开速度优先版】IPTV提取")

    async with async_playwright() as p:
        browser = await getattr(p, BROWSER_TYPE).launch(headless=HEADLESS, args=["--no-sandbox"])
        context = await browser.new_context(viewport={"width": 1920, "height": 1080})
        page = await context.new_page()

        await page.goto(TARGET_URL, timeout=PAGE_LOAD_TIMEOUT, wait_until="networkidle")
        print("✅ 页面加载完成")

        if ENGINE_SELECTOR:
            el = page.locator(ENGINE_SELECTOR).first
            if await el.count() > 0:
                await robust_click(el, description="引擎搜索")
                await asyncio.sleep(0.5)
                print("✅ 已点击引擎搜索")

        if MCAST_SELECTOR:
            tab = page.locator(MCAST_SELECTOR).first
            await tab.wait_for(state="attached", timeout=15000)
            await robust_click(tab, description="组播提取")
            await asyncio.sleep(0.5)
            print("✅ 已点击组播提取")

        if START_SELECTOR:
            btn = page.locator(START_SELECTOR).first
            await robust_click(btn, description="开始提取")
            await asyncio.sleep(0.5)
            print("✅ 已点击开始提取")

        await wait_for_ip_elements(page)

        rows = page.locator("div.ios-list-item").filter(has_text="频道:")
        total_ips = await rows.count()
        process_cnt = min(total_ips, MAX_IPS)
        print(f"📋 共 {total_ips} 个IP，处理前 {process_cnt} 个")

        raw = []
        for i in range(process_cnt):
            row = rows.nth(i)
            ip = await row.locator("div.item-title").first.inner_text()
            ip = ip.strip()
            if not IP_PATTERN.match(ip):
                print(f"⚠️ 跳过无效IP: {ip}")
                continue
            raw.extend(await extract_from_ip(page, row, ip))
            if i < process_cnt - 1:
                await asyncio.sleep(DELAY_BETWEEN_IPS)

        channel_map = defaultdict(list)
        seen = set()
        for g, n, u in raw:
            if ENABLE_DEDUPLICATION:
                key = (g, n, u)
                if key in seen:
                    continue
                seen.add(key)
            channel_map[(g, n)].append(u)

        print(f"📊 去重后：{len(channel_map)} 个频道，{sum(len(v) for v in channel_map.values())} 条链接")

        if ENABLE_SPEED_TEST and channel_map:
            channel_map = await run_speed_test(channel_map)

        final = []
        for (g, n), urls in channel_map.items():
            for u in urls:
                final.append((g, n, u))

        grouped = defaultdict(list)
        for g, n, u in final:
            grouped[g].append((n, u))

        cctv_g = next((g for g in grouped if "央视" in g), None)
        if cctv_g:
            def ckey(x):
                m = re.search(r"CCTV-(\d+)", x[0])
                return int(m.group(1)) if m else 999
            grouped[cctv_g].sort(key=ckey)

        with open(OUTPUT_DIR / OUTPUT_M3U_FILENAME, "w", encoding="utf-8") as f:
            f.write("#EXTM3U\n")
            for g in GROUP_ORDER:
                for n, u in grouped.get(g, []):
                    f.write(f'#EXTINF:-1 group-title="{g}",{n}\n{u}\n')

        with open(OUTPUT_DIR / OUTPUT_TXT_FILENAME, "w", encoding="utf-8") as f:
            for g in GROUP_ORDER:
                if g not in grouped:
                    continue
                f.write(f"{g},#genre#\n")
                for n, u in grouped.get(g, []):
                    f.write(f"{n},{u}\n")
                f.write("\n")

        print(f"\n🎉 全部完成！输出：")
        print(f"- {OUTPUT_M3U_FILENAME}")
        print(f"- {OUTPUT_TXT_FILENAME}")
        print(f"共 {len(final)} 条【秒开+1080P+】优质链接")

        await browser.close()

async def main_with_timeout():
    try:
        await asyncio.wait_for(_main(), timeout=SCRIPT_TIMEOUT)
    except asyncio.TimeoutError:
        print("❌ 脚本超时退出")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main_with_timeout())
