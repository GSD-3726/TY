#!/usr/bin/env python3
"""
IPTV 组播提取工具 - m3u8 分辨率+速度过滤排序增强版 (HTTP 测速，单位 Mbps)
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
# ======================== 【置顶：常用非频道配置】===========================
# ============================================================================

# 网站与运行
TARGET_URL              = "https://iptv.809899.xyz"    # 目标网站
HEADLESS                = True                         # 无头模式（服务器必开）
BROWSER_TYPE            = "chromium"                   # 浏览器类型
MAX_IPS                 = 20                           # 最多处理多少个IP
PAGE_LOAD_TIMEOUT       = 60000                        # 页面加载超时（毫秒）

# 输出文件
OUTPUT_M3U_FILENAME     = "iptv_channels.m3u"          # 输出m3u文件名
OUTPUT_TXT_FILENAME     = "iptv_channels.txt"          # 输出txt文件名

# 测速总开关
ENABLE_SPEED_TEST       = True                         # 是否启用测速

# 测速并发与超时
SPEED_TEST_CONCURRENCY  = 15                        # 同时测速数量
SPEED_TEST_TIMEOUT      = 2480                         # 测速整体超时（秒）
SPEED_TEST_VERBOSE      = False                        # 是否打印详细错误

# 测速参数
SPEED_TEST_DURATION     = 2                            # 非m3u8下载测速时长(秒)
TS_SAMPLE_COUNT         = 3                            # m3u8取几个ts片测速
TS_DOWNLOAD_TIMEOUT     = 2                            # 单个ts下载超时(秒)

# 速度过滤（Mbps）
ENABLE_SPEED_FACTOR_FILTER = True                     # 启用最低速度限制
MIN_SPEED_FACTOR        = 1.5                          # 最小速度要求

# 分辨率过滤
ENABLE_RESOLUTION_FILTER = True                        # 启用分辨率过滤
MIN_RESOLUTION_WIDTH    = 1920                         # 最小宽度
MIN_RESOLUTION_HEIGHT   = 1080                         # 最小高度
FALLBACK_TO_SPEED_WHEN_NO_RESOLUTION = True            # 无分辨率信息时是否保留

# 负载与等待
DELAY_BETWEEN_IPS       = 3.0                          # 处理两个IP之间的延迟
DELAY_AFTER_CLICK       = 0.5                          # 点击后等待时间
MAX_CHANNELS_PER_IP     = 0                            # 每个IP最多提取频道数(0=不限)

# 脚本全局超时
SCRIPT_TIMEOUT          = 3000                         # 脚本最大运行时间(秒)

# 功能开关
ENABLE_CHINESE_CLEAN    = True                         # 清理频道名非中文字符
ENABLE_DEDUPLICATION    = True                         # 链接去重
ENABLE_SCREENSHOTS      = False                        # 调试截图

# 央视名称映射开关
CCTV_USE_MAPPING        = True

# ============================================================================
# ============================ 频道分类配置（不动）===========================
# ============================================================================

OUTPUT_DIR = Path(__file__).parent

PAGE_CONFIG = {
    "engine_search": ["引索搜索", "引擎搜索", "关键词搜索"],
    "multicast_tab": ["组播提取"],
    "start_button": ["开始播放", "开始搜索", "开始提取"],
}

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

CCTV_NAME_MAPPING = {
    "1": "综合", "2": "财经", "3": "综艺", "4": "国际", "5": "体育",
    "5+": "体育赛事", "6": "电影", "7": "国防军事", "8": "电视剧",
    "9": "纪录", "10": "科教", "11": "戏曲", "12": "社会与法",
    "13": "新闻", "14": "少儿", "15": "音乐", "16": "奥林匹克",
    "17": "农业农村",
}

# ============================================================================
# 核心代码（以下全部不用改）
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

# ====================== 纯 HTTP 测速函数 =======================

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
            width, height, ts_urls = await resolve_m3u8_playlist(session, url, TS_DOWNLOAD_TIMEOUT)
            if not ts_urls:
                return None, None, None

            sample_urls = ts_urls[:TS_SAMPLE_COUNT]
            if not sample_urls:
                return None, None, None

            total_bytes = 0
            total_time = 0.0
            for u in sample_urls:
                start = time.monotonic()
                data = await fetch_url(session, u, TS_DOWNLOAD_TIMEOUT)
                elapsed = time.monotonic() - start
                if data and elapsed > 0:
                    total_bytes += len(data)
                    total_time += elapsed
            if total_time == 0 or total_bytes == 0:
                return None, None, None

            speed_mbps = (total_bytes / total_time) * 8 / 1_000_000
            return speed_mbps, width, height
    except Exception:
        return None, None, None

async def test_speed_direct(url: str, duration: int) -> Optional[float]:
    try:
        async with aiohttp.ClientSession() as session:
            start = time.monotonic()
            total_bytes = 0
            timeout = aiohttp.ClientTimeout(total=duration + 2)
            async with session.get(url, timeout=timeout) as resp:
                if resp.status != 200:
                    return None
                while True:
                    chunk = await resp.content.read(8192)
                    if not chunk:
                        break
                    total_bytes += len(chunk)
                    elapsed = time.monotonic() - start
                    if elapsed >= duration:
                        break
            elapsed = time.monotonic() - start
            if elapsed <= 0 or total_bytes == 0:
                return None
            speed_mbps = (total_bytes / elapsed) * 8 / 1_000_000
            return speed_mbps
    except Exception:
        return None

async def test_speed(url: str, group: str, name: str, semaphore: asyncio.Semaphore) -> Optional[Tuple[str, str, str, float, bool]]:
    async with semaphore:
        is_m3u8 = url.lower().endswith(".m3u8") or "m3u8" in url.lower()

        if is_m3u8:
            speed_mbps, width, height = await test_speed_ts(url)
            if speed_mbps is None:
                return None
            if ENABLE_SPEED_FACTOR_FILTER and speed_mbps < MIN_SPEED_FACTOR:
                return None
            resolution_ok = True
            if ENABLE_RESOLUTION_FILTER:
                if width is None or height is None:
                    resolution_ok = FALLBACK_TO_SPEED_WHEN_NO_RESOLUTION
                else:
                    resolution_ok = (width >= MIN_RESOLUTION_WIDTH and height >= MIN_RESOLUTION_HEIGHT)
            return (url, group, name, speed_mbps, resolution_ok)
        else:
            speed_mbps = await test_speed_direct(url, SPEED_TEST_DURATION)
            if speed_mbps is None:
                return None
            if ENABLE_SPEED_FACTOR_FILTER and speed_mbps < MIN_SPEED_FACTOR:
                return None
            resolution_ok = FALLBACK_TO_SPEED_WHEN_NO_RESOLUTION if ENABLE_RESOLUTION_FILTER else True
            return (url, group, name, speed_mbps, resolution_ok)

# ====================== 【简化：只输出 10% 20% 30%...100%】=======================
async def run_speed_test(channel_urls: Dict[Tuple[str, str], List[str]]) -> Dict[Tuple[str, str], List[str]]:
    total = sum(len(v) for v in channel_urls.values())
    print(f"🚀 开始测速，共 {total} 条链接")

    sem = asyncio.Semaphore(SPEED_TEST_CONCURRENCY)
    tasks = []
    for (g, n), urls in channel_urls.items():
        for u in urls:
            tasks.append(test_speed(u, g, n, sem))

    results = []
    finished = 0
    printed = set()

    for task in asyncio.as_completed(tasks):
        res = await task
        if res:
            results.append(res)
        finished += 1

        pct = (finished / len(tasks)) * 100
        for step in [10,20,30,40,50,60,70,80,90,100]:
            if pct >= step and step not in printed:
                print(f"测速进度：{step}%")
                printed.add(step)

    speed_map = defaultdict(list)
    for r in results:
        url, g, n, s, ok_res = r
        speed_map[(g, n)].append((url, s, ok_res))

    out = defaultdict(list)
    for key, items in speed_map.items():
        items.sort(key=lambda x: x[1], reverse=True)
        qualified = [u for u, s, ok in items if ok]
        if qualified:
            final = qualified[:MAX_LINKS_PER_CHANNEL]
        else:
            final = [u for u, s, ok in items][:MAX_LINKS_PER_CHANNEL]
        out[key] = final

    print(f"✅ 测速完成，保留 {sum(len(v) for v in out.values())} 条有效链接")
    return out

# ====================== IP 提取逻辑 ===============================

async def extract_from_ip(page, row, ip_text: str) -> List[Tuple[str, str, str]]:
    entries = []
    print(f"\n📌 处理 IP: {ip_text}")

    menu_btn = row.locator("button:has(i.fas.fa-list), button:has-text('≡'), button:has(i.fa-list)").first
    if await menu_btn.count() > 0:
        await robust_click(menu_btn, description="菜单按钮")
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
            raw_name = await item.locator(".item-title").first.inner_text(timeout=5000)
            link = await item.locator(".item-subtitle").first.inner_text(timeout=5000)
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

# ====================== 等待 IP：点击后等30秒，没数据再等30秒 ======================
async def wait_for_ip_elements(page, max_retries=2):
    for attempt in range(2):
        print(f"⏳ 第 {attempt+1} 次等待：30 秒后获取数据")
        await asyncio.sleep(30)
        
        try:
            ok = await page.wait_for_function("""
                () => {
                    const elements = document.querySelectorAll('div.item-title');
                    for (let el of elements) {
                        if (el.innerText.match(/\\d+\\.\\d+\\.\\d+\\.\\d+/)) return true;
                    }
                    return false;
                }
            """, timeout=5000)
            if ok:
                print("✅ IP 数据已加载")
                return True
        except Exception:
            print(f"⚠️ 第 {attempt+1} 次未获取到数据")
    print("❌ 两次等待后仍无数据，继续执行")
    return False

# ====================== 主流程 ===============================

async def _main():
    global ENABLE_SPEED_TEST
    print(f"[{time.strftime('%H:%M:%S')}] 🚀 脚本开始")

    try:
        import aiohttp
    except ImportError:
        print("❌ 请安装 aiohttp: pip install aiohttp")
        sys.exit(1)

    async with async_playwright() as p:
        browser = await getattr(p, BROWSER_TYPE).launch(headless=HEADLESS, args=["--no-sandbox"])
        context = await browser.new_context(viewport={"width": 1920, "height": 1080})
        page = await context.new_page()

        await page.goto(TARGET_URL, timeout=PAGE_LOAD_TIMEOUT, wait_until="networkidle")
        print("✅ 页面加载完成")

        if ENGINE_SELECTOR:
            elem = page.locator(ENGINE_SELECTOR).first
            if await elem.count() > 0:
                await robust_click(elem, description="引擎搜索")
                await asyncio.sleep(DELAY_AFTER_CLICK)
                print("✅ 点击引擎搜索")

        if MCAST_SELECTOR:
            tab = page.locator(MCAST_SELECTOR).first
            await tab.wait_for(state="attached", timeout=15000)
            await robust_click(tab, description="组播提取")
            await asyncio.sleep(DELAY_AFTER_CLICK)
            print("✅ 点击组播提取")

        if START_SELECTOR:
            btn = page.locator(START_SELECTOR).first
            await robust_click(btn, description="开始提取")
            await asyncio.sleep(DELAY_AFTER_CLICK)
            print("✅ 点击开始提取")

        print("⏳ 等待数据加载（30s + 30s）...")
        await wait_for_ip_elements(page)

        rows = page.locator("div.ios-list-item").filter(has_text="频道:")
        total_ips = await rows.count()
        process_cnt = min(total_ips, MAX_IPS) if MAX_IPS else total_ips
        print(f"📋 共 {total_ips} 个IP，处理前 {process_cnt} 个")

        raw = []
        for i in range(process_cnt):
            row = rows.nth(i)
            ip = await row.locator("div.item-title").first.inner_text()
            ip = ip.strip()
            if not IP_PATTERN.match(ip):
                print(f"⚠️ 跳过无效 IP: {ip}")
                continue
            raw.extend(await extract_from_ip(page, row, ip))
            if i < process_cnt - 1:
                await asyncio.sleep(DELAY_BETWEEN_IPS)

        channel_map = defaultdict(list)
        seen = set()
        for g, n, u in raw:
            if ENABLE_DEDUPLICATION:
                k = (g, n, u)
                if k in seen:
                    continue
                seen.add(k)
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

        print(f"\n🎉 完成！共导出 {len(final)} 条有效链接")
        await browser.close()

async def main_with_timeout():
    try:
        await asyncio.wait_for(_main(), timeout=SCRIPT_TIMEOUT)
    except asyncio.TimeoutError:
        print("❌ 脚本整体超时退出")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main_with_timeout())
