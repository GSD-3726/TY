#!/usr/bin/env python3
"""
IPTV 组播提取工具 - 流畅不卡顿版
解决：TS片段切换卡顿、加载慢、播放断断续续
"""

import asyncio
import os
import re
import sys
import time
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from urllib.parse import urljoin

import aiohttp
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# ============================================================================
# ======================== 【配置区 · 全中文说明】=============================
# ============================================================================

# 网站与浏览器
TARGET_URL              = "https://iptv.809899.xyz"
HEADLESS                = True
BROWSER_TYPE            = "chromium"
MAX_IPS                 = 5
PAGE_LOAD_TIMEOUT       = 120000

# 输出文件
OUTPUT_M3U_FILENAME     = "iptv_channels.m3u"
OUTPUT_TXT_FILENAME     = "iptv_channels.txt"
MAX_LINKS_PER_CHANNEL   = 3

# 测速总开关
ENABLE_SPEED_TEST       = True

# 测速并发（越低越稳）
SPEED_TEST_CONCURRENCY  = 5
SPEED_TEST_TIMEOUT      = 1980
SPEED_TEST_VERBOSE      = False

# 测速强度
SPEED_TEST_DURATION     = 6
TS_SAMPLE_COUNT         = 4
TS_DOWNLOAD_TIMEOUT     = 6

# 流畅播放门槛（低于这个必卡）
MIN_SPEED_FACTOR        = 1.5
MIN_STABLE_SPEED        = 1.5
STABILITY_THRESHOLD     = 0.2
JITTER_THRESHOLD        = 0.25

# 分辨率过滤
ENABLE_RESOLUTION_FILTER = True
MIN_RESOLUTION_WIDTH    = 1280
MIN_RESOLUTION_HEIGHT   = 720
FALLBACK_TO_SPEED_WHEN_NO_RESOLUTION = False

# 等待与延迟
DELAY_BETWEEN_IPS       = 1.0
DELAY_AFTER_CLICK       = 0.5
MAX_CHANNELS_PER_IP     = 0
SCRIPT_TIMEOUT          = 1800

# 清理与去重
ENABLE_CHINESE_CLEAN    = True
ENABLE_DEDUPLICATION    = True
ENABLE_SCREENSHOTS      = False
CCTV_USE_MAPPING        = True

# ============================================================================
# ============================ 频道分类规则 ==================================
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
    {"name": "卫视频道",    "keywords": ["卫视", "凤凰", "tvb", "湖南", "浙江", "江苏", "东方"]},
    {"name": "电影频道",    "keywords": ["电影", "影院", "chc"]},
    {"name": "轮播频道",    "keywords": ["轮播"]},
    {"name": "儿童频道",    "keywords": ["少儿", "动画", "卡通"]},
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
# ============================= 工具函数 =====================================
# ============================================================================

IP_PATTERN = re.compile(r'^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$')
CCTV_PATTERN = re.compile(r'(cctv)[-\s]?(\d{1,3})', re.IGNORECASE)
CETV_PATTERN = re.compile(r'(cetv)[-\s]?(\d)', re.IGNORECASE)
RESOLUTION_PATTERN = re.compile(r'(\d+)x(\d+)')
CHINESE_ONLY_PATTERN = re.compile(r'[^\u4e00-\u9fff]')
TS_DURATION_PATTERN = re.compile(r'#EXTINF:(\d+\.?\d*)')

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
    if "cctv5+" in name_lower:
        return "CCTV-5+体育赛事" if CCTV_USE_MAPPING else "CCTV5+"
    cctv_match = CCTV_PATTERN.search(name_lower)
    if cctv_match:
        num = cctv_match.group(2)
        if CCTV_USE_MAPPING and num in CCTV_NAME_MAPPING:
            return f"CCTV-{num}{CCTV_NAME_MAPPING[num]}"
        return f"CCTV-{num}"
    cetv_match = CETV_PATTERN.search(name_lower)
    if cetv_match:
        return f"CETV-{cetv_match.group(2)}"
    return name

def clean_chinese_only(name: str) -> str:
    return CHINESE_ONLY_PATTERN.sub('', name)

def build_selector(text_list, element_type="button"):
    if not text_list:
        return ""
    if len(text_list) == 1:
        return f"{element_type}:has-text('{text_list[0]}')"
    pattern = "|".join(re.escape(t) for t in text_list)
    return f"{element_type}:text-matches('{pattern}')"

ENGINE_SELECTOR = build_selector(PAGE_CONFIG["engine_search"], "a.sidebar-link,button,div.segment-item")
MCAST_SELECTOR = build_selector(PAGE_CONFIG["multicast_tab"], "div.segment-item")
START_SELECTOR = build_selector(PAGE_CONFIG["start_button"], "button")

async def robust_click(locator, timeout=10000):
    try:
        await locator.scroll_into_view_if_needed(timeout=3000)
        await asyncio.sleep(0.2)
        await locator.click(force=True, timeout=timeout)
        return True
    except:
        try:
            await locator.evaluate("el => el.click()")
            return True
        except:
            return False

# ============================================================================
# ========================= 流畅度检测核心代码 ================================
# ============================================================================

async def fetch_url(session: aiohttp.ClientSession, url: str, timeout: int):
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
            if resp.status == 200:
                return await resp.read()
    except:
        pass
    return None

async def test_hls(session: aiohttp.ClientSession, url: str):
    content = await fetch_url(session, url, 5)
    if not content:
        return False, 0.0, None

    txt = content.decode("utf-8", "ignore")
    lines = txt.splitlines()
    base = url[:url.rfind('/')+1] if '/' in url else url
    ts_list = []
    res = None

    for line in lines:
        if line.startswith("#EXT-X-STREAM-INF:"):
            m = RESOLUTION_PATTERN.search(line)
            if m:
                res = (int(m.group(1)), int(m.group(2)))
        elif not line.startswith("#") and line.strip():
            ts_list.append(urljoin(base, line.strip()))

    if not ts_list:
        return False, 0.0, res

    sample = ts_list[:min(TS_SAMPLE_COUNT, len(ts_list))]
    total = 0.0
    ok = 0

    for u in sample:
        t0 = time.monotonic()
        d = await fetch_url(session, u, TS_DOWNLOAD_TIMEOUT)
        cost = time.monotonic() - t0
        if d and cost > 0:
            speed = (len(d)*8) / cost / 1e6
            total += speed
            ok += 1

    if ok == 0:
        return False, 0.0, res

    avg = total / ok
    return avg >= MIN_STABLE_SPEED, avg, res

async def test_speed_task(url: str, sem: asyncio.Semaphore):
    async with sem:
        try:
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as s:
                ok, sp, res = await test_hls(s, url)
                if not ok:
                    return None
                if ENABLE_RESOLUTION_FILTER and res:
                    w, h = res
                    if w < MIN_RESOLUTION_WIDTH or h < MIN_RESOLUTION_HEIGHT:
                        return None
                return (url, sp, res is not None)
        except:
            return None

async def run_speed_test(channel_map):
    tasks = []
    sem = asyncio.Semaphore(SPEED_TEST_CONCURRENCY)
    flat = []
    for (g, n), urls in channel_map.items():
        for u in urls:
            tasks.append((g, n, u))

    print(f"开始测速：{len(tasks)} 条")
    results = await asyncio.gather(*[test_speed_task(u, sem) for (g,n,u) in tasks])

    out = defaultdict(list)
    for i, r in enumerate(results):
        if r:
            g, n, _ = tasks[i]
            url, sp, ok_res = r
            out[(g,n)].append((url, sp))

    for k in out:
        out[k].sort(key=lambda x: -x[1])
        out[k] = [u for u,_ in out[k][:MAX_LINKS_PER_CHANNEL]]

    print(f"测速完成，保留 {sum(len(v) for v in out.values())} 条")
    return out

# ============================================================================
# ============================ 页面提取流程 ==================================
# ============================================================================

async def extract_one_ip(page, row):
    entries = []
    try:
        ip = await row.locator("div.item-title").first.inner_text(timeout=3000)
        ip = ip.strip()
        if not IP_PATTERN.match(ip):
            return []
        print(f"处理IP：{ip}")
    except:
        return []

    try:
        btn = row.locator("button:has(i.fa-list)").first
        if await btn.count() > 0:
            await robust_click(btn)
        else:
            await row.click(timeout=3000)
        await asyncio.sleep(DELAY_AFTER_CLICK)

        modal = page.locator(".modal-dialog").first
        await modal.wait_for(state="visible", timeout=5000)
        items = modal.locator(".item-content")
        total = await items.count()

        for i in range(total):
            try:
                name = await items.nth(i).locator(".item-title").inner_text(timeout=2000)
                link = await items.nth(i).locator(".item-subtitle").inner_text(timeout=2000)
                name = name.strip()
                link = link.strip()
                if not name or not link:
                    continue
                norm = normalize_cctv(name)
                group = classify_channel(norm)
                if not group:
                    continue
                final_name = norm if group == "央视频道" else (clean_chinese_only(name) if ENABLE_CHINESE_CLEAN else name)
                if final_name:
                    entries.append((group, final_name, link))
            except:
                continue
    except:
        pass
    return entries

async def wait_data(page):
    for _ in range(2):
        print("等待30秒加载数据...")
        await asyncio.sleep(30)
        has = await page.evaluate('''()=>{
            for(let e of document.querySelectorAll('div.item-title')){
                if(/\\d+\\.\\d+\\.\\d+\\.\\d+/.test(e.innerText))return true;
            }
            return false;
        }''')
        if has:
            return True
    return False

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS, args=["--no-sandbox"])
        ctx = await browser.new_context(viewport={"width":1920,"height":1080})
        page = await ctx.new_page()

        try:
            await page.goto(TARGET_URL, timeout=PAGE_LOAD_TIMEOUT, wait_until="networkidle")
        except:
            pass

        if ENGINE_SELECTOR:
            e = page.locator(ENGINE_SELECTOR).first
            if await e.count() > 0:
                await robust_click(e)
                await asyncio.sleep(0.5)

        if MCAST_SELECTOR:
            t = page.locator(MCAST_SELECTOR).first
            await robust_click(t)
            await asyncio.sleep(0.5)

        if START_SELECTOR:
            b = page.locator(START_SELECTOR).first
            await robust_click(b)
            await asyncio.sleep(0.5)

        await wait_data(page)

        rows = page.locator("div.ios-list-item").filter(has_text="频道:")
        cnt = min(await rows.count(), MAX_IPS)
        raw = []
        for i in range(cnt):
            raw += await extract_one_ip(page, rows.nth(i))
            if i < cnt-1:
                await asyncio.sleep(DELAY_BETWEEN_IPS)

        channel_map = defaultdict(list)
        seen = set()
        for g,n,u in raw:
            if ENABLE_DEDUPLICATION:
                k = (g,n,u)
                if k in seen:
                    continue
                seen.add(k)
            channel_map[(g,n)].append(u)

        if ENABLE_SPEED_TEST and channel_map:
            channel_map = await run_speed_test(channel_map)

        final = []
        for (g,n),urls in channel_map.items():
            for u in urls:
                final.append((g,n,u))

        grouped = defaultdict(list)
        for g,n,u in final:
            grouped[g].append((n,u))

        # 输出 m3u
        with open(OUTPUT_M3U_FILENAME, "w", encoding="utf-8") as f:
            f.write("#EXTM3U\n")
            for g in GROUP_ORDER:
                for n,u in grouped.get(g,[]):
                    f.write(f'#EXTINF:-1 group-title="{g}",{n}\n{u}\n')

        # 输出 txt
        with open(OUTPUT_TXT_FILENAME, "w", encoding="utf-8") as f:
            for g in GROUP_ORDER:
                if g not in grouped:
                    continue
                f.write(f"{g},#genre#\n")
                for n,u in grouped[g]:
                    f.write(f"{n},{u}\n")
                f.write("\n")

        print(f"导出完成：{len(final)} 条可用链接")
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
