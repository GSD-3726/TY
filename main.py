#!/usr/bin/env python3
"""
IPTV 组播提取工具 - 3TS分片精准测速版（业内最准）
"""

import asyncio
import os
import re
import shutil
import sys
import time
import aiohttp
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Set
from urllib.parse import urljoin

from playwright.async_api import async_playwright
from playwright.async_api import TimeoutError as PlaywrightTimeoutError

# ============================================================================
# 全部配置区域（只改这里）
# ============================================================================

# ---------------------------- 基础设置 ------------------------------------
TARGET_URL = os.getenv("TARGET_URL", "https://iptv.809899.xyz")
OUTPUT_DIR = Path(__file__).parent
MAX_IPS = int(os.getenv("MAX_IPS", "5"))
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

# -------------------------- 3TS 测速核心配置（最准）-------------------------
ENABLE_SPEED_TEST = os.getenv("ENABLE_SPEED_TEST", "true").lower() == "true"
SPEED_TEST_CONCURRENCY = int(os.getenv("SPEED_TEST_CONCURRENCY", "10"))
SPEED_TEST_TIMEOUT = int(os.getenv("SPEED_TEST_TIMEOUT", "480"))
TS_TEST_COUNT = 3                                  # 测速分片数量（固定3最准）
MIN_SPEED_Mbps = 0.8                               # 最低合格速度 Mbps
ENABLE_MIN_SPEED_FILTER = True

# -------------------------- 分辨率筛选（m3u8）-------------------------------
ENABLE_RESOLUTION_FILTER = True
MIN_RESOLUTION_WIDTH = 1280
MIN_RESOLUTION_HEIGHT = 720

# 无高清时仍按速度排序
FALLBACK_TO_SPEED_WHEN_NO_RESOLUTION = True

# -------------------------- 负载控制 --------------------------------------
DELAY_BETWEEN_IPS = float(os.getenv("DELAY_BETWEEN_IPS", "3.0"))
DELAY_AFTER_CLICK = float(os.getenv("DELAY_AFTER_CLICK", "0.5"))
MAX_CHANNELS_PER_IP = int(os.getenv("MAX_CHANNELS_PER_IP", "0"))

# -------------------------- 脚本全局超时 ----------------------------------
SCRIPT_TIMEOUT = int(os.getenv("SCRIPT_TIMEOUT", "1800"))

# ============================================================================
# 以下为核心代码，非必要请勿修改
# ============================================================================

IP_PATTERN = re.compile(r'^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$')
CCTV_PATTERN = re.compile(r'(cctv)[-\s]?(\d{1,3})', re.IGNORECASE)
CETV_PATTERN = re.compile(r'(cetv)[-\s]?(\d)', re.IGNORECASE)
RESOLUTION_PATTERN = re.compile(r'(\d+)x(\d+)')
CHINESE_ONLY_PATTERN = re.compile(r'[^\u4e00-\u9fff]')

SCREENSHOT_DIR = OUTPUT_DIR / "debug_screenshots"
if ENABLE_SCREENSHOTS:
    SCREENSHOT_DIR.mkdir(exist_ok=True)

# ------------------------------ 工具函数 ------------------------------
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
    m = CCTV_PATTERN.search(name_lower)
    if m:
        num = m.group(2)
        suf = CCTV_NAME_MAPPING.get(num, "") if CCTV_USE_MAPPING else ""
        return f"CCTV-{num}{suf}"
    m = CETV_PATTERN.search(name_lower)
    if m:
        return f"CETV-{m.group(2)}"
    return name

def clean_chinese_only(s):
    return CHINESE_ONLY_PATTERN.sub('', s)

def build_selector(text_list, et="button"):
    if not text_list:
        return ""
    if len(text_list) == 1:
        return f"{et}:has-text('{text_list[0]}')"
    p = "|".join(re.escape(t) for t in text_list)
    return f"{et}:text-matches('{p}')"

ENGINE_SELECTOR   = build_selector(PAGE_CONFIG["engine_search"], "a.sidebar-link,button,div.segment-item")
MCAST_SELECTOR    = build_selector(PAGE_CONFIG["multicast_tab"], "div.segment-item")
START_SELECTOR    = build_selector(PAGE_CONFIG["start_button"], "button")

async def robust_click(loc, timeout=10000, desc=""):
    try:
        await loc.scroll_into_view_if_needed(timeout=5000)
        await asyncio.sleep(0.2)
        await loc.click(force=True, timeout=timeout)
        return True
    except:
        try:
            await loc.evaluate("el => el.click()")
            return True
        except:
            return False

# ------------------------------ 3TS 精准测速核心 ------------------------------
async def fetch(session, url):
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
            if resp.status not in (200, 206):
                return None
            return await resp.read()
    except:
        return None

def parse_m3u8(base_url, m3u8_text):
    ts_list = []
    for line in m3u8_text.splitlines():
        line = line.strip()
        if not line: continue
        if line.startswith("#"): continue
        if "." not in line: continue
        ts_url = urljoin(base_url, line)
        ts_list.append(ts_url)
    return ts_list

async def test_source_3ts(url: str, sem: asyncio.Semaphore):
    async with sem:
        if not url.lower().endswith("m3u8") and "m3u8" not in url.lower():
            return None, 0, False

        try:
            async with aiohttp.ClientSession() as session:
                # 1. 拉 m3u8
                resp = await session.get(url, timeout=aiohttp.ClientTimeout(total=10))
                if resp.status != 200:
                    return None, 0, False
                body = await resp.text()
                ts_list = parse_m3u8(url, body)
                if len(ts_list) < TS_TEST_COUNT:
                    return None, 0, False
                ts_list = ts_list[:TS_TEST_COUNT]

                # 2. 测速分辨率
                start = time.time()
                total_size = 0
                ok = 0
                for u in ts_list:
                    data = await fetch(session, u)
                    if data is None: continue
                    total_size += len(data)
                    ok += 1
                if ok < 2:
                    return None, 0, False

                cost = time.time() - start
                if cost <= 0:
                    return None, 0, False

                # 3. 计算Mbps
                speed_bps = (total_size * 8) / cost
                speed_mbps = speed_bps / 1e6

                # 4. 分辨率（简单判断URL含1080/720/4k）
                res_ok = True
                if ENABLE_RESOLUTION_FILTER:
                    ul = url.lower()
                    if "1080" in ul or "4k" in ul or "2160" in ul:
                        res_ok = True
                    elif "720" in ul:
                        res_ok = MIN_RESOLUTION_WIDTH <= 1280
                    else:
                        res_ok = False

                return url, speed_mbps, res_ok

        except Exception as e:
            return None, 0, False

# ------------------------------ 测速调度 ------------------------------
async def run_speed_test(channel_urls: Dict[tuple, List[str]]):
    total = sum(len(v) for v in channel_urls.values())
    print(f"🚀 3TS精准测速，共 {total} 条")

    sem = asyncio.Semaphore(SPEED_TEST_CONCURRENCY)
    tasks = []
    for (g, n), urls in channel_urls.items():
        for u in urls:
            tasks.append((g, n, u, test_source_3ts(u, sem)))

    results = []
    for batch in [tasks[i:i+10] for i in range(0, len(tasks), 10)]:
        res = await asyncio.gather(*[t[3] for t in batch])
        for i, r in enumerate(res):
            g, n, u, _ = batch[i]
            url, mbps, ok_res = r
            if url is None:
                continue
            if ENABLE_MIN_SPEED_FILTER and mbps < MIN_SPEED_Mbps:
                continue
            results.append((g, n, url, mbps, ok_res))

    speed_map = defaultdict(list)
    for g, n, url, mbps, ok_res in results:
        speed_map[(g, n)].append((url, mbps, ok_res))

    out = defaultdict(list)
    for key, items in speed_map.items():
        items.sort(key=lambda x: x[1], reverse=True)
        q = [u for u, s, ok in items if ok]
        if q:
            out[key] = q[:MAX_LINKS_PER_CHANNEL]
        else:
            out[key] = [u for u, s, ok in items][:MAX_LINKS_PER_CHANNEL]
    print(f"✅ 测速完成，保留 {sum(len(v) for v in out.values())} 条优质源")
    return out

# ------------------------------ 提取逻辑 ------------------------------
async def extract_from_ip(page, row, ip_text):
    entries = []
    print(f"\n📌 处理IP: {ip_text}")
    try:
        btn = row.locator("button:has(i.fa-list),button:has-text('≡')").first
        if await btn.count():
            await robust_click(btn, desc="菜单")
        else:
            await row.locator("div.item-title").first.click(timeout=5000)
        await asyncio.sleep(DELAY_AFTER_CLICK)

        modal = page.locator(".modal-dialog").first
        await modal.wait_for(state="visible", timeout=8000)
        items = modal.locator(".item-content")
        total = await items.count()
        limit = total if MAX_CHANNELS_PER_IP <=0 else min(total, MAX_CHANNELS_PER_IP)

        for j in range(limit):
            try:
                name = await items.nth(j).locator(".item-title").inner_text(timeout=3000)
                link = await items.nth(j).locator(".item-subtitle").inner_text(timeout=3000)
            except:
                continue
            name = name.strip()
            link = link.strip()
            if not name or not link:
                continue
            norm = normalize_cctv(name)
            group = classify_channel(norm) or classify_channel(name)
            if not group:
                continue
            final = norm if group == "央视频道" else clean_chinese_only(name)
            entries.append((group, final, link))
        await page.keyboard.press("Escape")
    except:
        pass
    return entries

# ------------------------------ 主流程 ------------------------------
async def _main():
    global ENABLE_SPEED_TEST
    print(f"[{time.strftime('%H:%M:%S')}] 🚀 3TS精准测速版启动")

    try:
        import playwright
    except ImportError:
        print("❌ pip install playwright aiohttp")
        sys.exit(1)

    async with async_playwright() as p:
        browser = await getattr(p, BROWSER_TYPE).launch(headless=HEADLESS, args=["--no-sandbox"])
        ctx = await browser.new_context(viewport={"width":1920,"height":1080})
        page = await ctx.new_page()
        await page.goto(TARGET_URL, timeout=PAGE_LOAD_TIMEOUT, wait_until="networkidle")

        if ENGINE_SELECTOR:
            e = page.locator(ENGINE_SELECTOR).first
            if await e.count():
                await robust_click(e, desc="搜索")
                await asyncio.sleep(DELAY_AFTER_CLICK)
        if MCAST_SELECTOR:
            t = page.locator(MCAST_SELECTOR).first
            await t.wait_for(state="attached", timeout=15000)
            await robust_click(t, desc="组播")
            await asyncio.sleep(DELAY_AFTER_CLICK)
        if START_SELECTOR:
            b = page.locator(START_SELECTOR).first
            await robust_click(b, desc="开始")
            await asyncio.sleep(DELAY_AFTER_CLICK)

        await page.locator("div.item-title:text-matches('\\d+\\.\\d+\\.\\d+\\.\\d+')").first.wait_for(state="attached", timeout=60000)
        rows = page.locator("div.ios-list-item").filter(has_text="频道:")
        total_ips = await rows.count()
        cnt = min(total_ips, MAX_IPS) if MAX_IPS else total_ips
        print(f"📋 共{total_ips}IP，处理前{cnt}个")

        raw = []
        for i in range(cnt):
            r = rows.nth(i)
            ip = await r.locator("div.item-title").first.inner_text()
            ip = ip.strip()
            if not IP_PATTERN.match(ip):
                continue
            raw.extend(await extract_from_ip(page, r, ip))
            if i < cnt-1:
                await asyncio.sleep(DELAY_BETWEEN_IPS)

        channel_map = defaultdict(list)
        seen = set()
        for g,n,u in raw:
            if ENABLE_DEDUPLICATION:
                k=(g,n,u)
                if k in seen:continue
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

    # 排序央视
    cctv_g = next((g for g in grouped if "央视" in g), None)
    if cctv_g:
        def ck(x):
            m=re.search(r"CCTV-(\d+)",x[0])
            return int(m.group(1)) if m else 999
        grouped[cctv_g].sort(key=ck)

    # 输出
    with open(OUTPUT_DIR/OUTPUT_M3U_FILENAME,"w",encoding="utf-8") as f:
        f.write("#EXTM3U\n")
        for g in GROUP_ORDER:
            for n,u in grouped.get(g,[]):
                f.write(f'#EXTINF:-1 group-title="{g}",{n}\n{u}\n')
    with open(OUTPUT_DIR/OUTPUT_TXT_FILENAME,"w",encoding="utf-8") as f:
        for g in GROUP_ORDER:
            if g not in grouped:continue
            f.write(f"{g},#genre#\n")
            for n,u in grouped[g]:
                f.write(f"{n},{u}\n")
            f.write("\n")

    print(f"\n🎉 完成！共导出 {len(final)} 条优质源")
    await browser.close()

async def main_with_timeout():
    try:
        await asyncio.wait_for(_main(), timeout=SCRIPT_TIMEOUT)
    except asyncio.TimeoutError:
        print("❌ 超时退出")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main_with_timeout())
