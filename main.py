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
HEADLESS = True  # GitHub 必须 True

# ------------------------ 页面加载超时 ------------------------------------
PAGE_LOAD_TIMEOUT = 120000
ACTION_WAIT_TIMEOUT = 10000

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
OUTPUT_M3U_FILENAME = "iptv_channels.m3u"
OUTPUT_TXT_FILENAME = "iptv_channels.txt"

# -------------------------- 功能开关 -------------------------------------
ENABLE_CHINESE_CLEAN = True
ENABLE_DEDUPLICATION = True

# -------------------------- 央视频道名称映射 -----------------------------
CCTV_USE_MAPPING = True
CCTV_NAME_MAPPING = {
    "1": "综合", "2": "财经", "3": "综艺", "4": "国际", "5": "体育",
    "5+": "体育赛事", "6": "电影", "7": "国防军事", "8": "电视剧",
    "9": "纪录", "10": "科教", "11": "戏曲", "12": "社会与法",
    "13": "新闻", "14": "少儿", "15": "音乐", "16": "奥林匹克",
    "17": "农业农村",
}

# -------------------------- 3TS 测速核心配置 -------------------------------
ENABLE_SPEED_TEST = True
SPEED_TEST_CONCURRENCY = 10
TS_TEST_COUNT = 3
MIN_SPEED_Mbps = 0.8
ENABLE_MIN_SPEED_FILTER = True

# -------------------------- 分辨率筛选 ------------------------------------
ENABLE_RESOLUTION_FILTER = True
MIN_RESOLUTION_WIDTH = 1280
MIN_RESOLUTION_HEIGHT = 720
FALLBACK_TO_SPEED_WHEN_NO_RESOLUTION = True

# -------------------------- 负载控制 --------------------------------------
DELAY_BETWEEN_IPS = 1.0
DELAY_AFTER_CLICK = 1.0
MAX_CHANNELS_PER_IP = 0

# -------------------------- 脚本全局超时 ----------------------------------
SCRIPT_TIMEOUT = 2400

# ============================================================================
# 工具函数
# ============================================================================

IP_PATTERN = re.compile(r'^\d+\.\d+\.\d+\.\d+$')
RESOLUTION_PATTERN = re.compile(r'(\d+)x(\d+)')
CHINESE_ONLY_PATTERN = re.compile(r'[^\u4e00-\u9fff]')

def clean_chinese_only(s):
    return CHINESE_ONLY_PATTERN.sub('', s)

def normalize_cctv(name: str) -> str:
    name_lower = name.lower()
    if "cctv5+" in name_lower:
        return "CCTV-5+体育赛事"
    m = re.search(r'cctv[-\s]?(\d+)', name_lower)
    if m:
        num = m.group(1)
        suf = CCTV_NAME_MAPPING.get(num, "")
        return f"CCTV-{num}{suf}"
    m = re.search(r'cetv[-\s]?(\d+)', name_lower)
    if m:
        return f"CETV-{m.group(1)}"
    return name

def build_classifier():
    rules = []
    for cat in CATEGORY_RULES:
        keywords = [kw.lower() for kw in cat["keywords"]]
        pat = re.compile('|'.join(re.escape(k) for k in keywords))
        rules.append((cat["name"], pat))
    def classify(name):
        nl = name.lower()
        for name, pat in rules:
            if pat.search(nl):
                return name
        return "其他频道"
    return classify

classify_channel = build_classifier()

# ------------------------------ 3TS 测速 ------------------------------

async def fetch(session, url):
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=7)) as resp:
            if resp.status in (200, 206):
                return await resp.read()
    except:
        pass
    return None

def parse_m3u8(base_url, text):
    ts = []
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("#"):
            continue
        if not line:
            continue
        ts.append(urljoin(base_url, line))
    return ts

async def test_source_3ts(url, sem):
    async with sem:
        if "m3u8" not in url.lower():
            return None, 0, False
        try:
            async with aiohttp.ClientSession() as s:
                resp = await s.get(url, timeout=aiohttp.ClientTimeout(total=10))
                if resp.status != 200:
                    return None, 0, False
                body = await resp.text()
                ts_list = parse_m3u8(url, body)
                if len(ts_list) < TS_TEST_COUNT:
                    return None, 0, False
                ts_list = ts_list[:TS_TEST_COUNT]

                total = 0
                ok = 0
                t0 = time.time()
                for u in ts_list:
                    data = await fetch(s, u)
                    if data:
                        total += len(data)
                        ok += 1
                if ok < 2:
                    return None, 0, False

                cost = time.time() - t0
                if cost <= 0:
                    return None, 0, False
                mbps = (total * 8 / 1e6) / cost

                res_ok = False
                ul = url.lower()
                if ENABLE_RESOLUTION_FILTER:
                    if "1080" in ul or "2160" in ul or "4k" in ul:
                        res_ok = True
                    elif "720" in ul and MIN_RESOLUTION_WIDTH <= 1280:
                        res_ok = True
                return url, mbps, res_ok
        except:
            return None, 0, False

async def run_speed_test(channel_map):
    sem = asyncio.Semaphore(SPEED_TEST_CONCURRENCY)
    tasks = []
    for (g, n), urls in channel_map.items():
        for u in urls:
            tasks.append((g, n, u, test_source_3ts(u, sem)))

    results = []
    for i in range(0, len(tasks), 10):
        batch = tasks[i:i+10]
        res = await asyncio.gather(*[t[3] for t in batch])
        for j, r in enumerate(res):
            g, n, u, _ = batch[j]
            url, mbps, ok_res = r
            if url and (not ENABLE_MIN_SPEED_FILTER or mbps >= MIN_SPEED_Mbps):
                results.append((g, n, url, mbps, ok_res))

    out = defaultdict(list)
    temp = defaultdict(list)
    for g, n, url, mbps, ok_res in results:
        temp[(g, n)].append((url, mbps, ok_res))

    for key, items in temp.items():
        items.sort(key=lambda x: x[1], reverse=True)
        good = [u for u, s, ok in items if ok]
        if good:
            out[key] = good[:MAX_LINKS_PER_CHANNEL]
        else:
            out[key] = [u for u, s, ok in items][:MAX_LINKS_PER_CHANNEL]
    return out

# ------------------------------ 主逻辑 ------------------------------

async def _main():
    print(f"[{time.strftime('%H:%M:%S')}] 🚀 3TS精准测速版启动")

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-gpu",
                "--disable-dev-shm-usage",
                "--disable-extensions",
                "--disable-blink-features=AutomationControlled"
            ]
        )
        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"
        )
        page = await context.new_page()

        try:
            await page.goto(TARGET_URL, timeout=PAGE_LOAD_TIMEOUT, wait_until="domcontentloaded")
            await asyncio.sleep(2)
        except:
            pass

        # 不强制等待IP元素，避免卡死
        rows = page.locator("div.ios-list-item")
        total = await rows.count()
        use = min(total, MAX_IPS) if MAX_IPS else total
        print(f"📋 找到 {total} 个条目，使用前 {use} 个")

        raw = []
        for i in range(use):
            try:
                row = rows.nth(i)
                title = await row.locator("div.item-title").inner_text(timeout=ACTION_WAIT_TIMEOUT)
                if not IP_PATTERN.match(title.strip()):
                    continue
                print(f"\n📌 处理: {title.strip()}")

                try:
                    btn = row.locator("button,div.item-title").first
                    await btn.click(timeout=ACTION_WAIT_TIMEOUT)
                    await asyncio.sleep(DELAY_AFTER_CLICK)
                except:
                    pass

                items = page.locator(".item-content")
                item_cnt = await items.count()
                for j in range(item_cnt):
                    try:
                        name = await items.nth(j).locator(".item-title").inner_text(timeout=3000)
                        link = await items.nth(j).locator(".item-subtitle").inner_text(timeout=3000)
                        name = name.strip()
                        link = link.strip()
                        if name and link:
                            norm = normalize_cctv(name)
                            cat = classify_channel(norm)
                            final = norm if cat == "央视频道" else clean_chinese_only(name)
                            raw.append((cat, final, link))
                    except:
                        continue

                try:
                    await page.keyboard.press("Escape")
                    await asyncio.sleep(0.5)
                except:
                    pass
            except:
                continue

        await browser.close()

    # 去重
    channel_map = defaultdict(list)
    seen = set()
    for g, n, u in raw:
        key = (g, n, u)
        if key in seen:
            continue
        seen.add(key)
        channel_map[(g, n)].append(u)

    # 测速
    if ENABLE_SPEED_TEST and channel_map:
        channel_map = await run_speed_test(channel_map)

    # 输出
    final = []
    for (g, n), urls in channel_map.items():
        for u in urls:
            final.append((g, n, u))

    grouped = defaultdict(list)
    for g, n, u in final:
        grouped[g].append((n, u))

    with open(OUTPUT_M3U_FILENAME, "w", encoding="utf-8") as f:
        f.write("#EXTM3U\n")
        for g in GROUP_ORDER:
            for n, u in grouped.get(g, []):
                f.write(f"#EXTINF:-1 group-title=\"{g}\",{n}\n{u}\n")

    with open(OUTPUT_TXT_FILENAME, "w", encoding="utf-8") as f:
        for g in GROUP_ORDER:
            lst = grouped.get(g)
            if not lst:
                continue
            f.write(f"{g},#genre#\n")
            for n, u in lst:
                f.write(f"{n},{u}\n")
            f.write("\n")

    print(f"\n🎉 完成！导出 {len(final)} 条优质源")

async def main_with_timeout():
    try:
        await asyncio.wait_for(_main(), timeout=SCRIPT_TIMEOUT)
    except asyncio.TimeoutError:
        print("⚠️ 超时退出，但已尽力采集")

if __name__ == "__main__":
    asyncio.run(main_with_timeout())
