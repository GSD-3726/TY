#!/usr/bin/env python3
"""
IPTV 组播提取 —— 【稳定可抓取版】
流程：爬取 → 去重 → 批量测速 → 输出 + 完整日志
仅新增逻辑，不改动原有能抓的页面结构
"""
import asyncio
import re
import sys
import time
from collections import defaultdict
from pathlib import Path
from playwright.async_api import async_playwright

# ============================================================================
# 【你原来能用的配置 —— 完全不动】
# ============================================================================
TARGET_URL = "https://iptv.809899.xyz"
OUTPUT_DIR = Path(__file__).parent

MAX_IPS = 10
HEADLESS = True
BROWSER = "chromium"

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
                                      "安徽", "江西", "湖北", "东南", "辽宁", "广东", "河北"]},
    {"name": "电影频道",    "keywords": ["电影", "影迷", "影院", "chc"]},
    {"name": "轮播频道",    "keywords": ["轮播"]},
    {"name": "儿童频道",    "keywords": ["少儿", "动画", "卡通", "金鹰", "嘉佳", "卡酷"]},
]

GROUP_ORDER = ["央视频道", "卫视频道", "电影频道", "4K专区", "儿童频道", "轮播频道"]

MAX_LINKS_PER_CHANNEL = 8
ENABLE_DEDUPLICATION = True

# -------------------------- FFmpeg 测速 -----------------------------
TEST_TIMEOUT = 4.0
CONCURRENCY = 3
MAX_ALLOW_DELAY = 3000

# -------------------------- 央视名称（你原来能用的版本）-----------------------------
CCTV_MAP = {
    "1": "综合", "2": "财经", "3": "综艺", "4": "国际", "5": "体育",
    "5+": "体育赛事", "6": "电影", "7": "国防军事", "8": "电视剧",
    "9": "纪录", "10": "科教", "11": "戏曲", "12": "社会与法",
    "13": "新闻", "14": "少儿", "15": "音乐", "16": "奥林匹克", "17": "农业农村"
}

# ============================================================================
# 【你原来能用的浏览器参数 —— 完全不动】
# ============================================================================
LAUNCH_ARGS = {
    "headless": HEADLESS,
    "args": [
        "--no-sandbox",
        "--disable-gpu",
        "--disable-dev-shm-usage",
        "--disable-extensions",
        "--no-first-run",
        "--single-process"
    ]
}

# ============================================================================
# 【你原来能用的工具函数 —— 完全不动】
# ============================================================================
def clean_name(name):
    return re.sub(r'[^\u4e00-\u9fff]', '', name)

def normalize_cctv(name):
    name_lower = name.lower()
    if "cctv5+" in name_lower:
        return f"CCTV-5+{CCTV_MAP.get('5+', '体育赛事')}"
    match = re.search(r'cctv[-\s]?(\d{1,2})', name_lower)
    if match:
        num = match.group(1)
        return f"CCTV-{num}{CCTV_MAP.get(num, '')}"
    match = re.search(r'cetv[-\s]?(\d)', name_lower)
    if match:
        return f"CETV-{match.group(1)}"
    return name

def build_selector(texts, tag="button"):
    if not texts:
        return ""
    return ",".join([f"{tag}:has-text('{t}')" for t in texts])

ENGINE_SEL = build_selector(PAGE_CONFIG["engine_search"], "a,button,div")
MCAST_SEL = build_selector(PAGE_CONFIG["multicast_tab"], "div")
START_SEL = build_selector(PAGE_CONFIG["start_button"], "button")

# ============================================================================
# ====================== 新增：FFmpeg 测速 + 日志 ======================
# ============================================================================
async def test_url(url):
    start = time.time()
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg",
            "-hide_banner",
            "-loglevel", "error",
            "-timeout", str(int(TEST_TIMEOUT * 1000)),
            "-i", url,
            "-t", "0.1",
            "-f", "null", "-",
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL
        )
        await asyncio.wait_for(proc.communicate(), TEST_TIMEOUT + 0.5)
        cost = int((time.time() - start) * 1000)
        if proc.returncode == 0 and cost <= MAX_ALLOW_DELAY:
            return (True, cost)
        return (False, cost)
    except:
        return (False, 9999)

async def batch_test(url_list):
    sem = asyncio.Semaphore(CONCURRENCY)
    async def wrap(u):
        async with sem:
            return await test_url(u)
    return await asyncio.gather(*[wrap(u) for u in url_list])

# ============================================================================
# ====================== 主流程：完全恢复你能抓的逻辑 ======================
# ============================================================================
async def main():
    print("=" * 60)
    print("📥 步骤1：开始爬取播放链接（原版稳定逻辑）")
    print("=" * 60)

    raw = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(**LAUNCH_ARGS)
        page = await browser.new_page(viewport={"width": 1280, "height": 720})

        try:
            await page.goto(TARGET_URL, timeout=120000)
            await page.wait_for_load_state("networkidle", timeout=30000)
        except:
            pass

        # 你原来的点击逻辑 —— 完全不动
        for sel in [ENGINE_SEL, MCAST_SEL, START_SEL]:
            try:
                await page.locator(sel).first.click(timeout=10000)
                await asyncio.sleep(1)
            except:
                continue

        await asyncio.sleep(8)

        # 你原来能抓到的选择器 —— 完全不动
        rows = page.locator("div.ios-list-item:has-text('频道:')")
        total = await rows.count()
        print(f"✅ 找到线路总数：{total}，抓取前 {MAX_IPS} 条")

        cnt = min(total, MAX_IPS)
        for i in range(cnt):
            try:
                row = rows.nth(i)
                await row.click(timeout=5000)
                await asyncio.sleep(1)
                items = page.locator(".modal-dialog .item-content")
                item_cnt = await items.count()
                print(f"  线路 {i+1}/{cnt} → 频道数：{item_cnt}")

                for j in range(min(item_cnt, 50)):
                    try:
                        name = await items.nth(j).locator(".item-title").inner_text()
                        link = await items.nth(j).locator(".item-subtitle").inner_text()
                        name, link = name.strip(), link.strip()
                        if not name or not link:
                            continue

                        norm = normalize_cctv(name)
                        group = None
                        for rule in CATEGORY_RULES:
                            if any(k in norm.lower() for k in rule["keywords"]):
                                group = rule["name"]
                                break
                        if not group:
                            continue

                        final = norm if group == "央视频道" else clean_name(name) or norm
                        raw.append((group, final, link))
                    except:
                        continue

                await page.keyboard.press("Escape")
                await asyncio.sleep(1)
            except:
                continue

        await browser.close()

    print(f"\n✅ 爬取完成：原始链接 {len(raw)} 条")

    # ======================================
    print("\n" + "="*60)
    print("📛 步骤2：统一去重")
    print("="*60)
    # ======================================
    channel_map = defaultdict(set)
    for g, n, u in raw:
        channel_map[(g, n)].add(u)

    total_after = sum(len(v) for v in channel_map.values())
    print(f"✅ 去重后：频道 {len(channel_map)} 个，链接 {total_after} 条")

    # ======================================
    print("\n" + "="*60)
    print("⚡ 步骤3：FFmpeg 批量测速")
    print("="*60)
    # ======================================
    test_list = []
    key_map = {}
    for (g, n), urls in channel_map.items():
        for u in urls:
            test_list.append(u)
            key_map[u] = (g, n)

    results = await batch_test(test_list)

    valid = defaultdict(list)
    ok = 0
    fail = 0

    for url, (ok_flag, ms) in zip(test_list, results):
        g, n = key_map[url]
        if ok_flag:
            valid[(g, n)].append((ms, url))
            print(f"✅  {n} | {ms}ms")
            ok +=1
        else:
            print(f"❌  {n} | 失败")
            fail +=1

    print(f"\n📊 测速完成：有效={ok}  |  无效={fail}")

    # ======================================
    # 排序 + 输出
    # ======================================
    final = []
    for (g, n), items in valid.items():
        items.sort()
        items = items[:MAX_LINKS_PER_CHANNEL]
        for ms, u in items:
            final.append((g, n, u))

    grouped = defaultdict(list)
    for g, n, u in final:
        grouped[g].append((n, u))

    with open("iptv_channels.m3u", "w", encoding="utf-8") as f:
        f.write("#EXTM3U\n")
        for g in GROUP_ORDER:
            for n, u in sorted(grouped.get(g, [])):
                f.write(f'#EXTINF:-1 group-title="{g}",{n}\n{u}\n')

    with open("iptv_channels.txt", "w", encoding="utf-8") as f:
        for g in GROUP_ORDER:
            f.write(f"{g},#genre#\n")
            for n, u in sorted(grouped.get(g, [])):
                f.write(f"{n},{u}\n")
            f.write("\n")

    print("\n🎉 全部完成！")
    print(f"📺 最终有效源：{len(final)} 条")

if __name__ == "__main__":
    asyncio.run(main())
