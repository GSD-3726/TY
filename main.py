#!/usr/bin/env python3
"""
IPTV 组播提取 · GitHub 稳定版
流程：爬取 → 去重 → 批量FFmpeg测速 → 输出 → 带完整日志
"""
import asyncio
import re
import sys
import time
from collections import defaultdict
from pathlib import Path
from playwright.async_api import async_playwright

# ============================================================================
# 【配置区】所有参数都在这里
# ============================================================================
TARGET_URL = "https://iptv.809899.xyz"
OUTPUT_DIR = Path(__file__).parent

MAX_IPS = 6                  # 最多爬几个线路
HEADLESS = True
BROWSER = "chromium"

# 频道输出数量
MAX_LINKS_PER_CHANNEL = 8
ENABLE_DEDUPLICATION = True

# -------------------------- FFmpeg 测速 -----------------------------
TEST_TIMEOUT = 4.0
CONCURRENCY = 3
MAX_ALLOW_DELAY = 3000  # 超过这个毫秒数直接丢弃

# -------------------------- 央视名称 -----------------------------
CCTV_MAP = {
    "1": "综合", "2": "财经", "3": "综艺", "4": "国际", "5": "体育",
    "5+": "体育赛事", "6": "电影", "7": "国防军事", "8": "电视剧",
    "9": "纪录", "10": "科教", "11": "戏曲", "12": "社会与法",
    "13": "新闻", "14": "少儿", "15": "音乐", "16": "奥林匹克", "17": "农业农村"
}

# 分类
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

# ============================================================================
# 浏览器启动参数
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
# 工具函数
# ============================================================================
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
    return name.strip()

def get_group(name):
    name = name.lower()
    for rule in CATEGORY_RULES:
        for kw in rule["keywords"]:
            if kw in name:
                return rule["name"]
    return "其他频道"

# ============================================================================
# FFmpeg 测速（带延迟）
# ============================================================================
async def check_stream(url):
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
        await asyncio.wait_for(proc.communicate(), timeout=TEST_TIMEOUT + 0.5)
        cost = int((time.time() - start) * 1000)
        if proc.returncode == 0 and cost <= MAX_ALLOW_DELAY:
            return (True, cost)
        return (False, cost)
    except:
        return (False, 9999)

# 并发测速
async def batch_check(url_list):
    sem = asyncio.Semaphore(CONCURRENCY)
    async def task(url):
        async with sem:
            ok, ms = await check_stream(url)
            return (url, ok, ms)
    tasks = [task(u) for u in url_list]
    return await asyncio.gather(*tasks)

# ============================================================================
# 主流程
# ============================================================================
async def main():
    print("=" * 60)
    print("📥 步骤1：开始爬取播放链接")
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
        await asyncio.sleep(8)

        rows = page.locator("div.ios-list-item:has-text('频道:')")
        total = await rows.count()
        print(f"✅ 找到线路总数：{total}，将抓取前 {MAX_IPS} 条")

        cnt = min(total, MAX_IPS)
        for i in range(cnt):
            try:
                await rows.nth(i).click(timeout=5000)
                await asyncio.sleep(1)
                items = page.locator(".modal-dialog .item-content")
                item_cnt = await items.count()
                print(f"  线路 {i+1}/{cnt}，频道数：{item_cnt}")
                for j in range(min(item_cnt, 80)):
                    try:
                        title = await items.nth(j).locator(".item-title").inner_text()
                        url = await items.nth(j).locator(".item-subtitle").inner_text()
                        title = title.strip()
                        url = url.strip()
                        if title and url and (url.startswith("http") or url.startswith("rtsp")):
                            raw.append((title, url))
                    except:
                        continue
                await page.keyboard.press("Escape")
                await asyncio.sleep(0.5)
            except Exception as e:
                continue
        await browser.close()

    print(f"\n✅ 爬取完成：原始链接共 {len(raw)} 条")

    # ======================================
    print("\n" + "="*60)
    print("📛 步骤2：去重（按频道+链接唯一）")
    print("="*60)
    # ======================================
    channel_map = defaultdict(set)
    for title, url in raw:
        nice_title = normalize_cctv(title)
        channel_map[nice_title].add(url)

    total_after_dedup = sum(len(v) for v in channel_map.values())
    print(f"✅ 去重后：共 {len(channel_map)} 个频道，{total_after_dedup} 条链接")

    # ======================================
    print("\n" + "="*60)
    print("⚡ 步骤3：FFmpeg 测速中...")
    print("="*60)
    # ======================================
    all_urls = []
    title_map = {}
    for title, urls in channel_map.items():
        for u in urls:
            all_urls.append(u)
            title_map[u] = title

    results = await batch_check(all_urls)
    ok_count = 0
    fail_count = 0

    valid_by_title = defaultdict(list)
    for url, ok, ms in results:
        title = title_map[url]
        if ok:
            ok_count += 1
            valid_by_title[title].append((ms, url))
            print(f"✅  {title} | {ms}ms | {url}")
        else:
            fail_count += 1
            print(f"❌  {title} | 失败 {ms}ms | {url}")

    print(f"\n📊 测速完成：有效={ok_count} 条，无效={fail_count} 条")

    # 按延迟排序，每个频道取前N条
    final = []
    for title, items in valid_by_title.items():
        items.sort()  # 延迟低在前
        items = items[:MAX_LINKS_PER_CHANNEL]
        g = get_group(title)
        for ms, url in items:
            final.append((g, title, url))

    # ======================================
    # 输出文件
    # ======================================
    grouped = defaultdict(list)
    for g, t, u in final:
        grouped[g].append((t, u))

    with open(OUTPUT_DIR / "iptv_channels.m3u", "w", encoding="utf-8") as f:
        f.write("#EXTM3U\n")
        for g in GROUP_ORDER:
            for t, u in sorted(grouped.get(g, [])):
                f.write(f'#EXTINF:-1 group-title="{g}",{t}\n{u}\n')

    with open(OUTPUT_DIR / "iptv_channels.txt", "w", encoding="utf-8") as f:
        for g in GROUP_ORDER:
            f.write(f"{g},#genre#\n")
            for t, u in sorted(grouped.get(g, [])):
                f.write(f"{t},{u}\n")
            f.write("\n")

    print("\n" + "="*60)
    print("🎉 全部完成！")
    print(f"📺 最终有效频道：{len(valid_by_title)} 个")
    print(f"🎞 最终有效源：{len(final)} 条")
    print("📁 已输出：iptv_channels.m3u / txt")
    print("="*60)

if __name__ == "__main__":
    asyncio.run(main())
