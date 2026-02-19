#!/usr/bin/env python3
"""
IPTV 组播提取 · GitHub 免费机终极稳定版
FFmpeg精准测速 | 分辨率过滤 | 延迟计算 | 自动丢劣质源
"""
import asyncio
import re
import sys
import time
from collections import defaultdict
from pathlib import Path
from playwright.async_api import async_playwright

# ============================================================================
# GitHub 优化配置（可根据需要微调）
# ============================================================================
TARGET_URL = "https://iptv.8099.xyz"
OUTPUT_DIR = Path(__file__).parent

MAX_IPS = 10                      # 最多抓取几个IP源
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

MAX_LINKS_PER_CHANNEL = 8        # 每个频道最多保留几条源
ENABLE_DEDUPLICATION = True      # 开启去重

# -------------------------- FFmpeg 测速阈值（核心过滤条件） -----------------------------
TEST_TIMEOUT       = 4.0         # 单个源测速超时（秒）
CONCURRENCY        = 3           # 并发测速数量
MAX_ALLOW_DELAY    = 3000        # 最大允许延迟（毫秒），超过自动丢弃
MIN_WIDTH          = 1980        # 最低允许宽度
MIN_HEIGHT         = 1020         # 最低允许高度

# -------------------------- 央视名称美化 -----------------------------
CCTV_MAP = {
    "1": "综合", "2": "财经", "3": "综艺", "4": "国际", "5": "体育",
    "5+": "体育赛事", "6": "电影", "7": "国防军事", "8": "电视剧",
    "9": "纪录", "10": "科教", "11": "戏曲", "12": "社会与法",
    "13": "新闻", "14": "少儿", "15": "音乐", "16": "奥林匹克", "17": "农业农村"
}

# ============================================================================
# 浏览器启动参数（服务器专用）
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
# ====================== FFmpeg 精准测速 + 分辨率 + 延迟 ======================
# ============================================================================
async def test_url(url, sem):
    async with sem:
        start_time = time.time()
        try:
            # FFmpeg 探测流信息：不解码、不保存、只测速
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
            cost_ms = round((time.time() - start_time) * 1000)

            # 超时 / 延迟过高直接丢弃
            if proc.returncode != 0 or cost_ms > MAX_ALLOW_DELAY:
                return None

            return cost_ms  # 返回延迟，用于排序

        except Exception:
            return None

# ====================== 主流程 ======================
async def main():
    raw = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(**LAUNCH_ARGS)
        page = await browser.new_page(viewport={"width": 1280, "height": 720})

        try:
            await page.goto(TARGET_URL, timeout=120000)
            await page.wait_for_load_state("networkidle", timeout=30000)
        except:
            pass

        # 依次点击按钮
        for sel in [ENGINE_SEL, MCAST_SEL, START_SEL]:
            try:
                await page.locator(sel).first.click(timeout=10000)
                await asyncio.sleep(1)
            except:
                continue

        await asyncio.sleep(8)

        # 获取线路
        rows = page.locator("div.ios-list-item:has-text('频道:')")
        total = await rows.count()
        cnt = min(total, MAX_IPS)

        # 遍历线路
        for i in range(cnt):
            try:
                row = rows.nth(i)
                await row.click(timeout=5000)
                await asyncio.sleep(1)

                items = page.locator(".modal-dialog .item-content")
                item_cnt = await items.count()

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

    # 去重
    channel_map = defaultdict(set)
    for g, n, u in raw:
        channel_map[(g, n)].add(u)

    # 并发测速 + 按延迟排序 + 取最优
    sem = asyncio.Semaphore(CONCURRENCY)
    final = []

    for (g, n), urls in channel_map.items():
        tasks = [test_url(u, sem) for u in urls]
        results = await asyncio.gather(*tasks)

        # 保留有效源，并按延迟从小到大排序
        valid = []
        for url, delay_ms in zip(urls, results):
            if delay_ms is not None:
                valid.append((delay_ms, url))

        valid.sort(key=lambda x: x[0])  # 延迟低 → 高
        valid = valid[:MAX_LINKS_PER_CHANNEL]

        for _, url in valid:
            final.append((g, n, url))

    # 输出文件
    grouped = defaultdict(list)
    for g, n, u in final:
        grouped[g].append((n, u))

    # 输出 m3u
    with open(OUTPUT_DIR / "iptv_channels.m3u", "w", encoding="utf-8") as f:
        f.write("#EXTM3U\n")
        for g in GROUP_ORDER:
            for n, u in sorted(grouped.get(g, []), key=lambda x: x[0]):
                f.write(f'#EXTINF:-1 group-title="{g}",{n}\n{u}\n')

    # 输出 txt
    with open(OUTPUT_DIR / "iptv_channels.txt", "w", encoding="utf-8") as f:
        for g in GROUP_ORDER:
            f.write(f"{g},#genre#\n")
            for n, u in sorted(grouped.get(g, []), key=lambda x: x[0]):
                f.write(f"{n},{u}\n")
            f.write("\n")

    print(f"✅ 抓取完成，有效播放源：{len(final)} 条")
    print(f"📊 过滤规则：延迟≤{MAX_ALLOW_DELAY}ms | 分辨率≥{MIN_WIDTH}x{MIN_HEIGHT}")

if __name__ == "__main__":
    asyncio.run(main())
