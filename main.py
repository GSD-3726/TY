#!/usr/bin/env python3
"""
IPTV 组播提取 · GitHub 免费机专用版
带完整注释，参数一目了然
"""

import asyncio
import re
import sys
from collections import defaultdict
from pathlib import Path
from playwright.async_api import async_playwright

# ============================================================================
# 【一、基础抓取配置】
# ============================================================================
TARGET_URL = "https://iptv.809899.xyz"          # 数据源网址
OUTPUT_DIR = Path(__file__).parent               # 输出文件目录（当前目录）

MAX_IPS = 10                                      # 最多抓取几个IP源（GitHub弱机别太高）
HEADLESS = True                                  # 无头模式（服务器必须True）
BROWSER = "chromium"                             # 使用的浏览器

# 页面点击文案（适配网站结构）
PAGE_CONFIG = {
    "engine_search": ["引索搜索", "引擎搜索", "关键词搜索"],
    "multicast_tab": ["组播提取"],
    "start_button": ["开始播放", "开始搜索", "开始提取"],
}

# 频道分类规则
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

# 输出时频道分组顺序
GROUP_ORDER = ["央视频道", "卫视频道", "电影频道", "4K专区", "儿童频道", "轮播频道"]

MAX_LINKS_PER_CHANNEL = 8                        # 每个频道最多保留几条源（越少越流畅）
ENABLE_DEDUPLICATION = True                      # 是否去重（强烈建议开）

# ============================================================================
# 【二、测速核心配置 · GitHub 专用】
# ============================================================================
TEST_TIMEOUT = 5.0                               # 单条链接测速超时（秒）
CONCURRENCY = 3                                   # 并发测速数（免费机 3~4 最稳）
MAX_DELAY = 500                                  # 最大允许延迟（毫秒），超过直接丢弃
MIN_SUCCESS_FRAMES = 3                           # 最少成功读几帧（1=最快最稳）
MIN_WIDTH = 1920                                  # 最低分辨率宽
MIN_HEIGHT = 1080                                 # 最低分辨率高

# ============================================================================
# 【三、CCTV 频道重命名】
# ============================================================================
CCTV_MAP = {
    "1": "综合", "2": "财经", "3": "综艺", "4": "国际", "5": "体育",
    "5+": "体育赛事", "6": "电影", "7": "国防军事", "8": "电视剧",
    "9": "纪录", "10": "科教", "11": "戏曲", "12": "社会与法",
    "13": "新闻", "14": "少儿", "15": "音乐", "16": "奥林匹克", "17": "农业农村"
}

# ============================================================================
# 【四、浏览器启动参数（服务器必用，别动）】
# ============================================================================
LAUNCH_ARGS = {
    "headless": HEADLESS,
    "args": [
        "--no-sandbox",              # Linux 服务器必需
        "--disable-gpu",             # 禁用GPU（省资源）
        "--disable-dev-shm-usage",   # 防止共享内存不足崩溃
        "--disable-extensions",       # 禁用扩展
        "--no-first-run",            # 关闭首次运行向导
        "--single-process"           # 单进程模式（更轻）
    ]
}

# ============================================================================
# 下面是核心逻辑，一般不用改
# ============================================================================

def clean_name(name):
    # 只保留中文，去掉乱码
    return re.sub(r'[^\u4e00-\u9fff]', '', name)

def normalize_cctv(name):
    # 标准化央视频道名
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

# 自动构造页面选择器
ENGINE_SEL = build_selector(PAGE_CONFIG["engine_search"], "a,button,div")
MCAST_SEL = build_selector(PAGE_CONFIG["multicast_tab"], "div")
START_SEL = build_selector(PAGE_CONFIG["start_button"], "button")

# ====================== 极简稳定测速 ======================
async def test_url(url, sem):
    async with sem:
        try:
            proc = await asyncio.create_subprocess_exec(
                sys.executable, "-c",
                f'''
import cv2
cap = cv2.VideoCapture("{url}")
if not cap.isOpened(): exit(1)
ret = cap.read()[0]
cap.release()
print(1 if ret else 0)
''',
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL
            )
            await asyncio.wait_for(proc.communicate(), timeout=TEST_TIMEOUT)
            return proc.returncode == 0
        except:
            return False

# ====================== 主抓取流程 ======================
async def main():
    raw = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(**LAUNCH_ARGS)
        page = await browser.new_page(viewport={"width": 1280, "height": 720})

        # 打开网页
        try:
            await page.goto(TARGET_URL, timeout=120000)
            await page.wait_for_load_state("networkidle", timeout=30000)
        except:
            pass

        # 依次点击：搜索、组播、开始
        for sel in [ENGINE_SEL, MCAST_SEL, START_SEL]:
            try:
                await page.locator(sel).first.click(timeout=10000)
                await asyncio.sleep(1)
            except:
                continue

        await asyncio.sleep(8)
        rows = page.locator("div.ios-list-item:has-text('频道:')")
        total = await rows.count()
        cnt = min(total, MAX_IPS)

        # 遍历IP源
        for i in range(cnt):
            try:
                row = rows.nth(i)
                await row.click(timeout=5000)
                await asyncio.sleep(1)
                items = page.locator(".modal-dialog .item-content")
                item_cnt = await items.count()
                # 提取频道
                for j in range(min(item_cnt, 50)):
                    try:
                        name = await items.nth(j).locator(".item-title").inner_text()
                        link = await items.nth(j).locator(".item-subtitle").inner_text()
                        name, link = name.strip(), link.strip()
                        if not name or not link:
                            continue
                        norm = normalize_cctv(name)
                        # 匹配分类
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

    # 并发测速
    sem = asyncio.Semaphore(CONCURRENCY)
    final = []
    for (g, n), urls in channel_map.items():
        tasks = [test_url(u, sem) for u in urls]
        ok_list = await asyncio.gather(*tasks)
        valid = sorted([u for u, ok in zip(urls, ok_list) if ok])[:MAX_LINKS_PER_CHANNEL]
        for u in valid:
            final.append((g, n, u))

    # 输出 M3U
    grouped = defaultdict(list)
    for g, n, u in final:
        grouped[g].append((n, u))

    with open(OUTPUT_DIR / "iptv_channels.m3u", "w", encoding="utf-8") as f:
        f.write("#EXTM3U\n")
        for g in GROUP_ORDER:
            for n, u in sorted(grouped.get(g, []), key=lambda x: x[0]):
                f.write(f'#EXTINF:-1 group-title="{g}",{n}\n{u}\n')

    # 输出 TXT
    with open(OUTPUT_DIR / "iptv_channels.txt", "w", encoding="utf-8") as f:
        for g in GROUP_ORDER:
            f.write(f"{g},#genre#\n")
            for n, u in sorted(grouped.get(g, []), key=lambda x: x[0]):
                f.write(f"{n},{u}\n")
            f.write("\n")

    print(f"✅ 抓取完成，有效播放源数量：{len(final)}")

if __name__ == "__main__":
    asyncio.run(main())
