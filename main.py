#!/usr/bin/env python3
"""
IPTV 组播提取工具 —— GitHub Actions 免费版专用
低并发 + 轻量测速 + 防超时 + 高稳定性
"""

import asyncio
import re
import subprocess
import sys
from collections import defaultdict
from pathlib import Path
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# ============================================================================
# GitHub 免费机专用配置（已全部优化好）
# ============================================================================

# ---------------------------- 基础设置 ------------------------------------
TARGET_URL = "https://iptv.809899.xyz"
OUTPUT_DIR = Path(__file__).parent
MAX_IPS = 8                        # GitHub 弱机，少抓一点
HEADLESS = True
BROWSER_TYPE = "chromium"

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

# ------------------------ 输出设置 ----------------------------------------
MAX_LINKS_PER_CHANNEL = 10
OUTPUT_M3U_FILENAME = "iptv_channels.m3u"
OUTPUT_TXT_FILENAME = "iptv_channels.txt"

# -------------------------- 功能开关 -------------------------------------
ENABLE_CHINESE_CLEAN = True
ENABLE_DEDUPLICATION = True
ENABLE_SCREENSHOTS = False

# -------------------------- 【GitHub 专用测速参数】 -----------------------
ENABLE_SPEED_TEST = True
TEST_TIMEOUT = 6.0               # 放宽一点，防止网络波动误杀
CONCURRENCY_LIMIT = 4            # 免费机核心弱，并发必须低

MIN_WIDTH = 1920
MIN_HEIGHT = 1080

MAX_ALLOWED_DELAY = 3000         # 免费机网络一般，放宽延迟
MIN_SUCCESS_FRAMES = 2           # 读2帧就够，更快更稳

# -------------------------- 央视名称映射 ---------------------------------
CCTV_USE_MAPPING = True
CCTV_NAME_MAPPING = {
    "1": "综合",
    "2": "财经",
    "3": "综艺",
    "4": "国际",
    "5": "体育",
    "5+": "体育赛事",
    "6": "电影",
    "7": "国防军事",
    "8": "电视剧",
    "9": "纪录",
    "10": "科教",
    "11": "戏曲",
    "12": "社会与法",
    "13": "新闻",
    "14": "少儿",
    "15": "音乐",
    "16": "奥林匹克",
    "17": "农业农村",
}

# -------------------------- GitHub 专用负载控制 ---------------------------
DELAY_BETWEEN_IPS = 3.0
DELAY_AFTER_CLICK = 0.8
MAX_CHANNELS_PER_IP = 40         # 每个IP不要抓太多，防卡死

# ============================================================================
# 核心代码（轻量稳定版）
# ============================================================================

LAUNCH_ARGS = {
    "headless": HEADLESS,
    "args": [
        "--no-sandbox",
        "--disable-dev-shm-usage",
        "--disable-gpu",
        "--disable-extensions",
        "--no-first-run",
        "--no-default-browser-check"
    ]
}

def ensure_browser_installed():
    try:
        import playwright
    except ImportError:
        print("❌ 请先执行: pip install playwright")
        sys.exit(1)

def build_classifier():
    patterns = []
    for rule in CATEGORY_RULES:
        if not rule["keywords"]:
            continue
        pattern = "|".join(re.escape(kw.lower()) for kw in rule["keywords"])
        patterns.append((rule["name"], re.compile(pattern)))
    def classify(name: str) -> str | None:
        name_lower = name.lower()
        for group_name, pattern in patterns:
            if pattern.search(name_lower):
                return group_name
        return None
    return classify

classify_channel = build_classifier()

def normalize_cctv(name: str) -> str:
    name_lower = name.lower()
    if "cctv5+" in name_lower:
        if CCTV_USE_MAPPING and "5+" in CCTV_NAME_MAPPING:
            return f"CCTV-5+{CCTV_NAME_MAPPING['5+']}"
        else:
            return "CCTV-5+"
    cctv_match = re.search(r'(cctv)[-\s]?(\d{1,3})', name_lower)
    if cctv_match:
        number = cctv_match.group(2)
        if CCTV_USE_MAPPING and number in CCTV_NAME_MAPPING:
            return f"CCTV-{number}{CCTV_NAME_MAPPING[number]}"
        else:
            return f"CCTV-{number}"
    cetv_match = re.search(r'(cetv)[-\s]?(\d)', name_lower)
    if cetv_match:
        return f"CETV-{cetv_match.group(2)}"
    return name

def clean_chinese_only(name: str) -> str:
    return re.sub(r'[^\u4e00-\u9fff]', '', name)

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
        await locator.click(force=True, timeout=timeout)
        return True
    except:
        try:
            await locator.evaluate('el => el.click()')
            return True
        except:
            return False

# ====================== 【轻量精准测速 · GitHub 专用】 ======================
async def test_single_stream(url, semaphore):
    async with semaphore:
        try:
            loop = asyncio.get_event_loop()
            t_start = loop.time()

            proc = await asyncio.create_subprocess_exec(
                sys.executable, "-c",
                f'''
import sys
import cv2
url = "{url}"
cap = cv2.VideoCapture(url)
if not cap.isOpened():
    sys.exit(1)
ok = 0
for _ in range({MIN_SUCCESS_FRAMES}):
    ret, frm = cap.read()
    if ret: ok +=1
if ok < {MIN_SUCCESS_FRAMES}:
    cap.release()
    sys.exit(1)
w = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
h = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
cap.release()
print(w, h)
''',
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
                stdin=asyncio.subprocess.DEVNULL
            )

            try:
                stdout, _ = await asyncio.wait_for(proc.communicate(), TEST_TIMEOUT)
            except asyncio.TimeoutError:
                try:
                    proc.kill()
                except:
                    pass
                return None

            cost_ms = round((loop.time() - t_start) * 1000)
            if proc.returncode != 0:
                return None

            try:
                w, h = map(int, stdout.decode().strip().split())
            except:
                return None

            return (cost_ms, w, h, url)

        except Exception:
            return None

async def batch_test_urls(url_list):
    sem = asyncio.Semaphore(CONCURRENCY_LIMIT)
    tasks = [test_single_stream(u, sem) for u in url_list]
    results = await asyncio.gather(*tasks)
    return [r for r in results if r is not None]

# ====================== 主流程 ======================
async def main():
    ensure_browser_installed()
    async with async_playwright() as p:
        browser = await getattr(p, BROWSER_TYPE).launch(**LAUNCH_ARGS)
        context = await browser.new_context(viewport={"width":1280,"height":720})
        page = await context.new_page()

        print("🌐 打开页面...")
        try:
            await page.goto(TARGET_URL, timeout=90000)
            await page.wait_for_load_state("networkidle", timeout=30000)
        except:
            pass

        if ENGINE_SELECTOR:
            el = page.locator(ENGINE_SELECTOR).first
            if await el.count()>0:
                await robust_click(el, description="引擎搜索")
                await asyncio.sleep(DELAY_AFTER_CLICK)

        if MCAST_SELECTOR:
            tab = page.locator(MCAST_SELECTOR).first
            try:
                await tab.wait_for(state="attached", timeout=15000)
                await robust_click(tab, description="组播提取")
                await asyncio.sleep(DELAY_AFTER_CLICK)
            except:
                pass

        if START_SELECTOR:
            btn = page.locator(START_SELECTOR).first
            if await btn.count()>0:
                await robust_click(btn, description="开始")
                await asyncio.sleep(DELAY_AFTER_CLICK)

        print("⏳ 等待扫描结果...")
        try:
            await page.locator("div.item-title:text-matches('\\d+\\.\\d+\\.\\d+\\.\\d+')").first.wait_for(timeout=60000)
        except:
            pass

        result_rows = page.locator("div.ios-list-item").filter(has_text="频道:")
        total = await result_rows.count()
        process_cnt = min(total, MAX_IPS) if MAX_IPS>0 else total
        print(f"📋 共{total}个IP，处理前{process_cnt}个")

        raw_entries = []
        for i in range(process_cnt):
            row = result_rows.nth(i)
            ip_text = await row.locator("div.item-title").first.inner_text()
            ip_text = ip_text.strip()
            if not re.match(r'^\d+\.\d+\.\d+\.\d+$', ip_text):
                continue
            print(f"\n📌 [{i+1}/{process_cnt}] {ip_text}")

            menu = row.locator("button:has(i.fa-list), button:has-text('≡')").first
            if await menu.count()>0:
                await robust_click(menu, description="菜单")
            else:
                try:
                    await row.locator("div.item-title").first.click(timeout=5000)
                except:
                    pass
            await asyncio.sleep(0.6)

            try:
                await page.locator(".modal-dialog").first.wait_for(state="visible", timeout=8000)
            except:
                await page.keyboard.press("Escape")
                continue

            items = page.locator(".modal-dialog .item-content")
            item_cnt = await items.count()
            extract_cnt = min(item_cnt, MAX_CHANNELS_PER_IP) if MAX_CHANNELS_PER_IP>0 else item_cnt

            for j in range(extract_cnt):
                it = items.nth(j)
                name = await it.locator(".item-title").first.inner_text()
                link = await it.locator(".item-subtitle").first.inner_text()
                name = name.strip()
                link = link.strip()
                if not name or not link:
                    continue
                norm = normalize_cctv(name)
                group = classify_channel(norm)
                if not group:
                    continue
                if group == "央视频道":
                    final = norm
                elif ENABLE_CHINESE_CLEAN:
                    final = clean_chinese_only(name) or name
                else:
                    final = name
                raw_entries.append((group, final, link))

            await page.keyboard.press("Escape")
            await asyncio.sleep(DELAY_AFTER_CLICK)
            if i < process_cnt-1:
                await asyncio.sleep(DELAY_BETWEEN_IPS)

        await browser.close()

    # 去重
    channel_map = defaultdict(list)
    seen = set()
    for g,n,u in raw_entries:
        key = (g,n,u)
        if ENABLE_DEDUPLICATION and key in seen:
            continue
        seen.add(key)
        channel_map[(g,n)].append(u)

    # 测速 + 过滤
    final_list = []
    for (group,name), urls in channel_map.items():
        print(f"\n🚀 测速：{name}（{len(urls)}条）")
        tested = await batch_test_urls(urls)

        passed = []
        for ms, w, h, url in tested:
            if ms > MAX_ALLOWED_DELAY:
                continue
            if w < MIN_WIDTH or h < MIN_HEIGHT:
                continue
            passed.append((ms, url))
            print(f"    ✅ {ms} ms | {w}x{h}")

        passed.sort(key=lambda x:x[0])
        top = passed[:MAX_LINKS_PER_CHANNEL]
        for ms, url in top:
            final_list.append((group,name,url))

    # 分组排序
    grouped = defaultdict(list)
    for g,n,u in final_list:
        grouped[g].append((n,u))

    cctv_group = next((k for k in grouped if "央视" in k), None)
    if cctv_group:
        def cctv_key(x):
            m = re.search(r'CCTV-(\d+)',x[0])
            return int(m.group(1)) if m else 999
        grouped[cctv_group].sort(key=cctv_key)

    for g in grouped:
        if g != cctv_group:
            grouped[g].sort(key=lambda x:x[0])

    # 输出
    with open(OUTPUT_DIR/OUTPUT_M3U_FILENAME,"w",encoding="utf-8") as f:
        f.write("#EXTM3U\n")
        for g in GROUP_ORDER:
            if g not in grouped: continue
            for n,u in grouped[g]:
                f.write(f'#EXTINF:-1 group-title="{g}",{n}\n{u}\n')

    with open(OUTPUT_DIR/OUTPUT_TXT_FILENAME,"w",encoding="utf-8") as f:
        for g in GROUP_ORDER:
            if g not in grouped: continue
            f.write(f"{g},#genre#\n")
            for n,u in grouped[g]:
                f.write(f"{n},{u}\n")
            f.write("\n")

    print(f"\n🎉 GitHub 免费版运行完成！有效源：{len(final_list)} 条")
    print(f"✅ 优化：低并发+轻量帧+防卡死+适配Actions")

if __name__ == "__main__":
    asyncio.run(main())
