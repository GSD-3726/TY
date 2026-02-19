#!/usr/bin/env python3
"""
IPTV 组播提取工具 —— 全配置置顶版（按钮文字可自定义）
"""

# ==================== 必须的导入 ====================
import asyncio
import os
import re
import subprocess
import shutil
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Set

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# ============================================================================
# 【全部配置 - 完全置顶】
# ============================================================================

# ---------------------------- 基础设置 ------------------------------------
TARGET_URL = "https://iptv.809899.xyz"
OUTPUT_DIR = Path(__file__).parent

MAX_IPS = 5                                # 最多处理前 N 个IP（0=全部）
DELAY_BETWEEN_IPS = 3.0                    # 切换IP间隔秒
DELAY_AFTER_CLICK = 0.5                    # 点击后等待
MAX_CHANNELS_PER_IP = 0                    # 每个IP最多提取频道数（0=不限）

HEADLESS = True                            # 无头模式
BROWSER_TYPE = "chromium"
PAGE_LOAD_TIMEOUT = 60000                  # 页面加载超时

# ---------------------------- 页面自定义配置 -------------------------------
TAB_NAME = "组播提取"                       # 这里可以改成你要的 tab 文字
START_BTN_NAME = "开始提取"                     # 开始按钮文字（支持多个）
START_BTN_NAME2 = "提取"

# ---------------------------- 测速配置（MB/s） -----------------------------
ENABLE_SPEED_TEST = True
SPEED_TEST_CONCURRENCY = 10
SPEED_TEST_DURATION = 3
SPEED_TEST_TIMEOUT = 480

ENABLE_SPEED_FILTER = True
MIN_SPEED_MB = 0.5                         # 最小速度 0.5 MB/s

ENABLE_RESOLUTION_FILTER = True
MIN_WIDTH = 1280
MIN_HEIGHT = 720

# ---------------------------- 输出配置 ------------------------------------
MAX_LINKS_PER_CHANNEL = 10
OUTPUT_M3U = "iptv_channels.m3u"
OUTPUT_TXT = "iptv_channels.txt"

ENABLE_DEDUPLICATION = True
ENABLE_SCREENSHOTS = False

# ---------------------------- 分类与排序 -----------------------------------
GROUP_ORDER = [
    "央视频道", "卫视频道", "电影频道", "4K专区", "儿童频道", "轮播频道"
]

# ============================================================================
# 正则
# ============================================================================
IP_PATTERN       = re.compile(r'^(?:\d{1,3}\.){3}\d{1,3}$')
RESOLUTION_PATTERN = re.compile(r'(\d+)x(\d+)')
CCTV_PATTERN     = re.compile(r'cctv[-\s]?(\d{1,3})', re.I)
CETV_PATTERN     = re.compile(r'cetv[-\s]?(\d)', re.I)
CHINESE_ONLY_PATTERN = re.compile(r'[^\u4e00-\u9fa5a-zA-Z0-9\-]')

SCREENSHOT_DIR = OUTPUT_DIR / "screenshots"
if ENABLE_SCREENSHOTS and not SCREENSHOT_DIR.exists():
    SCREENSHOT_DIR.mkdir()

# ============================================================================
# 工具函数
# ============================================================================
def build_classifier():
    rules = [
        ("4K专区",      ["4k"]),
        ("央视频道",    ["cctv", "cetv", "中央"]),
        ("卫视频道",    ["卫视", "凤凰", "tvb", "湖南", "浙江", "江苏", "东方",
                         "北京", "深圳", "山东", "天津", "贵州", "四川", "黑龙江",
                         "安徽", "江西", "湖北", "东南", "辽宁", "广东", "河北"]),
        ("电影频道",    ["电影", "影院", "chc"]),
        ("轮播频道",    ["轮播"]),
        ("儿童频道",    ["少儿", "动画", "卡通", "金鹰", "卡酷"]),
    ]
    def classify(name: str) -> str:
        name = name.lower()
        for g, kws in rules:
            if any(kw in name for kw in kws):
                return g
        return ""
    return classify

classify = build_classifier()

def normalize_name(name: str) -> str:
    n = name.lower()
    if "cctv5+" in n:
        return "CCTV-5+体育赛事"
    m = CCTV_PATTERN.search(name)
    if m:
        num = m.group(1)
        return f"CCTV-{num}"
    m = CETV_PATTERN.search(name)
    if m:
        return f"CETV-{m.group(1)}"
    return name.strip()

def clean_text(s: str) -> str:
    return CHINESE_ONLY_PATTERN.sub("", s).strip()

async def robust_click(locator, timeout=5000, desc=""):
    try:
        await locator.scroll_into_view_if_needed()
        await locator.click(force=True, timeout=timeout)
        return True
    except Exception as e:
        return False

# ============================================================================
# 测速函数（MB/s）
# ============================================================================
async def test_speed(url: str, group: str, name: str, sem: asyncio.Semaphore):
    async with sem:
        cmd = [
            "ffmpeg", "-i", url,
            "-t", str(SPEED_TEST_DURATION),
            "-f", "null", "-",
            "-loglevel", "warning", "-stats"
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE
        )
        try:
            _, stderr = await asyncio.wait_for(proc.communicate(), timeout=10)
        except:
            try:
                proc.kill()
            except:
                pass
            return None

        err = stderr.decode("utf-8", "ignore")
        speed_mb = 0.0
        m = re.search(r"total size:\s*(\d+)", err.lower())
        if m:
            size_bytes = int(m[1])
            speed_bytes_per_sec = size_bytes / max(SPEED_TEST_DURATION, 0.5)
            speed_mb = speed_bytes_per_sec / 1048576

        w, h = None, None
        m = RESOLUTION_PATTERN.search(err)
        if m:
            w, h = int(m[1]), int(m[2])

        ok = True
        if ENABLE_SPEED_FILTER and speed_mb < MIN_SPEED_MB:
            ok = False
        if ENABLE_RESOLUTION_FILTER:
            if not w or not h or w < MIN_WIDTH or h < MIN_HEIGHT:
                ok = False
        if speed_mb <= 0:
            ok = False

        return (url, group, name, speed_mb, ok)

# ============================================================================
# 测速调度：日志只输出前3条 + 无合格保留最快
# ============================================================================
async def run_speed_test(channel_map: Dict):
    sem = asyncio.Semaphore(SPEED_TEST_CONCURRENCY)
    tasks = []
    for (g, n), urls in channel_map.items():
        for u in urls:
            tasks.append(test_speed(u, g, n, sem))

    print(f"🚀 开始测速：共 {len(tasks)} 条")
    results = []
    log_cnt = 0

    for i, coro in enumerate(asyncio.as_completed(tasks)):
        res = await coro
        if not res:
            continue
        results.append(res)
        if log_cnt < 3:
            url, g, n, s, ok = res
            mark = "✅" if ok else "⚠️"
            print(f"{mark} 测速 {i+1} | {g} | {n[:25]} | {s:.2f} MB/s")
            log_cnt += 1

    grouped = defaultdict(list)
    for r in results:
        url, g, n, s, ok = r
        grouped[(g, n)].append((url, s, ok))

    out = {}
    for key in grouped:
        items = sorted(grouped[key], key=lambda x: x[1], reverse=True)
        passed = [u for u, s, ok in items if ok]
        if not passed and items:
            passed = [items[0][0]]
        else:
            passed = passed[:MAX_LINKS_PER_CHANNEL]
        out[key] = passed

    print("✅ 测速完成\n")
    return out

# ============================================================================
# IP提取逻辑
# ============================================================================
async def extract_channels_from_ip(page, row):
    entries = []
    try:
        btn = row.locator("button:has(i.fa-list), button:has-text('≡')").first
        if await btn.count() > 0:
            await robust_click(btn, desc="menu btn")
        else:
            await robust_click(row.locator("div.item-title"), desc="ip title")

        await asyncio.sleep(DELAY_AFTER_CLICK)
        modal = page.locator(".modal-dialog").first
        await modal.wait_for(state="visible", timeout=8000)
        items = modal.locator(".item-content")
        total = await items.count()
        limit = total if MAX_CHANNELS_PER_IP == 0 else min(total, MAX_CHANNELS_PER_IP)

        for i in range(limit):
            try:
                name = await items.nth(i).locator(".item-title").inner_text(timeout=3000)
                link = await items.nth(i).locator(".item-subtitle").inner_text(timeout=3000)
                name = clean_text(name.strip())
                link = link.strip()
                if not name or not link:
                    continue
                norm = normalize_name(name)
                g = classify(norm)
                if not g:
                    continue
                entries.append((g, norm, link))
            except:
                continue
        await page.keyboard.press("Escape")
    except:
        pass
    return entries

# ============================================================================
# 主流程
# ============================================================================
async def main():
    global ENABLE_SPEED_TEST
    if ENABLE_SPEED_TEST and not shutil.which("ffmpeg"):
        print("未找到 ffmpeg，已关闭测速")
        ENABLE_SPEED_TEST = False

    async with async_playwright() as p:
        browser = await getattr(p, BROWSER_TYPE).launch(headless=HEADLESS, args=["--no-sandbox"])
        context = await browser.new_context(viewport={"width": 1920, "height": 1080})
        page = await context.new_page()

        print(f"🌐 打开：{TARGET_URL}")
        await page.goto(TARGET_URL, timeout=PAGE_LOAD_TIMEOUT, wait_until="networkidle")

        # ===================== 自定义 tab 与按钮 =====================
        try:
            tab = page.locator(f"div:has-text('{TAB_NAME}')").first
            await tab.click(timeout=15000)
            await asyncio.sleep(DELAY_AFTER_CLICK)
            
            start_btn = page.locator(f"button:has-text('{START_BTN_NAME}'), button:has-text('{START_BTN_NAME2}')").first
            await start_btn.click(timeout=10000)
            await asyncio.sleep(1)
        except Exception as e:
            print("⚠️ 自动切换页面失败，继续…")

        rows = page.locator("div.ios-list-item").filter(has_text="频道:")
        total_ips = await rows.count()
        process_cnt = MAX_IPS if MAX_IPS != 0 else total_ips
        process_cnt = min(process_cnt, total_ips)
        print(f"IP 总数：{total_ips}，本次处理：{process_cnt}")

        raw = []
        for i in range(process_cnt):
            r = rows.nth(i)
            ip_text = await r.locator("div.item-title").inner_text()
            ip_text = ip_text.strip()
            if not IP_PATTERN.match(ip_text):
                continue

            print(f"\n📶 处理 IP [{i+1}/{process_cnt}]：{ip_text}")
            entries = await extract_channels_from_ip(page, r)
            raw.extend(entries)
            print(f"   提取频道：{len(entries)} 个")

            if i < process_cnt - 1:
                await asyncio.sleep(DELAY_BETWEEN_IPS)

        # 去重
        channel_map = defaultdict(list)
        seen = set()
        for g, n, u in raw:
            if ENABLE_DEDUPLICATION:
                key = (g, n, u)
                if key in seen:
                    continue
                seen.add(key)
            channel_map[(g, n)].append(u)

        # 测速
        if ENABLE_SPEED_TEST and channel_map:
            channel_map = await run_speed_test(channel_map)

        # 最终列表
        final = []
        for (g, n), urls in channel_map.items():
            for u in urls:
                final.append((g, n, u))

        # 分组
        grouped_out = defaultdict(list)
        for g, n, u in final:
            grouped_out[g].append((n, u))

        # 央视排序
        cctv_group = next((k for k in grouped_out if "央视" in k), None)
        if cctv_group:
            def cctv_sort(item):
                match = re.search(r"CCTV-(\d+)", item[0])
                return int(match.group(1)) if match else 999
            grouped_out[cctv_group].sort(key=cctv_sort)

        # 输出 M3U
        with open(OUTPUT_M3U, "w", encoding="utf-8") as f:
            f.write("#EXTM3U\n")
            for g in GROUP_ORDER:
                for n, u in grouped_out.get(g, []):
                    f.write(f'#EXTINF:-1 group-title="{g}",{n}\n{u}\n')

        # 输出 TXT
        with open(OUTPUT_TXT, "w", encoding="utf-8") as f:
            for g in GROUP_ORDER:
                lst = grouped_out.get(g, [])
                if not lst:
                    continue
                f.write(f"{g},#genre#\n")
                for n, u in lst:
                    f.write(f"{n},{u}\n")
                f.write("\n")

        print(f"\n🎉 全部完成！导出有效频道：{len(final)} 条")
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
