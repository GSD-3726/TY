#!/usr/bin/env python3

"""
IPTV 组播提取工具 — 神级优化版
（保留原配置 + 电视级质量评分系统）
"""

import asyncio
import json
import logging
import os
import re
import sys
import time
import shutil
import datetime
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple, Any
from urllib.parse import urljoin
import functools

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# ============================================================================
# ======================== 【配置区（完全保留你的参数）】 =====================
# ============================================================================

TARGET_URL = "https://iptv.809899.xyz"

HEADLESS = True
BROWSER_TYPE = "chromium"
MAX_IPS = 20
PAGE_LOAD_TIMEOUT = 120000

OUTPUT_DIR = Path(__file__).parent
OUTPUT_M3U_FILENAME = OUTPUT_DIR / "iptv_channels.m3u"
OUTPUT_TXT_FILENAME = OUTPUT_DIR / "iptv_channels.txt"

MAX_LINKS_PER_CHANNEL = 10

ENABLE_FFMPEG_TEST = True
FFMPEG_PATH = "ffmpeg"
FFMPEG_TEST_DURATION = 8
FFMPEG_CONCURRENCY = 2

MIN_AVG_FPS = 24
MIN_FRAMES = 210

# ============================================================================
# ======================== 频道分类（完全保留） ==============================
# ============================================================================

CATEGORY_RULES = [
    {"name": "4K专区", "keywords": ["4k"]},
    {"name": "央视频道", "keywords": ["cctv", "中央"]},
    {"name": "卫视频道", "keywords": ["卫视", "凤凰", "tvb"]},
    {"name": "电影频道", "keywords": ["电影", "chc"]},
    {"name": "儿童频道", "keywords": ["少儿", "动画", "卡通"]},
    {"name": "轮播频道", "keywords": ["轮播"]},
]

GROUP_ORDER = ["央视频道", "卫视频道", "电影频道", "4K专区", "儿童频道", "轮播频道"]

# ============================================================================
# ======================== 日志 =============================================
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger("IPTV-GOD")

# ============================================================================
# ======================== IPTV 神级评分模型 =================================
# ============================================================================

class IPTVGodScore:

    def final_score(self, fps, frames, elapsed):

        score = 0

        # FPS评分
        score += min(fps / 30 * 40, 40)

        # 解码帧数
        score += min(frames / 400 * 40, 40)

        # 连接时间
        score += max(0, 20 - elapsed)

        return max(0, min(score, 100))


score_engine = IPTVGodScore()

# ============================================================================
# ======================== 工具函数 ==========================================
# ============================================================================

def classify_channel(name: str):

    name_lower = name.lower()

    for rule in CATEGORY_RULES:
        for kw in rule["keywords"]:
            if kw in name_lower:
                return rule["name"]

    return "轮播频道"


# ============================================================================
# ======================== 神级 FFmpeg 三段测速 ================================
# ============================================================================

async def ffmpeg_god_test(url: str):

    if not shutil.which(FFMPEG_PATH):
        return {"ok": False, "score": 0}

    stages = [5, 10, 20]

    total_frames = 0
    total_fps = 0

    start_time = time.time()

    for duration in stages:

        cmd = [
            FFMPEG_PATH,
            "-hide_banner",
            "-loglevel", "error",

            "-fflags", "nobuffer",
            "-flags", "low_delay",

            "-analyzeduration", "2000000",
            "-probesize", "2000000",

            "-rw_timeout", "8000000",

            "-i", url,
            "-t", str(duration),
            "-f", "null",
            "-"
        ]

        try:

            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE
            )

            _, stderr = await proc.communicate()

            out = stderr.decode(errors="ignore")

            frames = sum(map(int, re.findall(r"frame=\s*(\d+)", out) or [0]))

            fps_list = list(map(float,
                                re.findall(r"fps=\s*([\d.]+)", out) or [0]))

            total_frames += frames
            if fps_list:
                total_fps += max(fps_list)

        except:
            continue

    elapsed = time.time() - start_time

    avg_fps = total_fps / len(stages)

    score = score_engine.final_score(avg_fps, total_frames, elapsed)

    return {
        "ok": score > 70,
        "score": round(score, 2),
        "fps": round(avg_fps, 2),
        "frames": total_frames,
        "elapsed": round(elapsed, 2)
    }


# ============================================================================
# ======================== 主流程 ===========================================
# ============================================================================

async def main():

    channel_map = defaultdict(list)

    async with async_playwright() as p:

        browser = await getattr(p, BROWSER_TYPE).launch(
            headless=HEADLESS,
            args=["--no-sandbox"]
        )

        page = await browser.new_page()

        logger.info(f"访问 {TARGET_URL}")

        await page.goto(
            TARGET_URL,
            timeout=PAGE_LOAD_TIMEOUT,
            wait_until="networkidle"
        )

        await asyncio.sleep(5)

        rows = page.locator("div.ios-list-item")

        total_rows = await rows.count()

        logger.info(f"找到地址 {total_rows}")

        process_count = min(total_rows, MAX_IPS) if MAX_IPS > 0 else total_rows

        raw_entries = []

        for i in range(process_count):

            try:

                row = rows.nth(i)

                name = await row.locator(".item-title").inner_text()
                link = await row.locator(".item-subtitle").inner_text()

                name = name.strip()
                link = link.strip()

                if not link.startswith("http"):
                    link = "http://" + link

                group = classify_channel(name)

                if ENABLE_FFMPEG_TEST:

                    result = await ffmpeg_god_test(link)

                    if not result["ok"]:
                        continue

                    score = result["score"]

                    channel_map[(group, name)].append((link, score))

            except:
                continue

        # 排序
        final_map = defaultdict(list)

        for (group, name), lst in channel_map.items():

            lst.sort(key=lambda x: -x[1])

            for url, _ in lst[:MAX_LINKS_PER_CHANNEL]:
                final_map[group].append((name, url))

        # ===============================
        # 输出 M3U
        # ===============================

        with open(OUTPUT_M3U_FILENAME, "w", encoding="utf-8") as f:

            f.write("#EXTM3U\n")

            for group in GROUP_ORDER:

                if group not in final_map:
                    continue

                for name, url in final_map[group]:
                    f.write(
                        f'#EXTINF:-1 group-title="{group}",{name}\n{url}\n'
                    )

        logger.info("✅ 神级 IPTV 生成完成")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
