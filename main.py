#!/usr/bin/env python3

import asyncio
import json
import logging
import re
from collections import defaultdict

from playwright.async_api import async_playwright

# =============================
# 配置
# =============================

TARGET_URL = "https://iptv.809899.xyz"
HEADLESS = True

# =============================
# 日志
# =============================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(message)s"
)

logger = logging.getLogger("IPTV-PRO")


# =============================
# ⭐ 工业级终极抓取器
# =============================

async def capture_channels(page):

    channels = []

    # ⭐ 监听所有 API 响应（核心！！！）
    async def on_response(response):

        try:
            url = response.url.lower()

            # IPTV Toolbox 常见接口特征
            if any(x in url for x in [
                "hotel",
                "multicast",
                "channel",
                "search",
                "list"
            ]):

                try:
                    data = await response.json()

                    # 兼容多数据结构
                    if isinstance(data, dict):
                        for k in data.values():
                            if isinstance(k, list):
                                channels.extend(k)

                    elif isinstance(data, list):
                        channels.extend(data)

                except:
                    pass

        except:
            pass

    page.on("response", on_response)

    logger.info("加载页面中...")

    await page.goto(
        TARGET_URL,
        wait_until="networkidle",
        timeout=120000
    )

    # ⭐ 等待数据加载
    for _ in range(8):
        await asyncio.sleep(2)

    return channels


# =============================
# ⭐ 频道清洗
# =============================

def clean_channels(raw_list):

    result = []

    for item in raw_list:

        try:

            name = item.get("name") or item.get("title")
            url = item.get("url") or item.get("link")

            if not name or not url:
                continue

            url = url.strip()

            if not url.startswith("http"):
                url = "http://" + url

            result.append({
                "name": name.strip(),
                "url": url
            })

        except:
            continue

    return result


# =============================
# ⭐ 主流程
# =============================

async def main():

    async with async_playwright() as p:

        browser = await p.chromium.launch(
            headless=HEADLESS,
            args=["--no-sandbox"]
        )

        page = await browser.new_page()

        logger.info("开始抓取 IPTV 数据")

        raw_channels = await capture_channels(page)

        logger.info(f"原始数据 {len(raw_channels)} 条")

        channels = clean_channels(raw_channels)

        logger.info(f"清洗后 {len(channels)} 条")

        # ⭐ 输出 M3U
        with open("iptv_pro.m3u", "w", encoding="utf8") as f:

            f.write("#EXTM3U\n")

            for ch in channels:
                f.write(
                    f'#EXTINF:-1,{ch["name"]}\n{ch["url"]}\n'
                )

        logger.info("✅ IPTV 工业级抓取完成")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
