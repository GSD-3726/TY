#!/usr/bin/env python3
"""
IPTV 组播提取 + FFmpeg 本地缓存加速版
生成指向 localhost 的 M3U，彻底解决卡顿
"""

import asyncio
import os
import re
import sys
import time
import statistics
import subprocess
import threading
import atexit
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from urllib.parse import urljoin

import aiohttp
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from aiohttp import web

# ============================================================================
# ======================== 【配置区】=========================================
# ============================================================================

TARGET_URL              = "https://iptv.809899.xyz"
HEADLESS                = True
MAX_IPS                 = 3

# 输出文件
OUTPUT_FINAL_M3U        = "local_iptv.m3u"
MAX_LINKS_PER_CHANNEL   = 1  # 中转模式只需要最好的1个源

# 本地服务配置
LOCAL_HOST              = "127.0.0.1"
LOCAL_PORT              = 8000
RELAY_DIR               = Path(__file__).parent / "relay_cache"
RELAY_DIR.mkdir(exist_ok=True)

# 测速配置 (保留，但只选第一)
ENABLE_SPEED_TEST       = True
SPEED_TEST_CONCURRENCY  = 5
MIN_STABLE_SPEED        = 1.0

# ============================================================================
# ========================== 频道分类与工具 (略作简化) =======================
# ============================================================================

PAGE_CONFIG = {
    "engine_search": ["引索搜索", "引擎搜索", "关键词搜索"],
    "multicast_tab": ["组播提取"],
    "start_button": ["开始播放", "开始搜索", "开始提取"],
}

CATEGORY_RULES = [
    {"name": "4K专区",      "keywords": ["4k"]},
    {"name": "央视频道",    "keywords": ["cctv", "cetv", "中央"]},
    {"name": "卫视频道",    "keywords": ["卫视", "凤凰", "tvb", "湖南", "浙江", "江苏", "东方"]},
    {"name": "电影频道",    "keywords": ["电影", "影院", "chc"]},
]

GROUP_ORDER = ["央视频道", "卫视频道", "电影频道", "4K专区"]

CCTV_NAME_MAPPING = {
    "1": "综合", "2": "财经", "3": "综艺", "4": "国际", "5": "体育",
    "5+": "体育赛事", "6": "电影", "7": "国防军事", "8": "电视剧",
    "9": "纪录", "10": "科教", "11": "戏曲", "12": "社会与法",
    "13": "新闻", "14": "少儿", "15": "音乐", "16": "奥林匹克",
    "17": "农业农村",
}

# 正则
IP_PATTERN = re.compile(r'^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$')
CCTV_PATTERN = re.compile(r'(cctv)[-\s]?(\d{1,3})', re.IGNORECASE)

def build_classifier():
    compiled = []
    for rule in CATEGORY_RULES:
        if not rule["keywords"]: continue
        pattern = re.compile("|".join(re.escape(kw.lower()) for kw in rule["keywords"]))
        compiled.append((rule["name"], pattern))
    return lambda name: next((group for group, pat in compiled if pat.search(name.lower())), None)

classify_channel = build_classifier()

def normalize_cctv(name: str) -> str:
    name_lower = name.lower()
    if "cctv5+" in name_lower: return "CCTV-5+体育赛事"
    cctv_match = CCTV_PATTERN.search(name_lower)
    if cctv_match:
        num = cctv_match.group(2)
        if num in CCTV_NAME_MAPPING: return f"CCTV-{num}{CCTV_NAME_MAPPING[num]}"
        return f"CCTV-{num}"
    return name

def build_selector(text_list, element_type="button"):
    if not text_list: return ""
    if len(text_list) == 1: return f"{element_type}:has-text('{text_list[0]}')"
    pattern = "|".join(re.escape(t) for t in text_list)
    return f"{element_type}:text-matches('{pattern}')"

ENGINE_SELECTOR = build_selector(PAGE_CONFIG["engine_search"], "a.sidebar-link,button,div.segment-item")
MCAST_SELECTOR = build_selector(PAGE_CONFIG["multicast_tab"], "div.segment-item")
START_SELECTOR = build_selector(PAGE_CONFIG["start_button"], "button")

async def robust_click(locator, timeout=10000):
    try:
        await locator.scroll_into_view_if_needed(timeout=3000)
        await asyncio.sleep(0.2)
        await locator.click(force=True, timeout=timeout)
        return True
    except:
        try:
            await locator.evaluate("el => el.click()")
            return True
        except:
            return False

# ============================================================================
# ========================= 测速核心 (简化版) ================================
# ============================================================================

async def fetch_url(session: aiohttp.ClientSession, url: str, timeout: int):
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
            if resp.status == 200: return await resp.read()
    except: pass
    return None

async def test_hls_simple(session: aiohttp.ClientSession, url: str):
    content = await fetch_url(session, url, 5)
    if not content: return False, 0.0
    txt = content.decode("utf-8", "ignore")
    lines = txt.splitlines()
    base = url[:url.rfind('/')+1] if '/' in url else url
    ts_list = []
    for line in lines:
        if not line.startswith("#") and line.strip():
            ts_list.append(urljoin(base, line.strip()))
            if len(ts_list) >= 2: break # 只测2个片
    
    if not ts_list: return False, 0.0
    
    total = 0.0
    ok = 0
    for u in ts_list[:1]: # 只测1个加速
        t0 = time.monotonic()
        d = await fetch_url(session, u, 10)
        cost = time.monotonic() - t0
        if d and cost > 0:
            speed = (len(d)*8) / cost / 1e6
            total += speed
            ok += 1
    return ok > 0, total / ok if ok else 0

async def test_speed_task(url: str, sem: asyncio.Semaphore):
    async with sem:
        try:
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as s:
                ok, sp = await test_hls_simple(s, url)
                return (url, sp) if ok else None
        except:
            return None

async def run_speed_test(channel_map):
    tasks = []
    sem = asyncio.Semaphore(SPEED_TEST_CONCURRENCY)
    flat = []
    for (g, n), urls in channel_map.items():
        for u in urls:
            tasks.append((g, n, u))

    print(f"开始测速：{len(tasks)} 条")
    results = await asyncio.gather(*[test_speed_task(u, sem) for (g,n,u) in tasks])

    out = defaultdict(list)
    for i, r in enumerate(results):
        if r:
            g, n, _ = tasks[i]
            url, sp = r
            out[(g,n)].append((url, sp))

    for k in out:
        out[k].sort(key=lambda x: -x[1])
        out[k] = [u for u,_ in out[k][:MAX_LINKS_PER_CHANNEL]]

    print(f"测速完成，保留 {len(out)} 个频道")
    return out

# ============================================================================
# ========================= FFmpeg 中转管理核心 ==============================
# ============================================================================

ffmpeg_processes = []

def cleanup_processes():
    print("\n正在关闭所有 FFmpeg 进程...")
    for p in ffmpeg_processes:
        try:
            p.terminate()
            p.wait(timeout=2)
        except:
            try: p.kill()
            except: pass
    print("清理完毕")

atexit.register(cleanup_processes)

def sanitize_filename(name):
    return re.sub(r'[\\/*?:"<>|]', "", name)

def start_ffmpeg_relay(source_url, channel_name):
    """启动单个 FFmpeg 进程"""
    safe_name = sanitize_filename(channel_name)
    channel_dir = RELAY_DIR / safe_name
    channel_dir.mkdir(exist_ok=True)
    
    # 输出 m3u8 路径
    output_m3u8 = channel_dir / "index.m3u8"
    
    # FFmpeg 参数：低延迟缓存，自动删除旧切片
    # -hls_flags delete_segments+append_list 防止磁盘爆满
    cmd = [
        "ffmpeg",
        "-y",
        "-fflags", "+genpts+discardcorrupt",
        "-rtsp_transport", "tcp",
        "-stimeout", "5000000",
        "-i", source_url,
        "-c", "copy",
        "-hls_time", "1.5",        # 切片时长
        "-hls_list_size", "4",     # 保留多少片
        "-hls_delete_threshold", "1",
        "-hls_flags", "delete_segments+append_list",
        "-method", "PUT",
        str(output_m3u8)
    ]
    
    # 启动进程 (不显示黑框，重定向输出)
    proc = subprocess.Popen(
        cmd, 
        stdout=subprocess.DEVNULL, 
        stderr=subprocess.DEVNULL,
        creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0
    )
    ffmpeg_processes.append(proc)
    return f"/{safe_name}/index.m3u8"

# ============================================================================
# ========================= 页面提取 (保留) ==================================
# ============================================================================

async def extract_one_ip(page, row):
    entries = []
    try:
        ip = await row.locator("div.item-title").first.inner_text(timeout=3000)
        ip = ip.strip()
        if not IP_PATTERN.match(ip): return []
        print(f"处理IP：{ip}")
    except: return []

    try:
        btn = row.locator("button:has(i.fa-list)").first
        if await btn.count() > 0: await robust_click(btn)
        else: await row.click(timeout=3000)
        await asyncio.sleep(0.5)

        modal = page.locator(".modal-dialog").first
        await modal.wait_for(state="visible", timeout=5000)
        items = modal.locator(".item-content")
        total = await items.count()

        for i in range(total):
            try:
                name = await items.nth(i).locator(".item-title").inner_text(timeout=2000)
                link = await items.nth(i).locator(".item-subtitle").inner_text(timeout=2000)
                name = name.strip()
                link = link.strip()
                if not name or not link: continue
                norm = normalize_cctv(name)
                group = classify_channel(norm)
                if not group: continue
                entries.append((group, norm, link))
            except: continue
    except: pass
    return entries

async def wait_data(page):
    for _ in range(2):
        print("等待30秒加载数据...")
        await asyncio.sleep(30)
        has = await page.evaluate('''()=>{
            for(let e of document.querySelectorAll('div.item-title')){
                if(/\\d+\\.\\d+\\.\\d+\\.\\d+/.test(e.innerText))return true;
            }
            return false;
        }''')
        if has: return True
    return False

# ============================================================================
# ========================= HTTP 服务器 ======================================
# ============================================================================

async def start_http_server():
    async def handle(request):
        return web.FileResponse(RELAY_DIR / request.path[1:])
    
    app = web.Application()
    app.add_routes([web.get("/{tail:.*}", handle)])
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, LOCAL_HOST, LOCAL_PORT)
    await site.start()
    print(f"\n本地服务器已启动: http://{LOCAL_HOST}:{LOCAL_PORT}")

# ============================================================================
# ========================= 主逻辑 ===========================================
# ============================================================================

async def main():
    # 1. 爬取数据
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS, args=["--no-sandbox"])
        ctx = await browser.new_context(viewport={"width":1920,"height":1080})
        page = await ctx.new_page()

        try:
            await page.goto(TARGET_URL, timeout=120000, wait_until="networkidle")
        except: pass

        if ENGINE_SELECTOR:
            e = page.locator(ENGINE_SELECTOR).first
            if await e.count() > 0: await robust_click(e)
        
        if MCAST_SELECTOR:
            t = page.locator(MCAST_SELECTOR).first
            await robust_click(t)
            
        if START_SELECTOR:
            b = page.locator(START_SELECTOR).first
            await robust_click(b)

        await wait_data(page)

        rows = page.locator("div.ios-list-item").filter(has_text="频道:")
        cnt = min(await rows.count(), MAX_IPS)
        raw = []
        for i in range(cnt):
            raw += await extract_one_ip(page, rows.nth(i))
            if i < cnt-1: await asyncio.sleep(1)
        await browser.close()

    # 2. 去重
    channel_map = defaultdict(list)
    seen = set()
    for g,n,u in raw:
        k = (g,n,u)
        if k in seen: continue
        seen.add(k)
        channel_map[(g,n)].append(u)

    # 3. 测速
    if ENABLE_SPEED_TEST and channel_map:
        channel_map = await run_speed_test(channel_map)

    if not channel_map:
        print("没有获取到有效频道，退出")
        return

    # 4. 启动 FFmpeg 中转
    print("\n正在启动 FFmpeg 中转进程 (这可能需要几秒钟)...")
    
    playlist_content = []
    playlist_content.append("#EXTM3U")
    
    # 按分组排序
    sorted_channels = []
    for g in GROUP_ORDER:
        for (group_name, channel_name), urls in channel_map.items():
            if group_name == g and urls:
                sorted_channels.append( (g, channel_name, urls[0]) )

    count = 0
    for group, name, url in sorted_channels:
        # 启动 FFmpeg
        local_path = start_ffmpeg_relay(url, name)
        full_url = f"http://{LOCAL_HOST}:{LOCAL_PORT}{local_path}"
        
        # 写入 M3U 内容
        playlist_content.append(f'#EXTINF:-1 group-title="{group}",{name}')
        playlist_content.append(full_url)
        count += 1
        print(f"[{count}] 中转中: {name}")
        
        # 稍微间隔防止瞬间启动太多 FFmpeg 卡死
        time.sleep(0.5) 

    # 5. 生成最终 M3U
    with open(OUTPUT_FINAL_M3U, "w", encoding="utf-8") as f:
        f.write("\n".join(playlist_content))
    
    print(f"\n成功！已生成 {count} 个频道。")
    print(f"请用播放器打开: {OUTPUT_FINAL_M3U}")
    print("\n注意：请不要关闭此窗口，关闭后中转将停止。")

    # 6. 启动 HTTP 服务并保持运行
    await start_http_server()
    
    # 永久挂起
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n用户退出")
