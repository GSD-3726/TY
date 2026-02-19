#!/usr/bin/env python3
"""
IPTV 组播提取工具 - 结构分析+稳定定位+接口优化 终极版
配置项全注释，修改只看顶部配置区即可
"""

import asyncio
import os
import re
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Set
from urllib.parse import urljoin

import aiohttp
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# ==============================================
# ================= 配置区（全中文说明）==================
# ==============================================

# 目标网站地址（要爬的 IPTV 网站）
TARGET_URL = os.getenv("TARGET_URL", "https://iptv.809899.xyz")

# 输出文件保存位置（默认脚本所在文件夹，不用改）
OUTPUT_DIR = Path(__file__).parent

# 最多爬多少个IP（越大越慢，建议 5~20）
MAX_IPS = int(os.getenv("MAX_IPS", "10"))

# 无头模式：True=不显示浏览器窗口，False=显示窗口（调试用）
HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"

# 浏览器类型（默认chromium，不用改）
BROWSER_TYPE = os.getenv("BROWSER_TYPE", "chromium")

# 页面加载超时（毫秒），网站慢就改大：60000=60秒
PAGE_LOAD_TIMEOUT = 60000

# 点击按钮后等待时间（秒），防止点太快页面没反应
DELAY_AFTER_CLICK = 0.5

# 切换下一个IP前等待秒数，防卡顿/防封
DELAY_BETWEEN_IPS = 3.0

# 每个IP最多提取多少频道（0=不限制）
MAX_CHANNELS_PER_IP = 0

# 脚本最大运行时间（秒），防止卡死
SCRIPT_TIMEOUT = 3000

# -------------------------- 测速设置 --------------------------
# 是否开启测速排序（True=测速，False=直接导出不测速）
ENABLE_SPEED_TEST = True

# 测速并发数（越大越快，建议 5~15）
SPEED_TEST_CONCURRENCY = 10

# 最低合格速度（Mbps），低于这个值直接丢掉
MIN_SPEED_FACTOR = 1.5

# 每个频道最多保留几条链接（按速度从快到慢取前N条）
MAX_LINKS_PER_CHANNEL = 10

# -------------------------- 分辨率筛选 --------------------------
# 是否开启分辨率过滤
ENABLE_RESOLUTION_FILTER = True

# 最小宽度：1920=1080P，1280=720P
MIN_RESOLUTION_WIDTH = 1920

# 最小高度：1080=1080P，720=720P
MIN_RESOLUTION_HEIGHT = 1080

# 无分辨率信息时，是否保留最快链接（建议True）
FALLBACK_TO_SPEED_WHEN_NO_RESOLUTION = True

# -------------------------- 输出文件名 --------------------------
OUTPUT_M3U_FILENAME = "iptv_channels.m3u"   # 电视/盒子通用格式
OUTPUT_TXT_FILENAME = "iptv_channels.txt"   # 文本直播源格式

# -------------------------- 频道分类规则 --------------------------
CATEGORY_RULES = [
    {"name": "4K专区",      "keywords": ["4k"]},
    {"name": "央视频道",    "keywords": ["cctv", "cetv", "中央"]},
    {"name": "卫视频道",    "keywords": ["卫视", "凤凰", "tvb", "湖南", "浙江", "江苏", "东方", "北京", "深圳", "山东"]},
    {"name": "电影频道",    "keywords": ["电影", "影院", "chc"]},
    {"name": "轮播频道",    "keywords": ["轮播"]},
    {"name": "儿童频道",    "keywords": ["少儿", "动画", "卡通", "金鹰", "卡酷"]},
]

# 频道分组在文件里的显示顺序
GROUP_ORDER = ["央视频道", "卫视频道", "电影频道", "4K专区", "儿童频道", "轮播频道"]

# ==============================================
# ================= 以下为核心代码，一般不用改 =================
# ==============================================

IP_PATTERN = re.compile(r'^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$')
CCTV_PATTERN = re.compile(r'(cctv)[-\s]?(\d{1,3})', re.IGNORECASE)
CETV_PATTERN = re.compile(r'(cetv)[-\s]?(\d)', re.IGNORECASE)
CHINESE_ONLY_PATTERN = re.compile(r'[^\u4e00-\u9fff]')

def build_classifier():
    compiled = []
    for rule in CATEGORY_RULES:
        if not rule["keywords"]:
            continue
        pattern = re.compile("|".join(re.escape(kw.lower()) for kw in rule["keywords"]))
        compiled.append((rule["name"], pattern))
    return lambda name: next((g for g, pat in compiled if pat.search(name.lower())), None)
classify_channel = build_classifier()

def normalize_cctv(name: str) -> str:
    name_lower = name.lower()
    if "cctv5+" in name_lower:
        return "CCTV-5+体育赛事"
    m = CCTV_PATTERN.search(name_lower)
    if m:
        num = m.group(2)
        mapping = {
            "1":"综合","2":"财经","3":"综艺","4":"国际","5":"体育",
            "5+":"体育赛事","6":"电影","7":"国防军事","8":"电视剧",
            "9":"纪录","10":"科教","11":"戏曲","12":"社会与法",
            "13":"新闻","14":"少儿","15":"音乐","16":"奥林匹克","17":"农业农村"
        }
        return f"CCTV-{num}{mapping.get(num, '')}"
    m = CETV_PATTERN.search(name_lower)
    if m:
        return f"CETV-{m.group(2)}"
    return name

def clean_chinese(name):
    return CHINESE_ONLY_PATTERN.sub('', name)

# ====================== 测速核心 ======================
async def fetch_url(session, url, timeout):
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
            if r.status == 200:
                return await r.read()
    except:
        return None

async def resolve_m3u8(session, url, timeout):
    data = await fetch_url(session, url, timeout)
    if not data:
        return None, None, []
    lines = data.decode('utf-8', 'ignore').splitlines()
    base = url[:url.rfind('/')+1] if '/' in url else url
    best_w, best_h, best_uri = 0,0,None
    i=0
    while i < len(lines):
        li = lines[i].strip()
        if li.startswith('#EXT-X-STREAM-INF:'):
            m = re.search(r'RESOLUTION=(\d+)x(\d+)', li)
            w,h = int(m.group(1)), int(m.group(2)) if m else (0,0)
            if i+1 < len(lines):
                uri = lines[i+1].strip()
                if w*h > best_w*best_h:
                    best_w,best_h,best_uri = w,h,uri
            i += 2
        else:
            i += 1
    if best_uri:
        return await resolve_m3u8(session, urljoin(base, best_uri), timeout)
    ts = []
    for li in lines:
        li = li.strip()
        if li and not li.startswith('#'):
            ts.append(urljoin(base, li))
    return best_w, best_h, ts

async def test_speed_ts(url):
    try:
        async with aiohttp.ClientSession() as s:
            w,h,ts = await resolve_m3u8(s, url, 1)
            if not ts:
                return None,None,None
            tb,tt = 0,0.0
            for u in ts[:3]:
                t0 = time.monotonic()
                d = await fetch_url(s,u,1)
                el = time.monotonic()-t0
                if d and el>0:
                    tb += len(d)
                    tt += el
            if tt == 0:
                return None,None,None
        mbps = (tb/tt)*8/1e6
        return mbps,w,h
    except:
        return None,None,None

async def test_speed_direct(url):
    try:
        async with aiohttp.ClientSession() as s:
            t0 = time.monotonic()
            tb = 0
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=4)) as r:
                if r.status!=200:
                    return None
                while True:
                    c = await r.content.read(8192)
                    if not c: break
                    tb += len(c)
                    if time.monotonic()-t0 >=2: break
            el = time.monotonic()-t0
            if el <=0: return None
        return (tb/el)*8/1e6
    except:
        return None

async def task_speed(url, g, n, sem):
    async with sem:
        if '.m3u8' in url.lower():
            sp,w,h = await test_speed_ts(url)
            if sp is None or sp < MIN_SPEED_FACTOR:
                return None
            ok = True
            if ENABLE_RESOLUTION_FILTER:
                if w is None or h is None:
                    ok = FALLBACK_TO_SPEED_WHEN_NO_RESOLUTION
                else:
                    ok = (w>=MIN_RESOLUTION_WIDTH and h>=MIN_RESOLUTION_HEIGHT)
            return (url,g,n,sp,ok)
        else:
            sp = await test_speed_direct(url)
            if sp is None or sp < MIN_SPEED_FACTOR:
                return None
            ok = FALLBACK_TO_SPEED_WHEN_NO_RESOLUTION if ENABLE_RESOLUTION_FILTER else True
            return (url,g,n,sp,ok)

async def run_speed_test(channel_map):
    total = sum(len(v) for v in channel_map.values())
    print(f"🚀 测速开始：{total} 条")
    sem = asyncio.Semaphore(SPEED_TEST_CONCURRENCY)
    tasks = [task_speed(u,g,n,sem) for (g,n),us in channel_map.items() for u in us]
    res = []
    done = 0
    printed = set()
    for coro in asyncio.as_completed(tasks):
        item = await coro
        if item:
            res.append(item)
        done += 1
        pct = int((done/len(tasks))*100)
        for s in [10,20,30,40,50,60,70,80,90,100]:
            if pct>=s and s not in printed:
                print(f"测速进度：{s}%")
                printed.add(s)
    out = defaultdict(list)
    temp = defaultdict(list)
    for u,g,n,sp,ok in res:
        temp[(g,n)].append((u,sp,ok))
    for key,items in temp.items():
        items.sort(key=lambda x:x[1], reverse=True)
        good = [u for u,sp,ok in items if ok]
        if good:
            out[key] = good[:MAX_LINKS_PER_CHANNEL]
        else:
            out[key] = [u for u,sp,ok in items][:MAX_LINKS_PER_CHANNEL]
    print(f"✅ 测速完成，保留 {sum(len(v) for v in out.values())} 条")
    return out

# ====================== 页面提取（结构定位，不依赖文字）======================
async def robust_click(loc):
    try:
        await loc.scroll_into_view_if_needed(timeout=3000)
        await asyncio.sleep(0.2)
        await loc.click(force=True, timeout=5000)
        return True
    except:
        try:
            await loc.evaluate('el=>el.click()')
            return True
        except:
            return False

async def extract_one_ip(page, row):
    entries = []
    try:
        await robust_click(row.locator(".item-title").first)
        await asyncio.sleep(0.5)
        modal = page.locator(".modal-dialog").first
        await modal.wait_for(state="visible", timeout=5000)
        items = modal.locator(".item-content")
        cnt = await items.count()
        limit = cnt if MAX_CHANNELS_PER_IP==0 else min(cnt, MAX_CHANNELS_PER_IP)
        for i in range(limit):
            try:
                name = await items.nth(i).locator(".item-title").inner_text(timeout=3000)
                url = await items.nth(i).locator(".item-subtitle").inner_text(timeout=3000)
                name = name.strip()
                url = url.strip()
                if not name or not url:
                    continue
                cname = normalize_cctv(name)
                g = classify_channel(cname) or classify_channel(name)
                if not g:
                    continue
                fname = cname if g=="央视频道" else clean_chinese(name)
                entries.append((g, fname, url))
            except:
                continue
    except:
        pass
    return entries

async def wait_ip_list(page):
    for round in range(2):
        print(f"⏳ 等待 {round+1}/2 次，30 秒后检查数据")
        await asyncio.sleep(30)
        try:
            ok = await page.wait_for_function("""
                () => {
                    for(let e of document.querySelectorAll('div.item-title')){
                        if(/\\d+\\.\\d+\\.\\d+\\.\\d+/.test(e.innerText)) return true;
                    }
                    return false;
                }
            """, timeout=5000)
            if ok:
                print("✅ IP 列表已加载")
                return True
        except:
            print(f"⚠️ 第 {round+1} 次未加载到")
    print("❌ 继续执行")
    return False

# ====================== 主流程 ======================
async def main_core():
    print(f"[{time.strftime('%H:%M:%S')}] 启动")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS, args=["--no-sandbox"])
        ctx = await browser.new_context(viewport={"width":1920,"height":1080})
        page = await ctx.new_page()
        await page.goto(TARGET_URL, timeout=PAGE_LOAD_TIMEOUT, wait_until="networkidle")
        print("✅ 页面加载完成")

        # 结构定位：不依赖文字
        try:
            await page.locator("div.segment-item").nth(0).click(timeout=10000)
            await asyncio.sleep(DELAY_AFTER_CLICK)
            await page.locator("div.segment-item").nth(1).click(timeout=10000)
            await asyncio.sleep(DELAY_AFTER_CLICK)
        except:
            pass

        # 点击开始按钮
        try:
            await page.locator("button").filter(has_text="开始提取").first.click(timeout=10000)
            await asyncio.sleep(DELAY_AFTER_CLICK)
            print("✅ 已点击开始提取")
        except:
            print("⚠️ 开始按钮未找到，继续等待数据")

        # 等待数据 30s + 30s
        await wait_ip_list(page)

        # 读取 IP 列表
        rows = page.locator("div.ios-list-item").filter(has_text="频道:")
        total = await rows.count()
        take = min(total, MAX_IPS)
        print(f"📋 共 {total} 个IP，处理前 {take} 个")

        raw = []
        for i in range(take):
            ip = await rows.nth(i).locator(".item-title").inner_text()
            ip = ip.strip()
            if not IP_PATTERN.match(ip):
                continue
            print(f"📌 处理 {ip}")
            raw += await extract_one_ip(page, rows.nth(i))
            if i < take-1:
                await asyncio.sleep(DELAY_BETWEEN_IPS)

        # 去重
        channel_map = defaultdict(list)
        seen = set()
        for g,n,u in raw:
            if (g,n,u) in seen:
                continue
            seen.add((g,n,u))
            channel_map[(g,n)].append(u)
        print(f"📊 去重后频道：{len(channel_map)}")

        # 测速
        if ENABLE_SPEED_TEST and channel_map:
            channel_map = await run_speed_test(channel_map)

        # 排序输出
        final = []
        for (g,n),us in channel_map.items():
            for u in us:
                final.append((g,n,u))
        grouped = defaultdict(list)
        for g,n,u in final:
            grouped[g].append((n,u))

        # CCTV 按数字排序
        for g in grouped:
            if "央视" in g:
                grouped[g].sort(key=lambda x: int(re.search(r'CCTV-(\d+)',x[0]).group(1)) if re.search(r'CCTV-(\d+)',x[0]) else 999)

        # 写入文件
        with open(OUTPUT_DIR/OUTPUT_M3U_FILENAME,'w',encoding='utf-8') as f:
            f.write("#EXTM3U\n")
            for g in GROUP_ORDER:
                for n,u in grouped.get(g,[]):
                    f.write(f'#EXTINF:-1 group-title="{g}",{n}\n{u}\n')
        with open(OUTPUT_DIR/OUTPUT_TXT_FILENAME,'w',encoding='utf-8') as f:
            for g in GROUP_ORDER:
                if g in grouped:
                    f.write(f"{g},#genre#\n")
                    for n,u in grouped[g]:
                        f.write(f"{n},{u}\n")
                    f.write("\n")

        print(f"\n🎉 完成！导出 {len(final)} 条链接")
        await browser.close()

async def main():
    try:
        await asyncio.wait_for(main_core(), SCRIPT_TIMEOUT)
    except asyncio.TimeoutError:
        print("❌ 脚本超时退出")

if __name__ == "__main__":
    asyncio.run(main())
