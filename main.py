#!/usr/bin/env python3
"""
IPTV 组播提取工具 - 【打开速度优先·极简配置版】
优先：首包延迟（秒开） → 1080P+ → 网速
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

# 尝试导入 tqdm
try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False
    class tqdm:
        def __init__(self, *args, **kwargs): pass
        def update(self,n=1): pass
        def close(self): pass
        def __enter__(self): return self
        def __exit__(self,*args): self.close()

# ======================================================================================
# ================================== 【置顶可改参数】===================================
# 👇 只改这里，其他全部别动 👇
# ======================================================================================

# 1. 目标网站（一般不用改）
TARGET_URL = "https://iptv.809899.xyz"

# 2. 浏览器模式：电脑运行用 true，服务器/盒子运行必须用 true
HEADLESS = True

# 3. 一次抓多少个IP（越大源越多，但越慢）
MAX_IPS = 15

# 4. 【打开速度核心】首包超时：超过这个秒数直接丢弃（越小越严格）
FIRST_PACKET_TIMEOUT = 2.0

# 5. 最小速度（Mbps）：低于这个速度不要
MIN_SPEED_FACTOR = 2.0

# 6. 分辨率：必须 1080P+（不想开就改成 False）
ENABLE_RESOLUTION_FILTER = True
MIN_RESOLUTION_WIDTH  = 1920
MIN_RESOLUTION_HEIGHT = 1080

# 7. 每个频道保留最快几条（越少越快，建议 3~5）
MAX_LINKS_PER_CHANNEL = 5

# 8. 测速并发：越大越快，电脑差就改 10
SPEED_TEST_CONCURRENCY = 15

# 9. 输出文件名
OUTPUT_M3U_FILENAME = "iptv_fast_channels.m3u"
OUTPUT_TXT_FILENAME = "iptv_fast_channels.txt"

# ======================================================================================
# ================================== 【以下代码请勿修改】===============================
# ======================================================================================

PAGE_LOAD_TIMEOUT = 60000
DELAY_BETWEEN_IPS = 2.0
DELAY_AFTER_CLICK = 0.5
MAX_CHANNELS_PER_IP = 0
SCRIPT_TIMEOUT = 3600
ENABLE_SPEED_TEST = True
FALLBACK_TO_SPEED_WHEN_NO_RESOLUTION = False
BROWSER_TYPE = "chromium"
OUTPUT_DIR = Path(__file__).parent
ENABLE_CHINESE_CLEAN = True
ENABLE_DEDUPLICATION = True
ENABLE_SCREENSHOTS = False
CCTV_USE_MAPPING = True

PAGE_CONFIG = {
    "engine_search": ["引索搜索","引擎搜索","关键词搜索"],
    "multicast_tab": ["组播提取"],
    "start_button": ["开始播放","开始搜索","开始提取"],
}

CATEGORY_RULES = [
    {"name":"4K专区","keywords":["4k"]},
    {"name":"央视频道","keywords":["cctv","cetv","中央"]},
    {"name":"卫视频道","keywords":["卫视","凤凰","tvb","湖南","浙江","江苏","东方","北京","深圳","山东","天津","贵州","四川","黑龙江","安徽","江西","湖北","东南","辽宁","广东","河北","甘肃","新疆","西藏","兵团","重庆","云南","广西","山西","陕西","吉林","内蒙古","河南","宁夏","青海"]},
    {"name":"电影频道","keywords":["电影","影迷","家庭影院","动作电影","光影","动作影院","喜剧影院","经典电影","爱电影","chc"]},
    {"name":"轮播频道","keywords":["轮播频道","轮播"]},
    {"name":"儿童频道","keywords":["少儿","动画","卡通","kids","金鹰卡通","嘉佳卡通","卡酷少儿","动漫秀场","优优宝贝"]},
]

GROUP_ORDER = ["央视频道","卫视频道","电影频道","4K专区","儿童频道","轮播频道"]

CCTV_NAME_MAPPING = {
    "1":"综合","2":"财经","3":"综艺","4":"国际","5":"体育",
    "5+":"体育赛事","6":"电影","7":"国防军事","8":"电视剧",
    "9":"纪录","10":"科教","11":"戏曲","12":"社会与法",
    "13":"新闻","14":"少儿","15":"音乐","16":"奥林匹克","17":"农业农村"
}

IP_PATTERN = re.compile(r'^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$')
CCTV_PATTERN = re.compile(r'(cctv)[-\s]?(\d{1,3})', re.IGNORECASE)
CETV_PATTERN = re.compile(r'(cetv)[-\s]?(\d)', re.IGNORECASE)
CHINESE_ONLY_PATTERN = re.compile(r'[^\u4e00-\u9fff]')
SCREENSHOT_DIR = OUTPUT_DIR / "debug_screenshots"
SCREENSHOT_DIR.mkdir(exist_ok=True)

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
    if "cctv5+" in name_lower or "cctv5＋" in name_lower or "cctv5加" in name_lower:
        return f"CCTV-5+{CCTV_NAME_MAPPING['5+']}" if CCTV_USE_MAPPING else "CCTV5+"
    m = CCTV_PATTERN.search(name_lower)
    if m:
        num = m.group(2)
        return f"CCTV-{num}{CCTV_NAME_MAPPING.get(num,'')}" if CCTV_USE_MAPPING else f"CCTV-{num}"
    m = CETV_PATTERN.search(name_lower)
    if m:
        return f"CETV-{m.group(2)}" if CCTV_USE_MAPPING else f"CETV{m.group(2)}"
    return name

def clean_chinese_only(name: str) -> str:
    return CHINESE_ONLY_PATTERN.sub('', name)

def build_selector(text_list, element_type="button"):
    if not text_list: return ""
    if len(text_list)==1: return f"{element_type}:has-text('{text_list[0]}')"
    p = "|".join(re.escape(t) for t in text_list)
    return f"{element_type}:text-matches('{p}')"

ENGINE_SELECTOR = build_selector(PAGE_CONFIG["engine_search"], "a.sidebar-link,button,div.segment-item")
MCAST_SELECTOR  = build_selector(PAGE_CONFIG["multicast_tab"], "div.segment-item")
START_SELECTOR  = build_selector(PAGE_CONFIG["start_button"], "button")

async def robust_click(locator, timeout=10000):
    try:
        await locator.scroll_into_view_if_needed(timeout=5000)
        await asyncio.sleep(0.2)
        await locator.click(force=True, timeout=timeout)
        return True
    except:
        try:
            await locator.evaluate('el=>el.click()')
            return True
        except:
            return False

async def fetch_url(session, url, timeout):
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
            if r.status==200: return await r.read()
    except: pass
    return None

async def resolve_m3u8_playlist(session, url, timeout):
    c = await fetch_url(session, url, timeout)
    if not c: return None,None,[]
    lines = c.decode('utf-8','ignore').splitlines()
    base = url[:url.rfind('/')+1] if '/' in url else url
    bw,bh,bu = 0,0,None
    i=0
    while i<len(lines):
        line = lines[i].strip()
        if line.startswith('#EXT-X-STREAM-INF:'):
            m = re.search(r'RESOLUTION=(\d+)x(\d+)', line)
            w,h = int(m.group(1)),int(m.group(2)) if m else (0,0)
            if i+1<len(lines):
                u = lines[i+1].strip()
                if w*h>bw*bh: bw,bh,bu=w,h,u
            i+=2
        else: i+=1
    if bu: return await resolve_m3u8_playlist(session, urljoin(base,bu), timeout)
    ts = [urljoin(base,l.strip()) for l in lines if l.strip() and not l.startswith('#')]
    return bw,bh,ts

async def test_speed_ts(url):
    try:
        async with aiohttp.ClientSession() as s:
            w,h,ts = await resolve_m3u8_playlist(s,url,1)
            if not ts: return None,None,None
            tb,tt = 0,0.0
            for u in ts[:2]:
                t0=time.monotonic()
                d=await fetch_url(s,u,1)
                e=time.monotonic()-t0
                if d: tb+=len(d); tt+=e
            if tt<=0 or tb==0: return None,None,None
            return (tb/tt)*8/1_000_000, w, h
    except: return None,None,None

async def test_speed_fast(url,g,n,sem):
    async with sem:
        try:
            t0=time.monotonic()
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=2)) as s:
                async with s.head(url,allow_redirects=True): pass
            lat=time.monotonic()-t0
            if lat>FIRST_PACKET_TIMEOUT: return None
            if not url.lower().endswith('.m3u8'): return None
            sp,ww,hh=await test_speed_ts(url)
            if sp is None or sp<MIN_SPEED_FACTOR: return None
            rok = ww and hh and ww>=MIN_RESOLUTION_WIDTH and hh>=MIN_RESOLUTION_HEIGHT
            if not rok: return None
            return url,g,n,sp,lat,rok
        except: return None

async def run_speed_test(cm):
    total=sum(len(v) for v in cm.values())
    print(f"🚀 测速（优先秒开）共 {total} 条")
    sem=asyncio.Semaphore(SPEED_TEST_CONCURRENCY)
    tasks=[test_speed_fast(u,g,n,sem) for (g,n),us in cm.items() for u in us]
    res,fin,p=[],0,set()
    for t in asyncio.as_completed(tasks):
        r=await t
        if r: res.append(r)
        fin+=1
        pp=int(fin/len(tasks)*100)
        for s in [10,20,30,40,50,60,70,80,90,100]:
            if pp>=s and s not in p: print(f"测速进度：{s}%");p.add(s)
    sm=defaultdict(list)
    for r in res:
        u,g,n,sp,lt,ok=r
        sm[(g,n)].append((u,sp,lt))
    out=defaultdict(list)
    for k,its in sm.items():
        its.sort(key=lambda x:(x[2],-x[1]))
        out[k]=[u for u,_,_ in its[:MAX_LINKS_PER_CHANNEL]]
    print(f"✅ 测速完成，保留 {sum(len(v) for v in out.values())} 条秒开源")
    return out

async def extract_from_ip(page,row,ip):
    e=[]
    print(f"\n📌 处理IP：{ip}")
    mb=row.locator("button:has(i.fas.fa-list),button:has-text('≡')").first
    if await mb.count()>0: await robust_click(mb)
    else: await row.locator("div.item-title").first.click(timeout=5000)
    await asyncio.sleep(DELAY_AFTER_CLICK)
    md=page.locator(".modal-dialog").first
    try: await md.wait_for(state="visible",timeout=8000)
    except: return e
    items=md.locator(".item-content")
    cnt=await items.count()
    lim=cnt if MAX_CHANNELS_PER_IP<=0 else min(cnt,MAX_CHANNELS_PER_IP)
    for i in range(lim):
        it=items.nth(i)
        try:
            na=await it.locator(".item-title").first.inner_text(timeout=3000)
            ur=await it.locator(".item-subtitle").first.inner_text(timeout=3000)
        except: continue
        na,ur=na.strip(),ur.strip()
        if not na or not ur: continue
        nna=normalize_cctv(na)
        gr=classify_channel(nna) or classify_channel(na)
        if not gr: continue
        fna=nna if gr=="央视频道" else (clean_chinese_only(na) if ENABLE_CHINESE_CLEAN else na)
        if not fna: continue
        e.append((gr,fna,ur))
    return e

async def wait_for_ip_elements(page):
    for _ in range(2):
        print("⏳ 等待IP数据 30秒...")
        await asyncio.sleep(30)
        try:
            ok=await page.wait_for_function("""()=>{
                for(let e of document.querySelectorAll('div.item-title'))
                    if(/\\d+\\.\\d+\\.\\d+\\.\\d+/.test(e.innerText))return true;
                return false;
            }""",timeout=5000)
            if ok: print("✅ IP数据已加载");return
        except:continue
    print("⚠️ 未获取到IP，继续执行")

async def _main():
    print(f"[{time.strftime('%H:%M:%S')}] 🚀 启动【秒开优先】IPTV抓取")
    async with async_playwright() as p:
        b=await getattr(p,BROWSER_TYPE).launch(headless=HEADLESS,args=["--no-sandbox"])
        ctx=await b.new_context(viewport={"width":1920,"height":1080})
        page=await ctx.new_page()
        await page.goto(TARGET_URL,timeout=PAGE_LOAD_TIMEOUT,wait_until="networkidle")
        print("✅ 页面加载完成")
        for sel,desc in [(ENGINE_SELECTOR,"引擎搜索"),(MCAST_SELECTOR,"组播提取"),(START_SELECTOR,"开始提取")]:
            e=page.locator(sel).first
            if await e.count()>0: await robust_click(e);await asyncio.sleep(0.5);print(f"✅ {desc}")
        await wait_for_ip_elements(page)
        rows=page.locator("div.ios-list-item").filter(has_text="频道:")
        total=await rows.count()
        proc=min(total,MAX_IPS)
        print(f"📋 共{total}IP，处理前{proc}个")
        raw=[]
        for i in range(proc):
            r=rows.nth(i)
            ip=(await r.locator("div.item-title").first.inner_text()).strip()
            if not IP_PATTERN.match(ip): print(f"⚠️ 跳过无效IP：{ip}");continue
            raw.extend(await extract_from_ip(page,r,ip))
            if i<proc-1: await asyncio.sleep(DELAY_BETWEEN_IPS)
        cm=defaultdict(list)
        seen=set()
        for g,n,u in raw:
            if ENABLE_DEDUPLICATION:
                k=(g,n,u)
                if k in seen:continue
                seen.add(k)
            cm[(g,n)].append(u)
        print(f"📊 去重后：{len(cm)}频道，{sum(len(v) for v in cm.values())}条链接")
        if ENABLE_SPEED_TEST and cm: cm=await run_speed_test(cm)
        final=[]
        for (g,n),us in cm.items():
            for u in us: final.append((g,n,u))
        grouped=defaultdict(list)
        for g,n,u in final: grouped[g].append((n,u))
        cg=next((g for g in grouped if "央视" in g),None)
        if cg: grouped[cg].sort(key=lambda x:int(re.search(r"CCTV-(\d+)",x[0]).group(1)) if re.search(r"CCTV-(\d+)",x[0]) else 999)
        with open(OUTPUT_DIR/OUTPUT_M3U_FILENAME,'w',encoding='utf-8') as f:
            f.write("#EXTM3U\n")
            for g in GROUP_ORDER:
                for n,u in grouped.get(g,[]):
                    f.write(f'#EXTINF:-1 group-title="{g}",{n}\n{u}\n')
        with open(OUTPUT_DIR/OUTPUT_TXT_FILENAME,'w',encoding='utf-8') as f:
            for g in GROUP_ORDER:
                if g not in grouped:continue
                f.write(f"{g},#genre#\n")
                for n,u in grouped[g]: f.write(f"{n},{u}\n")
                f.write("\n")
        print(f"\n🎉 完成！输出：")
        print(OUTPUT_M3U_FILENAME)
        print(OUTPUT_TXT_FILENAME)
        await b.close()

async def main_with_timeout():
    try: await asyncio.wait_for(_main(),SCRIPT_TIMEOUT)
    except asyncio.TimeoutError: print("❌ 脚本超时");sys.exit(1)

if __name__=="__main__":
    asyncio.run(main_with_timeout())
