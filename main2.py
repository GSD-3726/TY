#!/usr/bin/env python3
"""
IPTV 精简爬虫 - 仅保留爬取功能
目标: 最快速度爬取所有频道地址，按分类输出为 txt
优化策略: 并发详情页提取 + 智能延迟 + 反检测

用法:
  python3 iptv_crawler.py                    # 按配置区域 SCRAPE_TYPE 爬取
  python3 iptv_crawler.py --type migu        # 只爬咪咕源
  python3 iptv_crawler.py --type migu,hotel  # 同时爬咪咕和酒店
  python3 iptv_crawler.py --max-pages 5      # 限制页数
  python3 iptv_crawler.py --fast             # 快速模式(减少延迟)
  python3 iptv_crawler.py --concurrency 10   # 详情页并发数
"""

import asyncio
import random
import re
import sys
import time
import argparse
import datetime
from collections import defaultdict
from pathlib import Path
from urllib.parse import urlparse

from playwright.async_api import async_playwright

# ############################################################################
# 爬取 配置区域
# ############################################################################
TARGET_URL = "https://iptv.cqshushu.com/index.php"
DEFAULT_PROTOCOL = "http://"
IPS_PER_PAGE = 10
MAX_PAGES = 10
MAX_IPS = 0             # 0=不限制
MAX_DETAIL_PAGES = 30
OUTPUT_DIR = Path(__file__).parent
OUTPUT_TXT = OUTPUT_DIR / "iptv_channels.txt"

# 爬取类型: all / hotel / multicast / migu / other
# all=全部  hotel=酒店  multicast=组播  migu=咪咕  other=其他
# 多个类型用逗号分隔，依次爬取: "migu,hotel"
SCRAPE_TYPE = "migu,hotel"

# 每个频道最多保留的链接数（0=不限制，保留全部）
MAX_LINKS_PER_CHANNEL = 0

# 延迟配置 (正常模式)
PAGE_DELAY = (5.0, 8.0)
IP_DELAY = (2.0, 4.0)
DETAIL_WAIT = (2.0, 4.0)
DETAIL_PAGE_DELAY = (1.0, 2.0)

# 快速模式延迟
FAST_PAGE_DELAY = (2.0, 3.0)
FAST_IP_DELAY = (0.5, 1.5)
FAST_DETAIL_WAIT = (1.0, 2.0)
FAST_DETAIL_PAGE_DELAY = (0.5, 1.0)

# 超时
PAGE_TIMEOUT = 60000
IDLE_TIMEOUT = 15000
DETAIL_PAGE_TIMEOUT = 60000
DETAIL_IDLE_TIMEOUT = 5000
DETAIL_MAX_SECONDS = 120

# 并发
DETAIL_CONCURRENCY = 2

# 反检测 Stealth JS
STEALTH_JS = """
delete Navigator.prototype.webdriver;
Object.defineProperty(Navigator.prototype, 'webdriver', {
    value: undefined, writable: false, configurable: true
});
if (!window.chrome) window.chrome = {};
window.chrome.runtime = {
    connect: function() {
        return {
            onMessage: {addListener:function(){}},
            postMessage:function(){},
            onDisconnect: {addListener:function(){}}
        };
    },
    sendMessage: function() {},
    onConnect: {addListener:function(){}, removeListener:function(){}, hasListener:function(){return false;}},
    onMessage: {addListener:function(){}, removeListener:function(){}, hasListener:function(){return false;}},
    getURL: function(p) { return 'chrome-extension://invalid/'+p; },
    id: undefined
};
for (let k in window) {
    if (k.startsWith('cdc_') || k.startsWith('__webdriver') || k.startsWith('__driver') || k.startsWith('__selenium'))
        delete window[k];
}
const origQuery = window.navigator.permissions.query;
window.navigator.permissions.query = (p) =>
    p.name==='notifications' ? Promise.resolve({state:Notification.permission}) : origQuery(p);
"""

# User-Agent 池
UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
]

# ############################################################################
# 频道分类 配置区域
# ############################################################################
CATEGORY_RULES = [
    {"name": "央视频道", "keywords": ["cctv", "cetv", "央视"]},
    {"name": "卫视频道", "keywords": ["卫视"]},
    {"name": "影视频道", "keywords": ["影视", "影院", "chc", "剧场", "电影"]},
    {"name": "少儿频道", "keywords": ["少儿", "卡通", "动画", "动漫"]},
    {"name": "地方频道", "keywords": ["地方", "都市", "综合", "新闻", "公共"]},
]
GROUP_ORDER = ["央视频道", "卫视频道", "影视频道", "少儿频道"]

CCTV_MAP = {
    "1": "综合", "2": "财经", "3": "综艺", "4": "中文国际", "5": "体育",
    "5+": "体育赛事", "6": "电影", "7": "国防军事", "8": "电视剧",
    "9": "纪录", "10": "科教", "11": "戏曲", "12": "社会与法",
    "13": "新闻", "14": "少儿", "15": "音乐", "16": "奥林匹克", "17": "农业农村",
}
CCTV_ORDER = [f"CCTV-{k}{v}" for k, v in CCTV_MAP.items() if k != "5+"]
CCTV_ORDER.insert(5, "CCTV-5+体育赛事")
CCTV_ORDER.append("CCTV-4K")

CCTV_RE = re.compile(r'(cctv)[-\s]?(5\+|\d{1,3})', re.IGNORECASE)
CHINESE_ONLY = re.compile(r'[^\u4e00-\u9fff]')
INTERNAL_IP = re.compile(r'^(192\.168\.|10\.|172\.(1[6-9]|2[0-9]|3[0-1])\.|127\.0\.0\.1)')
CLEAR_SUFFIX_RE = re.compile(r'[\s\-_]*(高清|超清|4K|超高清|标清|HD|FHD|UHD|2K|蓝光|原画|流畅|720P|1080P|2160P)', re.IGNORECASE)

# ############################################################################
# 日志
# ############################################################################
class BJFormatter:
    def format(self, msg):
        dt = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8)))
        return f"{dt.strftime('%Y-%m-%d %H:%M:%S')} - {msg}"

_fmt = BJFormatter()

def log(msg):
    print(_fmt.format(msg), flush=True)

def log_warn(msg):
    print(_fmt.format(f"[WARN] {msg}"), flush=True)

# ############################################################################
# 频道分类工具函数
# ############################################################################
def build_classifier():
    compiled = []
    for rule in CATEGORY_RULES:
        pat = re.compile("|".join(re.escape(k.lower()) for k in rule["keywords"]))
        compiled.append((rule["name"], pat))
    return lambda name: next((g for g, p in compiled if p.search(name.lower())), None)

classify = build_classifier()

def norm_cctv(name: str) -> str:
    low = name.lower()
    if re.search(r'cctv[-\s]?4k', low):
        return "CCTV-4K"
    if re.search(r'cctv[-\s]?5\+', low):
        return "CCTV-5+体育赛事"
    m = CCTV_RE.search(low)
    if m:
        num = m.group(2)
        if num in CCTV_MAP:
            return f"CCTV-{num}{CCTV_MAP[num]}"
        return f"CCTV-{num}"
    return name

def unify_channel_name(raw_name: str) -> str:
    std_name = norm_cctv(raw_name)
    std_name = CLEAR_SUFFIX_RE.sub("", std_name)
    std_name = re.sub(r'[\s\-_]+$', "", std_name).strip()
    return std_name

def clean_cn(name: str) -> str:
    return CHINESE_ONLY.sub('', name)

def is_internal(url: str) -> bool:
    try:
        host = urlparse(url).hostname
        return bool(host and INTERNAL_IP.match(host))
    except:
        return False

def norm_type(t: str) -> str:
    m = {"all":"all","全部":"all","hotel":"hotel","酒店":"hotel",
         "multicast":"multicast","组播":"multicast","migu":"migu","咪咕":"migu",
         "other":"other","其他":"other"}
    return m.get(t.strip().lower(), "all")

# ############################################################################
# 人类行为模拟
# ############################################################################
async def human_scroll(page):
    d = random.randint(150, 400)
    for _ in range(random.randint(3, 6)):
        await page.evaluate(f'window.scrollBy(0, {d // 3})')
        await asyncio.sleep(random.uniform(0.05, 0.15))
    await asyncio.sleep(random.uniform(0.3, 0.8))

async def random_mouse(page):
    await page.mouse.move(random.randint(100, 800), random.randint(100, 600))
    await asyncio.sleep(random.uniform(0.1, 0.3))

# ############################################################################
# 核心爬取: IP列表
# ############################################################################
async def scrape_ip_list(ctx, filter_type: str, max_pages: int, delays: dict) -> list:
    entries = []
    seen = set()

    target_url = f"{TARGET_URL}?t={filter_type}&province=all&limit={IPS_PER_PAGE}" if filter_type != "all" else f"{TARGET_URL}?province=all&limit={IPS_PER_PAGE}"

    page = await ctx.new_page()
    await page.add_init_script(STEALTH_JS)

    for attempt in range(5):
        try:
            if page.is_closed():
                page = await ctx.new_page()
                await page.add_init_script(STEALTH_JS)

            await page.goto(target_url, timeout=PAGE_TIMEOUT, wait_until="commit")
            await asyncio.sleep(random.uniform(5, 8))
            try:
                await page.wait_for_load_state("networkidle", timeout=IDLE_TIMEOUT)
            except:
                pass

            if filter_type != "all":
                current_filter = await page.evaluate("() => document.querySelector('#typeSelect')?.value")
                if current_filter == filter_type:
                    break
                else:
                    await asyncio.sleep(random.uniform(2, 4))
            else:
                break
        except Exception as e:
            log_warn(f"页面初始化失败，重试 {attempt+1}/5: {e}")
            if not page.is_closed():
                await page.close()
            page = await ctx.new_page()
            await page.add_init_script(STEALTH_JS)
            await asyncio.sleep(3)

    if page.is_closed():
        log_warn("浏览器页面无法打开，放弃")
        return entries

    current_page = 1
    while current_page <= max_pages:
        await human_scroll(page)
        await random_mouse(page)

        try:
            page_entries = await page.evaluate("""
                () => {
                    const rows = document.querySelectorAll('table.iptv-table tbody tr');
                    return Array.from(rows).map(row => {
                        const cells = row.querySelectorAll('td');
                        if (cells.length < 6) return null;
                        const a = cells[0].querySelector('a');
                        if (!a) return null;
                        const onclick = a.getAttribute('onclick') || '';
                        const m = onclick.match(/gotoIP\\('([^']+)',\\s*'([^']+)'/);
                        return {
                            ip: a.innerText.trim(),
                            hash: m ? m[1] : '',
                            type: m ? m[2] : '',
                            status: cells[5].innerText.trim()
                        };
                    }).filter(x => x && x.ip && x.hash);
                }
            """)
        except Exception as e:
            log_warn(f"第{current_page}页提取失败: {e}")
            break

        new_count = 0
        for entry in page_entries:
            if filter_type != 'all' and entry['type'] != filter_type:
                continue
            if entry['ip'] in seen:
                continue
            if '失效' in entry['status']:
                continue
            seen.add(entry['ip'])
            entries.append(entry)
            new_count += 1

        log(f"第{current_page}页: +{new_count} IP (累计 {len(entries)})")

        if new_count == 0 and current_page > 1:
            break

        try:
            nxt = await page.query_selector('a:has-text("下一页")')
            if not nxt:
                break
            href = await nxt.get_attribute('href') or ''
            if 'page=' not in href:
                break
        except:
            break

        delay = random.uniform(*delays['page'])
        await asyncio.sleep(delay)

        try:
            await nxt.click()
            try:
                await page.wait_for_load_state("networkidle", timeout=IDLE_TIMEOUT)
            except:
                pass
            await asyncio.sleep(random.uniform(*delays['page']))
        except Exception as e:
            log_warn(f"翻页失败: {e}")
            break

        current_page += 1

    if not page.is_closed():
        await page.close()

    log(f"IP列表爬取完成: 共 {len(entries)} 个IP")
    return entries

# ############################################################################
# 核心爬取: 详情页频道提取
# ############################################################################
async def extract_channels_from_detail(ctx, detail_url: str, delays: dict) -> list:
    """从IP详情页提取全部频道URL"""
    channels = []
    page = None
    start_time = time.perf_counter()

    def is_overtime():
        return time.perf_counter() - start_time > DETAIL_MAX_SECONDS

    try:
        page = await ctx.new_page()
        await page.add_init_script(STEALTH_JS)

        # 1. 先访问首页（必须，否则后续JS跳转不生效）
        for retry in range(3):
            await page.goto(TARGET_URL, timeout=PAGE_TIMEOUT, wait_until="commit")
            await asyncio.sleep(random.uniform(3, 5))
            try:
                await page.wait_for_load_state("networkidle", timeout=IDLE_TIMEOUT)
            except:
                pass

            # 403/安全验证检测
            page_text = ""
            try:
                page_text = (await page.inner_text("body"))[:500]
            except:
                pass
            if "403" in page_text or "安全验证" in page_text or "暂时被拒绝" in page_text:
                wait_sec = random.uniform(15, 30)
                log_warn(f"触发防护(403/验证)，等待 {wait_sec:.0f}s 后重试 ({retry+1}/3)")
                await asyncio.sleep(wait_sec)
                continue
            break

        # 2. 用 gotoIP() 跳转到详情页（客户端路由，不能用 page.goto）
        p_match = re.search(r'[?&]p=([^&]+)', detail_url)
        t_match = re.search(r'[?&]t=([^&]+)', detail_url)
        if not p_match:
            log_warn(f"无法解析detail_url: {detail_url[:60]}")
            return channels

        p_hash = p_match.group(1)
        t_type = t_match.group(1) if t_match else 'hotel'

        # 等待 gotoIP 函数定义（403页面不会有这个函数）
        has_func = False
        for wait_i in range(15):
            try:
                has_func = await page.evaluate("typeof gotoIP === 'function'")
            except:
                pass
            if has_func:
                break
            await asyncio.sleep(1)

        if not has_func:
            # 可能触发了403，等待后重试一次
            await asyncio.sleep(random.uniform(5, 10))
            try:
                await page.goto(TARGET_URL, timeout=PAGE_TIMEOUT, wait_until="commit")
                await asyncio.sleep(5)
                try:
                    await page.wait_for_load_state("networkidle", timeout=IDLE_TIMEOUT)
                except: pass
                has_func = await page.evaluate("typeof gotoIP === 'function'")
            except: pass

        if not has_func:
            log_warn(f"gotoIP未加载(可能403): {p_hash[:20]}")
            return channels

        try:
            await page.evaluate(f"gotoIP('{p_hash}', '{t_type}')")
            await asyncio.sleep(random.uniform(*delays['detail_wait']))
            try:
                await page.wait_for_load_state("networkidle", timeout=DETAIL_IDLE_TIMEOUT)
            except:
                pass
        except Exception as e:
            log_warn(f"gotoIP失败: {e}")
            return channels

        # 安全验证检测
        page_title = await page.title()
        if "安全验证" in page_title or "暂时被拒绝" in (await page.inner_text("body"))[:300]:
            log_warn(f"触发安全验证: {detail_url[:60]}")
            return channels

        # 3. 找到 ?s= 链接的 href
        s_href = await page.evaluate("""
            () => {
                const a = document.querySelector('a[href*="?s="]');
                return a ? a.getAttribute('href') : null;
            }
        """)

        if not s_href:
            log_warn(f"未找到?s=链接: {p_hash[:20]}")
            return channels

        # 4. goto ?s= URL（从详情页跳转才能正常加载频道列表）
        channel_list_url = TARGET_URL + s_href if s_href.startswith('?') else s_href
        await page.goto(channel_list_url, timeout=DETAIL_PAGE_TIMEOUT, wait_until="commit")
        await asyncio.sleep(random.uniform(3, 5))
        try:
            await page.wait_for_load_state("networkidle", timeout=DETAIL_IDLE_TIMEOUT)
        except:
            pass

        # 检查是否成功
        page_title = await page.title()
        if "频道列表" not in page_title:
            log_warn(f"未跳转到频道列表: {page_title[:40]}")
            return channels

        # 5. 提取全部频道（含翻页）
        seen_page_urls = set()
        for page_num in range(1, MAX_DETAIL_PAGES + 1):
            if is_overtime():
                break

            try:
                await page.wait_for_selector('table.iptv-table tbody tr, table tbody tr', timeout=10000)
            except:
                if page_num == 1:
                    log_warn(f"未找到频道表格: {p_hash[:20]}")
                break

            page_channels = await page.evaluate("""
                () => {
                    const results = [];
                    const rows = document.querySelectorAll('table.iptv-table tbody tr, table tbody tr');
                    for (const row of rows) {
                        const cells = row.querySelectorAll('td');
                        if (cells.length < 3) continue;
                        const name = cells[1] ? cells[1].innerText.trim() : '';
                        let url = '';
                        const a = cells[2] ? cells[2].querySelector('a') : null;
                        if (a) {
                            url = a.getAttribute('href') || a.innerText.trim();
                        } else if (cells[2]) {
                            url = cells[2].innerText.trim();
                        }
                        if (name && url) results.push({name, url});
                    }
                    return results;
                }
            """)

            if not page_channels:
                break

            for ch in page_channels:
                name = ch.get('name', '').strip()
                url = ch.get('url', '').strip()
                if name and url:
                    url = url.replace('&amp;', '&')
                    if not url.startswith(('http://', 'https://')):
                        url = DEFAULT_PROTOCOL + url
                    channels.append((name, url))

            current_url = page.url
            if current_url in seen_page_urls:
                log_warn(f"  URL重复，停止翻页")
                break
            seen_page_urls.add(current_url)

            if page_num >= MAX_DETAIL_PAGES:
                break

            # 翻页: 找下一页链接
            nxt = None
            try:
                all_links = await page.evaluate("""
                    () => {
                        const results = [];
                        document.querySelectorAll('a').forEach(a => {
                            results.push({text: a.innerText.trim(), href: a.getAttribute('href') || ''});
                        });
                        return results;
                    }
                """)
                for link in all_links:
                    if '下一页' in link['text'] and link['href']:
                        nxt = link['href']
                        break
            except:
                pass


            if not nxt:
                try:
                    cur = page.url
                    if 'page=' in cur:
                        m = re.search(r'page=(\d+)', cur)
                        if m:
                            next_page = int(m.group(1)) + 1
                            nxt = re.sub(r'page=\d+', f'page={next_page}', cur)
                except:
                    pass

            if not nxt:
                break

            await asyncio.sleep(random.uniform(*delays['detail_page']))
            try:
                # 确保用完整URL（相对路径会重定向）
                if nxt.startswith('?'):
                    nxt = TARGET_URL + nxt
                await page.goto(nxt, timeout=DETAIL_PAGE_TIMEOUT, wait_until="commit")
                await asyncio.sleep(random.uniform(2, 3))
                try:
                    await page.wait_for_load_state("networkidle", timeout=DETAIL_IDLE_TIMEOUT)
                except:
                    pass
            except:
                break

    except Exception as e:
        log_warn(f"详情页异常: {e}")
    finally:
        if page and not page.is_closed():
            try:
                await page.close()
            except:
                pass

    # 去重
    seen = set()
    unique = []
    for name, url in channels:
        if url not in seen:
            seen.add(url)
            unique.append((name, url))
    return unique

# ############################################################################
# 并发详情页爬取
# ############################################################################
async def crawl_all_details(browser, entries: list, concurrency: int, delays: dict) -> list:
    all_channels = []

    async def _extract(i, entry):
        # 创建独立 context
        ctx = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent=random.choice(UA_POOL),
        )
        await ctx.add_init_script(STEALTH_JS)
        try:
            detail_url = f"{TARGET_URL}?p={entry['hash']}&t={entry['type']}"
            chs = await extract_channels_from_detail(ctx, detail_url, delays)
            if chs:
                log(f"[{i+1}/{len(entries)}] {entry['ip']}: {len(chs)} 个频道")
            else:
                log(f"[{i+1}/{len(entries)}] {entry['ip']}: 无频道")
            return chs
        finally:
            try: await ctx.close()
            except: pass

    # 用 asyncio.Semaphore 限制并发，任务启动错开避免同时请求
    sem = asyncio.Semaphore(concurrency)

    async def _throttled_extract(i, entry):
        async with sem:
            # 错开启动：每个任务间隔 3-5 秒
            await asyncio.sleep(random.uniform(3.0, 5.0) * (i % concurrency))
            return await _extract(i, entry)

    tasks = [asyncio.ensure_future(_throttled_extract(i, e)) for i, e in enumerate(entries)]

    done = 0
    for coro in asyncio.as_completed(tasks):
        try:
            chs = await coro
            all_channels.extend(chs)
        except Exception as e:
            log_warn(f"详情页任务异常: {e}")
        done += 1
        if done % 10 == 0 or done == len(tasks):
            log(f"进度: {done}/{len(entries)} ({done*100//len(entries)}%)")

    return all_channels

# ############################################################################
# URL去重
# ############################################################################
def deduplicate_urls(ch_map: dict) -> dict:
    url_to_ch = defaultdict(list)
    for (g, n), urls in ch_map.items():
        for u in urls:
            url_to_ch[u].append((g, n))

    url_chosen = {}
    for url, chs in url_to_ch.items():
        if len(chs) == 1:
            url_chosen[url] = chs[0]
        else:
            plus = [c for c in chs if '+' in c[1].lower()]
            url_chosen[url] = plus[0] if plus else max(chs, key=lambda c: len(c[1]))

    new_map = defaultdict(list)
    for (g, n), urls in ch_map.items():
        for u in urls:
            if url_chosen[u] == (g, n):
                new_map[(g, n)].append(u)
    return dict(new_map)

# ############################################################################
# 导出为 TXT 文件 (与原代码格式一致)
# ############################################################################
def export_txt(ch_map: dict, output_path: Path):
    now = datetime.datetime.now(
        datetime.timezone(datetime.timedelta(hours=8))
    ).strftime("%Y-%m-%d %H:%M:%S")

    groups = defaultdict(list)
    for (g, n), urls in ch_map.items():
        for u in urls:
            groups[g].append((n, u))

    cctv_weight = {name: idx for idx, name in enumerate(CCTV_ORDER)}

    with open(output_path, 'w', encoding='utf-8') as f:
        for grp in GROUP_ORDER:
            if grp not in groups:
                continue
            f.write(f"{grp},#genre#\n")
            chs = groups[grp]
            if grp == "央视频道":
                def cctv_sort_key(item):
                    return cctv_weight.get(item[0], 9999)
                chs_sorted = sorted(chs, key=cctv_sort_key)
            else:
                chs_sorted = sorted(chs, key=lambda x: x[0])
            if MAX_LINKS_PER_CHANNEL > 0:
                chs_sorted = chs_sorted[:MAX_LINKS_PER_CHANNEL]
            for n, u in chs_sorted:
                if n.strip():
                    f.write(f"{n},{u}\n")
        f.write("\n")
        f.write(f"更新时间,#genre#\n{now},https://example.com\n")

    total = sum(len(v) for v in groups.values())
    log(f"导出完成: {len(ch_map)} 个频道, {total} 条链接 -> {output_path}")

# ############################################################################
# 主流程
# ############################################################################
async def main():
    parser = argparse.ArgumentParser(description="IPTV精简爬虫 - 仅爬取功能")
    parser.add_argument("--type", default=SCRAPE_TYPE, help="抓取类型: all/hotel/multicast/migu/other，多类型逗号分隔")
    parser.add_argument("--max-pages", type=int, default=MAX_PAGES, help="最大翻页数")
    parser.add_argument("--max-ips", type=int, default=MAX_IPS, help="最大IP数, 0=不限")
    parser.add_argument("--concurrency", type=int, default=DETAIL_CONCURRENCY, help="详情页并发数")
    parser.add_argument("--fast", action="store_true", help="快速模式(减少延迟)")
    parser.add_argument("--headless", default="true", help="无头模式: true/false")
    parser.add_argument("--output", default=str(OUTPUT_TXT), help="输出文件路径")
    args = parser.parse_args()

    # 解析多类型: "migu,hotel" -> ["migu", "hotel"]
    type_list = [norm_type(t) for t in args.type.split(",") if t.strip()]
    if not type_list:
        type_list = ["all"]

    max_pages = args.max_pages
    max_ips = args.max_ips
    concurrency = args.concurrency
    headless = args.headless.lower() != "false"
    output_path = Path(args.output)

    if args.fast:
        delays = {
            'page': FAST_PAGE_DELAY,
            'ip': FAST_IP_DELAY,
            'detail_wait': FAST_DETAIL_WAIT,
            'detail_page': FAST_DETAIL_PAGE_DELAY,
        }
        log("⚡ 快速模式: 延迟已降低")
    else:
        delays = {
            'page': PAGE_DELAY,
            'ip': IP_DELAY,
            'detail_wait': DETAIL_WAIT,
            'detail_page': DETAIL_PAGE_DELAY,
        }

    start_time = time.time()
    log("=" * 60)
    log("IPTV 精简爬虫启动")
    log(f" 类型: {', '.join(type_list)} | 并发: {concurrency} | 快速: {args.fast}")
    log(f" 页数: {max_pages} | IP数: {'不限' if max_ips == 0 else max_ips}")
    log("=" * 60)

    raw_channels = []  # (group, name, url)

    async with async_playwright() as p:
        chrome_candidates = [
            str(Path(__file__).parent / ".bin/chrome"),
            "/usr/bin/google-chrome-stable",
            "/usr/bin/google-chrome",
            "/usr/bin/chromium-browser",
            "/usr/bin/chromium",
        ]
        chrome_path = None
        for c in chrome_candidates:
            if Path(c).exists() and Path(c).is_file():
                chrome_path = c
                break
        if chrome_path:
            log(f"Chrome路径: {chrome_path}")
        else:
            log("未找到Chrome, 使用Playwright默认")

        launch_opts = {
            "headless": headless,
            "args": [
                "--no-sandbox", "--disable-setuid-sandbox",
                "--disable-dev-shm-usage", "--disable-gpu",
                "--disable-features=IsolateOrigins,site-per-process",
                "--disable-blink-features=AutomationControlled",
                "--disable-web-security",
                "--disable-features=BlockInsecurePrivateNetworkRequests",
                "--disable-background-networking",
                "--disable-default-apps",
                "--disable-extensions",
                "--disable-sync",
                "--no-first-run",
                "--disable-translate",
            ]
        }
        if chrome_path:
            launch_opts["executable_path"] = chrome_path

        browser = await p.chromium.launch(**launch_opts)

        # 按类型依次爬取，共用同一个浏览器
        for type_idx, filter_type in enumerate(type_list):
            log("")
            log("=" * 60)
            log(f"[{type_idx+1}/{len(type_list)}] 开始爬取类型: {filter_type}")
            log("=" * 60)

            ctx = await browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent=random.choice(UA_POOL),
            )
            await ctx.add_init_script(STEALTH_JS)

            # 阶段1: 爬取IP列表
            log("--- 阶段1: 爬取IP列表 ---")
            entries = await scrape_ip_list(ctx, filter_type, max_pages, delays)

            if max_ips > 0:
                entries = entries[:max_ips]

            type_channels = []
            if entries:
                # 阶段2: 并发爬取详情页
                log(f"--- 阶段2: 并发爬取 {len(entries)} 个IP详情页 (并发={concurrency}) ---")
                all_channels = await crawl_all_details(browser, entries, concurrency, delays)

                for name, url in all_channels:
                    if is_internal(url):
                        continue
                    std_ch = unify_channel_name(name)
                    g = classify(std_ch)
                    if g:
                        fn = std_ch if g == "央视频道" else clean_cn(std_ch)
                        type_channels.append((g, fn, url))

            raw_channels.extend(type_channels)
            log(f"类型 {filter_type} 完成: {len(type_channels)} 条记录")

            try: await ctx.close()
            except: pass

            # 类型间间隔，避免给网站压力
            if type_idx < len(type_list) - 1:
                wait_sec = random.uniform(10, 20)
                log(f"等待 {wait_sec:.0f}s 后爬取下一类型...")
                await asyncio.sleep(wait_sec)

        try: await browser.close()
        except: pass

    # 构建频道映射并去重
    ch_map = defaultdict(list)
    for g, n, u in raw_channels:
        ch_map[(g, n)].append(u)

    ch_map = deduplicate_urls(ch_map)

    allowed = set(GROUP_ORDER)
    ch_map = {k: v for k, v in ch_map.items() if k[0] in allowed}

    total_raw = sum(len(v) for v in ch_map.values())
    log("")
    log("=" * 60)
    log(f"所有类型爬取完毕: {len(ch_map)} 个频道, {total_raw} 条链接")
    log("=" * 60)

    # 导出
    export_txt(ch_map, output_path)

    total_time = time.time() - start_time
    log("=" * 60)
    log(f"全部完成! 频道数: {len(ch_map)}, 有效链接: {total_raw}, 耗时: {total_time:.1f}s")
    log("=" * 60)

if __name__ == "__main__":
    asyncio.run(main())
