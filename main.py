import asyncio
import json
import logging
import random
import re
import sys
import time
import argparse
import shutil
import datetime
from collections import defaultdict
from pathlib import Path
from urllib.parse import urlparse
from typing import Dict, List, Tuple, Optional, Any
import functools

import aiohttp
from playwright.async_api import async_playwright

# ############################################################################
#                          网页爬取 配置区域
# ############################################################################

# 目标网站地址
TARGET_URL = "https://iptv.cqshushu.com/index.php"
# 默认协议, 用于无协议前缀的链接
DEFAULT_PROTOCOL = "http://"
# 每页显示IP数, 网站支持: 3 / 6 / 10
IPS_PER_PAGE = 10
# 最多爬取几页
MAX_PAGES = 4
# 每个频道最多保留几条链接
MAX_LINKS_PER_CHANNEL = 8
# 最多处理几个IP, 0表示不限制
MAX_IPS = 0

# 翻页前最小等待秒数, 防429
PAGE_DELAY_MIN = 5.0
# 翻页前最大等待秒数
PAGE_DELAY_MAX = 8.0
# 切换IP详情页最小等待秒数
IP_DELAY_MIN = 2.0
# 切换IP详情页最大等待秒数
IP_DELAY_MAX = 4.0
# 详情页加载后等待秒数
DETAIL_WAIT_MIN = 2.0
# 详情页最大等待秒数
DETAIL_WAIT_MAX = 4.0

# 是否无头模式, GitHub Actions设True
HEADLESS = True
# 页面加载超时毫秒
PAGE_TIMEOUT = 30000
# 网络空闲超时毫秒
IDLE_TIMEOUT = 15000

# ############################################################################
#                          FFmpeg测速 配置区域
# ############################################################################

# 是否启用FFmpeg测速
ENABLE_FFMPEG = True
# FFmpeg可执行文件路径
FFMPEG_PATH = "ffmpeg"
# 每条流测速时长秒数
FFMPEG_DURATION = 20
# 测速并发数
FFMPEG_CONCURRENCY = 6
# 最低平均帧率, 低于此值判定卡顿
MIN_AVG_FPS = 24
# 最低总帧数, 低于此值判定卡顿
MIN_FRAMES = 420

# ############################################################################
#                          连通性测试 配置区域
# ############################################################################

# 是否启用连通性预测试
ENABLE_CONNECTIVITY = True
# 连通性测试并发数
CONN_CONCURRENCY = 15
# 连通性测试超时秒数
CONN_TIMEOUT = 2

# ############################################################################
#                          缓存 配置区域
# ############################################################################

# 是否启用测速缓存
ENABLE_CACHE = True
# 缓存文件路径
CACHE_FILE = Path(__file__).parent / "iptv_speed_cache.json"
# 缓存有效期小时数
CACHE_EXPIRE_HOURS = 72

# 是否启用GitHub源
ENABLE_GITHUB = True
# GitHub源列表, 支持M3U和TXT格式
GITHUB_URLS = [
    "https://gh-proxy.com/https://github.com/vbskycn/iptv/blob/master/tv/iptv4.txt",
    "https://gh-proxy.com/https://github.com/GSD-3726/MMM/blob/main/iptv_channels.txt",
]
# 下载超时秒数
GITHUB_TIMEOUT = 30
# 下载重试次数
GITHUB_RETRIES = 3

# ############################################################################
#                          输出 配置区域
# ############################################################################

# 输出目录
OUTPUT_DIR = Path(__file__).parent
# M3U输出文件名
OUTPUT_M3U = OUTPUT_DIR / "iptv_channels.m3u"
# TXT输出文件名
OUTPUT_TXT = OUTPUT_DIR / "iptv_channels.txt"

# ############################################################################
#                          频道分类 配置区域
# ############################################################################

# 分类规则, 按优先级排列
CATEGORY_RULES = [
    {"name": "央视频道", "keywords": ["cctv", "cetv", "央视"]},
    {"name": "卫视频道", "keywords": ["卫视"]},
    {"name": "影视频道", "keywords": ["影视", "影院", "chc", "剧场", "电影"]},
    {"name": "少儿频道", "keywords": ["少儿", "卡通", "动画", "动漫"]},
    {"name": "地方频道", "keywords": ["地方", "都市", "综合", "新闻", "公共"]},
]
# 分组导出顺序
GROUP_ORDER = ["央视频道", "卫视频道", "影视频道", "少儿频道", "地方频道"]

# CCTV编号到名称映射
CCTV_MAP = {
    "1": "综合", "2": "财经", "3": "综艺", "4": "中文国际", "5": "体育",
    "5+": "体育赛事", "6": "电影", "7": "国防军事", "8": "电视剧",
    "9": "纪录", "10": "科教", "11": "戏曲", "12": "社会与法",
    "13": "新闻", "14": "少儿", "15": "音乐", "16": "奥林匹克", "17": "农业农村",
}
# CCTV标准排序
CCTV_ORDER = [f"CCTV-{k}{v}" for k, v in CCTV_MAP.items() if k != "5+"]
CCTV_ORDER.insert(5, "CCTV-5+体育赛事")

# 正则: 5\+ 放在 \d 前面, 保证优先匹配5+
CCTV_RE = re.compile(r'(cctv)[-\s]?(5\+|\d{1,3})', re.IGNORECASE)
# 正则: 只保留中文
CHINESE_ONLY = re.compile(r'[^\u4e00-\u9fff]')
# 正则: 内网IP
INTERNAL_IP = re.compile(r'^(192\.168\.|10\.|172\.(1[6-9]|2[0-9]|3[0-1])\.|127\.0\.0\.1)')

# 反检测JS注入
STEALTH_JS = """
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3,4,5]});
Object.defineProperty(navigator, 'languages', {get: () => ['zh-CN','zh','en']});
if (!window.chrome) window.chrome = {runtime: {}};
"""

# ############################################################################
#                          日志
# ############################################################################

class BJFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.datetime.fromtimestamp(
            record.created,
            datetime.timezone(datetime.timedelta(hours=8))
        )
        return dt.strftime("%Y-%m-%d %H:%M:%S")

logger = logging.getLogger('IPTV')
logger.setLevel(logging.INFO)
logger.handlers.clear()
_h = logging.StreamHandler(sys.stdout)
_h.setFormatter(BJFormatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(_h)

# ############################################################################
#                          工具函数
# ############################################################################

def build_classifier():
    compiled = []
    for rule in CATEGORY_RULES:
        pat = re.compile("|".join(re.escape(k.lower()) for k in rule["keywords"]))
        compiled.append((rule["name"], pat))
    return lambda name: next((g for g, p in compiled if p.search(name.lower())), None)

classify = build_classifier()


def norm_cctv(name: str) -> str:
    """标准化CCTV频道名"""
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


def clean_cn(name: str) -> str:
    """只保留中文字符"""
    return CHINESE_ONLY.sub('', name)


def is_internal(url: str) -> bool:
    """判断是否内网IP"""
    try:
        host = urlparse(url).hostname
        return bool(host and INTERNAL_IP.match(host))
    except:
        return False


def norm_type(t: str) -> str:
    """标准化抓取类型"""
    m = {
        "all": "all", "全部": "all",
        "hotel": "hotel", "酒店": "hotel",
        "multicast": "multicast", "组播": "multicast",
        "migu": "migu", "咪咕": "migu",
        "other": "other", "其他": "other",
    }
    return m.get(t.strip().lower(), "all")


def progress_bar(cur: int, total: int, ok: int, fail: int, last_pct: int) -> int:
    """打印进度条"""
    if total == 0:
        return 0
    pct = int(cur / total * 100)
    if pct == last_pct and cur != total:
        return last_pct
    bar = '█' * (pct // 5) + '░' * (20 - pct // 5)
    logger.info(f"[{pct:3d}%] {bar} ({cur}/{total}) 成功:{ok} 失败:{fail}")
    sys.stdout.flush()
    return pct


# ############################################################################
#                          人类行为模拟
# ############################################################################

async def human_scroll(page):
    """模拟人类滚动"""
    d = random.randint(150, 400)
    for _ in range(random.randint(3, 6)):
        await page.evaluate(f'window.scrollBy(0, {d // 3})')
        await asyncio.sleep(random.uniform(0.05, 0.15))
    await asyncio.sleep(random.uniform(0.3, 0.8))


async def random_mouse(page):
    """随机鼠标移动"""
    await page.mouse.move(random.randint(100, 800), random.randint(100, 600))
    await asyncio.sleep(random.uniform(0.1, 0.3))


# ############################################################################
#                          缓存
# ############################################################################

CACHE_EXPIRE_SEC = CACHE_EXPIRE_HOURS * 3600


def load_cache() -> dict:
    """加载测速缓存"""
    if not ENABLE_CACHE or not CACHE_FILE.exists():
        return {}
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            cache = json.load(f)
        now = time.time()
        valid = {}
        for url, data in cache.items():
            if isinstance(data, dict) and "ok" in data and "ts" in data:
                if now - data["ts"] < CACHE_EXPIRE_SEC:
                    valid[url] = data
        logger.info(f"缓存加载: {len(valid)} 条有效")
        return valid
    except Exception as e:
        logger.debug(f"缓存加载异常: {e}")
        return {}


def save_cache(cache: dict):
    """保存测速缓存"""
    if not ENABLE_CACHE:
        return
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning(f"缓存保存失败: {e}")


# ############################################################################
#                          连通性测试
# ############################################################################

async def check_url(url: str, timeout: int) -> bool:
    """测试URL是否可达"""
    if not url.startswith(('http://', 'https://')):
        return True
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as s:
            async with s.get(url, headers={'User-Agent': 'Mozilla/5.0'}, allow_redirects=True) as r:
                if r.status != 200:
                    return False
                try:
                    await r.content.readexactly(1024)
                except asyncio.IncompleteReadError as e:
                    return len(e.partial) > 0
                except:
                    return False
                return True
    except:
        return False


async def batch_connectivity(urls: List[str]) -> set:
    """批量连通性测试, 返回可达URL集合"""
    if not urls:
        return set()
    sem = asyncio.Semaphore(CONN_CONCURRENCY)

    async def _check(url):
        async with sem:
            return url, await check_url(url, CONN_TIMEOUT)

    tasks = [_check(u) for u in urls]
    ok_set = set()
    done = 0
    ok = 0
    fail = 0
    lp = -1
    for coro in asyncio.as_completed(tasks):
        url, is_ok = await coro
        done += 1
        if is_ok:
            ok += 1
            ok_set.add(url)
        else:
            fail += 1
        lp = progress_bar(done, len(tasks), ok, fail, lp)
    return ok_set


# ############################################################################
#                          FFmpeg测速
# ############################################################################

def retry_async(max_retries=2, delay=1.0):
    """异步重试装饰器"""
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            for attempt in range(1, max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries:
                        raise
                    await asyncio.sleep(delay)
            return None
        return wrapper
    return decorator


@retry_async(max_retries=2, delay=1.0)
async def test_stream(url: str) -> Dict[str, Any]:
    """用FFmpeg测试单条流, 返回 {ok, fps, frames, width, height}"""
    if not shutil.which(FFMPEG_PATH):
        return {"ok": False, "fps": 0.0, "frames": 0, "width": 0, "height": 0}

    headers = (
        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64)\r\n"
        "Referer: https://www.miguvideo.com/\r\n"
    )
    cmd = [
        FFMPEG_PATH, "-hide_banner", "-y",
        "-headers", headers,
        "-fflags", "nobuffer",
        "-rw_timeout", "5000000",
        "-i", url,
        "-t", str(FFMPEG_DURATION),
        "-f", "null", "-"
    ]
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE
        )
        try:
            _, stderr = await asyncio.wait_for(
                proc.communicate(),
                timeout=FFMPEG_DURATION + 5
            )
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            return {"ok": False, "fps": 0.0, "frames": 0, "width": 0, "height": 0}

        output = stderr.decode('utf-8', errors='ignore')

        # 提取帧数和帧率
        frame_matches = re.findall(r'frame=\s*(\d+)', output)
        fps_matches = re.findall(r'fps=\s*([\d.]+)', output)
        frames = int(frame_matches[-1]) if frame_matches else 0
        avg_fps = float(fps_matches[-1]) if fps_matches else 0.0

        # 提取分辨率
        width, height = 0, 0
        vm = re.search(r'Video:.*?(\d+)x(\d+)', output, re.IGNORECASE)
        if vm:
            width, height = int(vm.group(1)), int(vm.group(2))

        is_ok = frames >= MIN_FRAMES and avg_fps >= MIN_AVG_FPS
        return {"ok": is_ok, "fps": avg_fps, "frames": frames, "width": width, "height": height}
    except Exception as e:
        return {"ok": False, "fps": 0.0, "frames": 0, "width": 0, "height": 0}


async def ffmpeg_batch_test(
    channel_map: Dict[Tuple[str, str], List[str]]
) -> Dict[Tuple[str, str], List[str]]:
    """
    批量FFmpeg测速
    输入: {(分组, 频道名): [url1, url2, ...]}
    输出: 测试通过的, 每频道最多MAX_LINKS_PER_CHANNEL条, 按分辨率降序
    """
    if not channel_map:
        return {}

    cache = load_cache() if ENABLE_CACHE else {}
    new_cache = {}

    # 分离缓存命中和待测
    result_map = defaultdict(list)
    pending = []
    cached_ok = 0

    for (g, n), urls in channel_map.items():
        for u in urls:
            ci = cache.get(u)
            if ci and isinstance(ci, dict) and "ok" in ci:
                if time.time() - ci.get("ts", 0) < CACHE_EXPIRE_SEC:
                    if ci["ok"]:
                        result_map[(g, n)].append((
                            u, ci.get("fps", 0.0),
                            ci.get("w", 0), ci.get("h", 0)
                        ))
                        cached_ok += 1
                    continue
            if is_internal(u):
                continue
            pending.append((g, n, u))

    logger.info(f"FFmpeg待测: {len(pending)} 条 | 缓存命中: {cached_ok} 条")
    if not pending:
        final = {}
        for k, vs in result_map.items():
            vs.sort(key=lambda x: (-x[2] * x[3], -x[1]))
            final[k] = [u for u, _, _, _ in vs[:MAX_LINKS_PER_CHANNEL]]
        return final

    sem = asyncio.Semaphore(FFMPEG_CONCURRENCY)

    async def _test(item):
        g, n, u = item
        async with sem:
            res = await test_stream(u)
            return g, n, u, res

    tasks = [asyncio.ensure_future(_test(item)) for item in pending]

    done = 0
    ok = 0
    fail = 0
    lp = -1

    for coro in asyncio.as_completed(tasks):
        try:
            g, n, u, res = await coro
        except:
            continue
        done += 1
        if res["ok"]:
            ok += 1
            result_map[(g, n)].append((u, res["fps"], res["width"], res["height"]))
        else:
            fail += 1
        if ENABLE_CACHE:
            new_cache[u] = {
                "ok": res["ok"],
                "fps": res["fps"],
                "frames": res.get("frames", 0),
                "w": res.get("width", 0),
                "h": res.get("height", 0),
                "ts": time.time()
            }
        lp = progress_bar(done, len(tasks), ok, fail, lp)

    # 保存缓存
    if ENABLE_CACHE and new_cache:
        cache.update(new_cache)
        save_cache(cache)

    # 排序去重输出
    final = {}
    for k, vs in result_map.items():
        vs.sort(key=lambda x: (-x[2] * x[3], -x[1]))
        final[k] = [u for u, _, _, _ in vs[:MAX_LINKS_PER_CHANNEL]]
    logger.info(f"FFmpeg测速完成: {len(final)} 个频道通过")
    return final


# ############################################################################
#                          GitHub源下载与解析
# ############################################################################


async def download_github(url: str, session: aiohttp.ClientSession) -> str:
    """下载单个GitHub源文件, 带重试"""
    for attempt in range(1, GITHUB_RETRIES + 1):
        try:
            async with session.get(url, headers={'User-Agent': 'Mozilla/5.0'}) as r:
                if r.status == 200:
                    text = await r.text()
                    logger.info(f"GitHub下载成功: {url[:80]}")
                    return text
                logger.debug(f"GitHub HTTP {r.status}: {url[:80]}")
        except Exception as e:
            logger.debug(f"GitHub下载失败 ({attempt}/{GITHUB_RETRIES}): {e}")
            if attempt < GITHUB_RETRIES:
                await asyncio.sleep(2)
    return ""


def parse_m3u_content(content: str) -> List[Tuple[str, str, str]]:
    """解析M3U格式, 返回 [(分组, 频道名, url), ...]"""
    channels = []
    name = ""
    for line in content.splitlines():
        line = line.strip()
        if line.startswith("#EXTINF"):
            m = re.search(r'group-title="([^"]*)",(.+)', line)
            if m:
                group_hint = m.group(1).strip()
                name = m.group(2).strip()
            else:
                m2 = re.search(r'#EXTINF:-1.*?,(.+)', line)
                name = m2.group(1).strip() if m2 else ""
        elif line.startswith("http") and name:
            nn = norm_cctv(name)
            g = classify(nn)
            if g:
                fn = nn if g == "央视频道" else clean_cn(name)
                channels.append((g, fn, line.strip()))
            name = ""
    return channels


def parse_txt_content(content: str) -> List[Tuple[str, str, str]]:
    """解析TXT格式 (频道名,url), 返回 [(分组, 频道名, url), ...]"""
    channels = []
    for line in content.splitlines():
        line = line.strip()
        if not line or line.startswith('#') or line.endswith('#genre#'):
            continue
        if ',' in line:
            parts = line.split(',', 1)
            if len(parts) == 2:
                name = parts[0].strip()
                url = parts[1].strip()
                if '$' in url:
                    url = url.split('$')[0].strip()
                if name and url:
                    nn = norm_cctv(name)
                    g = classify(nn)
                    if g:
                        fn = nn if g == "央视频道" else clean_cn(name)
                        channels.append((g, fn, url))
    return channels


async def fetch_github_sources() -> List[Tuple[str, str, str]]:
    """下载并解析所有GitHub源"""
    if not ENABLE_GITHUB or not GITHUB_URLS:
        return []
    logger.info(f"--- GitHub源下载 ({len(GITHUB_URLS)} 个) ---")
    all_channels = []
    timeout = aiohttp.ClientTimeout(total=GITHUB_TIMEOUT)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = [download_github(url, session) for url in GITHUB_URLS]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for i, result in enumerate(results):
            if isinstance(result, Exception) or not result:
                logger.warning(f"GitHub源{i+1} 下载失败")
                continue
            content = result.strip()
            # 自动判断格式
            if content.startswith('#EXTM3U') or '#EXTINF' in content:
                channels = parse_m3u_content(content)
            else:
                channels = parse_txt_content(content)
            logger.info(f"GitHub源{i+1}: 解析到 {len(channels)} 个频道")
            all_channels.extend(channels)
    logger.info(f"GitHub源合计: {len(all_channels)} 条")
    return all_channels


# ############################################################################
#                          网页爬取: IP列表
# ############################################################################

async def scrape_ips(page, filter_type: str, max_pages: int) -> list:
    """从网站列表页爬取IP"""
    entries = []
    seen = set()

    await page.goto(TARGET_URL, timeout=PAGE_TIMEOUT, wait_until="domcontentloaded")
    try:
        await page.wait_for_load_state("networkidle", timeout=IDLE_TIMEOUT)
    except:
        pass
    await asyncio.sleep(random.uniform(3, 5))

    # 选择类型
    if filter_type != "all":
        try:
            await page.select_option("#typeSelect", filter_type)
            await asyncio.sleep(random.uniform(2, 4))
            try:
                await page.wait_for_load_state("networkidle", timeout=8000)
            except:
                pass
            logger.info(f"已选择类型: {filter_type}")
        except Exception as e:
            logger.warning(f"选择类型失败: {e}")

    # 设置每页数量
    try:
        await page.select_option("#limitSelect", str(IPS_PER_PAGE))
        await asyncio.sleep(random.uniform(2, 3))
        logger.info(f"已设置每页: {IPS_PER_PAGE}")
    except:
        pass

    # 逐页提取
    current_page = 1
    while current_page <= max_pages:
        logger.info(f"正在抓取第 {current_page} 页...")
        await human_scroll(page)
        await random_mouse(page)

        page_entries = await page.evaluate(r"""
            () => {
                const rows = document.querySelectorAll('table.iptv-table tbody tr');
                return Array.from(rows).map(row => {
                    const cells = row.querySelectorAll('td');
                    if (cells.length < 6) return null;
                    const a = cells[0].querySelector('a');
                    if (!a) return null;
                    const onclick = a.getAttribute('onclick') || '';
                    const m = onclick.match(/gotoIP\('([^']+)',\s*'([^']+)'\)/);
                    return {
                        ip: a.innerText.trim(),
                        hash: m ? m[1] : '',
                        type: m ? m[2] : '',
                        channel_count: cells[1].innerText.trim(),
                        type_info: cells[2].innerText.trim(),
                        online_time: cells[3].innerText.trim(),
                        update_time: cells[4].innerText.trim(),
                        status: cells[5].innerText.trim()
                    };
                }).filter(x => x && x.ip && x.hash);
            }
        """)

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

        logger.info(f"本页新增 {new_count} 个 (累计 {len(entries)} 个)")

        # 翻页
        nxt = await page.query_selector('a:has-text("下一页")')
        if not nxt:
            break
        href = await nxt.get_attribute('href') or ''
        if 'page=' not in href:
            break

        delay = random.uniform(PAGE_DELAY_MIN, PAGE_DELAY_MAX)
        logger.info(f"等待 {delay:.1f}s 后翻页...")
        await asyncio.sleep(delay)
        await nxt.click()
        try:
            await page.wait_for_load_state('networkidle', timeout=IDLE_TIMEOUT)
        except:
            pass
        await asyncio.sleep(random.uniform(PAGE_DELAY_MIN, PAGE_DELAY_MAX))
        current_page += 1

    return entries


# ############################################################################
#                          网页爬取: 详情页频道
# ############################################################################

async def extract_detail_channels(page, detail_url: str) -> list:
    """从IP详情页提取频道列表"""
    channels = []
    try:
        await page.goto(detail_url, timeout=PAGE_TIMEOUT, wait_until="domcontentloaded")
        try:
            await page.wait_for_load_state("networkidle", timeout=8000)
        except:
            pass
        await asyncio.sleep(random.uniform(DETAIL_WAIT_MIN, DETAIL_WAIT_MAX))

        # 点击查看频道列表
        for sel in ['a:has-text("查看频道列表")', 'a:has-text("频道")']:
            try:
                btn = await page.query_selector(sel)
                if btn:
                    await btn.click()
                    await asyncio.sleep(random.uniform(2, 3))
                    break
            except:
                pass

        # 提取当前页
        rows = await page.query_selector_all("table tbody tr")
        for row in rows:
            cells = await row.query_selector_all("td")
            if len(cells) >= 3:
                name = (await cells[1].inner_text()).strip()
                url_el = await cells[2].query_selector("a")
                if url_el:
                    url = await url_el.get_attribute("href") or (await cells[2].inner_text()).strip()
                else:
                    url = (await cells[2].inner_text()).strip()
                if name and url:
                    url = url.replace('&amp;', '&')
                    if not url.startswith(("http://", "https://")):
                        url = DEFAULT_PROTOCOL + url
                    channels.append((name, url))

        # 详情页翻页
        for _ in range(10):
            nxt = await page.query_selector('a:has-text("下一页")')
            if not nxt:
                break
            href = await nxt.get_attribute("href") or ""
            if "page=" not in href:
                break
            await asyncio.sleep(random.uniform(PAGE_DELAY_MIN, PAGE_DELAY_MAX))
            await nxt.click()
            try:
                await page.wait_for_load_state("networkidle", timeout=IDLE_TIMEOUT)
            except:
                pass
            rows = await page.query_selector_all("table tbody tr")
            for row in rows:
                cells = await row.query_selector_all("td")
                if len(cells) >= 3:
                    name = (await cells[1].inner_text()).strip()
                    url_el = await cells[2].query_selector("a")
                    if url_el:
                        url = await url_el.get_attribute("href") or (await cells[2].inner_text()).strip()
                    else:
                        url = (await cells[2].inner_text()).strip()
                    if name and url:
                        url = url.replace('&amp;', '&')
                        if not url.startswith(("http://", "https://")):
                            url = DEFAULT_PROTOCOL + url
                        channels.append((name, url))
    except Exception as e:
        logger.debug(f"详情页提取失败: {e}")
    return channels


# ############################################################################
#                          URL去重
# ############################################################################

def deduplicate_urls(ch_map: Dict[Tuple[str, str], List[str]]) -> Dict[Tuple[str, str], List[str]]:
    """全局URL去重, 同一URL归入名称最长的频道"""
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
#                          导出
# ############################################################################

def export(ch_map: Dict[Tuple[str, str], List[str]]):
    """导出M3U和TXT文件"""
    now = datetime.datetime.now(
        datetime.timezone(datetime.timedelta(hours=8))
    ).strftime("%Y-%m-%d %H:%M:%S")

    groups = defaultdict(list)
    for (g, n), urls in ch_map.items():
        for u in urls:
            groups[g].append((n, u))

    # M3U
    with open(OUTPUT_M3U, 'w', encoding='utf-8') as f:
        f.write("#EXTM3U\n")
        for grp in GROUP_ORDER:
            if grp not in groups:
                continue
            chs = sorted(groups[grp], key=lambda x: x[0])
            for n, u in chs:
                if n.strip():
                    f.write(f'#EXTINF:-1 group-title="{grp}",{n}\n{u}\n')
            f.write("\n")
        f.write(f'#EXTINF:-1 group-title="更新时间",{now}\nhttps://example.com\n')

    # TXT
    with open(OUTPUT_TXT, 'w', encoding='utf-8') as f:
        for grp in GROUP_ORDER:
            if grp not in groups:
                continue
            f.write(f"{grp},#genre#\n")
            for n, u in sorted(groups[grp], key=lambda x: x[0]):
                if n.strip():
                    f.write(f"{n},{u}\n")
            f.write("\n")
        f.write(f"更新时间,#genre#\n{now},https://example.com\n")

    logger.info(f"导出完成: {len(ch_map)} 个频道")


# ############################################################################
#                          主流程
# ############################################################################

async def main():
    parser = argparse.ArgumentParser(description="IPTV源抓取器 v3")
    parser.add_argument("--type", default="hotel", help="抓取类型: all/hotel/multicast/migu/other")
    parser.add_argument("--max-pages", type=int, default=MAX_PAGES, help="最多爬几页")
    parser.add_argument("--max-ips", type=int, default=MAX_IPS, help="最多处理几个IP, 0不限")
    parser.add_argument("--headless", default="true", help="无头模式: true/false")
    parser.add_argument("--skip-ffmpeg", action="store_true", help="跳过FFmpeg测速")
    parser.add_argument("--skip-scrape", action="store_true", help="跳过网页爬取, 用缓存测速")
    parser.add_argument("--skip-github", action="store_true", help="跳过GitHub源")
    args = parser.parse_args()

    ft = norm_type(args.type)
    max_pages = args.max_pages
    max_ips = args.max_ips
    headless = args.headless.lower() != "false"
    do_ffmpeg = ENABLE_FFMPEG and not args.skip_ffmpeg
    do_scrape = not args.skip_scrape

    start_time = time.time()
    logger.info("=" * 60)
    logger.info("IPTV源抓取器 v3 启动")
    logger.info(f"  类型: {ft}")
    logger.info(f"  每页IP: {IPS_PER_PAGE}")
    logger.info(f"  最大页: {max_pages}")
    logger.info(f"  网页爬取: {'是' if do_scrape else '否'}")
    logger.info(f"  GitHub源: {'是' if ENABLE_GITHUB and not args.skip_github else '否'}")
    logger.info(f"  FFmpeg测速: {'是' if do_ffmpeg else '否'}")
    logger.info("=" * 60)

    all_channels = []

    # ==================== GitHub源 ====================
    if ENABLE_GITHUB and not args.skip_github:
        github_chs = await fetch_github_sources()
        all_channels.extend(github_chs)

    # ==================== 网页爬取 ====================
    if do_scrape:
        logger.info("--- 开始网页爬取 ---")
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=headless,
                args=[
                    "--no-sandbox", "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage", "--disable-gpu",
                    "--single-process", "--disable-blink-features=AutomationControlled"
                ]
            )
            ctx = await browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            await ctx.add_init_script(STEALTH_JS)
            page = await ctx.new_page()

            try:
                # 爬取IP列表
                entries = await scrape_ips(page, ft, max_pages)
                logger.info(f"共获取 {len(entries)} 个IP")

                if max_ips > 0:
                    entries = entries[:max_ips]
                    logger.info(f"限制为前 {max_ips} 个IP")

                # 逐IP提取频道
                for i, entry in enumerate(entries):
                    try:
                        detail_url = f"{TARGET_URL}?p={entry['hash']}&t={entry['type']}"
                        logger.info(f"[{i + 1}/{len(entries)}] {entry['ip']}")
                        chs = await extract_detail_channels(page, detail_url)
                        for name, url in chs:
                            nn = norm_cctv(name)
                            g = classify(nn)
                            if g:
                                fn = nn if g == "央视频道" else clean_cn(name)
                                all_channels.append((g, fn, url))
                        await asyncio.sleep(random.uniform(IP_DELAY_MIN, IP_DELAY_MAX))
                    except Exception as e:
                        logger.warning(f"IP {entry['ip']} 失败: {e}")
                        try:
                            await page.close()
                        except:
                            pass
                        try:
                            page = await ctx.new_page()
                            logger.info("已重新创建页面")
                        except:
                            logger.error("无法恢复, 提前结束")
                            break

            except Exception as e:
                logger.exception(f"爬取异常: {e}")
            finally:
                try: await page.close()
                except: pass
                try: await ctx.close()
                except: pass
                try: await browser.close()
                except: pass

        logger.info(f"网页爬取完成: {len(all_channels)} 条原始记录")

    # ==================== 数据清洗 ====================
    # 过滤内网IP
    before = len(all_channels)
    all_channels = [(g, n, u) for g, n, u in all_channels if not is_internal(u)]
    if before != len(all_channels):
        logger.info(f"过滤内网IP: {before} -> {len(all_channels)}")

    # 按频道聚合
    ch_map = defaultdict(list)
    for g, n, u in all_channels:
        ch_map[(g, n)].append(u)

    # URL去重
    ch_map = deduplicate_urls(ch_map)

    # 过滤未分类
    allowed = set(GROUP_ORDER)
    ch_map = {k: v for k, v in ch_map.items() if k[0] in allowed}

    logger.info(f"清洗后: {len(ch_map)} 个频道, {sum(len(v) for v in ch_map.values())} 条链接")

    # ==================== 连通性测试 ====================
    if ENABLE_CONNECTIVITY and ch_map:
        logger.info("--- 连通性测试 ---")
        all_urls = []
        for urls in ch_map.values():
            all_urls.extend(urls)
        unique_urls = list(set(all_urls))

        ok_set = await batch_connectivity(unique_urls)

        filtered = defaultdict(list)
        for (g, n), urls in ch_map.items():
            valid = [u for u in urls if u in ok_set]
            if valid:
                filtered[(g, n)] = valid
        ch_map = dict(filtered)
        logger.info(f"连通性过滤: {len(ch_map)} 个频道")

    # ==================== FFmpeg测速 ====================
    if do_ffmpeg and ch_map:
        logger.info("--- FFmpeg测速 ---")
        ff_start = time.time()
        ch_map = await ffmpeg_batch_test(ch_map)
        logger.info(f"FFmpeg耗时: {time.time() - ff_start:.1f}s")

    # ==================== 导出 ====================
    export(ch_map)

    # ==================== 统计 ====================
    total_time = time.time() - start_time
    logger.info("=" * 60)
    logger.info("全部完成!")
    logger.info(f"  频道数: {len(ch_map)}")
    logger.info(f"  链接数: {sum(len(v) for v in ch_map.values())}")
    logger.info(f"  总耗时: {total_time:.1f}s")
    logger.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
