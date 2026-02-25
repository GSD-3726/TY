#!/usr/bin/env python3
"""
import asyncio
import os
import re
import sys
import time
import statistics
import hashlib
import json
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Set
from urllib.parse import urljoin

import aiohttp
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# 尝试导入 tqdm 进度条库，若失败则使用简单回退
try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False
    class tqdm:
        def __init__(self, *args, **kwargs):
            self.total = kwargs.get('total', 0)
            self.desc = kwargs.get('desc', '')
            self.unit = kwargs.get('it', 'it')
            self.n = 0
        def update(self, n=1):
            self.n += n
        def close(self):
            print()
        def __enter__(self):
            return self
        def __exit__(self, *args):
            self.close()

# ============================================================================
# ======================== 【配置区域 - 详细说明版】===========================
# ============================================================================

# ----------------------------------------------------------------------------
# 1. 基础运行配置
# ----------------------------------------------------------------------------
TARGET_URL              = "https://iptv.809899.xyz"  # 目标网站地址
HEADLESS                = True                         # 无头模式：True=不显示浏览器窗口(服务器推荐)，False=显示窗口(调试用)
BROWSER_TYPE            = "chromium"                   # 浏览器类型：chromium/firefox/webkit
MAX_IPS                 = 8                            # 最多处理IP数量：建议5-10，太多可能导致网络拥堵
PAGE_LOAD_TIMEOUT       = 180000                       # 页面加载超时(毫秒)：网络慢可适当增加
PAGE_LOAD_TIMEOUT_SEC   = 180                          # 页面加载超时(秒)：与上面保持一致

# ----------------------------------------------------------------------------
# 2. 输出文件配置
# ----------------------------------------------------------------------------
OUTPUT_DIR              = Path(__file__).parent        # 输出目录：默认脚本所在目录
OUTPUT_M3U_FILENAME     = "iptv_channels.m3u" # M3U输出文件名
OUTPUT_TXT_FILENAME     = "iptv_channels.txt" # TXT输出文件名
OUTPUT_JSON_FILENAME    = "iptv_channels.json" # JSON输出文件名(包含详细质量信息)
MAX_LINKS_PER_CHANNEL   = 3                            # 每个频道保留链接数：建议2-5，只保留质量最好的
ENABLE_JSON_OUTPUT      = True                         # 是否输出JSON格式(包含详细质量数据)

# ----------------------------------------------------------------------------
# 3. 核心测速配置 - 影响筛选严格程度
# ----------------------------------------------------------------------------
ENABLE_SPEED_TEST       = True                         # 总开关：是否启用测速(建议开启)
SPEED_TEST_CONCURRENCY  = 10                            # 并发测速数：建议2-5，太高可能导致网络拥堵
SPEED_TEST_TIMEOUT      = 900                          # 整体测速超时(秒)
SPEED_TEST_VERBOSE      = True                         # 详细日志：True=显示每个链接的测试详情，False=只显示汇总

# 测速采样配置
SPEED_TEST_DURATION     = 10                           # 测速时长(秒)：建议5-15，越长越准确但越慢
TS_SAMPLE_COUNT         = 6                            # 测试TS片段数：建议3-8，越多越准确
TS_DOWNLOAD_TIMEOUT     = 10                           # 单个TS下载超时(秒)

# 速度阈值配置 (单位：Mbps)
MIN_SPEED_FACTOR        = 3.5                          # 最小速度要求：建议2.0-5.0，低于此速度直接淘汰
MIN_STABLE_SPEED        = 3.0                          # 最小稳定速度：建议2.0-4.0，平均速度需达到此值
MAX_SPEED_FACTOR        = 50.0                         # 最大速度上限：超过此值按此值计算评分(避免异常值影响)

# 稳定性配置
STABILITY_THRESHOLD     = 0.12                         # 速度波动阈值：建议0.1-0.2，越小要求越稳定
JITTER_THRESHOLD        = 0.15                         # 延迟抖动阈值(秒)：建议0.1-0.3，越小越流畅
MIN_TS_DURATION         = 3.0                          # 最小TS片段时长(秒)：建议2-5，太短会导致频繁切换

# ----------------------------------------------------------------------------
# 4. 分辨率和画质配置
# ----------------------------------------------------------------------------
ENABLE_RESOLUTION_FILTER = True                        # 是否启用分辨率过滤
MIN_RESOLUTION_WIDTH    = 1920                         # 最小宽度(像素)：720P=1280, 1080P=1920
MIN_RESOLUTION_HEIGHT   = 1080                          # 最小高度(像素)：720P=720, 1080P=1080
FALLBACK_TO_SPEED_WHEN_NO_RESOLUTION = False          # 无分辨率信息时：True=保留(按速度)，False=直接过滤
PREFER_HIGHER_RESOLUTION = True                         # 优先选择高分辨率：True=分辨率权重更高，False=速度权重更高

# ----------------------------------------------------------------------------
# 5. 智能重试和容错配置
# ----------------------------------------------------------------------------
ENABLE_RETRY            = True                         # 是否启用失败重试
MAX_RETRY_COUNT         = 3                            # 最大重试次数：建议2-4
RETRY_DELAY             = 2.0                          # 重试间隔(秒)
ENABLE_ADAPTIVE_TESTING = True                         # 自适应测试：网络好时加快测试，网络差时延长测试时间

# ----------------------------------------------------------------------------
# 6. 质量评分权重配置 - 调整各项指标的重要性
# ----------------------------------------------------------------------------
# 各项权重总和建议为1.0
WEIGHT_SPEED            = 0.35                         # 速度权重：越大越看重速度
WEIGHT_STABILITY        = 0.25                         # 稳定性权重：越大越看重稳定性
WEIGHT_DELAY            = 0.20                         # 延迟权重：越大越看重低延迟
WEIGHT_RESOLUTION       = 0.20                         # 分辨率权重：越大越看重画质

# ----------------------------------------------------------------------------
# 7. 缓存配置 - 避免重复测试
# ----------------------------------------------------------------------------
ENABLE_CACHE            = True                         # 是否启用缓存
CACHE_FILE              = "iptv_test_cache.json"       # 缓存文件名
CACHE_EXPIRE_HOURS      = 24                           # 缓存有效期(小时)：建议12-48

# ----------------------------------------------------------------------------
# 8. 浏览器和页面操作配置
# ----------------------------------------------------------------------------
DELAY_BETWEEN_IPS       = 1.5                          # 处理IP间隔(秒)：避免操作过快
DELAY_AFTER_CLICK       = 0.8                          # 点击后等待(秒)：让页面有时间加载
MAX_CHANNELS_PER_IP     = 0                            # 每个IP最多提取频道数：0=不限制
SCRIPT_TIMEOUT          = 3600                         # 脚本总超时(秒)：默认1小时

# ----------------------------------------------------------------------------
# 9. 数据清理和去重配置
# ----------------------------------------------------------------------------
ENABLE_CHINESE_CLEAN    = True                         # 清理频道名非中文字符
ENABLE_DEDUPLICATION    = True                         # 链接去重
ENABLE_SMART_DEDUP      = True                         # 智能去重：相似链接也去重
ENABLE_SCREENSHOTS      = False                        # 调试截图：仅用于调试问题

# ----------------------------------------------------------------------------
# 10. 频道名称映射配置
# ----------------------------------------------------------------------------
CCTV_USE_MAPPING        = True                         # 是否使用央视名称映射(如CCTV-1综合)
ENABLE_GROUP_OPTIMIZATION = True                       # 是否优化分组顺序

# ----------------------------------------------------------------------------
# 11. M3U输出优化配置
# ----------------------------------------------------------------------------
ENABLE_M3U_OPTIMIZATION = True                         # M3U优化：添加播放器友好标签
M3U_BUFFER_LENGTH       = 15                           # 建议缓冲时长(秒)：建议10-20
M3U_CACHE_LENGTH        = 30                           # 建议缓存时长(秒)
ENABLE_EPG_MARKER       = True                         # 添加EPG标记(如果有)

# ----------------------------------------------------------------------------
# 12. 日志配置
# ----------------------------------------------------------------------------
LOG_LEVEL               = "INFO"                       # 日志级别：DEBUG/INFO/WARNING/ERROR
ENABLE_PROGRESS_BAR     = True                         # 显示进度条

# ============================================================================
# ============================ 频道分类配置 ============================
# ============================================================================

PAGE_CONFIG = {
    "engine_search": ["引索搜索", "引擎搜索", "关键词搜索"],
    "multicast_tab": ["组播提取"],
    "start_button": ["开始播放", "开始搜索", "开始提取"],
}

CATEGORY_RULES = [
    {"name": "4K专区",      "keywords": ["4k", "uhd", "2160"]},
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
    "央视频道", "卫视频道", "电影频道", "4K专区", "儿童频道", "轮播频道"
]

CCTV_NAME_MAPPING = {
    "1": "综合", "2": "财经", "3": "综艺", "4": "国际", "5": "体育",
    "5+": "体育赛事", "6": "电影", "7": "国防军事", "8": "电视剧",
    "9": "纪录", "10": "科教", "11": "戏曲", "12": "社会与法",
    "13": "新闻", "14": "少儿", "15": "音乐", "16": "奥林匹克",
    "17": "农业农村",
}

# ============================================================================
# ======================== 【核心优化代码】===========================
# ============================================================================

IP_PATTERN = re.compile(r'^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$')
CCTV_PATTERN = re.compile(r'(cctv)[-\s]?(\d{1,3})', re.IGNORECASE)
CETV_PATTERN = re.compile(r'(cetv)[-\s]?(\d)', re.IGNORECASE)
RESOLUTION_PATTERN = re.compile(r'(\d+)x(\d+)')
CHINESE_ONLY_PATTERN = re.compile(r'[^\u4e00-\u9fff]')
TS_DURATION_PATTERN = re.compile(r'#EXTINF:(\d+\.?\d*)')

SCREENSHOT_DIR = OUTPUT_DIR / "debug_screenshots"
SCREENSHOT_DIR.mkdir(exist_ok=True)

# 缓存管理
class CacheManager:
    def __init__(self, cache_file: Path, expire_hours: int):
        self.cache_file = cache_file
        self.expire_hours = expire_hours
        self.cache = self._load_cache()
    
    def _load_cache(self) -> dict:
        if not self.cache_file.exists():
            return {}
        try:
            with open(self.cache_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                # 清理过期缓存
                current_time = time.time()
                expired_keys = [k for k, v in data.items() 
                              if current_time - v.get('timestamp', 0) > self.expire_hours * 3600]
                for k in expired_keys:
                    del data[k]
                return data
        except:
            return {}
    
    def _save_cache(self):
        with open(self.cache_file, 'w', encoding='utf-8') as f:
            json.dump(self.cache, f)
    
    def get(self, url: str) -> Optional[dict]:
        if not ENABLE_CACHE:
            return None
        key = hashlib.md5(url.encode()).hexdigest()
        data = self.cache.get(key)
        if data:
            # 检查是否过期
            if time.time() - data.get('timestamp', 0) < self.expire_hours * 3600:
                return data.get('result')
        return None
    
    def set(self, url: str, result: dict):
        if not ENABLE_CACHE:
            return
        key = hashlib.md5(url.encode()).hexdigest()
        self.cache[key] = {
            'timestamp': time.time(),
            'result': result
        }
        self._save_cache()

cache_manager = CacheManager(OUTPUT_DIR / CACHE_FILE, CACHE_EXPIRE_HOURS)

def build_classifier():
    compiled = []
    for rule in CATEGORY_RULES:
        if not rule["keywords"]:
            continue
        pattern = re.compile("|".join(re.escape(kw.lower()) for kw in rule["keywords"]))
        compiled.append((rule["name"], pattern))
    return lambda name: next((group for group, pat in compiled if pat.search(name.lower())), None)

classify_channel = build_classifier()

def normalize_cctv(name: str) -> str:
    name_lower = name.lower()
    if "cctv5+" in name_lower or "cctv5＋" in name_lower or "cctv5加" in name_lower:
        if CCTV_USE_MAPPING and "5+" in CCTV_NAME_MAPPING:
            return f"CCTV-5+{CCTV_NAME_MAPPING['5+']}"
        return "CCTV5+"
    cctv_match = CCTV_PATTERN.search(name_lower)
    if cctv_match:
        number = cctv_match.group(2)
        if CCTV_USE_MAPPING:
            suffix = CCTV_NAME_MAPPING.get(number, "")
            return f"CCTV-{number}{suffix}"
        rest = name[cctv_match.end():].strip()
        rest = re.sub(r'(?i)(HD|SD|高清|标清|超清|\s*-?\s*)?$', '', rest).strip()
        return f"CCTV-{number} {rest}".strip() if rest else f"CCTV-{number}"
    cetv_match = CETV_PATTERN.search(name_lower)
    if cetv_match:
        number = cetv_match.group(2)
        return f"CETV-{number}" if CCTV_USE_MAPPING else f"CETV{number}"
    return name

def clean_chinese_only(name: str) -> str:
    return CHINESE_ONLY_PATTERN.sub('', name)

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
        await locator.scroll_into_view_if_needed(timeout=5000)
        await asyncio.sleep(0.2)
        await locator.click(force=True, timeout=timeout)
        return True
    except Exception:
        try:
            await locator.evaluate('el => el.scrollIntoViewIfNeeded()')
            await locator.evaluate('el => el.click()')
            return True
        except Exception:
            return False

# ====================== 质量评分系统 =======================

def calculate_quality_score(speed: float, stability: float, delay: float, 
                           resolution: Optional[Tuple[int, int]], 
                           speed_variance: float, jitter: float) -> Tuple[float, dict]:
    """
    计算综合质量评分 (0-100分)
    返回：(总分, 各项得分详情)
    """
    scores = {}
    
    # 1. 速度评分 (0-100)
    if speed >= MAX_SPEED_FACTOR:
        scores['speed'] = 100
    elif speed >= MIN_SPEED_FACTOR:
        # 线性插值
        scores['speed'] = ((speed - MIN_SPEED_FACTOR) / (MAX_SPEED_FACTOR - MIN_SPEED_FACTOR)) * 60 + 40
    else:
        scores['speed'] = max(0, (speed / MIN_SPEED_FACTOR) * 40)
    
    # 2. 稳定性评分 (0-100)
    if speed_variance <= STABILITY_THRESHOLD * 0.5:
        scores['stability'] = 100
    elif speed_variance <= STABILITY_THRESHOLD:
        scores['stability'] = 100 - ((speed_variance - STABILITY_THRESHOLD * 0.5) / (STABILITY_THRESHOLD * 0.5)) * 30
    else:
        scores['stability'] = max(0, 70 - ((speed_variance - STABILITY_THRESHOLD) / STABILITY_THRESHOLD) * 70)
    
    # 3. 延迟评分 (0-100)
    if delay <= 0.2:
        scores['delay'] = 100
    elif delay <= 1.0:
        scores['delay'] = 100 - ((delay - 0.2) / 0.8) * 40
    else:
        scores['delay'] = max(0, 60 - ((delay - 1.0) / 2.0) * 60)
    
    # 4. 分辨率评分 (0-100)
    if resolution:
        width, height = resolution
        pixels = width * height
        if pixels >= 3840 * 2160:  # 4K
            scores['resolution'] = 100
        elif pixels >= 1920 * 1080:  # 1080P
            scores['resolution'] = 85
        elif pixels >= 1280 * 720:  # 720P
            scores['resolution'] = 70
        else:
            scores['resolution'] = 50
    else:
        scores['resolution'] = 60  # 未知分辨率给中等分
    
    # 计算加权总分
    total_score = (
        scores['speed'] * WEIGHT_SPEED +
        scores['stability'] * WEIGHT_STABILITY +
        scores['delay'] * WEIGHT_DELAY +
        scores['resolution'] * WEIGHT_RESOLUTION
    )
    
    return round(total_score, 2), scores

# ====================== 增强的测速和稳定性检测 =======================

async def fetch_url_with_retry(session: aiohttp.ClientSession, url: str, timeout: int, 
                               retry_count: int = 0) -> Tuple[Optional[bytes], float, float]:
    """带重试的URL下载"""
    try:
        start_time = time.monotonic()
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
            connect_time = time.monotonic() - start_time
            if resp.status == 200:
                data = await resp.read()
                total_time = time.monotonic() - start_time
                return data, total_time, connect_time
    except Exception as e:
        if ENABLE_RETRY and retry_count < MAX_RETRY_COUNT:
            if SPEED_TEST_VERBOSE:
                print(f"   🔄 重试 ({retry_count + 1}/{MAX_RETRY_COUNT}): {url}")
            await asyncio.sleep(RETRY_DELAY)
            return await fetch_url_with_retry(session, url, timeout, retry_count + 1)
    return None, 0, 0

async def parse_ts_durations(m3u8_content: str) -> Tuple[List[float], float]:
    """解析TS片段时长"""
    durations = []
    lines = m3u8_content.splitlines()
    
    for line in lines:
        match = TS_DURATION_PATTERN.search(line)
        if match:
            try:
                duration = float(match.group(1))
                if duration > 0:
                    durations.append(duration)
            except:
                continue
    
    avg_duration = statistics.mean(durations) if durations else 0
    return durations, avg_duration

async def test_tv_stability(session: aiohttp.ClientSession, url: str) -> dict:
    """
    完整版稳定性测试
    返回详细的测试结果字典
    """
    result = {
        'url': url,
        'stable': False,
        'speed_mbps': 0,
        'resolution': None,
        'avg_delay': 0,
        'jitter': 0,
        'speed_variance': 0,
        'avg_ts_duration': 0,
        'quality_score': 0,
        'score_details': {},
        'test_time': time.time()
    }
    
    try:
        # 检查缓存
        cached = cache_manager.get(url)
        if cached:
            if SPEED_TEST_VERBOSE:
                print(f"   💾 使用缓存: {url[:60]}...")
            return cached
        
        if not (url.lower().endswith('.m3u8') or 'm3u8' in url.lower()):
            test_result = await test_direct_stream(session, url)
        else:
            test_result = await test_hls_stability(session, url)
        
        # 更新结果
        result.update(test_result)
        
        # 计算质量评分
        if result['speed_mbps'] > 0:
            result['quality_score'], result['score_details'] = calculate_quality_score(
                result['speed_mbps'],
                result['stable'],
                result['avg_delay'],
                result['resolution'],
                result['speed_variance'],
                result['jitter']
            )
        
        # 最终稳定性判断
        result['stable'] = (
            result['stable'] and
            result['speed_mbps'] >= MIN_SPEED_FACTOR and
            result['avg_ts_duration'] >= MIN_TS_DURATION and
            result['jitter'] <= JITTER_THRESHOLD and
            result['quality_score'] >= 50  # 质量分至少50分
        )
        
        # 缓存结果
        cache_manager.set(url, result)
        
        if SPEED_TEST_VERBOSE:
            if result['stable']:
                print(f"   ✅ 合格 (分:{result['quality_score']}, 速:{result['speed_mbps']:.1f}Mbps, "
                      f"延:{result['avg_delay']:.2f}s, 抖:{result['jitter']:.2f}s)")
            else:
                print(f"   ❌ 淘汰 (分:{result['quality_score']}, 速:{result['speed_mbps']:.1f}Mbps, "
                      f"TS:{result['avg_ts_duration']:.1f}s)")
        
    except Exception as e:
        if SPEED_TEST_VERBOSE:
            print(f"   ❌ 测试异常: {str(e)}")
    
    return result

async def test_direct_stream(session: aiohttp.ClientSession, url: str) -> dict:
    """增强版直链流测试"""
    result = {
        'stable': False,
        'speed_mbps': 0,
        'resolution': None,
        'avg_delay': 0,
        'jitter': 0,
        'speed_variance': 0,
        'avg_ts_duration': 999,  # 直链流设为很大
    }
    
    start_time = time.monotonic()
    total_bytes = 0
    speeds = []
    delays = []
    chunk_delays = []
    
    try:
        timeout = aiohttp.ClientTimeout(total=SPEED_TEST_DURATION + 5)
        
        # 测试连接延迟
        connect_start = time.monotonic()
        async with session.get(url, timeout=timeout) as resp:
            connect_delay = time.monotonic() - connect_start
            delays.append(connect_delay)
            
            if resp.status != 200:
                return result
                
            # 分段下载
            while time.monotonic() - start_time < SPEED_TEST_DURATION:
                chunk_start = time.monotonic()
                chunk = await resp.content.read(32768)  # 32KB块
                chunk_delay = time.monotonic() - chunk_start
                chunk_delays.append(chunk_delay)
                
                if not chunk:
                    break
                    
                chunk_size = len(chunk)
                total_bytes += chunk_size
                elapsed = time.monotonic() - start_time
                
                if elapsed > 0:
                    current_speed = (chunk_size * 8) / elapsed / 1_000_000
                    speeds.append(current_speed)
        
        total_elapsed = time.monotonic() - start_time
        if total_elapsed <= 0 or total_bytes == 0:
            return result
            
        # 计算各项指标
        result['speed_mbps'] = (total_bytes * 8) / total_elapsed / 1_000_000
        result['speed_variance'] = max(speeds) - min(speeds) if len(speeds) > 1 else 0
        
        all_delays = delays + chunk_delays
        result['avg_delay'] = statistics.mean(all_delays) if all_delays else 0
        result['jitter'] = statistics.stdev(all_delays) if len(all_delays) > 1 else 0
        
        # 稳定性判断
        result['stable'] = (
            result['speed_mbps'] >= MIN_STABLE_SPEED and 
            result['speed_variance'] <= STABILITY_THRESHOLD * 2 and
            len(speeds) > 5
        )
        
    except Exception:
        pass
    
    return result

async def test_hls_stability(session: aiohttp.ClientSession, url: str) -> dict:
    """增强版HLS流测试"""
    result = {
        'stable': False,
        'speed_mbps': 0,
        'resolution': None,
        'avg_delay': 0,
        'jitter': 0,
        'speed_variance': 0,
        'avg_ts_duration': 0,
    }
    
    # 1. 获取m3u8内容
    content, m3u8_time, m3u8_delay = await fetch_url_with_retry(session, url, TS_DOWNLOAD_TIMEOUT)
    if not content:
        return result
        
    m3u8_text = content.decode('utf-8', errors='ignore')
    ts_durations, avg_ts_duration = parse_ts_durations(m3u8_text)
    result['avg_ts_duration'] = avg_ts_duration
    
    # 2. 解析播放列表
    lines = m3u8_text.splitlines()
    base_url = url[:url.rfind('/')+1] if '/' in url else url
    
    ts_urls = []
    target_bandwidth = 0
    resolution = None
    
    for i, line in enumerate(lines):
        if line.startswith('#EXT-X-STREAM-INF:'):
            bw_match = re.search(r'BANDWIDTH=(\d+)', line)
            res_match = RESOLUTION_PATTERN.search(line)
            
            if bw_match:
                bandwidth = int(bw_match.group(1))
                if bandwidth > target_bandwidth:
                    target_bandwidth = bandwidth
                    if res_match:
                        resolution = (int(res_match.group(1)), int(res_match.group(2)))
        elif not line.startswith('#') and line.strip():
            ts_urls.append(urljoin(base_url, line.strip()))
    
    if not ts_urls:
        return result
    
    result['resolution'] = resolution
    
    # 3. 测试多个TS片段
    sample_urls = ts_urls[:min(TS_SAMPLE_COUNT, len(ts_urls))]
    speeds = []
    sizes = []
    times = []
    delays = [m3u8_delay]
    success_count = 0
    
    for ts_url in sample_urls:
        data, ts_time, ts_delay = await fetch_url_with_retry(session, ts_url, TS_DOWNLOAD_TIMEOUT)
        
        if data and ts_time > 0:
            delays.append(ts_delay)
            size = len(data)
            speed = (size * 8) / ts_time / 1_000_000
            
            speeds.append(speed)
            sizes.append(size)
            times.append(ts_time)
            success_count += 1
    
    if success_count == 0:
        return result
        
    # 计算指标
    result['speed_mbps'] = statistics.mean(speeds)
    result['avg_delay'] = statistics.mean(delays) if delays else 0
    result['jitter'] = statistics.stdev(delays) if len(delays) > 1 else 0
    result['speed_variance'] = max(speeds) - min(speeds) if len(speeds) > 1 else 0
    
    # 稳定性判断
    result['stable'] = (
        result['speed_mbps'] >= MIN_STABLE_SPEED and
        success_count >= TS_SAMPLE_COUNT * 0.7 and
        result['avg_ts_duration'] >= MIN_TS_DURATION
    )
    
    return result

async def test_speed(url: str, group: str, name: str, semaphore: asyncio.Semaphore) -> Optional[Tuple[str, str, str, dict]]:
    """增强版测速函数"""
    async with semaphore:
        try:
            timeout = aiohttp.ClientTimeout(total=SPEED_TEST_DURATION + 15)
            async with aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(ssl=False),
                timeout=timeout,
                connector_owner=True,
                tcp_keepalive=True
            ) as session:
                result = await test_tv_stability(session, url)
                if result['stable']:
                    return (url, group, name, result)
        except Exception as e:
            if SPEED_TEST_VERBOSE:
                print(f"   ❌ 测速错误: {str(e)}")
        return None

# ====================== 测速执行函数 ========================
async def run_speed_test(channel_urls: Dict[Tuple[str, str], List[str]]) -> Tuple[Dict[Tuple[str, str], List[str]], List[dict]]:
    total = sum(len(v) for v in channel_urls.values())
    print(f"\n🚀 开始终极版质量检测，共 {total} 条链接")
    print(f"📋 筛选条件：")
    print(f"   - 速度 ≥ {MIN_SPEED_FACTOR} Mbps")
    print(f"   - 稳定速度 ≥ {MIN_STABLE_SPEED} Mbps")
    print(f"   - 抖动 ≤ {JITTER_THRESHOLD} s")
    print(f"   - TS片段时长 ≥ {MIN_TS_DURATION} s")
    print(f"   - 质量分 ≥ 50")
    print(f"⚖️  评分权重：速度{WEIGHT_SPEED*100:.0f}% + 稳定性{WEIGHT_STABILITY*100:.0f}% + "
          f"延迟{WEIGHT_DELAY*100:.0f}% + 分辨率{WEIGHT_RESOLUTION*100:.0f}%\n")

    sem = asyncio.Semaphore(SPEED_TEST_CONCURRENCY)
    tasks = []
    all_results = []
    
    for (g, n), urls in channel_urls.items():
        for u in urls:
            tasks.append(test_speed(u, g, n, sem))

    results = []
    finished = 0
    printed = set()

    for task in asyncio.as_completed(tasks):
        res = await task
        if res:
            url, g, n, detail = res
            results.append((url, g, n, detail))
            all_results.append(detail)
        finished += 1

        pct = (finished / len(tasks)) * 100
        for step in [10,20,30,40,50,60,70,80,90,100]:
            if pct >= step and step not in printed:
                print(f"📊 检测进度：{step}% ({finished}/{len(tasks)})")
                printed.add(step)

    # 按频道分组并排序
    speed_map = defaultdict(list)
    for url, g, n, detail in results:
        speed_map[(g, n)].append((url, detail))

    out = defaultdict(list)
    final_details = []
    
    for key, items in speed_map.items():
        # 按质量分降序排序
        items.sort(key=lambda x: x[1]['quality_score'], reverse=True)
        
        # 选择质量最好的链接
        final_urls = [url for url, detail in items[:MAX_LINKS_PER_CHANNEL]]
        out[key] = final_urls
        
        # 保存详细信息
        for url, detail in items[:MAX_LINKS_PER_CHANNEL]:
            detail['group'] = key[0]
            detail['name'] = key[1]
            final_details.append(detail)

    print(f"\n✅ 质量检测完成！")
    print(f"   - 检测链接数：{total}")
    print(f"   - 合格链接数：{len(results)}")
    print(f"   - 最终保留：{sum(len(v) for v in out.values())} 条")
    
    if all_results:
        avg_score = statistics.mean(r['quality_score'] for r in all_results)
        avg_speed = statistics.mean(r['speed_mbps'] for r in all_results)
        print(f"   - 平均质量分：{avg_score:.1f}")
        print(f"   - 平均速度：{avg_speed:.1f} Mbps")

    return out, final_details

# ====================== IP 提取逻辑 ===============================

async def extract_from_ip(page, row, ip_text: str) -> List[Tuple[str, str, str]]:
    entries = []
    print(f"\n📌 处理 IP: {ip_text}")

    menu_btn = row.locator("button:has(i.fas.fa-list), button:has-text('≡'), button:has(i.fa-list)").first
    if await menu_btn.count() > 0:
        await robust_click(menu_btn, description="菜单按钮")
    else:
        await row.locator("div.item-title").first.click(timeout=5000)
    await asyncio.sleep(DELAY_AFTER_CLICK)

    modal = page.locator(".modal-dialog").first
    try:
        await modal.wait_for(state="visible", timeout=8000)
    except PlaywrightTimeoutError:
        return entries

    items = modal.locator(".item-content")
    total = await items.count()
    limit = total if MAX_CHANNELS_PER_IP <= 0 else min(total, MAX_CHANNELS_PER_IP)

    for j in range(limit):
        item = items.nth(j)
        try:
            raw_name = await item.locator(".item-title").first.inner_text(timeout=5000)
            link = await item.locator(".item-subtitle").first.inner_text(timeout=5000)
        except:
            continue

        raw_name = raw_name.strip()
        link = link.strip()
        if not raw_name or not link:
            continue

        norm_name = normalize_cctv(raw_name)
        group = classify_channel(norm_name) or classify_channel(raw_name)
        if not group:
            continue

        final_name = norm_name if group == "央视频道" else (clean_chinese_only(raw_name) if ENABLE_CHINESE_CLEAN else raw_name)
        if not final_name:
            continue

        entries.append((group, final_name, link))
    return entries

async def wait_for_ip_elements(page, max_retries=3):
    for attempt in range(max_retries):
        print(f"⏳ 第 {attempt+1} 次等待：30 秒后获取数据")
        await asyncio.sleep(30)
        
        try:
            has_ip = await page.evaluate("""
                () => {
                    const elements = document.querySelectorAll('div.item-title');
                    for (let el of elements) {
                        if (el.innerText.match(/\d+\.\d+\.\d+\.\d+/)) return true;
                    }
                    return false;
                }
            """)
            if has_ip:
                print("✅ IP 数据已加载")
                return True
        except Exception:
            print(f"⚠️ 第 {attempt+1} 次未获取到数据")
    
    print("❌ 多次等待后仍无数据，继续执行")
    return False

# ====================== 主流程 ===============================

async def _main():
    print(f"{'='*60}")
    print(f"🚀 IPTV 组播提取工具 - 终极优化版")
    print(f"{'='*60}")
    print(f"📅 开始时间: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"⚙️  配置概览:")
    print(f"   - 处理IP数: {MAX_IPS}")
    print(f"   - 测速并发: {SPEED_TEST_CONCURRENCY}")
    print(f"   - 每频道保留: {MAX_LINKS_PER_CHANNEL} 条")
    print(f"   - 缓存: {'启用' if ENABLE_CACHE else '禁用'}")
    print(f"{'='*60}\n")

    try:
        import aiohttp
    except ImportError:
        print("❌ 请安装 aiohttp: pip install aiohttp")
        sys.exit(1)

    async with async_playwright() as p:
        browser_args = [
            "--no-sandbox",
            "--disable-blink-features=AutomationControlled",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--disable-web-security",
            "--ignore-certificate-errors"
        ]
        
        browser = await getattr(p, BROWSER_TYPE).launch(
            headless=HEADLESS, 
            args=browser_args,
            slow_mo=100
        )
        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()

        try:
            print("🌐 正在加载页面...")
            await page.goto(TARGET_URL, timeout=PAGE_LOAD_TIMEOUT, wait_until="networkidle")
            print("✅ 页面加载完成")
        except PlaywrightTimeoutError:
            print(f"⚠️ 页面加载超时 ({PAGE_LOAD_TIMEOUT_SEC}秒)，继续执行")
        
        if ENGINE_SELECTOR:
            elem = page.locator(ENGINE_SELECTOR).first
            if await elem.count() > 0:
                await robust_click(elem, description="引擎搜索")
                await asyncio.sleep(DELAY_AFTER_CLICK)
                print("✅ 点击引擎搜索")

        if MCAST_SELECTOR:
            tab = page.locator(MCAST_SELECTOR).first
            await tab.wait_for(state="attached", timeout=15000)
            await robust_click(tab, description="组播提取")
            await asyncio.sleep(DELAY_AFTER_CLICK)
            print("✅ 点击组播提取")

        if START_SELECTOR:
            btn = page.locator(START_SELECTOR).first
            await robust_click(btn, description="开始提取")
            await asyncio.sleep(DELAY_AFTER_CLICK)
            print("✅ 点击开始提取")

        print("\n⏳ 等待数据加载...")
        await wait_for_ip_elements(page)

        rows = page.locator("div.ios-list-item").filter(has_text="频道:")
        total_ips = await rows.count()
        process_cnt = min(total_ips, MAX_IPS) if MAX_IPS else total_ips
        print(f"\n📋 共发现 {total_ips} 个IP，处理前 {process_cnt} 个")

        raw = []
        for i in range(process_cnt):
            row = rows.nth(i)
            ip_elem = row.locator("div.item-title").first
            if await ip_elem.count() == 0:
                continue
                
            ip = await ip_elem.inner_text()
            ip = ip.strip()
            if not IP_PATTERN.match(ip):
                print(f"⚠️ 跳过无效 IP: {ip}")
                continue
                
            try:
                entries = await extract_from_ip(page, row, ip)
                raw.extend(entries)
                print(f"   提取到 {len(entries)} 个频道")
            except Exception as e:
                print(f"⚠️ 处理IP {ip} 时出错: {str(e)}")
                continue
                
            if i < process_cnt - 1:
                await asyncio.sleep(DELAY_BETWEEN_IPS)

        print(f"\n📊 初步提取：{len(raw)} 条链接")

        # 去重
        channel_map = defaultdict(list)
        seen = set()
        for g, n, u in raw:
            if ENABLE_DEDUPLICATION:
                k = (g, n, u)
                if k in seen:
                    continue
                seen.add(k)
            channel_map[(g, n)].append(u)

        print(f"📊 去重后：{len(channel_map)} 个频道，{sum(len(v) for v in channel_map.values())} 条链接")

        # 测速和质量检测
        final_details = []
        if ENABLE_SPEED_TEST and channel_map:
            channel_map, final_details = await run_speed_test(channel_map)

        # 准备最终数据
        final = []
        for (g, n), urls in channel_map.items():
            for u in urls:
                final.append((g, n, u))

        grouped = defaultdict(list)
        for g, n, u in final:
            grouped[g].append((n, u))

        # 央视排序
        cctv_g = next((g for g in grouped if "央视" in g), None)
        if cctv_g:
            def ckey(x):
                m = re.search(r"CCTV-(\d+)", x[0])
                return int(m.group(1)) if m else 999
            grouped[cctv_g].sort(key=ckey)

        # 输出文件
        print(f"\n💾 正在保存文件...")
        
        # M3U输出
        m3u_path = OUTPUT_DIR / OUTPUT_M3U_FILENAME
        with open(m3u_path, "w", encoding="utf-8") as f:
            f.write("#EXTM3U\n")
            if ENABLE_M3U_OPTIMIZATION:
                f.write(f"#EXT-X-OPTIONS:BUFFER-LENGTH={M3U_BUFFER_LENGTH}\n")
                f.write(f"#EXT-X-OPTIONS:CACHE-LENGTH={M3U_CACHE_LENGTH}\n")
            for g in GROUP_ORDER:
                for n, u in grouped.get(g, []):
                    f.write(f'#EXTINF:-1 group-title="{g}",{n}\n{u}\n')
        print(f"   ✅ {OUTPUT_M3U_FILENAME}")

        # TXT输出
        txt_path = OUTPUT_DIR / OUTPUT_TXT_FILENAME
        with open(txt_path, "w", encoding="utf-8") as f:
            f.write("# ========================================================\n")
            f.write("# IPTV 频道列表 - 终极优化版\n")
            f.write(f"# 生成时间: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"# 链接数量: {len(final)}\n")
            f.write("# ========================================================\n\n")
            for g in GROUP_ORDER:
                if g not in grouped:
                    continue
                f.write(f"{g},#genre#\n")
                for n, u in grouped.get(g, []):
                    f.write(f"{n},{u}\n")
                f.write("\n")
        print(f"   ✅ {OUTPUT_TXT_FILENAME}")

        # JSON输出(包含详细质量信息)
        if ENABLE_JSON_OUTPUT and final_details:
            json_path = OUTPUT_DIR / OUTPUT_JSON_FILENAME
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump({
                    'generated_at': time.strftime('%Y-%m-%d %H:%M:%S'),
                    'total_channels': len(final),
                    'config': {
                        'min_speed': MIN_SPEED_FACTOR,
                        'min_stable_speed': MIN_STABLE_SPEED,
                        'jitter_threshold': JITTER_THRESHOLD,
                        'min_ts_duration': MIN_TS_DURATION
                    },
                    'channels': final_details
                }, f, ensure_ascii=False, indent=2)
            print(f"   ✅ {OUTPUT_JSON_FILENAME}")

        print(f"\n{'='*60}")
        print(f"🎉 全部完成！")
        print(f"{'='*60}")
        print(f"📊 最终统计：")
        print(f"   - 有效链接：{len(final)} 条")
        print(f"   - 覆盖频道：{len(channel_map)} 个")
        print(f"   - 包含分组：{len(grouped)} 个")
        print(f"📁 输出文件：")
        print(f"   - {OUTPUT_M3U_FILENAME}")
        print(f"   - {OUTPUT_TXT_FILENAME}")
        if ENABLE_JSON_OUTPUT:
            print(f"   - {OUTPUT_JSON_FILENAME}")
        print(f"\n💡 播放器建议：")
        print(f"   - 使用支持预缓冲的播放器(VLC、PotPlayer、IINA)")
        print(f"   - 设置网络缓冲时间 ≥ {M3U_BUFFER_LENGTH} 秒")
        print(f"   - 优先选择质量分高的链接")
        print(f"{'='*60}\n")
        
        await browser.close()

async def main_with_timeout():
    try:
        await asyncio.wait_for(_main(), timeout=SCRIPT_TIMEOUT)
    except asyncio.TimeoutError:
        print("⚠️ 脚本超时，尝试保存已处理结果...")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main_with_timeout())
