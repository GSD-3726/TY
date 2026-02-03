import requests
import json
import time
import random
import hashlib
import re
import unicodedata
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
import os
import xml.etree.ElementTree as ET

# -------------------------- 【新增核心】代理配置与生效验证（最前置执行，日志优先输出） --------------------------
# 从环境变量读取代理（与GitHub Actions配置一致，无需硬编码）
HTTP_PROXY = os.getenv("HTTP_PROXY")
HTTPS_PROXY = os.getenv("HTTPS_PROXY")
# 构造requests通用代理字典
PROXIES = {
    "http": HTTP_PROXY,
    "https": HTTPS_PROXY
}
# 全局设置urllib代理（覆盖pip/所有基于urllib的库，双保险）
os.environ['http_proxy'] = HTTP_PROXY or ""
os.environ['https_proxy'] = HTTPS_PROXY or ""
os.environ['no_proxy'] = os.getenv("NO_PROXY") or ""

# 【关键】代理生效验证日志（最先执行，一眼看出是否加载代理+是否国内IP）
print("="*50 + "【代理生效验证】" + "="*50)
print(f"当前加载的HTTP代理：{HTTP_PROXY if HTTP_PROXY else '未加载！'}")
print(f"当前加载的HTTPS代理：{HTTPS_PROXY if HTTPS_PROXY else '未加载！'}")
if HTTP_PROXY and HTTPS_PROXY:
    try:
        # 访问IP查询接口，获取代理出口IP（国内IP则代理生效）
        ip_resp = requests.get("https://httpbin.org/ip", proxies=PROXIES, timeout=10)
        ip_data = ip_resp.json()
        print(f"✅ 代理出口IP（国内则生效）：{ip_data}")
    except Exception as e:
        print(f"❌ 代理验证失败（可能代理失效/网络问题）：{str(e)}")
else:
    print("⚠️  未检测到代理配置！请检查GitHub Actions的env环境变量")
print("="*110 + "\n")

# -------------------------- 原有核心配置（无修改） --------------------------
LOCAL_EPG_CACHE = "epg.xml"
thread_mum = 10
headers = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "Origin": "https://m.miguvideo.com",
    "Pragma": "no-cache",
    "Referer": "https://m.miguvideo.com/",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
    "Support-Pendant": "1",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0",
    "appCode": "miguvideo_default_h5",
    "appId": "miguvideo",
    "channel": "H5",
    "sec-ch-ua": "\"Chromium\";v=\"136\", \"Microsoft Edge\";v=\"136\", \"Not.A/Brand\";v=\"99\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\"",
    "terminalId": "h5"
}

lives = ['热门', '央视', '卫视', '地方', '体育', '影视', '综艺', '少儿', '新闻', '教育', '熊猫', '纪实']
LIVE = {'热门': 'e7716fea6aa1483c80cfc10b7795fcb8', '体育': '7538163cdac044398cb292ecf75db4e0',
        '央视': '1ff892f2b5ab4a79be6e25b69d2f5d05', '卫视': '0847b3f6c08a4ca28f85ba5701268424',
        '地方': '855e9adc91b04ea18ef3f2dbd43f495b', '影视': '10b0d04cb23d4ac5945c4bc77c7ac44e',
        '新闻': 'c584f67ad63f4bc983c31de3a9be977c', '教育': 'af72267483d94275995a4498b2799ecd',
        '熊猫': 'e76e56e88fff4c11b0168f55e826445d', '综艺': '192a12edfef04b5eb616b878f031f32f',
        '少儿': 'fc2f5b8fd7db43ff88c4243e731ecede', '纪实': 'e1165138bdaa44b9a3138d74af6c6673'}

m3u_path = 'migu.m3u'
txt_path = 'migu.txt'
M3U_HEADER = f'#EXTM3U\n'
channels_dict = {}
processed_pids = set()
FLAG = 0
appVersion = "2600034600"
appVersionID = appVersion + "-99000-201600010010028"

# -------------------------- 原有工具函数（无修改） --------------------------
def extract_cctv_number(channel_name):
    match = re.search(r'CCTV[-\s]?(\d+)', channel_name)
    if match:
        try:
            return int(match.group(1))
        except:
            return 999
    if 'CCTV' in channel_name:
        if 'CGTN' in channel_name:
            if '法语' in channel_name:
                return 1001
            elif '西班牙语' in channel_name:
                return 1002
            elif '俄语' in channel_name:
                return 1003
            elif '阿拉伯语' in channel_name:
                return 1004
            elif '外语纪录' in channel_name:
                return 1005
            else:
                return 1000
        elif '美洲' in channel_name:
            return 1006
        elif '欧洲' in channel_name:
            return 1007
    return 9999

def extract_panda_number(channel_name):
    match = re.search(r'熊猫(\d+)', channel_name)
    if match:
        try:
            return int(match.group(1))
        except:
            return 999
    return 9999

def extract_satellite_first_char(channel_name):
    if not channel_name:
        return 'z'
    first_char = channel_name[0]
    normalized_char = unicodedata.normalize('NFKC', first_char)
    return normalized_char

def get_sort_key(channel_name):
    if 'CCTV' in channel_name:
        cctv_num = extract_cctv_number(channel_name)
        return (0, cctv_num, channel_name)
    if '熊猫' in channel_name:
        panda_num = extract_panda_number(channel_name)
        return (1, panda_num, channel_name)
    if is_satellite_channel(channel_name):
        first_char = extract_satellite_first_char(channel_name)
        return (2, first_char, channel_name)
    return (3, channel_name)

def is_cctv_channel(channel_name):
    return 'CCTV' in channel_name or 'CGTN' in channel_name

def is_satellite_channel(channel_name):
    return '卫视' in channel_name and 'CCTV' not in channel_name

def smart_classify_5_categories(channel_name):
    if channel_name in channels_dict:
        return None
    if '熊猫' in channel_name:
        return '🐼熊猫频道'
    if is_cctv_channel(channel_name):
        return '📺央视频道'
    if is_satellite_channel(channel_name):
        return '📡卫视频道'
    lower_name = channel_name.lower()
    entertainment_keywords = ['电影', '影视', '影院', '影迷', '少儿', '卡通', '动漫', '动画',
                              '综艺', '戏曲', '音乐', '秦腔', '嘉佳', '优漫', '新动漫', '经典动画']
    for keyword in entertainment_keywords:
        if keyword in channel_name:
            return '🎬影音娱乐'
    return '📰生活资讯'

def format_date_ymd():
    current_date = datetime.now()
    return f"{current_date.year}{current_date.month:02d}{current_date.day:02d}"

def writefile(path, content, mode='w'):
    with open(path, mode, encoding='utf-8') as f:
        f.write(content)

def md5(text):
    md5_obj = hashlib.md5()
    md5_obj.update(text.encode('utf-8'))
    return md5_obj.hexdigest()

def getSaltAndSign(pid):
    timestamp = str(int(time.time() * 1000))
    random_num = random.randint(0, 999999)
    salt = f"{random_num:06d}25"
    suffix = "2cac4f2c6c3346a5b34e085725ef7e33migu" + salt[:4]
    app_t = timestamp + pid + appVersion[:8]
    sign = md5(md5(app_t) + suffix)
    return {
        "salt": salt,
        "sign": sign,
        "timestamp": timestamp
    }

# -------------------------- 【修改】所有requests请求添加proxies=PROXIES（确保走代理） --------------------------
def get_content(pid):
    _headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
        "apipost-client-id": "465aea51-4548-495a-8709-7e532dbe3703",
        "apipost-language": "zh-cn",
        "apipost-machine": "3a214a07786002",
        "apipost-platform": "Win",
        "apipost-terminal": "web",
        "apipost-token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJwYXlsb2FkIjp7InVzZXJfaWQiOjM5NDY2NDM3MTIyMzAwMzEzNywidGltZSI6MTc2NTYzMjU2NSwidXVpZCI6ImJlNDJjOTMxLWQ4MjctMTFmMC1hNThiLTUyZTY1ODM4NDNhOSJ9fQ.QU0RXa0e-yB-fwJNjYt_OnyM6RteY3L1BaUWqCrdAB4",
        "apipost-version": "8.2.6",
        "cache-control": "no-cache",
        "content-type": "application/json",
        "pragma": "no-cache",
        "priority": "u=1, i",
        "sec-ch-ua": '"Chromium";v="136", "Microsoft Edge\";v="136", \"Not.A/Brand\";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "cookie": "apipost-token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJwYXlsb2FkIjp7InVzZXJfaWQiOjM5NDY2NDM3MTIyMzAwMzEzNywidGltZSI6MTc2NTYzMjU2NSwidXVpZCI6ImJlNDJjOTMxLWQ4MjctMTFmMC1hNThiLTUyZTY1ODM4NDNhOSJ9fQ.QU0RXa0e-yB-fwJNjYt_OnyM6RteY3L1BaUWqCrdAB4; SERVERID=236fe4f21bf23223c449a2ac2dc20aa4|1765632725|1765632691; SERVERCORSID=236fe4f21bf23223c449a2ac2dc20aa4|1765632725|1765632691",
        "Referer": "https://workspace.apipost.net/57a21612a051000/apis",
        "Referrer-Policy": "strict-origin-when-cross-origin"
    }
    result = getSaltAndSign(pid)
    rateType = "2" if pid == "608831231" else "3"
    URL = f"https://play.miguvideo.com/playurl/v1/play/playurl?sign={result['sign']}&rateType={rateType}&contId={pid}&timestamp={result['timestamp']}&salt={result['salt']}"
    params = URL.split("?")[1].split("&")
    body = {
        "option": {
            "scene": "http_request",
            "lang": "zh-cn",
            "globals": {},
            "project": {
                "request": {
                    "header": {
                        "parameter": [
                            {
                                "key": "Accept",
                                "value": "*/*",
                                "is_checked": 1,
                                "field_type": "String",
                                "is_system": 1
                            },
                            {
                                "key": "Accept-Encoding",
                                "value": "gzip, deflate, br",
                                "is_checked": 1,
                                "field_type": "String",
                                "is_system": 1
                            },
                            {
                                "key": "User-Agent",
                                "value": "PostmanRuntime-ApipostRuntime/1.1.0",
                                "is_checked": 1,
                                "field_type": "String",
                                "is_system": 1
                            },
                            {
                                "key": "Connection",
                                "value": "keep-alive",
                                "is_checked": 1,
                                "field_type": "String",
                                "is_system": 1
                            }
                        ]
                    },
                    "query": {"parameter": []},
                    "body": {"parameter": []},
                    "cookie": {"parameter": []},
                    "auth": {"type": "noauth"},
                    "pre_tasks": [],
                    "post_tasks": []
                }
            },
            "env": {
                "env_id": "1",
                "env_name": "默认环境",
                "env_pre_url": "",
                "env_pre_urls": {
                    "1": {"server_id": "1", "name": "默认服务", "sort": 1000, "uri": ""},
                    "default": {"server_id": "1", "name": "默认服务", "sort": 1000, "uri": ""}
                },
                "environment": {}
            },
            "cookies": {"switch": 1, "data": []},
            "system_configs": {
                "send_timeout": 0,
                "auto_redirect": -1,
                "max_redirect_time": 5,
                "auto_gen_mock_url": -1,
                "request_param_auto_json": -1,
                "proxy": {
                    "type": 2, "envfirst": 1, "bypass": [], "protocols": ["http"],
                    "auth": {"authenticate": -1, "host": "", "username": "", "password": ""}
                },
                "ca_cert": {"open": -1, "path": "", "base64": ""},
                "client_cert": {}
            },
            "custom_functions": {},
            "collection": [{
                "target_id": "3c5fd6a9786002", "target_type": "api", "parent_id": "0", "name": "MIGU",
                "request": {
                    "auth": {"type": "inherit"},
                    "body": {
                        "mode": "None", "parameter": [], "raw": "", "raw_parameter": [],
                        "raw_schema": {"type": "object"}, "binary": None
                    },
                    "pre_tasks": [], "post_tasks": [],
                    "header": {"parameter": [
                        {"description": "", "field_type": "string", "is_checked": 1, "key": " AppVersion",
                         "value": "2600034600", "not_None": 1, "schema": {"type": "string"},
                         "param_id": "3c60653273e0b3"},
                        {"description": "", "field_type": "string", "is_checked": 1, "key": "TerminalId",
                         "value": "android", "not_None": 1, "schema": {"type": "string"}, "param_id": "3c6075c1f3e0e1"},
                        {"description": "", "field_type": "string", "is_checked": 1, "key": "X-UP-CLIENT-CHANNEL-ID",
                         "value": "2600034600-99000-201600010010028", "not_None": 1, "schema": {"type": "string"},
                         "param_id": "3c60858bb3e10c"}
                    ]},
                    "query": {"parameter": [
                        {"param_id": "3c5fd74233e004", "field_type": "string", "is_checked": 1, "key": "sign",
                         "not_None": 1, "value": params[0].split("=")[1], "description": ""},
                        {"param_id": "3c6022f433e030", "field_type": "string", "is_checked": 1, "key": "rateType",
                         "not_None": 1, "value": params[1].split("=")[1], "description": ""},
                        {"param_id": "3c60354133e05b", "field_type": "string", "is_checked": 1, "key": "contId",
                         "not_None": 1, "value": params[2].split("=")[1], "description": ""},
                        {"param_id": "3c605e4bf860b1", "field_type": "String", "is_checked": 1, "key": "timestamp",
                         "not_None": 1, "value": params[3].split("=")[1], "description": ""},
                        {"param_id": "3c605e4c3860b2", "field_type": "String", "is_checked": 1, "key": "salt",
                         "not_None": 1, "value": params[4].split("=")[1], "description": ""}
                    ], "query_add_equal": 1},
                    "cookie": {"parameter": [], "cookie_encode": 1},
                    "restful": {"parameter": []},
                    "tabs_default_active_key": "query"
                },
                "parents": [], "method": "POST", "protocol": "http/1.1", "url": URL, "pre_url": ""
            }],
            "database_configs": {}
        },
        "test_events": [{
            "type": "api",
            "data": {"target_id": "3c5fd6a9786002", "project_id": "57a21612a051000", "parent_id": "0",
                     "target_type": "api"}
        }]
    }
    body = json.dumps(body, separators=(",", ":"))
    url = "https://workspace.apipost.net/proxy/v2/http"
    # 【新增】添加proxies=PROXIES，确保该请求走代理
    resp = requests.post(url, headers=_headers, data=body, proxies=PROXIES).json()
    return json.loads(resp["data"]["data"]["response"]["body"])

def getddCalcu720p(url, pID):
    puData = url.split("&puData=")[1]
    keys = "cdabyzwxkl"
    ddCalcu = []
    for i in range(0, int(len(puData) / 2)):
        ddCalcu.append(puData[int(len(puData)) - i - 1])
        ddCalcu.append(puData[i])
        if i == 1:
            ddCalcu.append("v")
        if i == 2:
            ddCalcu.append(keys[int(format_date_ymd()[2])])
        if i == 3:
            ddCalcu.append(keys[int(pID[6])])
        if i == 4:
            ddCalcu.append("a")
    return f'{url}&ddCalcu={"".join(ddCalcu)}&sv=10004&ct=android'

def append_All_Live(live, flag, data):
    try:
        if data["pID"] in processed_pids:
            return
        processed_pids.add(data["pID"])

        respData = get_content(data["pID"])
        playurl = getddCalcu720p(respData["body"]["urlInfo"]["url"], data["pID"])

        if playurl != "":
            z = 1
            while z <= 6:
                # 【新增】添加proxies=PROXIES，确保重定向请求走代理
                obj = requests.get(playurl, allow_redirects=False, proxies=PROXIES)
                location = obj.headers.get("Location", "")
                if not location:
                    continue
                if location.startswith("http://hlsz"):
                    playurl = location
                    break
                if z <= 6:
                    time.sleep(0.15)
                z += 1

        if z != 7:
            ch_name = data["name"]
            if "CCTV" in ch_name:
                ch_name = ch_name.replace("CCTV", "CCTV-")
            if "熊猫" in ch_name:
                ch_name = ch_name.replace("高清", "")

            category = smart_classify_5_categories(ch_name)
            if category is None:
                return

            sort_key = get_sort_key(ch_name)
            m3u_item = f'#EXTINF:-1 group-title="{category}",{ch_name}\n{playurl}\n'
            txt_item = f"{ch_name},{playurl}\n"
            channels_dict[ch_name] = [m3u_item, txt_item, category, sort_key]
            print(f'频道 [{ch_name}]【{category}】更新成功！')
        else:
            print(f'频道 [{data["name"]}] 更新失败！')
    except Exception as e:
        print(f'频道 [{data["name"]}] 更新失败！错误：{e}')

def update(live, url):
    global FLAG
    pool = ThreadPoolExecutor(thread_mum)
    # 【新增】添加proxies=PROXIES，确保咪咕接口请求走代理
    response = requests.get(url, headers=headers, proxies=PROXIES).json()
    dataList = response["body"]["dataList"]
    for flag, data in enumerate(dataList):
        pool.submit(append_All_Live, live, FLAG + flag, data)
    pool.shutdown()
    FLAG += len(dataList)

# -------------------------- 原有主函数（无修改） --------------------------
def main():
    writefile(m3u_path, M3U_HEADER, 'w')
    writefile(txt_path, "", 'w')

    for live in lives:
        print(f"\n分类 ----- [{live}] ----- 开始更新. . .")
        url = f'https://program-sc.miguvideo.com/live/v2/tv-data/{LIVE[live]}'
        update(live, url)

    category_channels = defaultdict(list)
    for ch_name, (m3u_item, txt_item, category, sort_key) in channels_dict.items():
        category_channels[category].append((sort_key, ch_name, m3u_item, txt_item))

    for category in category_channels:
        category_channels[category].sort(key=lambda x: x[0])

    category_order = [
        '📺央视频道',
        '📡卫视频道',
        '🐼熊猫频道',
        '🎬影音娱乐',
        '📰生活资讯'
    ]

    for category in category_order:
        if category in category_channels:
            for sort_key, ch_name, m3u_item, txt_item in category_channels[category]:
                writefile(m3u_path, m3u_item, 'a')

    for category in category_order:
        if category in category_channels and category_channels[category]:
            writefile(txt_path, f"{category},#genre#\n", 'a')
            for sort_key, ch_name, m3u_item, txt_item in category_channels[category]:
                writefile(txt_path, txt_item, 'a')

    total_channels = len(channels_dict)
    category_stats = {}
    for category in category_order:
        category_stats[category] = len(category_channels.get(category, []))

    print(f"\n✅ 双格式文件生成完成！")
    print(f"📁 M3U格式：{m3u_path}")
    print(f"📁 TXT格式：{txt_path}")
    print(f"📊 总计频道数：{total_channels}")
    print("\n📋 5分类统计：")
    for category in category_order:
        count = category_stats[category]
        percentage = (count / total_channels * 100) if total_channels > 0 else 0
        print(f"  {category}: {count} 个 ({percentage:.1f}%)")

if __name__ == "__main__":
    main()
