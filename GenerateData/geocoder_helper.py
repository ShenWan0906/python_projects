import time
from opencage.geocoder import OpenCageGeocode

# 你的 OpenCage API Key
API_KEY = 'e64126c3b3ad41839934013615ab86d1'

# 初始化 geocoder
geocoder = OpenCageGeocode(API_KEY)

# 缓存已查询的地址
cache = {}

# 最大请求限制，OpenCage 每分钟最多 2500 次请求
MAX_REQUESTS_PER_MINUTE = 2500
request_interval = 60 / MAX_REQUESTS_PER_MINUTE  # 计算每次请求之间的间隔（秒）


def get_coordinates_with_cache(address):
    """
    根据地址获取经纬度，支持缓存和降级获取
    :param address: str - 完整地址字符串（如 "Al Amal District, Riyadh, Saudi Arabia"）
    :return: (latitude, longitude) or (None, None)
    """
    # 如果缓存中已有结果，直接返回
    if address in cache:
        print(f"📍 缓存命中: {address} → {cache[address]}")
        return cache[address]

    print(f"📍 尝试获取坐标: {address}")
    # 分割地址成片段 (倒序保留国家、省、市…)
    parts = [part.strip() for part in address.split(',')]

    while parts:
        query = ", ".join(parts)
        results = geocoder.geocode(query)

        if results and len(results) > 0:
            geometry = results[0]['geometry']
            lat, lng = geometry['lat'], geometry['lng']
            cache[address] = (lat, lng)  # 将结果缓存
            print(f"✅ 找到坐标 [{query}] → ({lat}, {lng})")
            return lat, lng
        else:
            print(f"❌ 无法找到地址: {query}，尝试上一级...")
            parts.pop(0)  # 去掉最前面详细部分 (比如 District)

    print("❌ 全部级别都无法获取坐标")
    cache[address] = (None, None)  # 将无法找到的结果缓存
    return None, None


def batch_get_coordinates(addresses):
    """
    批量获取多个地址的经纬度，自动限速
    :param addresses: list - 多个地址字符串
    :return: dict - 地址到经纬度的映射
    """
    results = {}
    for idx, address in enumerate(addresses):
        print(f"🌍 正在处理第 {idx + 1}/{len(addresses)} 个地址: {address}")

        lat, lng = get_coordinates_with_cache(address)  # 获取经纬度
        results[address] = (lat, lng)

        # 限速处理
        if (idx + 1) % MAX_REQUESTS_PER_MINUTE == 0:
            print("💤 达到请求上限，等待 60 秒...")
            time.sleep(60)  # 等待 60 秒，避免超过 API 限制

        else:
            time.sleep(request_interval)  # 每个请求之间加间隔，确保限速

    return results
