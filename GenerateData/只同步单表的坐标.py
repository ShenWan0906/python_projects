import pandas as pd
import random
import math
import re
from sqlalchemy import text
from dbhelp import engine
from difflib import SequenceMatcher
from tqdm import tqdm
import time
import logging

# === 日志配置 ===
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def clean_string(s):
    if not isinstance(s, str):
        return ""
    return re.sub(r"[^\w\s]", "", s.strip().lower())


def similarity(a, b):
    return SequenceMatcher(None, a, b).ratio()


def random_point_within_radius(lat_center, lon_center, radius_km):
    radius_deg = radius_km / 111.0
    angle = random.uniform(0, 2 * math.pi)
    r = radius_deg * math.sqrt(random.uniform(0, 1))
    lat_offset = r * math.cos(angle)
    lon_offset = r * math.sin(angle) / math.cos(math.radians(lat_center))
    lat = lat_center + lat_offset
    lon = lon_center + lon_offset
    return round(lat, 6), round(lon, 6)  # ✅ 保留6位小数


def build_geo_index(geo_df):
    geo_index = {}
    for _, row in geo_df.iterrows():
        province = clean_string(row['Region'])
        city = clean_string(row['City'])
        district = clean_string(row['district'])
        lat = row['latitude']
        lon = row['longitude']
        if province not in geo_index:
            geo_index[province] = {}
        if city not in geo_index[province]:
            geo_index[province][city] = {}
        if district not in geo_index[province][city]:
            geo_index[province][city][district] = []
        geo_index[province][city][district].append((lat, lon))
    return geo_index


def match_address(province, city, district, geo_index):
    best_province, best_province_score = None, 0
    for prov in geo_index.keys():
        score = similarity(province, prov)
        if score > best_province_score:
            best_province, best_province_score = prov, score
    if best_province_score < 0.6:
        return None, {
            'fail_reason': f'省份匹配失败（{best_province_score:.2f}）| 输入: "{province}" vs 最佳命中: "{best_province}"'
        }

    best_city, best_city_score = None, 0
    for c in geo_index[best_province].keys():
        score = similarity(city, c)
        if score > best_city_score:
            best_city, best_city_score = c, score
    if best_city_score < 0.6:
        return None, {
            'fail_reason': f'城市匹配失败（{best_city_score:.2f}）| 输入: "{city}" vs 最佳命中: "{best_city}"'
        }

    best_district, best_district_score = None, 0
    for d in geo_index[best_province][best_city].keys():
        score = similarity(district, d)
        if score > best_district_score:
            best_district, best_district_score = d, score
    if best_district_score < 0.6:
        return None, {
            'fail_reason': f'区县匹配失败（{best_district_score:.2f}）| 输入: "{district}" vs 最佳命中: "{best_district}"'
        }

    avg_score = (best_province_score + best_city_score + best_district_score) / 3
    points = geo_index[best_province][best_city][best_district]
    return random.choice(points), {'score': avg_score}


def batch_update_mysql(conn, update_batch):
    if not update_batch:
        return
    meter_ids = [item['meter_id'] for item in update_batch]
    lats = [item['lat'] for item in update_batch]
    lons = [item['lon'] for item in update_batch]

    def build_case(field_name, ids, values):
        return "CASE {} \n{}\nEND".format(
            field_name,
            "\n".join([f"WHEN '{id_}' THEN {val}" for id_, val in zip(ids, values)])
        )

    sql = f"""
        UPDATE dev_meter_id
        SET
            latitude = {build_case('meter_id', meter_ids, lats)},
            longitude = {build_case('meter_id', meter_ids, lons)}
        WHERE meter_id IN ({", ".join(f"'{id_}'" for id_ in meter_ids)})
    """

    conn.execute(text(sql))


def main():
    logger.info("📥 加载地理地址数据...")
    geo_df = pd.read_csv("GeoAdministrativeUnits new.csv")
    geo_index = build_geo_index(geo_df)
    logger.info("🌐 地址索引构建完成")

    logger.info("📥 读取设备数据...")
    with engine.connect() as conn:
        devices = conn.execute(text("SELECT * FROM dev_meter_id where latitude is null or longitude is null")).mappings().all()
    logger.info(f"📦 总设备数: {len(devices)}")

    update_batch = []
    batch_size = 100
    stats = {
        'matched': 0,
        'match_fail': 0,
        'update_fail': 0,
        'total': len(devices),
        'start_time': time.time()
    }

    logger.info("🚀 开始处理和匹配地址...")
    with engine.begin() as conn:
        for idx, row in enumerate(tqdm(devices, desc="处理设备", unit="条"), 1):
            meter_id = row['meter_id']
            province = clean_string(row['province_name'])
            city = clean_string(row['city_name'])
            district = clean_string(row['region_name'])

            match_result, info = match_address(province, city, district, geo_index)
            if match_result:
                lat, lon = random_point_within_radius(match_result[0], match_result[1], radius_km=10)
                update_batch.append({'meter_id': meter_id, 'lat': lat, 'lon': lon})
                stats['matched'] += 1
                tqdm.write(f"✅ 匹配成功 | meter_id: {meter_id} | 匹配度: {info['score'] * 100:.1f}% | 坐标: ({lat}, {lon})")
            else:
                stats['match_fail'] += 1
                tqdm.write(f"❌ 匹配失败 | meter_id: {meter_id} | 原因: {info['fail_reason']}")

            if len(update_batch) >= batch_size or idx == len(devices):
                try:
                    batch_update_mysql(conn, update_batch)
                except Exception as e:
                    stats['update_fail'] += len(update_batch)
                    logger.error(f"❌ 批量更新失败: {str(e)}")
                update_batch = []

    duration = time.time() - stats['start_time']
    logger.info(f"\n✅ 全部完成，耗时: {duration:.2f} 秒")
    logger.info(f"📊 匹配成功: {stats['matched']}")
    logger.info(f"❌ 匹配失败: {stats['match_fail']}")
    logger.info(f"⚠️ 更新失败: {stats['update_fail']}")
    logger.info(f"⚡ 平均速度: {stats['total'] / max(duration, 0.001):.1f} 条/秒")


if __name__ == "__main__":
    main()
