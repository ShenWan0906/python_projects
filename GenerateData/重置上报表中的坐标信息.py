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

# === 设置日志 ===
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# === 设置最大查询条数 ===
LIMIT_COUNT = 9000  # 可根据需要调整


def clean_string(s):
    if not isinstance(s, str):
        return ""
    return re.sub(r"[^\w\s]", "", s.strip().lower())


def similarity(a, b):
    return SequenceMatcher(None, a, b).ratio()


def random_point_within_radius(lat_center, lon_center, radius_km):
    radius_deg = radius_km / 111.0
    angle = random.uniform(0, 2 * math.pi)
    r = radius_deg * math.sqrt(random.uniform(0, 1))  # 均匀分布在圆内
    lat_offset = r * math.cos(angle)
    lon_offset = r * math.sin(angle) / math.cos(math.radians(lat_center))
    return lat_center + lat_offset, lon_center + lon_offset


def build_geo_index(geo_df):
    start_time = time.time()
    province_index = {}

    for _, row in geo_df.iterrows():
        province = clean_string(row['Region'])
        city = clean_string(row['City'])
        district = clean_string(row['district'])
        lat = row['latitude']
        lon = row['longitude']

        if province not in province_index:
            province_index[province] = {}
        if city not in province_index[province]:
            province_index[province][city] = {}
        if district not in province_index[province][city]:
            province_index[province][city][district] = []

        province_index[province][city][district].append((lat, lon))

    logger.info(f"🌳 地址索引构建完成 | 耗时: {time.time() - start_time:.2f}s")
    return province_index


def match_address(province, city, district, geo_index):
    best_province = None
    best_province_score = 0

    for prov in geo_index.keys():
        score = similarity(province, prov)
        if score > best_province_score:
            best_province = prov
            best_province_score = score

    if best_province_score < 0.5:
        return None, 0

    best_city = None
    best_city_score = 0
    for c in geo_index[best_province].keys():
        score = similarity(city, c)
        if score > best_city_score:
            best_city = c
            best_city_score = score

    if best_city_score < 0.5:
        return None, (best_province_score + best_city_score) / 2

    best_district = None
    best_district_score = 0
    for dist in geo_index[best_province][best_city].keys():
        score = similarity(district, dist)
        if score > best_district_score:
            best_district = dist
            best_district_score = score

    if best_district_score < 0.5:
        return None, (best_province_score + best_city_score + best_district_score) / 3

    points = geo_index[best_province][best_city][best_district]
    return random.choice(points), (best_province_score + best_city_score + best_district_score) / 3


def batch_update_mysql(conn, update_batch):
    if not update_batch:
        return

    device_ids = [item['device_id'] for item in update_batch]
    meter_ids = [item['meter_id'] for item in update_batch]
    lats = [item['lat'] for item in update_batch]
    lons = [item['lon'] for item in update_batch]

    def build_case(field_name, ids, values):
        return "CASE {} \n{}\nEND".format(
            field_name,
            "\n".join([f"WHEN '{id_}' THEN {val}" for id_, val in zip(ids, values)])
        )

    sql1 = f"""
        UPDATE device_latest_report_message
        SET
            latitude = {build_case('device_id', device_ids, lats)},
            longitude = {build_case('device_id', device_ids, lons)}
        WHERE device_id IN ({", ".join(f"'{id_}'" for id_ in device_ids)})
    """

    sql2 = f"""
        UPDATE dev_device_instance
        SET
            install_latitude = {build_case('id', device_ids, lats)},
            install_longitude = {build_case('id', device_ids, lons)}
        WHERE id IN ({", ".join(f"'{id_}'" for id_ in device_ids)})
    """

    sql3 = f"""
        UPDATE dev_meter_id
        SET
            latitude = {build_case('meter_id', meter_ids, lats)},
            longitude = {build_case('meter_id', meter_ids, lons)}
        WHERE meter_id IN ({", ".join(f"'{id_}'" for id_ in meter_ids)})
    """

    conn.execute(text(sql1))
    conn.execute(text(sql2))
    conn.execute(text(sql3))


def main():
    logger.info("⏳ 开始读取地理地址数据...")
    geo_df = pd.read_csv('GeoAdministrativeUnits new.csv')
    geo_index = build_geo_index(geo_df)

    logger.info("⏳ 查询数据库设备数据...")
    with engine.connect() as conn:
        device_query = text("""
            SELECT d.id as device_id, d.second_id, 
                   m.province_name, m.city_name, m.region_name
            FROM dev_device_instance d
            LEFT JOIN dev_meter_id m ON d.second_id = m.meter_id
            WHERE d.second_id IS NOT NULL
              AND m.province_name IS NOT NULL
              AND m.city_name IS NOT NULL
              AND (d.install_longitude IS NULL OR d.install_latitude IS NULL)
            LIMIT :limit
        """)
        # ✅ 返回 dict-like 行对象
        devices = conn.execute(device_query, {'limit': LIMIT_COUNT}).mappings().all()

    logger.info(f"🔍 共查询到 {len(devices)} 条有效设备（未设置安装坐标）")

    stats = {
        'total': len(devices),
        'matched': 0,
        'update_fail': 0,
        'match_fail': 0,
        'start_time': time.time()
    }

    update_batch = []
    batch_size = 100

    logger.info("🚀 开始处理设备数据...")
    with engine.begin() as conn:
        for idx, row in enumerate(tqdm(devices, desc="处理设备", unit="条"), 1):
            device_id = row['device_id']
            meter_id = row['second_id']
            province = clean_string(row['province_name'])
            city = clean_string(row['city_name'])
            district = clean_string(row['region_name'])

            result, score = match_address(province, city, district, geo_index)

            if result:
                lat, lon = random_point_within_radius(result[0], result[1], radius_km=10)
                update_batch.append({
                    'device_id': device_id,
                    'meter_id': meter_id,
                    'lat': lat,
                    'lon': lon
                })
                stats['matched'] += 1
            else:
                stats['match_fail'] += 1
                tqdm.write(f"❌ 匹配失败 (最高相似度: {score:.2f}) | 设备ID: {device_id}")

            if len(update_batch) >= batch_size or idx == len(devices):
                try:
                    batch_update_mysql(conn, update_batch)
                except Exception as e:
                    stats['update_fail'] += len(update_batch)
                    logger.error(f"❌ 批量更新失败 | 错误: {str(e)}")
                update_batch = []

    duration = time.time() - stats['start_time']
    logger.info(f"\n✅ 处理完成! 总耗时: {duration:.2f}秒")
    logger.info(f"📊 处理统计结果:")
    logger.info(f"  设备总数     : {stats['total']}")
    logger.info(f"  匹配成功更新 : {stats['matched']} ({(stats['matched'] / stats['total']) * 100:.1f}%)")
    logger.info(f"  匹配失败     : {stats['match_fail']} ({(stats['match_fail'] / stats['total']) * 100:.1f}%)")
    logger.info(f"  更新失败     : {stats['update_fail']} ({(stats['update_fail'] / stats['total']) * 100:.1f}%)")
    logger.info(f"  平均处理速度 : {stats['total'] / max(duration, 0.001):.1f} 条/秒")


if __name__ == "__main__":
    main()
