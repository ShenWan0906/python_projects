import pandas as pd
import random
import re
from sqlalchemy import text
from dbhelp import engine
from difflib import SequenceMatcher

# === 设置最大查询条数 ===
LIMIT_COUNT = 9000  # 可根据需要调整

# === 读取地理地址参考 CSV 文件 ===
geo_df = pd.read_csv('GeoAdministrativeUnits new.csv')

# === 字符清洗函数 ===
def clean_string(s):
    if not isinstance(s, str):
        return ""
    return re.sub(r"[^\w\s]", "", s.strip().lower())

# === 计算相似度函数 ===
def similarity(a, b):
    return SequenceMatcher(None, a, b).ratio()

# === 查询设备和 meter 数据（未设置安装坐标、且地址信息完整） ===
with engine.connect() as conn:
    device_query = text("""
        SELECT d.id as device_id, d.second_id, 
               m.province_name, m.city_name, m.region_name
        FROM dev_device_instance d
        LEFT JOIN dev_meter_id m ON d.second_id = m.meter_id
        WHERE d.second_id IS NOT NULL
          AND m.meter_id IS NOT NULL
          AND m.province_name IS NOT NULL
          AND m.city_name IS NOT NULL
          AND m.region_name IS NOT NULL
          AND (d.install_longitude IS NULL OR d.install_latitude IS NULL)
        LIMIT :limit
    """)
    devices = conn.execute(device_query, {'limit': LIMIT_COUNT}).fetchall()

print(f"🔍 共查询到 {len(devices)} 条有效设备（未设置安装坐标）")

# === 初始化统计量 ===
matched_count = 0
update_fail_count = 0
match_fail_count = 0

# === 遍历设备并进行匹配与更新 ===
with engine.begin() as conn:  # 自动事务控制
    for idx, row in enumerate(devices, 1):
        device_id = row.device_id
        meter_id = row.second_id
        province = clean_string(row.province_name)
        city = clean_string(row.city_name)
        district = clean_string(row.region_name)

        best_match = None
        best_score = 0

        for _, geo_row in geo_df.iterrows():
            region_score = similarity(province, clean_string(geo_row['Region']))
            city_score = similarity(city, clean_string(geo_row['City']))
            district_score = similarity(district, clean_string(geo_row['district']))
            avg_score = (region_score + city_score + district_score) / 3

            if avg_score > best_score:
                best_score = avg_score
                best_match = geo_row

        if best_score >= 0.6:
            lat = best_match['latitude'] + random.uniform(-0.001, 0.001)
            lon = best_match['longitude'] + random.uniform(-0.001, 0.001)

            print(f"✅ 设备ID {device_id} 匹配成功 (相似度: {best_score:.2f}) → lat: {lat:.6f}, lon: {lon:.6f}")

            try:
                # 同时更新两个表
                update_report = text("""
                    UPDATE device_latest_report_message
                    SET latitude = :lat, longitude = :lon
                    WHERE device_id = :device_id
                """)
                update_device = text("""
                    UPDATE dev_device_instance
                    SET install_latitude = :lat, install_longitude = :lon
                    WHERE id = :device_id
                """)
                update_meterId = text("""
                     UPDATE dev_meter_id
                     SET latitude = :lat, longitude = :lon
                     WHERE meter_id = :meter_id
                 """)
                conn.execute(update_report, {'lat': lat, 'lon': lon, 'device_id': device_id})
                conn.execute(update_device, {'lat': lat, 'lon': lon, 'device_id': device_id})
                conn.execute(update_meterId, {'lat': lat, 'lon': lon, 'meter_id': meter_id})
                matched_count += 1
            except Exception as e:
                print(f"❌ 更新失败 设备ID {device_id} | 错误: {str(e)}")
                update_fail_count += 1
        else:
            print(f"❌ 匹配失败 (最高相似度: {best_score:.2f}) | 设备ID: {device_id}")
            match_fail_count += 1

# === 最终统计结果 ===
print("\n📊 处理统计结果")
print(f"📦 设备总数: {len(devices)}")
print(f"✅ 匹配成功并更新坐标: {matched_count}")
print(f"❌ 匹配失败: {match_fail_count}")
print(f"❌ 数据库更新失败: {update_fail_count}")
