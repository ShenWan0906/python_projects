import pandas as pd
import random
import math
from sqlalchemy import text
from dbhelp import engine

# === 1. 读取 CSV 文件 ===
geo_df = pd.read_csv('GeoAdministrativeUnitsnew.csv')
print(f"📍 读取到 {len(geo_df)} 个行政区域中心点")

# === 2. 定义：在某个中心点 100km 半径范围内生成随机经纬度 ===
def random_point_nearby(lat, lon, radius_km=100):
    # 地球半径（km）
    earth_radius = 6371.0
    # 随机距离（0 ~ radius_km）
    r = radius_km * math.sqrt(random.random())
    # 随机角度（弧度）
    theta = random.random() * 2 * math.pi
    # 经纬度偏移（角度制）
    dlat = (r / earth_radius) * (180 / math.pi)
    dlon = (r / (earth_radius * math.cos(math.pi * lat / 180))) * (180 / math.pi)
    return lat + dlat * math.sin(theta), lon + dlon * math.cos(theta)

# === 3. 查询数据库记录总数 ===
with engine.connect() as conn:
    total_count = conn.execute(text("SELECT COUNT(*) FROM device_latest_report_message")).scalar()
print(f"📦 表中共有 {total_count:,} 条记录")

# === 4. 按区域分配数据（每个区域平均分配）===
batch_size = 50000  # 每批次更新 5 万行，避免内存爆炸
region_count = len(geo_df)
per_region = total_count // region_count

print(f"📊 每个区域分配大约 {per_region} 条设备坐标")

# === 5. 主循环 ===
offset = 0
with engine.connect() as conn:
    for idx, geo_row in geo_df.iterrows():
        region = geo_row['Region']
        lat_center = geo_row['latitude']
        lon_center = geo_row['longitude']
        print(f"\n🌍 正在更新区域: {region} ({lat_center}, {lon_center})")

        # 从数据库中查询要更新的设备ID
        query_devices = text(f"""
            SELECT id FROM device_latest_report_message
            ORDER BY id
            LIMIT {per_region} OFFSET {offset}
        """)
        device_ids = [row.id for row in conn.execute(query_devices)]
        if not device_ids:
            break

        # 为每个设备生成随机坐标
        updated_rows = []
        for dev_id in device_ids:
            lat, lon = random_point_nearby(lat_center, lon_center, 100)
            updated_rows.append({'id': dev_id, 'lat': lat, 'lon': lon})

        # 批量更新（推荐使用 PostgreSQL 的批量 UPDATE）
        update_sql = """
            UPDATE device_latest_report_message
            SET latitude = data.lat, longitude = data.lon
            FROM (VALUES {}) AS data(id, lat, lon)
            WHERE device_latest_report_message.id = data.id
        """

        # 构建 VALUES 子句
        values_clause = ",".join(
            f"('{r['id']}', {r['lat']}, {r['lon']})" for r in updated_rows
        )
        conn.execute(text(update_sql.format(values_clause)))
        conn.commit()

        offset += per_region
        print(f"✅ 区域 {region} 更新完成，共 {len(updated_rows)} 条")

print("\n🎉 所有区域坐标更新完成！")
