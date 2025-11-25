import logging
import math
import random
import time
import pandas as pd
from sqlalchemy import text
from dbhelp import get_engine, DB_CONFIG

# === 日志配置 ===
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# === 工具函数 ===
def random_point(lat_center, lon_center, radius_km=1000):
    """
    生成方圆 radius_km 公里内随机坐标（WGS84，经纬度）
    """
    radius_deg = radius_km / 111.0  # 近似换算
    angle = random.uniform(0, 2 * math.pi)
    r = radius_deg * math.sqrt(random.uniform(0, 1))
    lat = lat_center + r * math.cos(angle)
    lon = lon_center + r * math.sin(angle) / math.cos(math.radians(lat_center))
    return round(lat, 6), round(lon, 6)


def convert_numpy_types(obj):
    """
    将 numpy 数据类型转换为 Python 原生类型
    """
    if hasattr(obj, 'item'):
        return obj.item()
    elif isinstance(obj, dict):
        return {k: convert_numpy_types(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_numpy_types(item) for item in obj]
    else:
        return obj


def build_region_hierarchy(df_region):
    """
    构建区域层级关系（保持原始列，包括 latitude/longitude 如果存在）
    返回 dict，包含 DataFrame 对象和映射
    """
    level_counts = df_region['level'].value_counts()
    logger.info(f"区域层级分布: {level_counts.to_dict()}")

    hierarchy = {}

    for prov_level in [1, '1', 'province']:
        if prov_level in df_region['level'].values:
            provinces = df_region[df_region['level'] == prov_level]
            hierarchy['provinces'] = provinces
            logger.info(f"找到省份层级: {prov_level}, 数量: {len(provinces)}")
            break

    for city_level in [2, '2', 'city']:
        if city_level in df_region['level'].values:
            cities = df_region[df_region['level'] == city_level]
            hierarchy['cities'] = cities
            city_to_province = cities.set_index('region_id')['parent_id'].to_dict()
            hierarchy['city_to_province'] = city_to_province
            logger.info(f"找到城市层级: {city_level}, 数量: {len(cities)}")
            break

    for area_level in [3, '3', 'area', 'district']:
        if area_level in df_region['level'].values:
            areas = df_region[df_region['level'] == area_level]
            hierarchy['areas'] = areas
            area_to_city = areas.set_index('region_id')['parent_id'].to_dict()
            hierarchy['area_to_city'] = area_to_city
            logger.info(f"找到区域层级: {area_level}, 数量: {len(areas)}")
            break

    if not hierarchy:
        top_levels = level_counts.head(3).index.tolist()
        if len(top_levels) >= 1:
            hierarchy['provinces'] = df_region[df_region['level'] == top_levels[0]]
        if len(top_levels) >= 2:
            hierarchy['cities'] = df_region[df_region['level'] == top_levels[1]]
        if len(top_levels) >= 3:
            hierarchy['areas'] = df_region[df_region['level'] == top_levels[2]]

    return hierarchy


def get_random_location(hierarchy):
    """
    从区域层级中随机选择一个位置并尽可能返回该区域的经纬度中心。
    返回: province_id, city_id, region_id, address_str, lat_center, lon_center
    如果某一级没有经纬度，则回退到上一级；若全部缺失，返回 (None, None)
    """
    provinces = hierarchy.get('provinces')
    cities = hierarchy.get('cities')
    areas = hierarchy.get('areas')

    if provinces is None or provinces.empty:
        return None, None, None, "Unknown Location", None, None

    prov = provinces.sample(1).iloc[0]

    # 城市选择
    if cities is not None and not cities.empty:
        province_cities = cities[cities['parent_id'] == prov['region_id']]
        if not province_cities.empty:
            city = province_cities.sample(1).iloc[0]
        else:
            city = cities.sample(1).iloc[0]
    else:
        city = {
            'region_id': int(prov['region_id']) * 100,
            'name_en': f"City of {prov.get('name_en', prov.get('name', ''))}",
            'parent_id': int(prov['region_id'])
        }

    # 区域选择
    if areas is not None and not areas.empty:
        # 注意：city 可能是 Series 或 dict
        city_id_val = city['region_id'] if isinstance(city, dict) else city['region_id']
        city_areas = areas[areas['parent_id'] == city_id_val]
        if not city_areas.empty:
            area = city_areas.sample(1).iloc[0]
        else:
            area = areas.sample(1).iloc[0]
    else:
        area = {
            'region_id': int(city['region_id']) * 100,
            'name_en': f"Area of {city.get('name_en', city.get('name', ''))}",
            'parent_id': int(city['region_id'])
        }

    # 组合地址
    address_parts = []
    for node in (prov, city, area):
        if isinstance(node, dict):
            name = node.get('name_en') or node.get('name')
        else:
            name = node.get('name_en') if 'name_en' in node else node.get('name') if 'name' in node else None
        if name:
            address_parts.append(str(name))

    address_str = " ".join(address_parts) if address_parts else "Unknown Location"

    # 优先从 area -> city -> province 获取坐标
    def extract_latlon(node):
        if node is None:
            return None, None
        if isinstance(node, dict):
            lat = node.get('latitude') or node.get('lat')
            lon = node.get('longitude') or node.get('lon')
        else:
            lat = node.get('latitude') if 'latitude' in node else (node.get('lat') if 'lat' in node else None)
            lon = node.get('longitude') if 'longitude' in node else (node.get('lon') if 'lon' in node else None)
        # 处理 pandas 的 NaN
        if pd.isna(lat) or pd.isna(lon):
            return None, None
        try:
            return float(lat), float(lon)
        except Exception:
            return None, None

    lat_center, lon_center = extract_latlon(area)
    if lat_center is None:
        lat_center, lon_center = extract_latlon(city)
    if lat_center is None:
        lat_center, lon_center = extract_latlon(prov)

    # 如果依然没有坐标，返回 None
    if lat_center is None or lon_center is None:
        return int(prov['region_id']), int(city['region_id']), int(area['region_id']), address_str, None, None

    return int(prov['region_id']), int(city['region_id']), int(area['region_id']), address_str, lat_center, lon_center


# === 主流程 ===
def main(batch_size=100000):
    logger.info("🔄 连接数据库...")
    engine = get_engine(DB_CONFIG)
    logger.info("✅ 数据库连接成功")

    with engine.connect() as conn:
        # 1️⃣ 检查设备表结构
        logger.info("🔄 检查 dev_device_instance 表结构...")
        device_columns = conn.execute(text("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'dev_device_instance'
            ORDER BY ordinal_position
        """)).fetchall()

        device_columns = [col[0] for col in device_columns]
        logger.info(f"设备表字段数量: {len(device_columns)}")

        lat_fields = [col for col in device_columns if 'lat' in col.lower()]
        lon_fields = [col for col in device_columns if 'lon' in col.lower()]

        logger.info(f"纬度字段: {lat_fields}")
        logger.info(f"经度字段: {lon_fields}")

        lat_field = lat_fields[0] if lat_fields else None
        lon_field = lon_fields[0] if lon_fields else None

        if lat_field and lon_field:
            coord_fields = f"{lat_field}, {lon_field}"
            logger.info(f"使用坐标字段: {lat_field}, {lon_field}")
        else:
            coord_fields = "id"
            logger.warning("⚠️ 未找到坐标字段，将使用默认坐标")

        # 2️⃣ 读取区域数据（包含 latitude/longitude）
        logger.info("🔄 读取 alabo_region 区域表...")
        df_region = pd.DataFrame(conn.execute(
            text("SELECT region_id, parent_id, name_en, level, latitude, longitude FROM alabo_region")
        ).mappings().all())

        if df_region.empty:
            logger.error("❌ alabo_region 表为空，无法继续执行")
            return

        logger.info(f"读取到区域数据: {len(df_region)} 条")

        hierarchy = build_region_hierarchy(df_region)

        if 'provinces' not in hierarchy or hierarchy['provinces'].empty:
            logger.error("❌ 没有找到省份数据，无法继续")
            return

        # 3️⃣ 清空关键字段
        logger.info("🔄 清空 dev_device_instance 的关键字段...")
        conn.execute(text("""
            UPDATE dev_device_instance
            SET province_id=NULL, city_id=NULL, region_id=NULL,
                install_latitude=NULL, install_longitude=NULL,
                install_address=NULL, address=NULL
        """))
        conn.commit()
        logger.info("✅ 清空完成")

        # 4️⃣ 处理设备数据分批
        total_devices = conn.execute(text("SELECT COUNT(*) FROM dev_device_instance")).scalar()
        logger.info(f"📦 dev_device_instance 总记录数: {total_devices}")

        estimated_batches = (total_devices + batch_size - 1) // batch_size
        logger.info(f"预计处理批次: {estimated_batches}")

        offset = 0
        start_time = time.time()
        batch_index = 1
        processed_count = 0

        while offset < total_devices:
            batch_start_time = time.time()
            logger.info(
                f"🔄 开始处理第 {batch_index} 批数据 (offset={offset}, 进度: {processed_count}/{total_devices})...")

            devices = pd.DataFrame(conn.execute(
                text(
                    f"SELECT id, {coord_fields} FROM dev_device_instance ORDER BY id LIMIT {batch_size} OFFSET {offset}")
            ).mappings().all())

            if devices.empty:
                break

            update_records = []
            for _, row in devices.iterrows():
                try:
                    province_id, city_id, region_id, address, lat_center, lon_center = get_random_location(hierarchy)
                    if province_id is None:
                        continue

                    # 优先使用区域中心坐标（area -> city -> province），若不存在则使用设备原始坐标，最后退回到北京
                    if lat_center is None or lon_center is None:
                        # 如果设备表存在坐标字段，尝试使用设备已有坐标
                        if lat_field and lon_field and lat_field in row and lon_field in row and pd.notna(row[lat_field]) and pd.notna(row[lon_field]):
                            try:
                                lat_center = float(row[lat_field])
                                lon_center = float(row[lon_field])
                            except Exception:
                                lat_center, lon_center = 39.9042, 116.4074
                        else:
                            # 退回到北京市中心（仅作为最后的兜底）
                            lat_center, lon_center = 39.9042, 116.4074

                    lat, lon = random_point(lat_center, lon_center, radius_km=100)

                    record = {
                        'id': str(row['id']),
                        'province_id': province_id,
                        'city_id': city_id,
                        'region_id': region_id,
                        'install_latitude': float(lat),
                        'install_longitude': float(lon),
                        'address': str(address),
                        'install_address': str(address)
                    }

                    update_records.append(record)
                except Exception as e:
                    logger.error(f"处理设备 {row['id']} 时出错: {e}")
                    continue

            if not update_records:
                logger.warning(f"第 {batch_index} 批没有生成更新记录")
                offset += batch_size
                batch_index += 1
                continue

            try:
                update_sql = """
                    UPDATE dev_device_instance
                    SET province_id = :province_id,
                        city_id = :city_id,
                        region_id = :region_id,
                        install_latitude = :install_latitude,
                        install_longitude = :install_longitude,
                        address = :address,
                        install_address = :install_address
                    WHERE id = :id
                """

                converted_records = [convert_numpy_types(record) for record in update_records]

                chunk_size = 10000
                for i in range(0, len(converted_records), chunk_size):
                    chunk = converted_records[i:i + chunk_size]
                    conn.execute(text(update_sql), chunk)
                    conn.commit()

                batch_duration = time.time() - batch_start_time
                processed_count += len(update_records)
                logger.info(f"✅ 第 {batch_index} 批完成，更新 {len(update_records)} 条记录，耗时 {batch_duration:.2f} 秒")

            except Exception as e:
                logger.error(f"批量更新第 {batch_index} 批时出错: {e}")
                conn.rollback()

            offset += batch_size
            batch_index += 1

        total_duration = time.time() - start_time
        logger.info(f"\n🎯 全部完成，耗时 {total_duration:.2f} 秒")
        logger.info(f"⚡ 平均速度: {processed_count / max(total_duration, 0.001):.1f} 条/秒")
        logger.info(f"📊 总处理记录: {processed_count}/{total_devices}")


if __name__ == "__main__":
    main()
