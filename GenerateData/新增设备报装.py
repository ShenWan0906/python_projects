import logging
import math
import random
import time
import pandas as pd
from sqlalchemy import text
from dbhelp import get_engine, DB_CONFIG4

# === 日志 ===
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# === 工具函数 ===
def random_point(lat_center, lon_center, radius_km=100):
    radius_deg = radius_km / 111.0
    angle = random.uniform(0, 2 * math.pi)
    r = radius_deg * math.sqrt(random.uniform(0, 1))
    lat = lat_center + r * math.cos(angle)
    lon = lon_center + r * math.sin(angle) / math.cos(math.radians(lat_center))
    return round(lat, 6), round(lon, 6)


def convert_numpy_types(obj):
    if hasattr(obj, 'item'):
        return obj.item()
    elif isinstance(obj, dict):
        return {k: convert_numpy_types(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_numpy_types(item) for item in obj]
    else:
        return obj


# [优化点1]：预处理地理数据为字典结构，极大提升查找速度
def build_fast_hierarchy(df_region):
    logger.info("正在构建高速地理查找表...")

    # 转为字典，方便通过 ID 快速获取详情
    region_dict = df_region.set_index('region_id').to_dict('index')

    # 构建层级关系 map: {parent_id: [child_id1, child_id2, ...]}
    provinces = df_region[df_region['level'].astype(str) == '1']['region_id'].tolist()

    # 预先分组，避免在循环中反复筛选 DataFrame
    # group_dict: {parent_id: [child_id_list]}
    children_map = df_region.groupby('parent_id')['region_id'].apply(list).to_dict()

    return {
        'provinces': provinces,  # 省份 ID 列表
        'children_map': children_map,  # 父子关系映射
        'info': region_dict  # 详情查找表
    }


# [优化点2]：使用字典查找代替 Pandas 筛选
def get_random_location_fast(hierarchy_data):
    provinces = hierarchy_data['provinces']
    children_map = hierarchy_data['children_map']
    info = hierarchy_data['info']

    if not provinces: return None

    # 1. 随机选省
    prov_id = random.choice(provinces)
    prov_info = info.get(prov_id)

    # 2. 随机选市
    city_ids = children_map.get(prov_id, [])
    if not city_ids: return None
    city_id = random.choice(city_ids)
    city_info = info.get(city_id)

    # 3. 随机选区
    area_ids = children_map.get(city_id, [])
    if not area_ids: return None
    area_id = random.choice(area_ids)
    area_info = info.get(area_id)

    # 4. 组合地址
    address = f"{prov_info['name_en']} {city_info['name_en']} {area_info['name_en']}"

    # 5. 找坐标 (优先用区，没有用市，再没有用省)
    target_node = area_info if pd.notna(area_info.get('latitude')) else (
        city_info if pd.notna(city_info.get('latitude')) else prov_info)

    if pd.isna(target_node.get('latitude')):
        return None

    return (
        int(prov_id),
        int(city_id),
        int(area_id),
        address,
        float(target_node['latitude']),
        float(target_node['longitude'])
    )


# === 主流程 ===
def main(add_count=1000000, batch_size=5000):
    logger.info("连接数据库...")
    engine = get_engine(DB_CONFIG4)

    with engine.connect() as conn:
        # 1. 获取基础数据
        max_id = conn.execute(text("SELECT COALESCE(MAX(CAST(id AS BIGINT)), 0) FROM dev_device_instance")).scalar()
        logger.info(f"当前最大 ID: {max_id}")

        template_row = conn.execute(
            text("SELECT * FROM dev_device_instance ORDER BY RANDOM() LIMIT 1")).mappings().first()
        if not template_row:
            return
        template = dict(template_row)
        template.pop("id", None)

        # 2. 加载区域数据并建立索引
        logger.info("加载区域数据...")
        df_region = pd.DataFrame(conn.execute(text(
            "SELECT region_id, parent_id, name_en, level, latitude, longitude FROM alabo_region")).mappings().all())

        # 使用优化后的构建函数
        hierarchy_data = build_fast_hierarchy(df_region)

        start_time = time.time()

        logger.info(f"开始生成 {add_count} 条数据，分批执行，每批 {batch_size} 条...")

        # [优化点3]：外层循环分批，避免内存溢出
        total_inserted = 0
        id_counter = max_id

        # 计算需要多少个批次
        total_batches = math.ceil(add_count / batch_size)

        for batch_idx in range(total_batches):
            batch_start_time = time.time()
            current_batch_records = []

            # 计算当前批次的大小（最后一批可能不足 batch_size）
            current_size = min(batch_size, add_count - total_inserted)

            for _ in range(current_size):
                id_counter += 1
                id_length = len(template_row["id"])
                new_id_str = str(id_counter).zfill(id_length)

                loc_data = get_random_location_fast(hierarchy_data)

                # 如果运气不好没随机到有坐标的区域，就重试一次或跳过（这里简单处理为循环直到获取到）
                while loc_data is None:
                    loc_data = get_random_location_fast(hierarchy_data)

                prov_id, city_id, region_id, addr, lat_c, lon_c = loc_data
                lat, lon = random_point(lat_c, lon_c, radius_km=100)

                new_row = template.copy()
                new_row.update({
                    "id": new_id_str,
                    "province_id": prov_id,
                    "city_id": city_id,
                    "region_id": region_id,
                    "install_latitude": lat,
                    "install_longitude": lon,
                    "address": addr,
                    "install_address": addr,
                    "creator_name": "系统生成"
                })
                current_batch_records.append(convert_numpy_types(new_row))

            # [优化点4]：生成一批，插入一批，然后释放内存
            if current_batch_records:
                cols = ", ".join(current_batch_records[0].keys())
                vals = ", ".join([f":{k}" for k in current_batch_records[0].keys()])
                insert_sql = f"INSERT INTO dev_device_instance ({cols}) VALUES ({vals})"

                conn.execute(text(insert_sql), current_batch_records)
                conn.commit()

                total_inserted += len(current_batch_records)

                elapsed = time.time() - batch_start_time
                logger.info(
                    f"批次 {batch_idx + 1}/{total_batches} 完成: 插入 {len(current_batch_records)} 条, 耗时 {elapsed:.2f}s")

        logger.info(f"全部完成！共插入 {total_inserted} 条，总耗时 {time.time() - start_time:.2f} 秒")


if __name__ == "__main__":
    main(add_count=1000000, batch_size=5000)