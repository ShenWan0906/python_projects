import pandas as pd
import random
from sqlalchemy import text
from dbhelp import engine
from tqdm import tqdm

# 1️⃣ 读取 alabo_region 全部地区数据
sql_alabo_region = text("SELECT region_id, parent_id, name_en, level FROM alabo_region")
df_region = pd.read_sql(sql_alabo_region, con=engine)

print("alabo_region 总记录数: ", len(df_region))
print(df_region.head())  # 看前几条数据长啥样

# 2️⃣ 分别筛选出 省、市、区
# 如果 level 是数字类型，请使用数字比较 (1, 2, 3)，否则保留字符串 "1", "2", "3"
df_province = df_region[df_region['level'] == "1"]
df_city = df_region[df_region['level'] == "2"]
df_area = df_region[df_region['level'] == "3"]  # 修改为 df_area 而不是 df_region3

print("省份数: ", len(df_province))
print("城市数: ", len(df_city))
print("区县数: ", len(df_area))  # 之前使用了 df_region3，这里修正为 df_area

# 3️⃣ 构造完整的 省-市-区 组合
region_list = []

for _, prov in df_province.iterrows():
    prov_id = prov['region_id']  # 之前为 'id'，修改为 'region_id'
    prov_name = prov['name_en']

    cities = df_city[df_city['parent_id'] == prov_id]
    for _, city in cities.iterrows():
        city_id = city['region_id']  # 之前为 'id'，修改为 'region_id'
        city_name = city['name_en']

        areas = df_area[df_area['parent_id'] == city_id]
        for _, area in areas.iterrows():
            area_id = area['region_id']  # 之前为 'id'，修改为 'region_id'
            area_name = area['name_en']

            region_list.append({
                'province_id': prov_id,
                'city_id': city_id,
                'region_id': area_id,
                'address': f'{prov_name} / {city_name} / {area_name}'
            })

print(f'📌 可用地区组合数: {len(region_list)}')

# 4️⃣ 读取 dev_device_instance 需要赋值的数据
sql_dev_device_instance = text("SELECT id FROM dev_device_instance WHERE creator_name = 'system'")
df_devices = pd.read_sql(sql_dev_device_instance, con=engine)

print(f"📌 需要赋值的设备数: {len(df_devices)}")

# 5️⃣ 为每条设备数据分配随机地址
assigned_data = []

# tqdm进度条
for _, row in tqdm(df_devices.iterrows(), total=df_devices.shape[0], desc="Assigning regions"):
    random_region = random.choice(region_list)

    assigned_data.append({
        'id': row['id'],
        'province_id': random_region['province_id'],
        'city_id': random_region['city_id'],
        'region_id': random_region['region_id'],
        'address': random_region['address']
    })

df_assigned = pd.DataFrame(assigned_data)

# 6️⃣ 批量更新 dev_device_instance 表
# 方法：生成 SQL 批量执行

with engine.begin() as conn:
    for _, row in tqdm(df_assigned.iterrows(), total=df_assigned.shape[0], desc="Updating DB"):
        update_sql = text("""
            UPDATE dev_device_instance
            SET province_id = :province_id,
                city_id = :city_id,
                region_id = :region_id,
                address = :address
            WHERE id = :id
        """)
        conn.execute(update_sql, {
            'province_id': row['province_id'],
            'city_id': row['city_id'],
            'region_id': row['region_id'],
            'address': row['address'],
            'id': row['id']
        })

print("✅ 设备数据地区赋值完成")


