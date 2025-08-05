import pandas as pd
import random
import re
import time
import uuid
from sqlalchemy import text
from dbhelp import engine
from difflib import SequenceMatcher

# === 1. 读取CSV文件 ===
geo_df = pd.read_csv('GeoAdministrativeUnits new.csv')

# === 2. 查询设备表 ===
with engine.connect() as conn:
    device_query = text("""
        SELECT id, name, address
        FROM dev_device_instance
        WHERE address is not null  
    """)
    result = conn.execute(device_query)
    devices = result.fetchall()

print(f"🔍 查询到 {len(devices)} 条设备数据")

# === 3. 清理字符串函数 ===
def clean_string(s):
    if not isinstance(s, str):
        return ""
    s = s.lower()
    s = re.sub(r"[^\w\s]", "", s)
    s = s.strip()
    return s

# === 4. 相似度函数 ===
def similarity(a, b):
    return SequenceMatcher(None, a, b).ratio()

# === 5. 统计量初始化 ===
matched_count = 0
address_null_count = 0
address_format_error_count = 0
match_fail_count = 0
insert_total_count = 0

# === 6. Insert SQL ===
insert_query = text("""
    INSERT INTO device_latest_report_message (
        id, device_id, device_name, device_type, product_id, product_name,
        recv_time, frozen_time, total_accumulate_flow, forward_total_flow, reverse_total_flow,
        min_flow, max_flow, average_flow, instantaneous_flow,
        min_water_temperature, max_water_temperature, average_water_temperature, instantaneous_water_temperature,
        min_pressure, max_pressure, average_pressure, instantaneous_pressure,
        valve_status, valve_open, day_quota, month_quota, day_remain_quota, month_remain_quota,
        measure_battery_remain_days, com_battery_remain_days, valve_battery_remain_days, exist_flow_minutes,
        status_byte_str, alarm_info, error_info,
        signal_strength, signal_strength_guide, signal_noise_ratio,
        ecl0_time, ecl1_time, ecl2_time, send_pag_nums, receive_pag_nums,
        community_id, community_ident,
        psm_timer, edrx_timer,
        longitude, latitude,
        protocol_code, protocol_code_name, message_type, up_type, data_type, data_up_type,
        original_msg, config_info, creator_id, create_time
    )
    VALUES (
        :id, :device_id, :device_name, :device_type, :product_id, :product_name,
        :recv_time, :frozen_time, :total_accumulate_flow, :forward_total_flow, :reverse_total_flow,
        :min_flow, :max_flow, :average_flow, :instantaneous_flow,
        :min_water_temperature, :max_water_temperature, :average_water_temperature, :instantaneous_water_temperature,
        :min_pressure, :max_pressure, :average_pressure, :instantaneous_pressure,
        :valve_status, :valve_open, :day_quota, :month_quota, :day_remain_quota, :month_remain_quota,
        :measure_battery_remain_days, :com_battery_remain_days, :valve_battery_remain_days, :exist_flow_minutes,
        :status_byte_str, :alarm_info, :error_info,
        :signal_strength, :signal_strength_guide, :signal_noise_ratio,
        :ecl0_time, :ecl1_time, :ecl2_time, :send_pag_nums, :receive_pag_nums,
        :community_id, :community_ident,
        :psm_timer, :edrx_timer,
        :longitude, :latitude,
        :protocol_code, :protocol_code_name, :message_type, :up_type, :data_type, :data_up_type,
        :original_msg, :config_info, :creator_id, :create_time
    )
""")

# === 7. 主处理逻辑 ===
# 在外部创建数据库连接
with engine.connect() as conn:
    for idx, row in enumerate(devices, start=1):
        device_id = row.id
        device_name = row.name
        address = row.address

        if not address or not isinstance(address, str):
            print(f"⚠️ 设备ID {device_id} address 为空，跳过")
            address_null_count += 1
            continue

        address_clean = address.replace("'", "").strip()
        parts = [part.strip() for part in address_clean.split('/')]

        if len(parts) != 3:
            print(f"⚠️ 设备ID {device_id} 地址格式不正确: {address}")
            address_format_error_count += 1
            continue

        region, city, district = parts
        print(f"\n📍 设备ID {device_id} | 解析地址: {region} / {city} / {district}")

        region_clean = clean_string(region)
        city_clean = clean_string(city)
        district_clean = clean_string(district)

        best_match = None
        best_score = 0

        for _, geo_row in geo_df.iterrows():
            region_score = similarity(region_clean, clean_string(geo_row['Region']))
            city_score = similarity(city_clean, clean_string(geo_row['City']))
            district_score = similarity(district_clean, clean_string(geo_row['district']))
            avg_score = (region_score + city_score + district_score) / 3

            if avg_score > best_score:
                best_score = avg_score
                best_match = geo_row

        if best_match is not None and best_score >= 0.6:
            lat = best_match['latitude']
            lon = best_match['longitude']

            # 添加小范围的随机偏移量，避免重叠
            lat_random = lat + random.uniform(-0.001, 0.001)
            lon_random = lon + random.uniform(-0.001, 0.001)

            print(f"✅ 匹配成功 (相似度: {best_score:.2f}) | 匹配: {best_match['Region']} / {best_match['City']} / {best_match['district']}")

            row_dict = {
                'id': str(uuid.uuid4()),
                'device_id': device_id,
                'device_name': device_name,
                'device_type': 'watermeter',
                'product_id': '1001',
                'product_name': 'U-WR2-25',
                'recv_time': None,
                'frozen_time': None,
                'total_accumulate_flow': 0.000,
                'forward_total_flow': 0.000,
                'reverse_total_flow': 0.000,
                'min_flow': 0.000,
                'max_flow': 0.000,
                'average_flow': 0.000,
                'instantaneous_flow': 0.000,
                'min_water_temperature': 25.47,
                'max_water_temperature': 25.87,
                'average_water_temperature': 25.71,
                'instantaneous_water_temperature': 25.72,
                'min_pressure': 0.00,
                'max_pressure': 0.00,
                'average_pressure': 0.00,
                'instantaneous_pressure': 0.00,
                'valve_status': 'B2',
                'valve_open': 0,
                'day_quota': 0.00,
                'month_quota': 100,
                'day_remain_quota': 80.00,
                'month_remain_quota': 100,
                'measure_battery_remain_days': 3846,
                'com_battery_remain_days': 710,
                'valve_battery_remain_days': 5992,
                'exist_flow_minutes': 0,
                'status_byte_str': '00000000',
                'alarm_info': '00100000000000100001000000000100',
                'error_info': '00000000',
                'signal_strength': -88.8,
                'signal_strength_guide': -77.8,
                'signal_noise_ratio': 8.7,
                'ecl0_time': 255,
                'ecl1_time': 1101,
                'ecl2_time': 2,
                'send_pag_nums': 5383,
                'receive_pag_nums': 5383,
                'community_id': "",
                'community_ident': '493',
                'psm_timer': 0,
                'edrx_timer': 0,
                'longitude': lon_random,
                'latitude': lat_random,
                'protocol_code': 0,
                'protocol_code_name': 0,
                'message_type': 0,
                'up_type': 0,
                'data_type': 0,
                'data_up_type': 0,
                'original_msg': 0,
                'config_info': 0,
                'creator_id': 'Automatically generate',
                'create_time': int(time.time() * 1000),
            }

            try:
                # 执行单条插入并立即提交
                conn.execute(insert_query, row_dict)
                conn.commit()
                insert_total_count += 1
                matched_count += 1
                print(f"✅ 设备ID {device_id} 插入成功！")
            except Exception as e:
                print(f"❌ 插入失败 设备ID {device_id} | 错误信息: {str(e)}")
                conn.rollback()
        else:
            print(f"❌ 匹配失败 (最高相似度: {best_score:.2f}) | 地址: {address}")
            match_fail_count += 1

# === 8. 打印统计 ===
total_records = len(devices)
print("\n📊 处理结果统计")
print(f"📄 设备总数: {total_records}")
print(f"✅ 匹配成功 (并插入记录): {matched_count}")
print(f"⚠️ address 为空: {address_null_count}")
print(f"⚠️ 地址格式错误: {address_format_error_count}")
print(f"❌ 匹配失败: {match_fail_count}")
print(f"📝 坐标插入总数: {insert_total_count}")