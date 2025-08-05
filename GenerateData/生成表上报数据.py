from dbhelp import engine
import pandas as pd
from sqlalchemy import text, Table, MetaData, inspect
from geocoder_helper import get_coordinates_with_cache
import uuid
import time

def format_address(original_address):
    # 1. 拆分原始地址（以 `/` 为分隔符）
    parts = original_address.split(" / ")

    # 2. 提取相关部分，并转换成标准格式
    if len(parts) == 3:
        # 如果有三部分（通常是 省/市/区）
        city = parts[1]
        district = parts[2].replace("Dist.", "District")  # 将 "Dist." 替换为 "District"
        formatted_address = f"{district}, {city}, Saudi Arabia"
    elif len(parts) == 2:
        # 如果只有两部分（如 省/市）
        city = parts[1]
        formatted_address = f"{city}, Saudi Arabia"
    else:
        # 默认处理，仅城市
        formatted_address = f"{parts[0]}, Saudi Arabia"

    return formatted_address


# 查询条件数据
sql_dev_device_instance = text("select a.*,b.longitude,b.latitude from dev_device_instance a LEFT JOIN device_latest_report_message b on a.id = b.device_id where a.creator_name = 'system' and b.latitude is null")
df_sql_dev_device_instance = pd.read_sql(sql_dev_device_instance, con=engine)

print(f"✅ 数据查询完成，查询到 {df_sql_dev_device_instance.shape[0]} 条数据")

if not df_sql_dev_device_instance.empty:
    # 加载表元数据
    metadata = MetaData()
    # 使用 SQLAlchemy Inspector 获取表结构
    inspector = inspect(engine)
    columns = inspector.get_columns('device_latest_report_message')
    print(columns)  # 输出表结构，调试时查看列名

    # 获取目标表结构
    device_latest_report_message = Table('device_latest_report_message', metadata, autoload_with=engine)

    # 遍历这个 id 列表
    for index, row in df_sql_dev_device_instance.iterrows():
        formatted_address = format_address(row["address"])
        lat, lng = get_coordinates_with_cache(formatted_address)

        if lat is None or lng is None:
            print(f"❌ 无法获取坐标: {row['address']}")
            continue  # 跳过该行数据

        # 生成插入的行数据
        row_dict = {
            'id': str(uuid.uuid4()),  # 使用 GUID
            'device_id': row["id"],
            'device_name': row["name"],
            'device_type': 'watermeter',
            'product_id': '1001',
            'product_name': 'U-WR2-25',
            'recv_time': '',
            'frozen_time': '',
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
            'longitude': lng,
            'latitude': lat,
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
            # 执行插入操作
            with engine.begin() as conn:
                conn.execute(device_latest_report_message.insert().values(row_dict))
            print(f"✅ 数据已插入设备 {row['id']} 的最新报告表")

            time.sleep(0.1)  # 修改：加入延迟，避免请求频繁

        except Exception as e:
            print(f"❌ 插入数据失败: {e}")
            continue  # 继续插入其他数据
