from dbhelp import engine
from sqlalchemy import text, Table, MetaData, inspect
from datetime import datetime, timedelta
import random
import time
import uuid

# 加载表结构
metadata = MetaData()
dev_message = Table('dev_message', metadata, autoload_with=engine)

def generate_dev_messages():
    end_date = datetime.now() - timedelta(days=1)
    start_date = end_date - timedelta(days=90)
    total_accumulate_flow = 0.0
    dev_messages = []

    current_date = start_date
    while current_date <= end_date:
        total_accumulate_flow += round(random.uniform(10, 100), 3)
        row_dict = {
            "id": str(uuid.uuid4()),
            "device_id": "2025042800002",
            "device_name": "2025042800002",
            "device_type": "watermeter",
            "product_id": "1001",
            "product_name": "U-WR2",
            "recv_time": current_date.strftime("%Y-%m-%d 11:00:00.000000"),
            "frozen_time": current_date.strftime("%Y-%m-%d 00:00:00.000000"),
            "total_accumulate_flow": f"{total_accumulate_flow:.3f}",
            # 其他字段照旧
            "forward_total_flow": "0.000",
            "reverse_total_flow": "0.000",
            "min_flow": "0.000",
            "max_flow": "0.000",
            "average_flow": "0.000",
            "instantaneous_flow": "0.000",
            "min_water_temperature": "21.60",
            "max_water_temperature": "21.99",
            "average_water_temperature": "21.96",
            "instantaneous_water_temperature": "21.99",
            "min_pressure": "0.00",
            "max_pressure": "0.00",
            "average_pressure": "0.00",
            "instantaneous_pressure": "0.00",
            "valve_status": "99",
            "day_quota": "0.00",
            "month_quota": "100",
            "day_remain_quota": "80.00",
            "month_remain_quota": "100",
            "measure_battery_remain_days": "3849",
            "com_battery_remain_days": "94",
            "valve_battery_remain_days": "5933",
            "exist_flow_minutes": "0",
            "status_byte_str": "00000000",
            "alarm_info": "00101000000000100001011100000100",
            "error_info": "00000000",
            "signal_strength": "-85.0",
            "signal_strength_guide": "-75.1",
            "signal_noise_ratio": "17.1",
            "ecl0_time": "842",
            "ecl1_time": "2644",
            "ecl2_time": "6",
            "send_pag_nums": 14669,
            "receive_pag_nums": 16206,
            "community_id": "201421890",
            "community_ident": "493",
            "psm_timer": "0",
            "edrx_timer": "0",
            "longitude": 0,
            "latitude": 0,
            "protocol_code": "ara_water",
            "protocol_code_name": None,
            "message_type": 1,
            "up_type": 2,
            "data_type": 2,
            "data_up_type": 1,
            "original_msg": "68 32 30 32 34 ...",  # 可简化
            "config_info": "11111111111111111111111111100000",
            "creator_id": "Automatically generate",
            "create_time": int(current_date.timestamp() * 1000)
        }
        dev_messages.append(row_dict)
        current_date += timedelta(days=1)

    try:
        with engine.begin() as conn:
            conn.execute(dev_message.insert(), dev_messages)
        print(f"✅ 插入了 {len(dev_messages)} 条 dev_message 数据")
    except Exception as e:
        print(f"❌ 批量插入失败: {e}")

generate_dev_messages()
