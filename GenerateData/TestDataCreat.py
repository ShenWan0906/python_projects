import pandas as pd
import random
from faker import Faker
from datetime import datetime
from dbhelp import engine  # 你自己的封装
from tqdm import tqdm  # 进度条展示（可选）
import uuid

fake = Faker("zh_CN")


def generate_device_row(i):
    timestamp = int(datetime.now().timestamp() * 1000)
    uid = str(uuid.uuid4())  # 同一个ID用于两张表

    device = {
        "id": uid,
        "photo_url": "http://125.124.12.205:5174/api/file/Y5gIY6a4LaoRWIAFnbv9BAbeHAME-bKQ.jpg?accessKey=2deafc11f6cb5e7548b70522af3c3711",
        "name": timestamp,
        'device_type': 'watermeter',
        'product_id': '1001',
        'product_name': 'U-WR2-25',
        "install_address": "威星智能",
        "state": "offline",
        "creator_id": "04f7094c638c72f329c7f04489852b15",
        "creator_name": "NWC",
        "create_time": timestamp,
        "size": "DN15mm=1/2″",
        "brand": "VIEWSHINE",
        "model": "U-WR2-15",
        "address": "浙江省杭州市余杭区",
        "active_state": "inactive",
        "device_state": "notUsed",
        "building_type": "Govermental",
        "second_id": f"M_{i:06d}",
        "install_state": 0,
    }

    asset_bind = {
        "id": str(uuid.uuid4()),  # asset_bind自己的ID
        "target_type": "org",
        "target_id": "1f4c4a03-9a10-4a84-b561-01be1b73c09a",
        "target_key": "77fed374bc13008dadbe7e3d18d3d8d6",
        "asset_type": "meterId",  # 对应设备ID
        "asset_id": uid,
        "relation": "owner",
        "permission": "15",
        "update_time": 1753066666
    }

    return device, asset_bind


# 字段定义
device_columns = [
    "id", "name", "photo_url", "device_type", "product_id", "product_name", "install_address",
    "state", "creator_id", "creator_name", "create_time", "size", "brand", "model", "address",
    "active_state", "device_state", "building_type", "second_id", "install_state"
]

asset_bind_columns = [
    "id", "target_type", "target_id", "target_key", "asset_type", "asset_id", "relation", "permission", "update_time"
]

# 分批插入
batch_size = 5000
total = 1000000 # 百万

for start in tqdm(range(0, total, batch_size)):
    device_rows = []
    bind_rows = []

    for i in range(start, min(start + batch_size, total)):
        device, asset_bind = generate_device_row(i)
        device_rows.append(device)
        bind_rows.append(asset_bind)

    df_device = pd.DataFrame(device_rows, columns=device_columns)
    df_bind = pd.DataFrame(bind_rows, columns=asset_bind_columns)

    # 插入两张表
    df_device.to_sql("dev_device_instance", con=engine, if_exists="append", index=False)
    df_bind.to_sql("s_dimension_assets_bind", con=engine, if_exists="append", index=False)
