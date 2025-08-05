import random
import uuid
import pandas as pd
from datetime import datetime, timedelta
import os

# 配置项
group_ids = [0]
creator_ids = ['user01', 'user02', 'user03']
org_ids = ['84f1bc8a-5385-42dc-9d02-1feaf4e1caa8']

# 获取桌面路径
desktop_path = os.path.join(os.path.expanduser("~"), "Desktop")
output_file = os.path.join(desktop_path, "device_command_stat_12months.xlsx")

# 起始时间
today = datetime.today()
start_date = today - timedelta(days=365)

# 所有记录
records = []

# 遍历过去 365 天
for day_offset in range(365):
    day = start_date + timedelta(days=day_offset)
    sta_time = int(datetime(day.year, day.month, day.day).timestamp() * 1000)
    records_per_day = random.randint(1, 10)  # 每天生成 1~10 条

    for _ in range(records_per_day):
        record_id = str(uuid.uuid4())
        all_counts = random.randint(50, 200)  # 每条数据命令总数模拟 50~200 次
        # 成功率控制在 80%~90% 之间
        suc_counts = int(all_counts * random.uniform(0.8, 0.9))
        group_id = random.choice(group_ids)
        creator_id = random.choice(creator_ids)
        org_id = random.choice(org_ids)
        # 模拟 create_time 为当天任意时间
        random_hour = random.randint(0, 23)
        random_minute = random.randint(0, 59)
        create_datetime = datetime(day.year, day.month, day.day, random_hour, random_minute)
        create_time = int(create_datetime.timestamp() * 1000)

        records.append([
            record_id,
            all_counts,
            suc_counts,
            sta_time,
            group_id,
            creator_id,
            org_id,
            create_time
        ])

# 列名按照数据库字段
columns = ['id', 'all_counts', 'suc_counts', 'sta_time', 'group_id', 'creator_id', 'org_id', 'create_time']

# 使用 pandas 写入 Excel
df = pd.DataFrame(records, columns=columns)
df.to_excel(output_file, index=False)

print(f"数据已生成，共 {len(records)} 条记录，Excel 文件保存在：{output_file}")
