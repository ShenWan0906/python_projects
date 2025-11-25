import pandas as pd
import urllib.parse
from sqlalchemy import create_engine, text
from difflib import SequenceMatcher
import time

# === 数据库配置 ===
DB_CONFIG = {
    "host": "127.0.0.1",
    "port": "5432",
    "user": "postgres",
    "password": "123456",
    "database": "postgres"
}

# === 创建数据库连接 ===
print("🔄 正在连接到数据库...")

user = urllib.parse.quote_plus(DB_CONFIG["user"])
password = urllib.parse.quote_plus(DB_CONFIG["password"])
host = DB_CONFIG["host"]
port = DB_CONFIG["port"]
db = urllib.parse.quote_plus(DB_CONFIG["database"])

conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
engine = create_engine(conn_str, echo=False, pool_pre_ping=True)

# 测试连接
with engine.connect() as conn:
    current_db = conn.execute(text("SELECT current_database(), current_user")).fetchone()
    print(f"✅ 已连接到数据库: {current_db[0]} 用户: {current_db[1]}")

# === 读取数据 ===
geo_df = pd.read_sql("SELECT id, province, city, district FROM geo_centers", engine)
region_df = pd.read_sql("SELECT id, parent_id, name_en, level FROM alabo_region", engine)

print(f"📍 geo_centers 共 {len(geo_df)} 条数据")
print(f"📍 alabo_region 共 {len(region_df)} 条数据")

# === 构建区域层级 ===
provinces = region_df[region_df["level"] == 1][["id", "name_en"]]
cities = region_df[region_df["level"] == 2][["id", "name_en", "parent_id"]]
districts = region_df[region_df["level"] == 3][["id", "name_en", "parent_id"]]


# === 模糊匹配函数 ===
def best_match(name, candidates):
    """返回最相似的ID和匹配度"""
    if not isinstance(name, str) or not name.strip():
        return None, 0
    name = name.strip().lower()
    best_score, best_id = 0, None
    for _, row in candidates.iterrows():
        score = SequenceMatcher(None, name, str(row["name_en"]).lower()).ratio()
        if score > best_score:
            best_score, best_id = score, row["id"]
    return best_id, best_score


# === 更新语句 ===
update_sql = text("""
    UPDATE geo_centers
    SET province_id = :province_id,
        city_id = :city_id,
        region_id = :region_id
    WHERE id = :id
""")

total = len(geo_df)
updated_count = 0
start_time = time.time()

print(f"\n🔄 开始逐条匹配并实时更新（强制更新最相似项）...\n")

# === 修改为每条记录独立事务 ===
for i, row in geo_df.iterrows():
    # 为每条记录创建独立连接和事务
    with engine.begin() as conn:
        # 匹配省、市、区
        p_id, p_score = best_match(row["province"], provinces)
        c_id, c_score = best_match(row["city"], cities)
        r_id, r_score = best_match(row["district"], districts)

        # 强制更新（取最相似项），None 表示匹配不到
        conn.execute(update_sql, {
            "province_id": int(p_id) if p_id else None,
            "city_id": int(c_id) if c_id else None,
            "region_id": int(r_id) if r_id else None,
            "id": int(row["id"])
        })

    updated_count += 1

    # 匹配度低于 0.5 警告
    warn_flag = "⚠️" if min(p_score, c_score, r_score) < 0.5 else "✅"

    # 实时日志输出
    print(
        f"[{i + 1}/{total}] {warn_flag} 更新ID={row['id']} | "
        f"省:{row['province']}({p_score:.2f}→{p_id}), "
        f"市:{row['city']}({c_score:.2f}→{c_id}), "
        f"区:{row['district']}({r_score:.2f}→{r_id})"
    )

    # 可选：每 100 条暂停 0.1 秒，降低数据库压力
    if (i + 1) % 100 == 0:
        time.sleep(0.1)

elapsed = round(time.time() - start_time, 2)
print(f"\n🎯 全部匹配并实时更新完成！共更新 {updated_count} 条记录 ✅，耗时 {elapsed} 秒")