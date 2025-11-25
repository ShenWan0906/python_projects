from sqlalchemy import create_engine, text
import urllib.parse
import sys

print("🔄 正在连接到数据库...")


# === 数据库配置 ===
# 你可以随时切换 DB_CONFIG 的引用
DB_CONFIG = {
    "type": "postgresql",   # 支持 "mysql" 或 "postgresql"
    "user": "root",
    "password": "p@ssw0rd.",
    "host": "192.168.18.133",
    "port": 5432,
    "database": "postgres",
}

DB_CONFIG2 = {
    "type": "mysql",
    "user": "root",
    "password": "p@ssw0rd.",
    "host": "192.168.16.59",
    "port": 3308,
    "database": "hesv4",
}

DB_CONFIG3 = {
    "type": "mysql",
    "user": "root",
    "password": "p@ssw0rd.",
    "host": "109.123.246.112",
    "port": 3306,
    "database": "hes-jar",
}

DB_CONFIG4 = {
    "type": "postgresql",   # 支持 "mysql" 或 "postgresql"
    "user": "hesuser",
    "password": "aD3dB3sE3cN1sH0f",
    "host": "192.168.18.132",
    "port": 5433,
    "database": "hes",
}


# === 构造数据库引擎 ===
def get_engine(config=DB_CONFIG4):
    db_type = config.get("type", "mysql").lower()
    password = urllib.parse.quote_plus(config["password"])

    if db_type == "mysql":
        conn_str = (
            f"mysql+pymysql://{config['user']}:{password}"
            f"@{config['host']}:{config['port']}/{config['database']}"
            f"?charset=utf8mb4"
        )
    elif db_type == "postgresql":
        conn_str = (
            f"postgresql+psycopg2://{config['user']}:{password}"
            f"@{config['host']}:{config['port']}/{config['database']}"
        )
    else:
        raise ValueError(f"❌ 不支持的数据库类型: {db_type}")

    return create_engine(conn_str, echo=False, pool_pre_ping=True)


# === 测试数据库连接 ===
def check_db_connection(engine):
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✅ 数据库连接正常！")
        return True
    except Exception as e:
        print("❌ 数据库连接失败:", e)
        return False


# === 启动连接测试 ===
engine = get_engine(DB_CONFIG4)
if not check_db_connection(engine):
    sys.exit("❌ 程序终止，数据库无法连接")
