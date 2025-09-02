from sqlalchemy import create_engine, text
import urllib.parse
import sys

print("🔄 正在连接到数据库...")

DB_CONFIG = {
    "user": "root",
    "password": "p@ssw0rd.",
    "host": "109.123.246.112",
    "port": 3306,
    "database": "hes-jar",
}

DB_CONFIG2 = {
    "user": "root",
    "password": "p@ssw0rd.",
    "host": "192.168.16.59",
    "port": 3308,
    "database": "hesv4",
}

def get_engine():
    password = urllib.parse.quote_plus(DB_CONFIG['password'])
    conn_str = (
        f"mysql+pymysql://{DB_CONFIG['user']}:{password}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    return create_engine(conn_str)

def check_db_connection(engine):
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✅ 数据库连接正常！")
        return True
    except Exception as e:
        print("❌ 数据库连接失败:", e)
        return False

engine = get_engine()
if not check_db_connection(engine):
    sys.exit("❌ 程序终止，数据库无法连接")
