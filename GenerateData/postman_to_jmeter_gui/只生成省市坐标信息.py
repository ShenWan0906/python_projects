import logging
import time
import pandas as pd
from sqlalchemy import text
from dbhelp import get_engine, DB_CONFIG5

# === 日志配置 ===
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def main(batch_size=5000):  # 涉及空间运算，批次建议缩小一点点以保证事务稳定
    logger.info("🔄 连接数据库...")
    engine = get_engine(DB_CONFIG5)

    with engine.connect() as conn:
        # 1️⃣ 读取区域数据（仅读取市级 Level 2，因为设备通常归属于市）
        logger.info("🔄 读取 alabo_region 区域表 (市级)...")
        # 注意：这里我们通过 parent_id 关联把省 ID 也查出来，方便一次性更新
        query_region = """
            SELECT 
                c.region_id as city_id, 
                c.parent_id as province_id, 
                c.name_en as city_name,
                p.name_en as province_name
            FROM alabo_region c
            LEFT JOIN alabo_region p ON c.parent_id = p.region_id
            WHERE c.level = '2' 
        """
        df_region = pd.DataFrame(conn.execute(text(query_region)).mappings().all())

        if df_region.empty:
            logger.error("❌ 未找到市级区域数据")
            return

        # 2️⃣ 准备设备数据
        total_devices = conn.execute(text("SELECT COUNT(*) FROM dev_device_instance")).scalar()
        logger.info(f"📦 总设备数: {total_devices}")

        offset = 0
        batch_index = 1

        # 核心 SQL：使用 PostGIS 在指定 region_id 的 geom 范围内生成 1 个随机点
        # ST_GeneratePoints 生成的是 MultiPoint，所以用 (ST_Dump).geom 转为 Point
        # 然后用 ST_X 和 ST_Y 提取经纬度
        update_sql = text("""
            UPDATE dev_device_instance
            SET 
                province_id = :province_id,
                city_id = :city_id,
                region_id = :city_id,
                install_address = :address,
                address = :address,
                -- ST_GeneratePoints 返回 MultiPoint，必须用 ST_GeometryN 提取出其中的 Point
                install_longitude = ST_X(ST_GeometryN(sub.random_pt, 1)),
                install_latitude = ST_Y(ST_GeometryN(sub.random_pt, 1))
            FROM (
                SELECT 
                    ST_GeneratePoints(geom, 1) as random_pt, 
                    region_id 
                FROM alabo_region
            ) AS sub
            WHERE dev_device_instance.id = :device_id
            AND sub.region_id = :city_id
        """)

        while offset < total_devices:
            batch_start_time = time.time()

            # 查出一批设备 ID
            devices = conn.execute(
                text(f"SELECT id FROM dev_device_instance ORDER BY id LIMIT {batch_size} OFFSET {offset}")
            ).fetchall()

            if not devices:
                break

            update_params = []
            for dev in devices:
                # 随机分配一个市
                target_city = df_region.sample(1).iloc[0]
                full_address = f"{target_city['province_name']} {target_city['city_name']}"

                update_params.append({
                    "province_id": int(target_city['province_id']),
                    "city_id": int(target_city['city_id']),
                    "address": full_address,
                    "device_id": dev[0]
                })

            # 执行批量更新
            try:
                # 注意：由于使用了 FROM 语句，SQLAlchemy 的 executemany 可能在某些驱动下表现不同
                # 这里我们分小块提交事务
                conn.execute(update_sql, update_params)
                conn.commit()

                duration = time.time() - batch_start_time
                logger.info(f"✅ 第 {batch_index} 批处理完成 ({len(update_params)}条), 耗时 {duration:.2f}s")
            except Exception as e:
                logger.error(f"❌ 更新批次 {batch_index} 失败: {e}")
                conn.rollback()

            offset += batch_size
            batch_index += 1

    logger.info("🎯 脚本执行完毕！所有设备已随机分布在各市的多边形区域内。")


if __name__ == "__main__":
    main()