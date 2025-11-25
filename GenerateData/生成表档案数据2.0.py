import pandas as pd
import random
from sqlalchemy import text
from dbhelp import engine
from tqdm import tqdm
import time
import logging
from datetime import datetime

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    print("🔄 开始处理设备地区数据分配...")

    # 1️⃣ 读取 alabo_region 全部地区数据
    print("📊 读取地区数据...")
    sql_alabo_region = text("SELECT region_id, parent_id, name_en, level FROM alabo_region")
    df_region = pd.read_sql(sql_alabo_region, con=engine)

    print(f"alabo_region 总记录数: {len(df_region)}")

    # 2️⃣ 分别筛选出 省、市、区
    df_province = df_region[df_region['level'] == "1"]
    df_city = df_region[df_region['level'] == "2"]
    df_area = df_region[df_region['level'] == "3"]

    print(f"省份数: {len(df_province)}")
    print(f"城市数: {len(df_city)}")
    print(f"区县数: {len(df_area)}")

    # 3️⃣ 构造完整的 省-市-区 组合
    print("🔗 构建地区组合关系...")
    region_list = []

    for _, prov in tqdm(df_province.iterrows(), total=len(df_province), desc="处理省份"):
        prov_id = prov['region_id']
        prov_name = prov['name_en']

        cities = df_city[df_city['parent_id'] == prov_id]
        for _, city in cities.iterrows():
            city_id = city['region_id']
            city_name = city['name_en']

            areas = df_area[df_area['parent_id'] == city_id]
            for _, area in areas.iterrows():
                area_id = area['region_id']
                area_name = area['name_en']

                region_list.append({
                    'province_id': prov_id,
                    'prov_name': prov_name,
                    'city_id': city_id,
                    'city_name': city_name,
                    'region_id': area_id,
                    'region_name': area_name,
                    'address': f'{prov_name} / {city_name} / {area_name}'
                })

    print(f'📌 可用地区组合数: {len(region_list)}')

    # 4️⃣ 读取 dev_device_instance 需要赋值的数据
    print("📋 读取设备数据...")
    sql_dev_device_instance = text("SELECT id FROM dev_device_instance")
    df_devices = pd.read_sql(sql_dev_device_instance, con=engine)

    print(f"📌 需要赋值的设备数: {len(df_devices)}")

    # 5️⃣ 为每条设备数据分配随机地址
    print("🎯 分配随机地区...")
    assigned_data = []

    for _, row in tqdm(df_devices.iterrows(), total=df_devices.shape[0], desc="分配地区"):
        random_region = random.choice(region_list)

        assigned_data.append({
            'id': row['id'],
            'province_id': random_region['province_id'],
            'province_name': random_region['prov_name'],
            'city_id': random_region['city_id'],
            'city_name': random_region['city_name'],
            'region_id': random_region['region_id'],
            'region_name': random_region['region_name'],
            'address': random_region['address']
        })

    df_assigned = pd.DataFrame(assigned_data)

    # 6️⃣ 使用修复后的安全更新方法
    print("🚀 开始极安全更新数据库...")
    success = ultra_safe_single_update_fixed(df_assigned)

    if success:
        print("✅ 设备数据地区赋值完成！")
    else:
        print("❌ 更新过程中出现问题，请检查日志")


def ultra_safe_single_update_fixed(df_assigned):
    """
    修复后的单条记录更新方法，解决SQLAlchemy 2.0事务问题
    """
    total_records = len(df_assigned)

    print(f"🔄 开始单条记录更新，共 {total_records} 条记录")

    success_count = 0
    fail_count = 0

    # 创建进度条
    pbar = tqdm(total=total_records, desc="更新进度")

    for index, row in df_assigned.iterrows():
        # 尝试更新当前记录，最多重试5次
        record_success = False
        retry_count = 0
        max_retries = 5

        while not record_success and retry_count < max_retries:
            try:
                # 修复：使用 engine.begin() 而不是 engine.connect() + conn.begin()
                with engine.begin() as conn:
                    # 设置很短的锁等待时间
                    conn.execute(text("SET innodb_lock_wait_timeout = 10"))

                    update_sql = text("""
                        UPDATE dev_device_instance
                        SET province_id = :province_id,
                            province_name = :province_name,
                            city_id = :city_id,
                            city_name = :city_name,
                            region_id = :region_id,
                            region_name = :region_name,    
                            address = :address
                        WHERE id = :id
                    """)
                    result = conn.execute(update_sql, {
                        'province_id': row['province_id'],
                        'province_name': row['province_name'],
                        'city_id': row['city_id'],
                        'city_name': row['city_name'],
                        'region_id': row['region_id'],
                        'region_name': row['region_name'],
                        'address': row['address'],
                        'id': row['id']
                    })

                    # 检查是否真的更新了记录
                    if result.rowcount == 0:
                        logger.warning(f"记录 {row['id']} 未找到，可能已被删除")

                record_success = True
                success_count += 1

            except Exception as e:
                retry_count += 1
                error_msg = str(e)

                if "Lock wait timeout" in error_msg or "1205" in error_msg:
                    # 锁超时，等待后重试
                    wait_time = 5 * retry_count
                    logger.warning(f"记录 {row['id']} 更新锁超时，第 {retry_count} 次重试，等待 {wait_time} 秒")
                    time.sleep(wait_time)
                else:
                    # 其他错误
                    wait_time = 2 ** retry_count
                    logger.warning(f"记录 {row['id']} 更新失败，第 {retry_count} 次重试，错误: {error_msg}")
                    time.sleep(wait_time)

                if retry_count >= max_retries:
                    logger.error(f"记录 {row['id']} 更新失败，已达到最大重试次数")
                    fail_count += 1
                    save_failed_record(row, index)

        # 更新进度条
        pbar.update(1)
        pbar.set_postfix(成功=f"{success_count}", 失败=f"{fail_count}")

        # 每处理一定数量记录后暂停
        if success_count > 0 and success_count % 100 == 0:
            time.sleep(0.5)  # 短暂暂停

        # 每处理1000条记录后显示一次状态
        if success_count > 0 and success_count % 1000 == 0:
            logger.info(f"已处理 {success_count} 条记录，失败 {fail_count} 条")

    pbar.close()

    print(f"📊 更新完成统计:")
    print(f"  成功记录: {success_count}")
    print(f"  失败记录: {fail_count}")
    print(f"  成功率: {success_count / total_records * 100:.2f}%")

    return fail_count == 0


def ultra_safe_batch_update(df_assigned):
    """
    备选方案：小批次更新，效率更高
    """
    batch_size = 50  # 小批次大小
    total_batches = (len(df_assigned) + batch_size - 1) // batch_size

    print(f"🔄 开始小批次更新，共 {total_batches} 批，每批 {batch_size} 条记录")

    success_count = 0
    fail_count = 0

    pbar = tqdm(total=total_batches, desc="批次进度")

    for batch_num in range(total_batches):
        start_idx = batch_num * batch_size
        end_idx = min((batch_num + 1) * batch_size, len(df_assigned))
        batch_df = df_assigned.iloc[start_idx:end_idx]

        batch_success = False
        retry_count = 0
        max_retries = 3

        while not batch_success and retry_count < max_retries:
            try:
                with engine.begin() as conn:
                    conn.execute(text("SET innodb_lock_wait_timeout = 30"))

                    # 使用 executemany 批量更新
                    update_sql = text("""
                        UPDATE dev_device_instance
                        SET province_id = :province_id,
                            province_name = :province_name,
                            city_id = :city_id,
                            city_name = :city_name,
                            region_id = :region_id,
                            region_name = :region_name,    
                            address = :address
                        WHERE id = :id
                    """)

                    params = []
                    for _, row in batch_df.iterrows():
                        params.append({
                            'province_id': row['province_id'],
                            'province_name': row['province_name'],
                            'city_id': row['city_id'],
                            'city_name': row['city_name'],
                            'region_id': row['region_id'],
                            'region_name': row['region_name'],
                            'address': row['address'],
                            'id': row['id']
                        })

                    conn.execute(update_sql, params)

                batch_success = True
                success_count += len(batch_df)

            except Exception as e:
                retry_count += 1
                error_msg = str(e)

                if "Lock wait timeout" in error_msg or "1205" in error_msg:
                    wait_time = 10 * retry_count
                    logger.warning(f"批次 {batch_num} 更新锁超时，第 {retry_count} 次重试，等待 {wait_time} 秒")
                else:
                    wait_time = 5 * retry_count
                    logger.warning(f"批次 {batch_num} 更新失败，第 {retry_count} 次重试，错误: {error_msg}")

                time.sleep(wait_time)

                if retry_count >= max_retries:
                    logger.error(f"批次 {batch_num} 更新失败，已达到最大重试次数")
                    fail_count += len(batch_df)
                    save_failed_batch(batch_df, batch_num)

        pbar.update(1)
        pbar.set_postfix(成功=f"{success_count}", 失败=f"{fail_count}")

        # 批次间暂停
        if batch_success:
            time.sleep(0.1)

    pbar.close()

    print(f"📊 更新完成统计:")
    print(f"  成功记录: {success_count}")
    print(f"  失败记录: {fail_count}")
    print(f"  成功率: {success_count / len(df_assigned) * 100:.2f}%")

    return fail_count == 0


def save_failed_record(record, index):
    """保存失败记录到文件"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"failed_records_{timestamp}.csv"

    # 如果是第一个失败记录，创建文件并写入header
    if index == 0:
        pd.DataFrame([record]).to_csv(filename, index=False)
    else:
        # 追加到现有文件
        pd.DataFrame([record]).to_csv(filename, mode='a', header=False, index=False)

    logger.info(f"失败记录已保存到: {filename}")


def save_failed_batch(batch_df, batch_num):
    """保存失败批次到文件"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"failed_batch_{batch_num}_{timestamp}.csv"
    batch_df.to_csv(filename, index=False)
    logger.info(f"失败批次已保存到: {filename}")


if __name__ == "__main__":
    start_time = time.time()

    try:
        main()
    except KeyboardInterrupt:
        logger.info("用户中断执行")
    except Exception as e:
        logger.error(f"执行过程中发生未预期错误: {str(e)}")
        import traceback

        traceback.print_exc()
    finally:
        # 计算总耗时
        end_time = time.time()
        total_time = end_time - start_time
        print(f"⏰ 总执行时间: {total_time:.2f} 秒 ({total_time / 60:.2f} 分钟)")