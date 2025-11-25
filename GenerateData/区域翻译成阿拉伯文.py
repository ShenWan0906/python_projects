import logging
import time
import re
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import text
from deep_translator import GoogleTranslator
from dbhelp import get_engine, DB_CONFIG

# === 日志配置 ===
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# === 工具函数 ===
def contains_arabic(text: str) -> bool:
    """判断字符串中是否包含阿拉伯文字符"""
    if not text:
        return False
    return bool(re.search(r'[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF]', text))

def needs_translation(name_ar: str) -> bool:
    """判断当前 name_ar 是否需要翻译"""
    if pd.isna(name_ar) or name_ar.strip() == "":
        return True
    # 如果没有阿拉伯文字符，说明还没翻译成功
    if not contains_arabic(name_ar):
        return True
    return False

def translate_text(row, translator, max_retries=5):
    """翻译英文为阿拉伯文，带重试"""
    region_id = row["region_id"]
    name_en = row["name_en"]

    if not name_en or pd.isna(name_en):
        logger.warning(f"⚠️ region_id={region_id} 的 name_en 为空，跳过。")
        return region_id, "", name_en

    for attempt in range(max_retries):
        try:
            result = translator.translate(name_en).strip()
            if contains_arabic(result):
                return region_id, result, name_en
            else:
                logger.warning(f"⚠️ 翻译仍为英文或异常，region_id={region_id}, result='{result}'")
        except Exception as e:
            logger.warning(f"⚠️ 翻译失败 (尝试 {attempt + 1}/{max_retries}) - region_id={region_id}: {e}")
        if attempt < max_retries - 1:
            time.sleep(2)  # 重试间隔
    logger.error(f"❌ 翻译最终失败 region_id={region_id}, name_en='{name_en}'")
    return region_id, "", name_en

# === 主流程 ===
def main(batch_size=100, max_workers=5, test_mode=True):
    logger.info("🔄 连接数据库...")
    engine = get_engine(DB_CONFIG)
    logger.info("✅ 数据库连接成功")

    translator = GoogleTranslator(source="auto", target="ar")

    with engine.connect() as conn:
        # 读取所有数据
        df_region = pd.DataFrame(conn.execute(
            text("SELECT region_id, name_en, name_ar FROM alabo_region")
        ).mappings().all())

        # 只处理需要重新翻译的记录
        records_to_translate = df_region[df_region["name_ar"].apply(needs_translation)]
        total_records = len(records_to_translate)
        logger.info(f"📊 待翻译记录数: {total_records} / {len(df_region)}")

        if total_records == 0:
            logger.info("✅ 无需翻译，所有数据已正确翻译。")
            return

        start_time = time.time()
        processed_count = 0
        batch_index = 1

        for i in range(0, total_records, batch_size):
            batch = records_to_translate.iloc[i:i + batch_size]
            logger.info(f"🌀 开始处理第 {batch_index} 批 ({i} ~ {i + len(batch)}) ...")

            translated_results = []
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(translate_text, row, translator): row["region_id"] for _, row in batch.iterrows()}
                for future in as_completed(futures):
                    region_id, name_ar, name_en = future.result()
                    if name_ar:
                        translated_results.append({"region_id": region_id, "name_en": name_en, "name_ar": name_ar})

            # 测试模式：打印翻译结果，不更新数据库
            if test_mode:
                for rec in translated_results:
                    print(f"region_id={rec['region_id']}, name_en='{rec['name_en']}', name_ar='{rec['name_ar']}'")
            else:
                # 批量更新数据库
                if translated_results:
                    try:
                        update_sql = """
                            UPDATE alabo_region
                            SET name_ar = :name_ar
                            WHERE region_id = :region_id
                        """
                        conn.execute(text(update_sql), translated_results)
                        conn.commit()
                        processed_count += len(translated_results)
                        logger.info(f"✅ 第 {batch_index} 批完成，更新 {len(translated_results)} 条记录")
                    except Exception as e:
                        conn.rollback()
                        logger.error(f"❌ 第 {batch_index} 批更新失败: {e}")

            # 防止 API 限速
            if i + batch_size < total_records:
                logger.info("⏳ 等待 5 秒防止被限速...")
                time.sleep(5)

            batch_index += 1

        total_duration = time.time() - start_time
        logger.info("🎯 翻译完成")
        if not test_mode:
            logger.info(f"📊 成功翻译记录: {processed_count}/{total_records}")
        logger.info(f"⏱️ 总耗时: {total_duration:.2f} 秒")

if __name__ == "__main__":
    # test_mode=True 只打印结果，不更新数据库
    main(batch_size=100, max_workers=5, test_mode=True)
