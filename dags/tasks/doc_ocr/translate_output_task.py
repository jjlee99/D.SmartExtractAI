from airflow.decorators import task
from pathlib import Path
import shutil, os
from airflow.models import Variable
import uuid
import json
import logging
from utils.translate.translate_util import is_korean, translate, detect_lang

from utils.com import json_util
from utils.db import dococr_query_util

TEMP_FOLDER = Variable.get("TEMP_FOLDER", default_var="/opt/airflow/data/temp")
RESULT_FOLDER = Variable.get("RESULT_FOLDER", default_var="/opt/airflow/data/result")

# 로거 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@task(pool='ocr_pool') 
def translate_output_task(table_list:list,**context):
    """
    지정된 테이블에서 가장 큰 ID (최근에 삽입된) 로우의 영문 텍스트를 한국어로 번역하고
    원본 테이블을 업데이트한 후, 번역 내역을 TB_OCR_TRN_LOG 테이블에 기록합니다.

    Args:
        table_name (str): 데이터를 읽고 번역할 테이블 이름.
        id_col (str): 각 레코드를 식별하며 시퀀스 번호 역할도 하는 ID 컬럼 이름.
    """
    for table_info in table_list:
        table_name = table_info["table_name"]
        id_col_name = table_info["id_col_name"]
        logger.info(f"테이블 '{table_name}' 번역, 업데이트 및 로그 기록 시작...")

        check_table_exists = dococr_query_util.check_map("checkTableExists", params=(table_name,))
        if check_table_exists<=0:
            logger.info(f"테이블 '{table_name}'은 아직 존재하지 않습니다.")
            return 

        # 1. 가장 큰 ID를 가진 로우를 찾습니다. (ID가 시퀀스 번호 역할)
        trn_target_row_list = dococr_query_util.select_translate_target_list(table_name, id_col_name)
        
        if not trn_target_row_list:
            logger.info(f"테이블 '{table_name}'에 대상이 없습니다.")
            return 

        for trn_target_row in trn_target_row_list:
            row_data = trn_target_row
            
            latest_id = ""
            updates_for_main_table = {}
            log_entries_to_insert = []
            
            # 3. 각 컬럼을 순회하며 영문 텍스트를 찾아 번역합니다.
            for col_name, col_value in row_data.items():
                if col_name == id_col_name:
                    latest_id = col_value
                    continue

                if isinstance(col_value, str) and col_value.strip():
                    lang_code = detect_lang(col_value)
                    if lang_code != "" and lang_code != "ko":
                        logger.info(f"테이블 '{table_name}', {id_col_name}, {lang_code} 컬럼 '{col_name}': '{col_value[:50]}...' 번역 시작")
                        translated_text = translate(col_value, from_lang=lang_code, to_lang="ko")

                        if "Error: 번역에 실패했습니다." in translated_text:
                            logger.error(f"테이블 '{table_name}', {id_col_name} {lang_code}, 컬럼 '{col_name}': 번역 실패로 이 컬럼에 대한 업데이트 및 로그 기록을 건너뜁니다.")
                        else:
                            updates_for_main_table[col_name] = (col_value,translated_text)
                            log_entries_to_insert.append((table_name, str(latest_id), col_name, col_value, translated_text))
                            logger.info(f"테이블 '{table_name}', {id_col_name} {lang_code}, 컬럼 '{col_name}': 번역 완료. 업데이트 및 로그 대기 중.")
                    else:
                        logger.debug(f"테이블 '{table_name}', {id_col_name} {lang_code}, 컬럼 '{col_name}': 이미 한국어이거나 번역 불필요. 건너뜁니다.")
                else:
                    logger.debug(f"테이블 '{table_name}', {id_col_name} {lang_code}, 컬럼 '{col_name}': 문자열이 아니거나 비어있습니다. 건너뜁니다.")

            # 4. 번역된 컬럼이 있다면 원본 테이블을 업데이트하고 로그 테이블에 기록합니다. 없다면 로그 테이블에 빈 로그를 기록합니다.
            if updates_for_main_table:
                dococr_query_util.update_for_translate(table_name,id_col_name,str(latest_id),updates_for_main_table,update_origin_at=False)
            else:
                params = (table_name,latest_id,id_col_name,latest_id,latest_id)
                dococr_query_util.insert_map("insertTranslateLog", params=params)
                logger.info(f"테이블 '{table_name}', {id_col_name} {lang_code}: 번역할 영문 컬럼이 없거나 모든 번역이 실패했습니다. 업데이트/로그할 내용이 없습니다.")
