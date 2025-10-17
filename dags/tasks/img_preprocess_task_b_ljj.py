from airflow.decorators import task
from collections import Counter
from pathlib import Path
from PIL import Image
import numpy as np
import cv2, os
from typing import Any,List
import uuid
from utils.com import file_util, json_util
from utils.img import type_convert_util
from airflow.models import Variable,XCom
from collections import Counter
import pytesseract
from scipy.ndimage import interpolation as inter
from typing import List, Tuple

RESULT_FOLDER = Variable.get("RESULT_FOLDER", default_var="/opt/airflow/data/result")
TEMP_FOLDER = Variable.get("TEMP_FOLDER", default_var="/opt/airflow/data/temp")
result_map = {}           # 최종 결과 파일 경로 관리

#---------------------------------한국어 번역 코드(translate_util사용)-----------------------------------
import mysql.connector
from mysql.connector import Error
import logging
from utils.translate.translate_util import is_korean, translate


# 로거 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# DB 연결 설정
DB_CONFIG = {
    "host": "192.168.10.18",
    "database": "dococr",
    "user": "digitalflow",
    "password": "digital10",
    "port": "3306"
}
from airflow.providers.mysql.hooks.mysql import MySqlHook
def get_db_connection():
    """데이터베이스 연결을 설정하고 반환합니다."""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        if conn.is_connected():
            logger.info("데이터베이스에 성공적으로 연결되었습니다.")
            return conn
    except Error as e:
        logger.error(f"데이터베이스 연결 오류: {e}")
        return None

def close_db_connection(conn, cursor):
    """데이터베이스 연결과 커서를 닫습니다."""
    if cursor:
        cursor.close()
    if conn and conn.is_connected():
        conn.close()
        logger.info("데이터베이스 연결이 닫혔습니다.")

def translate_update_and_log_db(table_name: str, id_col: str):
    """
    지정된 테이블에서 가장 큰 ID (최근에 삽입된) 로우의 영문 텍스트를 한국어로 번역하고
    원본 테이블을 업데이트한 후, 번역 내역을 TB_OCR_TRN_LOG 테이블에 기록합니다.

    Args:
        table_name (str): 데이터를 읽고 번역할 테이블 이름.
        id_col (str): 각 레코드를 식별하며 시퀀스 번호 역할도 하는 ID 컬럼 이름.
    """
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        if conn is None:
            return

        # autocommit을 False로 설정하여 수동 트랜잭션 관리를 합니다.
        # 이렇게 하면 메인 테이블 업데이트와 로그 기록이 하나의 트랜잭션으로 묶일 수 있습니다.
        conn.autocommit = False 
        cursor = conn.cursor(dictionary=True) # 딕셔너리 형태로 결과를 가져오기 위해 dictionary=True 설정

        logger.info(f"테이블 '{table_name}' 번역, 업데이트 및 로그 기록 시작...")

        # 1. 가장 큰 ID를 가진 로우를 찾습니다. (ID가 시퀀스 번호 역할)
        try:
            max_id_query = f"""
                SELECT {id_col}
                FROM {table_name}
                ORDER BY {id_col} DESC
                LIMIT 1;
            """
            cursor.execute(max_id_query)
            latest_row_info_list = cursor.fetchall() 

            if not latest_row_info_list:
                logger.info(f"테이블 '{table_name}'에 로우가 없습니다. 다음 테이블로 이동합니다.")
                return 

            latest_row_info = latest_row_info_list[0]
            latest_id = latest_row_info[id_col]
            logger.info(f"테이블 '{table_name}'에서 가장 최근 로우 ({id_col}: {latest_id})를 선택했습니다.")

            # 2. 선택된 로우의 모든 컬럼 데이터를 가져옵니다.
            select_row_query = f"SELECT * FROM {table_name} WHERE {id_col} = %s;"
            cursor.execute(select_row_query, (latest_id,))
            row_data_list = cursor.fetchall()

            if not row_data_list:
                logger.warning(f"테이블 '{table_name}'에서 {id_col} {latest_id}에 해당하는 로우를 찾을 수 없습니다. 건너뜁니다.")
                return

            row_data = row_data_list[0]

            updates_for_main_table = {}
            log_entries_to_insert = []
            
            # 3. 각 컬럼을 순회하며 영문 텍스트를 찾아 번역합니다.
            for col_name, col_value in row_data.items():
                if col_name == id_col:
                    continue

                if isinstance(col_value, str) and col_value.strip():
                    if not is_korean(col_value):
                        logger.info(f"테이블 '{table_name}', {id_col} {latest_id}, 컬럼 '{col_name}': '{col_value[:50]}...' 번역 시작")
                        translated_text = translate(col_value, from_lang="en", to_lang="ko")

                        if "Error: 번역에 실패했습니다." in translated_text:
                            logger.error(f"테이블 '{table_name}', {id_col} {latest_id}, 컬럼 '{col_name}': 번역 실패로 이 컬럼에 대한 업데이트 및 로그 기록을 건너뜁니다.")
                        else:
                            updates_for_main_table[col_name] = translated_text
                            log_entries_to_insert.append((table_name, str(latest_id), col_name, col_value, translated_text))
                            logger.info(f"테이블 '{table_name}', {id_col} {latest_id}, 컬럼 '{col_name}': 번역 완료. 업데이트 및 로그 대기 중.")
                    else:
                        logger.debug(f"테이블 '{table_name}', {id_col} {latest_id}, 컬럼 '{col_name}': 이미 한국어이거나 번역 불필요. 건너뜁니다.")
                else:
                    logger.debug(f"테이블 '{table_name}', {id_col} {latest_id}, 컬럼 '{col_name}': 문자열이 아니거나 비어있습니다. 건너뜁니다.")

            # 4. 번역된 컬럼이 있다면 원본 테이블을 업데이트하고 로그 테이블에 기록합니다.
            if updates_for_main_table:
                try:
                    # 원본 테이블 업데이트
                    set_clauses = [f"{col} = %s" for col in updates_for_main_table.keys()]
                    set_values = list(updates_for_main_table.values())
                    
                    update_main_query = f"UPDATE {table_name} SET {', '.join(set_clauses)} WHERE {id_col} = %s;"
                    update_main_values = set_values + [latest_id]

                    cursor.execute(update_main_query, tuple(update_main_values))
                    logger.info(f"테이블 '{table_name}', {id_col} {latest_id}: 원본 테이블 업데이트 준비 완료.")

                    # 로그 테이블에 번역 내역 삽입
                    if log_entries_to_insert:
                        insert_log_query = """
                            INSERT INTO TB_OCR_TRN_LOG (TRN_TABLE_NAME, TRN_TABLE_PK, TRN_COL_ID, ORI_TEXT, TRN_TEXT)
                            VALUES (%s, %s, %s, %s, %s)
                        """
                        cursor.executemany(insert_log_query, log_entries_to_insert)
                        logger.info(f"테이블 '{table_name}', {id_col} {latest_id}: 로그 테이블 삽입 준비 완료 ({len(log_entries_to_insert)}개 항목).")
                    
                    conn.commit() # 모든 변경 사항을 한 번에 커밋
                    logger.info(f"테이블 '{table_name}', {id_col} {latest_id}: 원본 테이블 업데이트 및 로그 기록 모두 완료.")

                except Error as e:
                    logger.error(f"테이블 '{table_name}', {id_col} {latest_id}: 업데이트 또는 로그 기록 실패: {e}")
                    conn.rollback() # 오류 발생 시 모든 변경 사항 롤백
                except Exception as e:
                    logger.error(f"테이블 '{table_name}', {id_col} {latest_id}: 예상치 못한 오류 발생 중 업데이트 또는 로그 기록 실패: {e}")
                    conn.rollback() # 오류 발생 시 모든 변경 사항 롤백
            else:
                logger.info(f"테이블 '{table_name}', {id_col} {latest_id}: 번역할 영문 컬럼이 없거나 모든 번역이 실패했습니다. 업데이트/로그할 내용이 없습니다.")

        except Error as e:
            logger.error(f"테이블 '{table_name}' 처리 중 데이터베이스 오류 발생: {e}")
            if conn:
                conn.rollback()
        except Exception as e:
            logger.error(f"테이블 '{table_name}' 처리 중 예상치 못한 오류 발생: {e}")
            if conn:
                conn.rollback()

    except Error as e:
        logger.error(f"전체 데이터베이스 작업 중 오류 발생: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.autocommit = True # 작업 완료 후 autocommit을 다시 True로 설정 (선택 사항)
        close_db_connection(conn, cursor)

if __name__ == "__main__":
    table_configs = {
        "TB_OCR_BILD_BASIC_INFO": {"id_col": "BILD_SEQ_NUM"},
        "TB_OCR_FLR_STATUS": {"id_col": "BILD_SEQ_NUM"},
        "TB_OCR_OWN_STATUS": {"id_col": "BILD_SEQ_NUM"},
    }

    for table_name, config in table_configs.items():
        translate_update_and_log_db(
            table_name=table_name,
            id_col=config["id_col"]
        )




#---------------------------------DB 적재 코드-----------------------------------

import re
import mysql.connector
from typing import Dict, List, Any
import json # 예시 출력을 위한 임시 모듈
import os # 예시 출력을 위한 임시 모듈



# DB 연결 정보
DB_CONFIG = {
    "host": "192.168.10.18",
    "database": "dococr",
    "user": "digitalflow",
    "password": "digital10",
    "port": "3306" 
}


def clean_and_convert_numeric(value_str: Any) -> float | None:
    """
    문자열에서 숫자만 추출하여 float이나 실수로 변환할 수 있도록 함.(스키마 상의 decimal 인식 위함)
    '㎡', '%', 'm' 등의 단위 문자를 제거하고, 숫자가 없으면 None을 반환.
    """
    if value_str is None:
        return None
    
    s = str(value_str).strip()
    if not s: # 빈 문자열인 경우 None 반환
        return None

    # 숫자, 소수점, 마이너스 부호만 남기고 모두 제거
    cleaned_str = re.sub(r'[^\d.-]', '', s)
    
    # 숫자로 시작하지 않거나, 소수점이 두 개 이상이거나, 마이너스 부호가 중간에 있는 경우 등 유효하지 않은 숫자 형식 처리
    if not cleaned_str or cleaned_str.count('.') > 1 or (cleaned_str.count('-') > 1) or (cleaned_str.startswith('.') and len(cleaned_str) == 1):
        return None
    
    try:
        return float(cleaned_str)
    except ValueError:
        return None

# 차장님 제공 실데이타
def create_sample_ocr_data() -> Dict[str, List[Dict[str, Any]]]:
    """
    제공된 이미지의 OCR 결과와 유사한 샘플 데이터를 생성하여 반환합니다.
    TB_OCR_BILD_BASIC_INFO 테이블 스키마에 맞춰 컬럼명을 매핑하고 데이터 타입을 변환합니다.
    """


    sample_data={
        "TB_OCR_BILD_BASIC_INFO": [
            {
                "BILD_ID": "2120101430000183",
                "UNIQUE_NUM": "2671025622107480002",
                "BILD_NAME": "",
                "UNIT_HOUSEHOLD_POP": "0호0가구0세대",
                "LAND_LOC_ADDR": "부산광역시기장군정관읍매학리",
                "LAND_LOT_NUM": "7482",
                "ROAD_NM_ADDR": "부산광역시기장군정관읍정관중앙로56",
                "LAND_AREA_SQM": "1417m", # 단위가 'm'로 되어 있어, 문자열로 변환됨. 
                "TOT_FLR_AREA_SQM": "3.053.27ㅠ", # 천단위로 쉼표가 포함되어 있고, 단위가 'ㅠ'로 되어 있어, 문자열로 변환됨.
                "ADMIN_DIST_NM": "일반상업지역",
                "SECT_TYPE": "택지개발예정지구외2",
                "ZONE_TYPE": "제1종지구단위계획구역",
                "BILD_AREA_SQM":"725.88m", # 단위가 'm'로 되어 있어, 문자열로 변환됨. 
                "FAR_CALC_AREA_SQM": "3013.27m",  # 단위가 'm'로 되어 있어, 문자열로 변환됨. 
                "MAIN_STRUCT_TYPE": "철근콘크리트구조",
                "MAIN_PURPOSE_TYPE": "의료시설외2",
                "TOT_FLR_CNT": "지하:1층지상:5층",
                "BILD_COVERAGE_RATIO": "51.23",
                "FLR_AREA_RATIO": "212.65",
                "BILD_HEIGHT_METER": "21.350",
                "ROOF_STRUCT_TYPE": "(철근)콘크리트",
                "ANNEX_BILD_TYPE": "동m",
                "LNDSCP_AREA_SQM": "a", # a -> m로 추정,  0으로 변경필요.
                "PUB_OPEN_AREA_SQM":"가이", # 가이 -> m로 추정,  0으로 변경필요.
                "SETBACK_AREA_SQM": "at",# at -> m로 추정,  0으로 변경필요.
                "SETBACK_DIST_METER": "m" # 0으로 변경필요.
            }
        ],
        "TB_OCR_FLR_STATUS": [
            {
                "FLR_CLASS_TYPE": "주1",
                "FLR_NUM_TEXT": "지1층",
                "FLR_STRUCT_TYPE": "철근콘크리트구조",
                "FLR_PURP_TYPE": "펌프실",
                "FLR_AREA_SQM": "40"
            },
            {
                "FLR_CLASS_TYPE": "주1",
                "FLR_NUM_TEXT": "1층",
                "FLR_STRUCT_TYPE": "철근콘크콘크리트구조",
                "FLR_PURP_TYPE": "제2종근린생활시설(금융업소:은행)",
                "FLR_AREA_SQM": "169.69"
            },
            {
                "FLR_CLASS_TYPE": "주1",
                "FLR_NUM_TEXT": "1층",
                "FLR_STRUCT_TYPE": "철근콘크리트구조",
                "FLR_PURP_TYPE": "공용계단등",
                "FLR_AREA_SQM": "1121.69"
            },
            {
                "FLR_CLASS_TYPE": "주1",
                "FLR_NUM_TEXT": "1층",
                "FLR_STRUCT_TYPE": "철근콘크리트구조",
                "FLR_PURP_TYPE": "제1종근린생활시설(소매점:약국)",
                "FLR_AREA_SQM": "107.36"
            }
        ],
        "TB_OCR_OWN_STATUS": [
            {
                "OWNR_FULL_NM": "백상호",
                "OWNR_ADDR_FULL": "THSSAHeat센덤종로25103동2802호(우동대우월드마SAE)",
                "OWNR_SHARE_RATIO": "11",
                "OWNR_CHG_DATE": "2016.7.8.",
                "OWNR_ID_REG_NUM": "6410051",
                "ADDR_CHG_CATEGORY": "소유권이전"
            },
            {
                "OWNR_FULL_NM": "",
                "OWNR_ADDR_FULL": "이하여백",
                "OWNR_SHARE_RATIO": "",
                "OWNR_CHG_DATE": "", # 값이 없을때는 날짜 기본값으로 처리?
                "OWNR_ID_REG_NUM": "",
                "ADDR_CHG_CATEGORY": ""
            }
        ]
    }
    

    return sample_data



# DB 적재
def transfer_data_to_db(data_map: Dict[str, List[Dict[str, Any]]]):
    """
    주어진 맵 형태의 데이터를 데이터베이스로 이관합니다.
    각 맵의 키(key)는 테이블 이름으로 간주하고, 값(value)은 해당 테이블의 레코드 리스트로 간주합니다.
    AUTO_INCREMENT 시퀀스 컬럼은 INSERT 문에서 제외됩니다.
    """
    conn = None
    try:
        # DB_CONFIG에서 port 값을 int로 변환
        db_config_int_port = DB_CONFIG.copy()
        db_config_int_port['port'] = int(db_config_int_port['port'])

        conn = mysql.connector.connect(**db_config_int_port)
        cur = conn.cursor()
        

        # TB_OCR_BILD_BASIC_INFO에 먼저 데이터 삽입 및 BILD_SEQ_NUM 얻기
        bild_basic_info_records = data_map.get("TB_OCR_BILD_BASIC_INFO", [])
        if not bild_basic_info_records:
            print("경고: 'TB_OCR_BILD_BASIC_INFO' 테이블에 삽입할 데이터가 없습니다. 건너뜁니다.")
            return # 필수 데이터가 없으므로 종료

        # BILD_SEQ_NUM을 제외한 컬럼 목록 준비
        bild_columns_to_insert = list(bild_basic_info_records[0].keys())
        if "BILD_SEQ_NUM" in bild_columns_to_insert:
            bild_columns_to_insert.remove("BILD_SEQ_NUM")

        bild_placeholders = ', '.join(['%s'] * len(bild_columns_to_insert))
        bild_column_names_quoted = ', '.join([f"`{col}`" for col in bild_columns_to_insert])
        bild_insert_sql = f"INSERT INTO `TB_OCR_BILD_BASIC_INFO` ({bild_column_names_quoted}) VALUES ({bild_placeholders})"

        inserted_bild_seq_num = None
        try:
            bild_values = [bild_basic_info_records[0].get(col, None) for col in bild_columns_to_insert]
            cur.execute(bild_insert_sql, bild_values)
            inserted_bild_seq_num = cur.lastrowid # 삽입된 AUTO_INCREMENT 값 가져오기
            conn.commit()
            print(f"TB_OCR_BILD_BASIC_INFO에 데이터 삽입 완료. BILD_SEQ_NUM: {inserted_bild_seq_num}")
        except mysql.connector.Error as e:
            print(f"TB_OCR_BILD_BASIC_INFO에 데이터 삽입 중 오류 발생: {e} - 데이터: {bild_basic_info_records[0]}")
            conn.rollback()
            return # 핵심 데이터 삽입 실패 시 종료

        # 다른 테이블 데이터 삽입 루프
        # TB_OCR_BILD_BASIC_INFO는 이미 처리했으므로 건너뛰거나, data_map에서 제거하고 시작
        tables_to_process = ["TB_OCR_FLR_STATUS", "TB_OCR_OWN_STATUS"]

        for table_name in tables_to_process:
            records = data_map.get(table_name, [])
            if not records:
                print(f"경고: '{table_name}' 테이블에 삽입할 데이터가 없습니다. 건너뜁니다.")
                continue

            # 이 테이블들의 스키마에 BILD_SEQ_NUM이 있다고 가정
            # AUTO_INCREMENT 컬럼 이름 설정 (FLR_SEQ_NUM 또는 OWNR_SEQ_NUM)
            auto_increment_col = None
            if table_name == "TB_OCR_FLR_STATUS":
                auto_increment_col = "FLR_SEQ_NUM"
            elif table_name == "TB_OCR_OWN_STATUS":
                auto_increment_col = "OWNR_SEQ_NUM"

            # 컬럼 목록 준비 (AUTO_INCREMENT 컬럼 제외)
            columns_to_insert = list(records[0].keys())
            if auto_increment_col and auto_increment_col in columns_to_insert:
                columns_to_insert.remove(auto_increment_col)
            
            # BILD_SEQ_NUM 컬럼 추가
            # BILD_SEQ_NUM이 이미 records[0].keys()에 포함되어 있지 않다면 추가해야 합니다.
            # (만약 샘플 데이터에 BILD_SEQ_NUM을 포함시키지 않았다면)
            if "BILD_SEQ_NUM" not in columns_to_insert:
                columns_to_insert.insert(0, "BILD_SEQ_NUM") # 첫 번째 컬럼으로 추가 (순서가 중요하진 않음)


            # INSERT SQL 문 생성
            placeholders = ', '.join(['%s'] * len(columns_to_insert))
            column_names_quoted = ', '.join([f"`{col}`" for col in columns_to_insert])
            insert_sql = f"INSERT INTO `{table_name}` ({column_names_quoted}) VALUES ({placeholders})"

            for record in records:
                try:
                    # BILD_SEQ_NUM 값을 레코드에 추가 (또는 덮어쓰기)
                    record["BILD_SEQ_NUM"] = inserted_bild_seq_num
                    
                    # record 딕셔너리에서 columns_to_insert 순서에 맞춰 값 추출
                    values = [record.get(col, None) for col in columns_to_insert]
                    cur.execute(insert_sql, values)
                except mysql.connector.Error as e:
                    print(f"'{table_name}'에 데이터 삽입 중 오류 발생: {e} - 데이터: {record}")
                    # conn.rollback() # 필요에 따라 오류 레코드만 건너뛰고 다음 레코드 처리

        conn.commit() # 모든 삽입 작업 완료 후 최종 커밋
        print("모든 데이터 이관 작업 완료.")

    except mysql.connector.Error as e:
        print(f"데이터베이스 연결 또는 작업 중 오류 발생: {e}")
    finally:
        if conn and conn.is_connected():
            cur.close()
            conn.close()
            print("데이터베이스 연결 종료.")

# --- 메인 실행 부분 ---
if __name__ == "__main__":
    # 1. 샘플 데이터 생성
    ocr_result_data = create_sample_ocr_data()
    
    # 2. 생성된 샘플 데이터 출력 (확인용)
    print("--- 생성된 샘플 데이터 ---")
    print(json.dumps(ocr_result_data, indent=2, ensure_ascii=False))
    print("-------------------------\n")

    # 3. 데이터베이스로 이관
    print("--- 데이터베이스 이관 시작 ---")
    transfer_data_to_db(ocr_result_data)
    print("--- 데이터베이스 이관 종료 ---")


#---------------------------------와일드 카드, 특수문자 제거, 공백 제거 코드----------------------------
def _extract_word_details_from_tesseract_data(data: dict) -> list:
    """
    pytesseract image_to_data 결과(dict)에서 단어 리스트 추출
    """
    word_details = []
    num_items = len(data['level'])
    for i in range(num_items):
        # 기호제거
        # clean_text = re.sub(r'[_\,\|\,\=]', '', data['text'][i])
        clean_text = re.sub(r'[_,|=~^*#@%$&;:?!\[\]{}<>/’※.\-()]', '', data['text'][i])
        if int(data['conf'][i]) > -1 and data['text'][i].strip():
            word_details.append({
                'level': data['level'][i],
                'page_num': data['page_num'][i],
                'block_num': data['block_num'][i],
                'par_num': data['par_num'][i],
                'line_num': data['line_num'][i],
                'word_num': data['word_num'][i],
                'left': data['left'][i],
                'top': data['top'][i],
                'width': data['width'][i],
                'height': data['height'][i],
                'conf': data['conf'][i],
                'text': clean_text,
            })
    return word_details

#-------------------------------------행렬 정보 추출 후 json 삽입-------------------------------------
import glob
import os
import json
import numpy as np
from collections import defaultdict
from utils.com import json_util
import csv
import pymysql

def get_all_y_gaps_from_json_files(json_file_list: list):
    """
    모든 json 파일에서 block_box의 y값을 추출하여,
    정렬 후 y값의 차이(행간 차이,gap) 리스트를 반환.
    """
    y_list = []
    for json_path in json_file_list:
        try:
            data = json_util.load(json_path)
            if not isinstance(data, dict):
                try:
                    data = json.loads(data)
                except Exception:
                    data = None
            if isinstance(data, dict):
                block_box = data.get('block_box', None)
                if block_box and len(block_box) >= 1:
                    y = block_box[1]
                    y_list.append(y)
        except Exception as e:
            print(f"Error reading {json_path}: {e}")
    if len(y_list) < 2:
        print("x값이 2개 미만입니다.")
        return [], []
    y_list.sort()
    gaps = [y_list[i+1] - y_list[i] for i in range(len(y_list)-1)]
    return y_list, gaps


def cluster_by_gap_analysis(y_list, method: str = "mean+std"):
    """
    Gap 분석을 통해 클러스터링. 예를들어,평균+표준편차로 계산한다고 할때 y축 차이(gaps)의 차이를 오름차순으로 비교하여 
    평균+표준편차보다 작으면 같은 클러스터이고, 크면 다른 클러스터임.

    method: 'mean+std', 'median+std', 'percentile75' 등 선택 가능
    
    """
    if len(y_list) < 2:
        return [y_list] if y_list else []

    y_list = sorted(y_list)
    gaps = [y_list[i+1] - y_list[i] for i in range(len(y_list)-1)]

    # 임계값 자동 계산
    if method == "mean+std":
        gap_threshold = np.mean(gaps) + np.std(gaps)
    elif method == "median+std":
        gap_threshold = np.median(gaps) + np.std(gaps)
    elif method == "percentile75":
        gap_threshold = np.percentile(gaps, 75)
    else:
        gap_threshold = np.mean(gaps) + np.std(gaps)  # 기본값

    clusters = []
    current_cluster = [y_list[0]]

    for i in range(1, len(y_list)):
        gap = y_list[i] - y_list[i-1]
        if gap <= gap_threshold:
            current_cluster.append(y_list[i])
        else:
            clusters.append(current_cluster)
            current_cluster = [y_list[i]]

    clusters.append(current_cluster)
    return clusters

def extract_yx_pairs(json_file_list):
    """
    (y,x) 추출 및 yx_pairs 생성. 
    """
    yx_pairs = []
    for json_path in json_file_list:
        try:
            data = json_util.load(json_path)
            if not isinstance(data, dict):
                try:
                    data = json.loads(data)
                except Exception:
                    data = None
            if isinstance(data, dict):
                block_box = data.get('block_box', None)
                if block_box and len(block_box) >= 2:
                    x = block_box[0]
                    y = block_box[1]
                    yx_pairs.append((y, x, json_path))
        except Exception as e:
            print(f"Error reading {json_path}: {e}")
    return yx_pairs


def create_2d_matrix_from_clusters(json_file_list, clusters, yx_pairs=None):
    """
    행 클러스터 정보를 기반으로 2차원 리스트를 생성.
    각 행은 클러스터를 나타내고, 각 열은 x축 정보를 나타냄.
    Args:
        json_file_list (list): json 파일 경로 리스트
        clusters (list): y값 클러스터 리스트 (각 클러스터는 y값들의 리스트)
        yx_pairs (list): (y, x, json_path) 리스트 (optional)
    Returns:
        matrix (list): 2차원 리스트 [[x1, x2, ...], [x1, x2, ...], ...]
        cluster_info (list): 각 행의 y값 정보
    """
    if yx_pairs is None:
        yx_pairs = extract_yx_pairs(json_file_list)
    # 2차원 리스트 초기화
    matrix = []
    cluster_info = []
    # 각 클러스터(행)에 대해 처리
    for row_idx, cluster in enumerate(clusters):
        row_x_values = []
        row_y_values = []
        # 해당 클러스터의 y값 범위 내에 있는 모든 (y, x) 쌍 찾기
        cluster_min_y = min(cluster)
        cluster_max_y = max(cluster)
        # 해당 클러스터에 속하는 모든 x값 찾기
        cluster_x_values = []
        for y, x, json_path in yx_pairs:
            if cluster_min_y <= y <= cluster_max_y:
                cluster_x_values.append(x)
        # x값을 오름차순으로 정렬
        cluster_x_values.sort()
        matrix.append(cluster_x_values)
        cluster_info.append({
            'row': row_idx + 1,
            'y_range': [cluster_min_y, cluster_max_y],
            'x_count': len(cluster_x_values),
            'x_values': cluster_x_values
        })
    return matrix, cluster_info

def create_coordinate_mapping(matrix, cluster_info, yx_pairs):
    """
    2차원 리스트를 기반으로 좌표 매핑을 생성.
    Args:
        matrix (list): 2차원 리스트
        cluster_info (list): 각 행의 y값 정보
        yx_pairs (list): (y, x, json_path) 리스트
    Returns:
        coordinates (list): [행, 열, x, y] 형태의 좌표 리스트
    """
    coordinates = []
    # yx_pairs를 (x, y) 쌍으로 빠르게 찾을 수 있도록 set/dict 생성
    xy_set = set((x, y) for y, x, _ in yx_pairs)
    for row_idx, row in enumerate(matrix):
        # 해당 행의 y값 범위
        y_range = cluster_info[row_idx]['y_range']
        for col_idx, x_value in enumerate(row):
            # 이 x값에 대해, 해당 y_range 내에 있는 y값을 찾음
            # 여러 y가 있을 수 있으니 모두 좌표로 추가
            for y, x, _ in yx_pairs:
                if x == x_value and y_range[0] <= y <= y_range[1]:
                    coordinates.append([row_idx + 1, col_idx + 1, x_value, y])
    return coordinates

def cleansing():
    # 임시 데이터가 저장된 기본 경로
    base_path = '/opt/airflow/data/temp'
    # 처리할 폴더 목록
    folder_names = ['building_user_status', 'building_info', 'building_detail','doc_subject']
    # 각 폴더별로 반복
    for folder_name in folder_names:
        folder_path = os.path.join(base_path, folder_name)
        print(f"\n=== {folder_name} 폴더 처리 시작 ===")
        # 폴더 내 모든 json 파일 리스트 생성
        json_file_list = glob.glob(os.path.join(folder_path, '*.json'))
        # 파일 리스트에서 모든 y 좌표와 각 블록 간 y 간격 추출
        y_list, y_gaps = get_all_y_gaps_from_json_files(json_file_list)
        if not y_list:
            # 만약 y 좌표 리스트가 비어 있으면 해당 폴더 처리 건너뜀
            print("y_list가 비어있어 종료합니다.")
            continue
        # (y, x, 파일경로)로 pairing된 리스트 추출
        yx_pairs = extract_yx_pairs(json_file_list)
        # y값의 평균+표준편차 기준으로 그룹핑(클러스터링) 
        clusters_by_gap = cluster_by_gap_analysis(y_list, "mean+std")
        # 각 클러스터에 따라 2차원 매트릭스 및 클러스터 정보 구성
        matrix, cluster_info = create_2d_matrix_from_clusters(json_file_list, clusters_by_gap, yx_pairs)
        # 매트릭스, 클러스터 정보를 이용해 (y, x) → (row, col)로 매핑
        coordinates = create_coordinate_mapping(matrix, cluster_info, yx_pairs)
        # (y, x) → (row, col) 변환 결과 출력
        print("\n=== (y, x) → (row, col) 매핑 ===")
        xy_to_row_col = {(x, y): [row, col] for row, col, x, y in coordinates}
        for y, x, json_path in yx_pairs:
            row_col = xy_to_row_col.get((x, y), None)
            if row_col:
                print(f"(y={y}, x={x}) → (row={row_col[0]}, col={row_col[1]})")
            else:
                print(f"(y={y}, x={x}) → 매핑 없음")
        # 각 json 파일별로 row, col 정보를 추가 저장하고 결과 출력
        print("\n=== 파일별 row_col_info 추가 결과 ===")
        for y, x, json_path in yx_pairs:
            try:
                data = json_util.load(json_path)
                block_box = data.get('block_box', None) if isinstance(data, dict) else None
                row_col = xy_to_row_col.get((x, y), None)
                # block_box가 있고 row/col 매핑이 있으면 해당 정보 추가 후 저장
                if block_box and len(block_box) >= 2 and row_col and isinstance(data, dict):
                    data['row'] = row_col[0]
                    data['col'] = row_col[1]
                    json_util.save(json_path, data)
                    print(f"파일명: {os.path.basename(json_path)} → row: {row_col[0]}, col: {row_col[1]}")
                elif not row_col:
                    print(f"파일명: {os.path.basename(json_path)} → row, col 매핑 없음")
            except Exception as e:
                # 파일 저장/처리 중 오류 발생시 에러 메시지 출력
                print(f"Error updating {json_path}: {e}")
        print(f"=== {folder_name} 폴더 처리 완료 ===")

if __name__ == '__main__':
    cleansing()





#-------------------------------------file_util의 get_config 정보 테이블 이관-------------------------------------
import pymysql
import json

from utils.com.file_util import get_config

# DB 연결 설정 (환경에 맞게 수정)
conn = pymysql.connect(
    host='192.168.10.18',
    user='digitalflow',
    password='digital10',
    db='dococr',
    charset='utf8mb4',
    autocommit=True
)
cursor = conn.cursor()

# 1. a_class 정보 추출
def insert_doc():
    doc_name = "general_building_register"
    layout_name = "general_building_register"
    config = get_config(doc_name, layout_name)
    
    for idx, area in enumerate(config['layout_list']):
        section_name
    # config가 None인 경우 예외 처리
    if config is None:
        raise ValueError(f"설정을 가져올 수 없습니다. {layout_name} 설정을 확인해주세요.")
    
    
    # TB_DI_DOC_CLASS
    # sql = """
    # INSERT INTO TB_DI_DOC_CLASS (DOC_NAME)
    # VALUES (%s)
    # """
    # cursor.execute(sql, (doc_name,))
    # print('1')
    # doc_class_id = cursor.lastrowid
    doc_class_id = 4
    
    
    # TB_DI_LAYOUT_CLASS (img_preprocess, classify_ai)
    img_preprocess = json.dumps(config['classify']['img_preprocess'], ensure_ascii=False)
    classify_ai = json.dumps(config['classify']['classify_ai'], ensure_ascii=False)
    print('2')
    sql = """
    INSERT INTO TB_DI_LAYOUT_CLASS (DOC_CLASS_ID, LAYOUT_NAME, IMG_PREPROCESS_INFO, CLASSIFY_AI_INFO)
    VALUES (%s, %s, %s, %s)
    """
    cursor.execute(sql, (doc_class_id, layout_name, img_preprocess, classify_ai))
    layout_class_id = cursor.lastrowid
    print('3')

    # TB_DI_SECTION_CLASS (area_name, separate_area, separate_block, ocr)
    for idx, area in enumerate(config['ocr']['area_list']):
        # section_id = idx + 1  # 실제 환경에 맞게 지정
        section_name = area['area_name']
        separate_area = json.dumps(area['separate_area'], ensure_ascii=False)
        separate_block = json.dumps(area['separate_block'], ensure_ascii=False)
        ocr = json.dumps(area['ocr'], ensure_ascii=False)

        sql = """
        INSERT INTO TB_DI_SECTION_CLASS (LAYOUT_CLASS_ID,
        SECTION_NAME,SEPARATE_AREA_INFO, 
        SEPARATE_BLOCK_INFO, OCR_INFO
        )
        VALUES (%s,%s, %s, %s, %s)
        """
        cursor.execute(sql, (layout_class_id ,section_name, separate_area, separate_block, ocr))

if __name__ == "__main__":
    try:
        insert_layout()
        print("데이터 이관이 완료되었습니다.")
    except Exception as e:
        print(f"오류 발생: {e}")
    finally:
        cursor.close()
        conn.close()




#-------------------------------------행렬정보 및 ocr 데이터 수정 후, 검증 테이블로 이관-------------------------------------
import os
import json
import pymysql

# DB 연결 설정 (환경에 맞게 수정)
conn = pymysql.connect(
    host='192.168.10.18',
    user='digitalflow',
    password='digital10',
    db='dococr',
    charset='utf8mb4',
    autocommit=True
)
cursor = conn.cursor()

def get_section_class_id_map():
    sql = "SELECT SECTION_NAME, SECTION_CLASS_ID FROM TB_DI_SECTION_CLASS"
    cursor.execute(sql)
    return {row[0]: row[1] for row in cursor.fetchall()}

def migrate_blocks_from_json():
    base_dirs = [
        '/opt/airflow/data/temp/building_detail',
        '/opt/airflow/data/temp/building_info',
        '/opt/airflow/data/temp/building_user_status'
    ]
    section_class_id_map = get_section_class_id_map()

    for base_dir in base_dirs:
        section_name = os.path.basename(base_dir)
        section_class_id = section_class_id_map.get(section_name)
        if section_class_id is None:
            print(f"섹션 정보 없음: {section_name}")
            continue
        for filename in os.listdir(base_dir):
            if filename.endswith('.json'):
                file_path = os.path.join(base_dir, filename)
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                except Exception as e:
                    print(f"파일 읽기 오류: {file_path}, {e}")
                    continue
                # block_id = data.get('block_id')
                row = data.get('row')
                column = data.get('col')
                # full_text는 중첩 구조에서 추출
                full_text = None
                if (
                    isinstance(data.get('ocr'), dict) and
                    isinstance(data['ocr'].get('tesseract'), dict)
                ):
                    full_text = data['ocr']['tesseract'].get('full_text')

                if row is None or column is None or full_text is None:
                    print(f"필수 데이터 누락: {file_path}")
                    continue

                # BLOCK_TYPE 결정
                if section_name in ['building_detail', 'building_info', 'building_user_status']:
                    block_type = 'table'
                else:
                    block_type = 'header'

                # TB_DI_BLOCK_CLASS에 insert
                sql_block = """
                INSERT INTO TB_DI_BLOCK_CLASS (
                 SECTION_CLASS_ID, BLOCK_ROW_NUM, BLOCK_COL_NUM, BLOCK_TYPE, DEFAULT_TEXT
                ) VALUES ( %s, %s, %s, %s, %s)
                """
                cursor.execute(sql_block, (section_class_id, row, column, block_type, full_text))
                print(f"이관 완료: {file_path}")

if __name__ == "__main__":
    try:
        migrate_blocks_from_json()
        print("블록 데이터 이관이 완료되었습니다.")
    except Exception as e:
        print(f"오류 발생: {e}")
    finally:
        cursor.close()
        conn.close() 




#--------------------------------------공통 교정 사항 수정 및 테이블 이관------------------------------------


import glob
import os
import json
import pymysql
import csv
from utils.com import json_util

def load_correction_dict(csv_path):
    correction_dict = {}
    with open(csv_path, 'r', encoding='cp949') as f:
        reader = csv.DictReader(f)
        for row in reader:
            ocr = row['OCR_TEXT'].strip().replace('"', '')
            fix = row['FIX_TEXT'].strip().replace('"', '')
            if ocr and fix:
                correction_dict[ocr] = fix
    return correction_dict

def fix_ocr_text(ocr_text, correction_dict):
    fixed_text = ocr_text
    for wrong, right in correction_dict.items():
        if wrong in fixed_text:
            fixed_text = fixed_text.replace(wrong, right)
    return fixed_text

def upload_common_crctn_from_json():
    # DB 연결 설정
    conn = pymysql.connect(
        host='192.168.10.18',
        user='digitalflow',
        password='digital10',
        db='dococr',
        charset='utf8mb4',
        autocommit=True
    )
    cursor = conn.cursor()
    base_path = '/opt/airflow/data/temp'
    folder_names = ['building_user_status', 'building_info', 'building_detail', 'doc_subject']
    correction_dict = load_correction_dict('/opt/airflow/data/fixing_dict/TB_FIX_OCR_COM.csv')

    for folder_name in folder_names:
        folder_path = os.path.join(base_path, folder_name)
        json_file_list = glob.glob(os.path.join(folder_path, '*.json'))
        for json_path in json_file_list:
            try:
                data = json_util.load(json_path)
                if not isinstance(data, dict):
                    try:
                        data = json.loads(data)
                    except Exception:
                        data = None
                if isinstance(data, dict):
                    ocr = data.get('ocr', {})
                    if isinstance(ocr, dict):
                        tesseract = ocr.get('tesseract', {})
                        if isinstance(tesseract, dict):
                            full_text = tesseract.get('full_text', '')
                            for wrong, right in correction_dict.items():
                                count = full_text.count(wrong)
                                if count > 0:
                                    fixed_text = fix_ocr_text(full_text, {wrong: right})
                                    # DB에 insert
                                    sql = """
                                    INSERT INTO TB_DI_COMMON_CRCTN (ERROR_TEXT, CRRCT_TEXT, CRCTN_CNT)
                                    VALUES (%s, %s, %s)
                                    """
                                    cursor.execute(sql, (full_text, fixed_text, count))
                                    print(f"DB 업로드 완료: {json_path} | ERROR_TEXT: {full_text} | CRRCT_TEXT: {fixed_text} | CRCTN_CNT: {count}")
            except Exception as e:
                print(f"Error exporting {json_path}: {e}")
    cursor.close()
    conn.close()

if __name__ == '__main__':
    upload_common_crctn_from_json() 


#========================sjh========================






# --- 사전 설정 ---
# Tesseract-OCR 설치 경로를 지정해야 할 수 있습니다 (Windows 사용자).
# 시스템 PATH에 Tesseract가 추가되어 있다면 이 줄은 주석 처리하거나 삭제해도 됩니다.
pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'

def split_image_by_vertical_gaps(image_path: str, output_dir: str = "output_vertical_split", min_gap_width: int = 20):
    """
    이미지 내 텍스트 사이의 수직 공백을 감지하여 이미지를 분할합니다.

    Args:
        image_path (str): 원본 이미지 파일 경로.
        output_dir (str): 잘라낸 이미지를 저장할 디렉토리 이름.
        min_gap_width (int): 분할 기준으로 삼을 최소 공백의 너비 (픽셀).

    Returns:
        list[str]: 저장된 개별 이미지 파일의 경로 리스트.
    """
    # 1. 이미지 로드 및 기본 검사
    if not os.path.exists(image_path):
        print(f"오류: 원본 이미지 파일을 찾을 수 없습니다. 경로: {image_path}")
        return []

    img = cv2.imread(image_path)
    if img is None:
        print(f"오류: 이미지를 로드할 수 없습니다. 파일이 손상되었거나 지원하지 않는 형식일 수 있습니다.")
        return []

    # 2. 이미지 전처리 (텍스트를 흰색, 배경을 검은색으로)
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    # Otsu의 이진화는 임계값을 자동으로 계산해줍니다.
    _, thresh = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)

    # 3. 모폴로지 연산으로 텍스트 블록들을 하나로 연결
    # h = img.shape[0]
    # 모폴로지를 위한 최적 커널 크기 = 5,3 or 3,5
    kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (5,3))
    # kernel = np.ones((10,10), np.uint8)
    closed = cv2.morphologyEx(thresh, cv2.MORPH_CLOSE, kernel, iterations=2)

    # 4. 연결된 텍스트 블록의 윤곽선(Contour) 찾기
    contours, _ = cv2.findContours(closed, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

    # 5. 각 윤곽선의 경계 상자(bounding box)를 구하고 x좌표 기준으로 정렬
    bounding_boxes = [cv2.boundingRect(c) for c in contours]
    # 너무 작은 노이즈성 윤곽선은 제거 (너비와 높이가 10픽셀 이상인 것만)
    bounding_boxes = [b for b in bounding_boxes if b[2] > 10 and b[3] > 10]
    bounding_boxes.sort(key=lambda x: x[0])

    # --- 추가: 텍스트 블록 및 공백 시각화 디버깅 이미지 생성 ---
    # 원본 이미지를 복사하여 그 위에 사각형을 그립니다.
    debug_img = img.copy()
    for x, y, w, h in bounding_boxes:
        cv2.rectangle(debug_img, (x, y), (x + w, y + h), (0, 255, 0), 2) # 초록색

    # 6. 텍스트 블록 사이에서 min_gap_width보다 넓은 모든 수평 공백 찾기
    qualifying_gaps = []
    if len(bounding_boxes) >= 2:
        for i in range(len(bounding_boxes) - 1):
            prev_box = bounding_boxes[i]
            curr_box = bounding_boxes[i+1]
            
            gap_start = prev_box[0] + prev_box[2]
            gap_end = curr_box[0]
            gap_width = gap_end - gap_start
            
            if gap_width >= min_gap_width:
                qualifying_gaps.append({'start': gap_start, 'end': gap_end, 'width': gap_width})

    # 시각화: 분할 기준으로 선택된, 가장 왼쪽의 유효 공백을 빨간색으로 표시
    if qualifying_gaps:
        # bounding_boxes가 x좌표 기준으로 정렬되어 있으므로, qualifying_gaps도 왼쪽부터 순서대로 정렬되어 있음
        first_gap = qualifying_gaps[0]
        cv2.rectangle(debug_img, (first_gap['start'], 0), (first_gap['end'], img.shape[0]), (0, 0, 255), 2)

        # 시각화: 분할 기준을 충족하는 모든 공백을 빨간색으로 표시
        for gap in qualifying_gaps[1:]:
            cv2.rectangle(debug_img, (gap['start'], 0), (gap['end'], img.shape[0]), (0, 0, 255), 2)

    os.makedirs(output_dir, exist_ok=True) # 저장할 디렉토리 생성
    debug_image_path = os.path.join(output_dir, "debug_contour_split.png")
    cv2.imwrite(debug_image_path, debug_img)
    print(f"텍스트 블록 및 분할 영역 시각화 이미지를 '{debug_image_path}'에 저장했습니다.")
    # --- 시각화 코드 끝 ---
    
    # 7. 분할 기준을 충족하는 공백 중 가장 왼쪽에 있는 공백을 기준으로 이미지 분할
    cut_points = [0]  # 시작점 추가
    if qualifying_gaps:
        # bounding_boxes가 x좌표 기준으로 정렬되어 있으므로, qualifying_gaps도 왼쪽부터 순서대로 정렬되어 있음
        # 비슷한 크기의 공백 중 가장 왼쪽에 위치하는 공백을 기준으로 함.
        first_gap = qualifying_gaps[0]
        print(f"가장 왼쪽의 유효 공백(너비: {first_gap['width']}px, 시작점: {first_gap['start']})을 기준으로 분할합니다.")
        cut_point = first_gap['start'] + first_gap['width'] // 2
        cut_points.append(cut_point)
        
        # 모든 공백을 다 검출
        # print(f"{len(qualifying_gaps)}개의 분할 기준(너비>{min_gap_width}px)을 충족하는 공백을 기준으로 분할합니다.")
        # for gap in qualifying_gaps:
        #     cut_point = gap['start'] + gap['width'] // 2
        #     cut_points.append(cut_point)
    
    cut_points.append(img.shape[1]) # 끝점 추가
    # 중복된 분할 지점 제거 및 정렬
    cut_points = sorted(list(set(cut_points)))

    if len(cut_points) <= 2 or len(bounding_boxes) < 2:
        print("분할할 만큼 충분히 넓은 수직 공백을 찾지 못했습니다. 이미지를 통째로 처리합니다.")
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, "full_image.png")
        cv2.imwrite(output_path, img)
        return [output_path]

    # 6. 이미지 자르기 및 저장
    # os.makedirs(output_dir, exist_ok=True) # 시각화 부분에서 이미 생성됨
    cropped_image_paths = []
    print(f"\n'{image_path}'에서 {len(cut_points) - 1}개의 수직 영역으로 분할합니다.")

    for i in range(len(cut_points) - 1):
        x_start = cut_points[i]
        x_end = cut_points[i+1]
        
        # 분할된 영역이 너무 좁으면 무시
        if x_end - x_start < 10:
            continue
        
        cropped_image = img[:, x_start:x_end]

        # 잘라낸 이미지가 비어있지 않은 경우에만 저장
        if cropped_image.size > 0:
            output_path = os.path.join(output_dir, f"v_split_{i}.png")
            cv2.imwrite(output_path, cropped_image)
            cropped_image_paths.append(output_path)
            print(f"  - 영역 {i}: x좌표({x_start} ~ {x_end}) -> '{output_path}' 파일로 저장됨")

    return cropped_image_paths

def intelligently_join_words(word_details: list[dict], space_threshold_ratio: float = 0.3) -> str:
    """
    OCR로 인식된 단어들을 간격에 따라 지능적으로 연결
    - 단어 간 간격이 임계값(space_threshold_ratio)보다 작으면 붙여쓰고, 크면 띄어씀
    - 같은 줄에 있는 단어들끼리만 비교함

    Args:
        word_details (list[dict]): Tesseract의 image_to_data에서 나온 상세 단어 정보 리스트.
        space_threshold_ratio (float): 문자 평균 높이 대비 띄어쓰기 간격 임계값 비율.

    Returns:
        str: 띄어쓰기가 보정된 전체 텍스트.
    """
    if not word_details:
        return ""

    # 단어들을 block_num, par_num, line_num 기준으로 그룹화
    lines = {}
    for word in word_details:
        # 유효한 단어만 처리 (텍스트가 있고, 신뢰도가 0 이상)
        if word['text'].strip() and int(word['conf']) >= 0:
            line_key = (word['block_num'], word['par_num'], word['line_num'])
            if line_key not in lines:
                lines[line_key] = []
            lines[line_key].append(word)

    full_text_parts = []
    # 정렬된 라인 키를 순회
    for line_key in sorted(lines.keys()):
        words_in_line = sorted(lines[line_key], key=lambda w: w['left'])
        if not words_in_line:
            continue

        # 라인별 평균 문자 높이를 기준으로 띄어쓰기 임계값 설정
        avg_height = np.mean([w['height'] for w in words_in_line if w['height'] > 0])
        space_threshold = avg_height * space_threshold_ratio

        line_text = words_in_line[0]['text']
        for i in range(len(words_in_line) - 1):
            gap = words_in_line[i+1]['left'] - (words_in_line[i]['left'] + words_in_line[i]['width'])
            line_text += '' if gap < space_threshold else ' '
            line_text += words_in_line[i+1]['text']
        full_text_parts.append(line_text)

    return '\n'.join(full_text_parts)




# #임시 ocr 알고리즘 - 제외 가능
# def ocr_on_image_list(image_paths: list[str], lang: str = 'kor+eng') -> list[dict]:
#     """
#     이미지 파일 경로 리스트를 받아 각 이미지에 대해 OCR을 수행하고,
#     경로와 단어 단위의 상세 데이터를 함께 반환
#     """
#     ocr_results = []
#     print("\n--- 개별 이미지 상세 OCR 시작 (image_to_data) ---")
#     for path in image_paths:
#         try:
#             # --psm 4: 텍스트를 가변 크기의 단일 열로 가정합니다. 분할된 이미지에 더 적합할 수 있습니다.
#             # --psm 6: 텍스트를 균일한 단일 블록으로 가정합니다.
#             config = '--oem 3 --psm 6'
#             # output_type=pytesseract.Output.DICT로 설정하여 상세 데이터를 딕셔너리 형태로 받습니다.
#             data = pytesseract.image_to_data(path, lang=lang, config=config, output_type=pytesseract.Output.DICT)
#             ocr_results.append({'path': path, 'data': data})
            
#             # 간단한 결과 요약 출력
#             # 신뢰도 -1은 Tesseract가 인식 결과로 간주하지 않는 항목(예: 페이지/블록 정보)이므로 제외합니다.
#             # 텍스트가 있는 단어만 필터링합니다.
#             words = [word for i, word in enumerate(data['text']) if int(data['conf'][i]) != -1 and word.strip() != '']
#             full_text = ' '.join(words)
#             print(f"'{path}' -> {len(words)}개 단어 인식. 전체 텍스트: '{full_text}'")

#         except Exception as e:
#             print(f"'{path}' 처리 중 오류 발생: {e}")
#     return ocr_results



# # 여러 파일을 대상으로 위 함수들(set2 이후 수직영역분리, ocr, 띄어쓰기)적용하기 위한 임시 함수 - 제외 가능
# def process_image_file(source_image_path: str, min_gap_width: int = 30):
#     """
#     단일 이미지 파일에 대해 분할, OCR, JSON 저장을 수행함

#     Args:
#         source_image_path (str): 처리할 이미지 파일의 전체 경로.
#         min_gap_width (int): 분할 기준으로 삼을 최소 공백 너비.
#     """
#     print(f"\n{'='*20}\nProcessing: {os.path.basename(source_image_path)}\n{'='*20}")

#     # 2. 이미지를 텍스트 사이의 수직 공백 기준으로 분할하여 파일로 저장
#     cropped_paths = split_image_by_vertical_gaps(source_image_path, min_gap_width=min_gap_width)

#     if cropped_paths:
#         # 3. 잘라낸 각 이미지에 대해 OCR 수행 (상세 데이터 요청)
#         final_data_with_paths = ocr_on_image_list(cropped_paths)

#         # 4. 모든 OCR 결과를 하나의 텍스트로 통합하고, 상세 데이터도 준비
#         all_text_blocks = []
#         detailed_ocr_results = []
#         for result in final_data_with_paths:
#             path = result['path']
#             data = result['data']
            
#             # 상세 데이터를 보다 다루기 쉬운 '리스트의 딕셔너리' 형태로 변환
#             word_details = []
#             num_items = len(data['level'])
#             for i in range(num_items):
#                 # 신뢰도가 0 이상인 인식된 단어/기호만 포함
#                 if int(data['conf'][i]) > -1:
#                     word_details.append(
#                         {
#                             'level': data['level'][i],
#                             'page_num': data['page_num'][i],
#                             'block_num': data['block_num'][i],
#                             'par_num': data['par_num'][i],
#                             'line_num': data['line_num'][i],
#                             'word_num': data['word_num'][i],
#                             'left': data['left'][i],
#                             'top': data['top'][i],
#                             'width': data['width'][i],
#                             'height': data['height'][i],
#                             'conf': data['conf'][i],
#                             'text': data['text'][i],
#                         }
#                     )
#             detailed_ocr_results.append({
#                 "image_path": os.path.basename(path),
#                 "words": word_details,
#             })

#             # 지능적 띄어쓰기 로직을 적용하여 텍스트 블록 생성
#             block_text = intelligently_join_words(word_details, space_threshold_ratio=0.55)
#             all_text_blocks.append(block_text)

#         # 모든 컬럼의 텍스트를 줄바꿈으로 연결하여 최종 텍스트 생성
#         full_text = '\n'.join(all_text_blocks)
        
#         # 5. 최종 결과를 JSON 파일로 저장
#         # 원본 이미지와 같은 폴더에, 같은 이름으로 .json 확장자만 붙여서 저장
#         output_dir = os.path.dirname(source_image_path)
#         base_name = os.path.basename(source_image_path)
#         file_name_without_ext = os.path.splitext(base_name)[0]
#         json_output_path = os.path.join(output_dir, f"{file_name_without_ext}_ocr.json")
        
#         result_dict = {
#             "source_image": base_name,
#             "full_text": full_text,
#             "details": detailed_ocr_results,
#         }
        
#         with open(json_output_path, 'w', encoding='utf-8') as f:
#             json.dump(result_dict, f, ensure_ascii=False, indent=4)

#         print(f"-> 최종 OCR 결과가 JSON 파일로 저장되었습니다: {json_output_path}")
#     else:
#         print(f"-> '{os.path.basename(source_image_path)}'에서 분할할 영역을 찾지 못했거나 오류가 발생했습니다.")


# def main():
#     """지정된 디렉토리의 모든 이미지 파일에 대해 OCR 처리를 실행합니다."""
#     data_directory = r'C:\Users\Digital_026\Desktop\task_folder\source\test\data'
#     allowed_extensions = {'.png', '.jpg', '.jpeg', '.bmp', '.tiff'}

#     if not os.path.isdir(data_directory):
#         print(f"오류: 디렉토리를 찾을 수 없습니다 -> {data_directory}")
#         return

#     image_files = [f for f in os.listdir(data_directory) if os.path.splitext(f)[1].lower() in allowed_extensions]

#     if not image_files:
#         print(f"'{data_directory}'에서 처리할 이미지 파일을 찾지 못했습니다.")
#         return

#     print(f"총 {len(image_files)}개의 이미지 파일을 처리합니다.")
#     for filename in image_files:
#         image_path = os.path.join(data_directory, filename)
#         process_image_file(image_path)

#     print(f"\n{'='*20}\n모든 작업이 완료되었습니다.\n{'='*20}")


# if __name__ == '__main__':
#     main()



@task(pool='ocr_pool') 
def img_preprocess_task(step_info:dict,file_info:dict,target_key:str="_origin")->dict:
    process_id = str(uuid.uuid4())
    print("empty map check",result_map)
    result_map["process_id"] = f"_{process_id}_pre"
    result_map["folder_path"] = file_info["file_id"]
    result_map["cache"] = {}
    result_map["save_path"] = {}
    function_map = {
        #common
        "cache": {"function": cache, "input_type": "file_path", "output_type": "file_path","param":"cache_key"},
        "load": {"function": load, "input_type": "any", "output_type": "file_path","param":"cache_key"},
        "save": {"function": save, "input_type": "file_path", "output_type": "file_path","param":"save_key"},
        #set
        "calc_angle_set1": {"function": calc_angle_set1, "input_type": "np_bgr", "output_type": "np_bgr","param":"angle_key,iterations,iter_save"},
        "calc_angle_set2": {"function": calc_angle_set2, "input_type": "np_bgr", "output_type": "np_bgr","param":"angle_key,delta,limit,iterations,iter_save"},
        "calc_angle_set3": {"function": calc_angle_set3, "input_type": "np_bgr", "output_type": "np_bgr","param":"angle_key,iterations,iter_save"},
        "calc_angle_set4": {"function": calc_angle_set4, "input_type": "np_bgr", "output_type": "np_bgr","param":"angle_key,iterations,iter_save"},
        "text_orientation_set": {"function": text_orientation_set, "input_type": "np_bgr", "output_type": "np_bgr","param":"angle_key,iterations,iter_save"},
        "del_blank_set1": {"function": del_blank_set1, "input_type": "np_bgr", "output_type": "np_bgr", "param": "padding"},
        "del_blank_set2": {"function": del_blank_set2, "input_type": "np_bgr", "output_type": "np_bgr", "param": "line_ratios,padding_ratios,iter_save"},
        # ocr
        "separate_areas_set1": {"function": separate_areas_set1, "input_type": "np_bgr", "output_type": "np_bgr", "param": "area_type,offset,width,height,iter_save"},
        "separate_areas_set2": {"function": separate_areas_set2, "input_type": "np_bgr", "output_type": "np_bgr", "param": "area_name,line_length_ratio,min_line_gap,iter_save"},
        #preprocess
        "scale1": {"function": scale1, "input_type": "np_bgr", "output_type": "np_bgr"},
        "gray": {"function": gray, "input_type": "np_bgr", "output_type": "np_gray"},
        "denoising1": {"function": denoising1, "input_type": "np_bgr", "output_type": "np_bgr"},
        "denoising2": {"function": denoising2, "input_type": "np_bgr", "output_type": "np_bgr"},
        "threshold": {"function": threshold, "input_type": "np_gray", "output_type": "np_gray"},
        "morphology1": {"function": morphology1, "input_type": "np_bgr", "output_type": "np_bgr"},
        "canny": {"function": canny, "input_type": "np_bgr", "output_type": "np_bgr"},
        "thinner": {"function": thinner, "input_type": "np_bgr", "output_type": "np_bgr"},
        "before_angle1": {"function": before_angle1, "input_type": "np_bgr", "output_type": "np_gray","param":""},
        "calc_angle1": {"function": calc_angle1, "input_type": "np_gray", "output_type": "np_gray","param":"angle_key"},
        "before_angle2": {"function": before_orientation, "input_type": "np_bgr", "output_type": "np_gray","param":""},
        "calc_angle2": {"function": calc_orientation, "input_type": "any", "output_type": "np_bgr","param":"angle_key"},
        "rotate": {"function": rotate, "input_type": "np_bgr", "output_type": "np_bgr","param":"angle_key"},
        
        "line_tracking": {"function": line_tracking, "input_type": "np_gray", "output_type": "np_gray","param":"iter_save"},
        
    }
    output = file_info["file_path"][target_key]
    before_output_type = "file_path"
    for stepinfo in step_info["step_list"]:
        print("step :",stepinfo["name"])
        function_info = function_map[stepinfo["name"]]
        convert_param = stepinfo.get("convert_param", {})
        input = type_convert_util.convert_type(output,before_output_type,function_info["input_type"],params=convert_param)
        output = function_info["function"](input,**stepinfo["param"])
        before_output_type = function_info["output_type"]
    file_info["file_path"].update(result_map["save_path"])
    result = type_convert_util.convert_type(output,before_output_type,"file_path")
    file_info["file_path"]["_result"] = result
    return file_info

def cache(file_path:str,cache_key:str)->str:
    result_map["cache"][f"filepath_{cache_key}"] = file_path
    return file_path

def load(_,cache_key:str)->str:
    return result_map["cache"][f"filepath_{cache_key}"]

def save(file_path:str,save_key:str,tmp_save:bool=True)->str:
    if tmp_save:
        save_path = Path(TEMP_FOLDER) / result_map["folder_path"] / f"{save_key}.png"
        save_path = file_util.file_copy(file_path,save_path)
    else:
        save_path = file_path
    result_map["save_path"][save_key]=save_path
    return file_path

def scale1(img_np_bgr:np.ndarray,width:int,height:int) -> np.ndarray:
    """
    이미지를 지정한 크기(width, height)에 맞게 비율을 유지하며 리사이즈하고,
    남는 공간은 흰색(255)으로 채웁니다.

    :param img_np_bgr: BGR 채널을 가진 numpy 배열(OpenCV 이미지)
    :param width: 결과 이미지의 가로 크기
    :param height: 결과 이미지의 세로 크기
    :return: 크기 조정 및 중앙 정렬된 BGR numpy 배열
    """
    original_height, original_width = img_np_bgr.shape[:2]

    # 비율을 유지하면서 목표 크기를 넘지 않는 최대 크기 계산
    scale_ratio = min(width/original_width, height/original_height)

    # 새로운 크기 계산
    new_width = int(original_width * scale_ratio)
    new_height = int(original_height * scale_ratio)
    
    if scale_ratio < 1.0:
        resized_image = cv2.resize(img_np_bgr, (new_width, new_height), interpolation=cv2.INTER_AREA)
    else:
        resized_image = cv2.resize(img_np_bgr, (new_width, new_height), interpolation=cv2.INTER_CUBIC)
    background = np.full((height, width, 3), 255, dtype=np.uint8)

    # 리사이징된 이미지를 하얀색 배경 중앙에 붙여넣기
    paste_x = (width - new_width) // 2
    paste_y = (height - new_height) // 2
    background[paste_y:paste_y+new_height, paste_x:paste_x+new_width] = resized_image

    return background

def gray(img_np_bgr: np.ndarray) -> np.ndarray:
    """
    이미지를 그레이스케일로 변환합니다.

    :param img_np_bgr: BGR 채널을 가진 numpy 배열(OpenCV 이미지)
    :return: 그레이스케일로 변환된 numpy 배열
    """
    return cv2.cvtColor(img_np_bgr, cv2.COLOR_BGR2GRAY)

def denoising1(img_np_bgr: np.ndarray) -> np.ndarray:
    """
    컬러 이미지에 대해 Non-Local Means 알고리즘으로 노이즈를 제거합니다.

    :param img_np_bgr: BGR 채널을 가진 numpy 배열(OpenCV 이미지)
    :return: 노이즈가 제거된 BGR numpy 배열
    """
    denoised_np_bgr = cv2.fastNlMeansDenoisingColored(
        src=img_np_bgr,
        h=3,                  # 밝기 성분 강도
        hColor=3,             # 색상 성분 강도
        templateWindowSize=7, # 검사 패치 크기
        searchWindowSize=21   # 검색 윈도우 크기
    )
    return denoised_np_bgr

def denoising2(img_np_bgr: np.ndarray) -> np.ndarray:
    """
    컬러 이미지에 대해 미디언 블러(median blur)로 노이즈를 제거합니다.

    :param img_np_bgr: BGR 채널을 가진 numpy 배열(OpenCV 이미지)
    :return: 노이즈가 제거된 BGR numpy 배열
    """
    denoised_np_bgr = cv2.medianBlur(img_np_bgr, 3)
    return denoised_np_bgr

def threshold(img_np_gray:np.ndarray,thresh:int=127,type:int=cv2.THRESH_BINARY) -> np.ndarray:
    """
    이미지를 임계값 기준으로 이진화합니다.
    thresh : 임계값
    type : 다음 코드값에 따라 지정된 작업을 실행합니다.
      cv2.THRESH_BINARY : 임계값보다 크면 255(흰색) 작으면 0(검정)
      cv2.THRESH_BINARY_INV : 임계값보다 크면 0(검정) 작으면 255(흰색)
      cv2.THRESH_TOZERO : 임계값 이하만 0(검정) 그 외 현상유지
      cv2.THRESH_TOZERO_INV : 임계값 이상만 0(검정) 그 외 현상유지
      cv2.THRESH_OTSU : 이 옵션을 추가하면 임계값을 자동 지정함
    """
    ret, binary_np_gray = cv2.threshold(img_np_gray, thresh=thresh, maxval=255, type=type)
    return binary_np_gray

def adaptive_threshold(img_np_gray:np.ndarray,type:int=cv2.THRESH_BINARY,block:int=11) -> np.ndarray:
    """
    국소적 자동 임계값을 기준으로 이진화합니다.
    type : 다음 코드값에 따라 지정된 작업을 실행합니다.
      cv2.THRESH_BINARY : 임계값보다 크면 255(흰색) 작으면 0(검정)
      cv2.THRESH_BINARY_INV : 임계값보다 크면 0(검정) 작으면 255(흰색)
      cv2.THRESH_TOZERO : 임계값 이하만 0(검정) 그 외 현상유지
      cv2.THRESH_TOZERO_INV : 임계값 이상만 0(검정) 그 외 현상유지
    block : 작을수록 잡음 제거력이 떨어지며, 클수록 이미지 뭉개짐이 높아집니다
    """
    binary_np_gray = cv2.adaptiveThreshold(
        img_np_gray, maxValue=255,
        adaptiveMethod=cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
        thresholdType=type,
        blockSize=block, C=2
    )
    return binary_np_gray

def morphology1(img_np_bgr: np.ndarray) -> np.ndarray:
    """
    형태학적 연산(열기 → 닫기)을 적용해 노이즈를 제거하고 객체 경계를 보정합니다.

    :param img_np_bgr: 이진화된 numpy 배열(OpenCV 이미지)
    :return: 형태학적 연산이 적용된 numpy 배열
    """
    kernel = np.ones((3,3), np.uint8) #커널은 중앙 홀수로 작업
    open_img_np_bgr = cv2.morphologyEx(img_np_bgr, cv2.MORPH_OPEN, kernel)
    morphology_img_bin = cv2.morphologyEx(open_img_np_bgr, cv2.MORPH_CLOSE, kernel)
    return morphology_img_bin

def canny(img_np_bgr: np.ndarray) -> np.ndarray:
    # 엣지 검출
    edges = cv2.Canny(img_np_bgr, 30, 100, apertureSize=3)
    return edges

def thinner(img_np_bgr: np.ndarray) -> np.ndarray:
    # 전처리 예시: 선 굵기 줄이기
    kernel = np.ones((3,3), np.uint8)
    edges = cv2.erode(img_np_bgr, kernel, iterations=1)
    return edges
def del_blank_set1(img_np_bgr: np.ndarray, padding: int = 5) -> np.ndarray:
    """
    이미지에서 불필요한 상하좌우 공백을 제거하고 약간의 여백(padding)을 줍니다.
    :param img_np_bgr: BGR 채널을 가진 numpy 배열(OpenCV 이미지)
    :param padding: 잘라낸 이미지 주위에 추가할 픽셀 수
    :return: 공백이 제거된 BGR numpy 배열
    """
    # 1. 그레이스케일 변환 및 이진화 (콘텐츠를 흰색으로)
    gray = cv2.cvtColor(img_np_bgr, cv2.COLOR_BGR2GRAY)
    # 배경이 주로 흰색(255)이므로, 콘텐츠(검정)를 찾기 위해 반전(THRESH_BINARY_INV)
    _, thresh = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)

    # 2. 콘텐츠 영역 찾기
    contours, _ = cv2.findContours(thresh, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

    if not contours:
        # 콘텐츠가 없으면 원본 반환
        print("콘텐츠가 없습니다.")
        return img_np_bgr

    # 3. 모든 콘텐츠를 포함하는 하나의 큰 바운딩 박스 계산
    all_points = np.concatenate(contours, axis=0)
    x, y, w, h = cv2.boundingRect(all_points)

    # 4. 패딩 적용
    h_img, w_img = img_np_bgr.shape[:2]
    x_start = max(0, x - padding)
    y_start = max(0, y - padding)
    x_end = min(w_img, x + w + padding)
    y_end = min(h_img, y + h + padding)

    # 5. 원본 이미지에서 해당 영역 잘라내기
    cropped_img = img_np_bgr[y_start:y_end, x_start:x_end]
    print('공백 제거 완료')
    return cropped_img

def separate_areas_set1(
    img_np_bgr: np.ndarray,
    area_name: str,
    area_type: str = "top_left",
    offset: List[int] = None,
    width: int = None,
    height: int = None,
    iter_save: bool = False,
    **kwargs
) -> np.ndarray:
    """
    이미지를 설정에 따라 단일 영역으로 분리(crop)합니다.
    file_util.py의 설정 방식에 맞춰 단일 영역 처리를 위해 수정되었습니다.

    :param img_np_bgr: BGR 채널을 가진 numpy 배열(OpenCV 이미지)
    :param area_type: 기준점 ('top_left', 'top_center' 등)
    :param offset: [x, y] 오프셋
    :param width: 영역 너비 (-1은 끝까지)
    :param height: 영역 높이 (-1은 끝까지)
    :param kwargs: 추가 파라미터 (사용되지 않음)
    :param iter_save: 비교를 위한 원본 이미지 저장 여부
    :return: 분리된 이미지(np.ndarray).
    """
    if offset is None:
        offset = [0, 0]
    h_img, w_img = img_np_bgr.shape[:2]
    
    # iter_save가 True일 경우, 영역  분리 전 원본 이미지를 저장합니다.
    if iter_save:
        file_path = type_convert_util.convert_type(img_np_bgr, "np_bgr", "file_path")
        save(file_path, "separate_areas_set1_original",result_map=result_map)

    if width is None or height is None:
        print(f"경고: 영역의 너비(width) 또는 높이(height)가 지정되지 않아 건너뜁니다.")
        return img_np_bgr


    # 1. 기준점(anchor) 계산
    anchor_points = {
        "top_left": (0, 0), "top_center": (w_img // 2, 0), "top_right": (w_img, 0),
        "center_left": (0, h_img // 2), "center": (w_img // 2, h_img // 2), "center_right": (w_img, h_img // 2),
        "bottom_left": (0, h_img), "bottom_center": (w_img // 2, h_img), "bottom_right": (w_img, h_img)
    }
    anchor_x, anchor_y = anchor_points.get(area_type, (0, 0))
    if area_type not in anchor_points:
        print(f"경고: area_type '{area_type}'이 유효하지 않아 top_left로 처리합니다.")

    # 2. 시작 좌표 계산 (offset 적용)
    start_x = anchor_x + offset[0]
    start_y = anchor_y + offset[1]

    # 3. 너비와 높이 계산 (-1 처리)
    crop_w = width if width != -1 else w_img - start_x
    crop_h = height if height != -1 else h_img - start_y

    # 4. 최종 좌표 계산 (이미지 경계 내로 조정)
    x1 = max(0, start_x)
    y1 = max(0, start_y)
    x2 = min(w_img, start_x + crop_w)
    y2 = min(h_img, start_y + crop_h)

    if x1 >= x2 or y1 >= y2:
        print(f"경고: 영역이 이미지 범위를 벗어나 유효하지 않습니다. ({x1},{y1},{x2},{y2})")
        return img_np_bgr

    # 5. 이미지 자르기
    img_np_bgr = img_np_bgr[y1:y2, x1:x2]
    print(f"영역 분리 완료: pos=({x1}, {y1}), size=({x2-x1}, {y2-y1})")

    # 6. json 위치정보 저장
    info = {
        "area_name" : area_name,
        "area_pos" : [x1,y1,x2-x1,y2-y1]
    }
    json_path = Path(TEMP_FOLDER) / result_map["folder_path"] / f"{area_name}_set1.json"
    json_util.save(json_path, info)
    return img_np_bgr


def separate_areas_set2(
    img_np_bgr: np.ndarray,
    area_name: str,
    first_axis: str = "horizontal",  # or "vertical"
    horizontal_line_ratio: float = 0.7,
    horizontal_min_gap: int = 15,
    vertical_line_ratio: float = 0.7,
    vertical_min_gap: int = 15,
    iter_save: bool = False,
    **kwargs
) -> np.ndarray:
    """
    이미지에서 수평선 또는 수직선을 기준으로 반복적으로 영역을 분리합니다.
    각 분할 방향은 사용자가 지정한 순서에 따라 while문을 통해 반복적으로 수행되며,
    더 이상 선이 검출되지 않을 때까지 분할됩니다.
 
    :param img_np_bgr: 입력 BGR 이미지 (np.ndarray).
    :param area_name: 현재 영역 이름 (저장 시 접두사로 사용).
    :param first_axis: 먼저 분리할 축 ("horizontal" 또는 "vertical").
    :param horizontal_line_ratio: 수평선 길이 비율.
    :param horizontal_min_gap: 수평선 기반 최소 영역 높이.
    :param vertical_line_ratio: 수직선 길이 비율.
    :param vertical_min_gap: 수직선 기반 최소 영역 너비.
    :param iter_save: 디버깅 이미지 저장 여부.
    :return: 원본 이미지 그대로 반환.
    """
    from collections import deque
    from pathlib import Path
    import cv2
    # result_map, json_util, type_convert_util, save 등 외부 의존성은 정의되었다고 가정
    
    image_queue = deque()
    image_queue.append((img_np_bgr, area_name))
    final_sub_images = []
    iteration = 0

    while image_queue:
        cur_img, cur_name = image_queue.popleft()
        
        # 1. 우선 축 분할
        if first_axis == "horizontal":
            sub_imgs, split_occurred_primary = _separate_areas_by_horizontal_lines(
                cur_img, cur_name,
                horizontal_line_ratio, horizontal_min_gap, iter_save
            )
        else:
            sub_imgs, split_occurred_primary = _separate_areas_by_vertical_lines(
                cur_img, cur_name,
                vertical_line_ratio, vertical_min_gap, iter_save
            )

        if split_occurred_primary: # 분할이 발생했다면
            # 부모 JSON 업데이트 로직을 여기에 직접 삽입
            parent_name = cur_name # 현재 처리 중인 영역이 부모가 됨
            save_path = Path(TEMP_FOLDER) / result_map["folder_path"] / f"{parent_name}_set2.json"
            load_path = save_path if save_path.exists() else Path(TEMP_FOLDER) / result_map["folder_path"] / f"{parent_name}_set1.json"

            if load_path.exists():
                meta = json_util.load(load_path) or {}
                meta['child'] = 1
                json_util.save(save_path, meta)
            
            for img, name in sub_imgs:
                image_queue.append((img, name))
            continue

        # 2. 보조 축 분할
        second_axis = "vertical" if first_axis == "horizontal" else "horizontal"
        if second_axis == "horizontal":
            sub_imgs, split_occurred_secondary = _separate_areas_by_horizontal_lines(
                cur_img, cur_name,
                horizontal_line_ratio, horizontal_min_gap, iter_save
            )
        else:
            sub_imgs, split_occurred_secondary = _separate_areas_by_vertical_lines(
                cur_img, cur_name,
                vertical_line_ratio, vertical_min_gap, iter_save
            )

        if split_occurred_secondary: # 분할이 발생했다면
            # 부모 JSON 업데이트 로직을 여기에 직접 삽입
            parent_name = cur_name # 현재 처리 중인 영역이 부모가 됨
            save_path = Path(TEMP_FOLDER) / result_map["folder_path"] / f"{parent_name}_set2.json"
            load_path = save_path if save_path.exists() else Path(TEMP_FOLDER) / result_map["folder_path"] / f"{parent_name}_set1.json"

            if load_path.exists():
                meta = json_util.load(load_path) or {}
                meta['child'] = 1
                json_util.save(save_path, meta)
            
            for img, name in sub_imgs:
                image_queue.append((img, name))
        else: # 더 이상 분할되지 않으면 최종 결과로 간주
            final_sub_images.append((cur_img, cur_name))
        
        iteration += 1

    print(f"[완료] '{area_name}'에 대해 반복 분할 완료 (총 {iteration}회 루프 실행됨)")
    return img_np_bgr


def _separate_areas_by_horizontal_lines(
    img_np_bgr: np.ndarray,
    area_name: str,
    horizontal_line_ratio: float = 0.7,
    horizontal_min_gap: int = 15,
    iter_save: bool = False,
    **kwargs
) -> Tuple[List[Tuple[np.ndarray, str]], bool]: # 반환값 변경
    gray = cv2.cvtColor(img_np_bgr, cv2.COLOR_BGR2GRAY)
    binary = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)[1]
    dilated_binary = cv2.dilate(binary, cv2.getStructuringElement(cv2.MORPH_RECT, (3, 3)), iterations=1)
    h, w = dilated_binary.shape

    horizontal_kernel_size = int(w * horizontal_line_ratio)
    if horizontal_kernel_size < 1:
        return [], False # 변경: 분할되지 않음

    detected_lines = cv2.morphologyEx(dilated_binary, cv2.MORPH_OPEN,
                                      cv2.getStructuringElement(cv2.MORPH_RECT, (horizontal_kernel_size, 1)),
                                      iterations=2)

    contours, _ = cv2.findContours(detected_lines, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    y_coords = [cv2.boundingRect(c)[1] + cv2.boundingRect(c)[3] // 2 for c in contours]
    split_points = sorted(set([0] + y_coords + [h]))

    meta_path = Path(TEMP_FOLDER) / result_map["folder_path"] / f"{area_name}_set2.json"
    if not meta_path.exists():
        meta_path = Path(TEMP_FOLDER) / result_map["folder_path"] / f"{area_name}_set1.json"

    meta = json_util.load(meta_path) or {}
    orig_x = meta.get("area_pos", [0, 0, 0, 0])[0]
    orig_y = meta.get("area_pos", [0, 0, 0, 0])[1]

    results = []
    for i in range(len(split_points) - 1):
        y1, y2 = split_points[i], split_points[i + 1]
        if y2 - y1 < horizontal_min_gap:
            continue
        sub_img = img_np_bgr[y1:y2, :]
        sub_name = f"{area_name}_sub_h_{i}"
        save(type_convert_util.convert_type(sub_img, "np_bgr", "file_path"), sub_name)

        sub_meta = {
            "area_name": sub_name,
            "area_pos": [orig_x, orig_y + y1, w, y2 - y1],
            "child": 0
        }
        sub_meta_path = Path(TEMP_FOLDER) / result_map["folder_path"] / f"{sub_name}_set2.json"
        json_util.save(sub_meta_path, sub_meta)

        results.append((sub_img, sub_name))
    
    # 변경: 실제로 분할이 발생했는지 여부를 반환
    return results, len(results) > 1

def _separate_areas_by_vertical_lines(
    img_np_bgr: np.ndarray,
    area_name: str,
    vertical_line_ratio: float = 0.7,
    vertical_min_gap: int = 15,
    iter_save: bool = False,
    **kwargs
) -> Tuple[List[Tuple[np.ndarray, str]], bool]: # 반환값 변경
    gray = cv2.cvtColor(img_np_bgr, cv2.COLOR_BGR2GRAY)
    binary = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)[1]
    dilated_binary = cv2.dilate(binary, cv2.getStructuringElement(cv2.MORPH_RECT, (3, 3)), iterations=1)
    h, w = dilated_binary.shape

    vertical_kernel_size = int(h * vertical_line_ratio)
    if vertical_kernel_size < 1:
        return [], False # 변경: 분할되지 않음

    detected_lines = cv2.morphologyEx(dilated_binary, cv2.MORPH_OPEN,
                                      cv2.getStructuringElement(cv2.MORPH_RECT, (1, vertical_kernel_size)),
                                      iterations=2)

    contours, _ = cv2.findContours(detected_lines, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    x_coords = [cv2.boundingRect(c)[0] + cv2.boundingRect(c)[2] // 2 for c in contours]
    split_points = sorted(set([0] + x_coords + [w]))

    meta_path = Path(TEMP_FOLDER) / result_map["folder_path"] / f"{area_name}_set2.json"
    if not meta_path.exists():
        meta_path = Path(TEMP_FOLDER) / result_map["folder_path"] / f"{area_name}_set1.json"
    meta = json_util.load(meta_path) or {}
    orig_x = meta.get("area_pos", [0, 0, 0, 0])[0]
    orig_y = meta.get("area_pos", [0, 0, 0, 0])[1]

    results = []
    for i in range(len(split_points) - 1):
        x1, x2 = split_points[i], split_points[i + 1]
        if x2 - x1 < vertical_min_gap:
            continue
        sub_img = img_np_bgr[:, x1:x2]
        sub_name = f"{area_name}_sub_v_{i}"
        save(type_convert_util.convert_type(sub_img, "np_bgr", "file_path"), sub_name)

        sub_meta = {
            "area_name": sub_name,
            "area_pos": [orig_x + x1, orig_y, x2 - x1, h],
            "child": 0
        }
        sub_meta_path = Path(TEMP_FOLDER) / result_map["folder_path"] / f"{sub_name}_set2.json"
        json_util.save(sub_meta_path, sub_meta)

        results.append((sub_img, sub_name))

    # 변경: 실제로 분할이 발생했는지 여부를 반환
    return results, len(results) > 1


# # 새로운 함수를 추가함 _update_parent_json
# def separate_areas_set2(
#     img_np_bgr: np.ndarray,
#     area_name: str,
#     first_axis: str = "horizontal",  # or "vertical"
#     horizontal_line_ratio: float = 0.7,
#     horizontal_min_gap: int = 15,
#     vertical_line_ratio: float = 0.7,
#     vertical_min_gap: int = 15,
#     iter_save: bool = False,
#     **kwargs
# ) -> np.ndarray:
#     """
#     이미지에서 수평선 또는 수직선을 기준으로 반복적으로 영역을 분리합니다.
#     각 분할 방향은 사용자가 지정한 순서에 따라 while문을 통해 반복적으로 수행되며,
#     더 이상 선이 검출되지 않을 때까지 분할됩니다.

#     :param img_np_bgr: 입력 BGR 이미지 (np.ndarray).
#     :param area_name: 현재 영역 이름 (저장 시 접두사로 사용).
#     :param first_axis: 먼저 분리할 축 ("horizontal" 또는 "vertical").
#     :param horizontal_line_ratio: 수평선 길이 비율.
#     :param horizontal_min_gap: 수평선 기반 최소 영역 높이.
#     :param vertical_line_ratio: 수직선 길이 비율.
#     :param vertical_min_gap: 수직선 기반 최소 영역 너비.
#     :param iter_save: 디버깅 이미지 저장 여부.
#     :return: 원본 이미지 그대로 반환.
#     """
#     from collections import deque
#     image_queue = deque()
#     image_queue.append((img_np_bgr, area_name))
#     final_sub_images = []
#     iteration = 0

#     def _update_parent_json(parent_name: str):
#         """부모 영역의 JSON에 child=1 플래그를 업데이트합니다."""
#         # 저장할 경로는 항상 _set2.json 입니다.
#         save_path = Path(TEMP_FOLDER) / result_map["folder_path"] / f"{parent_name}_set2.json"
        
#         # 불러올 경로는 _set2.json이 우선이며, 없으면 _set1.json을 찾습니다.
#         load_path = save_path if save_path.exists() else Path(TEMP_FOLDER) / result_map["folder_path"] / f"{parent_name}_set1.json"

#         if load_path.exists():
#             meta = json_util.load(load_path) or {}
#             meta['child'] = 1  # 자식이 있음을 표시
#             json_util.save(save_path, meta)

#     while image_queue:
#         cur_img, cur_name = image_queue.popleft()

#         # 1. 우선 축 분할
#         if first_axis == "horizontal":
#             sub_imgs = _separate_areas_by_horizontal_lines(
#                 cur_img, cur_name,
#                 horizontal_line_ratio, horizontal_min_gap, iter_save
#             )
#         else:
#             sub_imgs = _separate_areas_by_vertical_lines(
#                 cur_img, cur_name,
#                 vertical_line_ratio, vertical_min_gap, iter_save
#             )

#         if len(sub_imgs) > 1:
#             _update_parent_json(cur_name)
#             for i, (img, name) in enumerate(sub_imgs):
#                 image_queue.append((img, name))

#             continue  # 새로 생긴 항목부터 다시 반복

#         # 2. 보조 축 분할
#         second_axis = "vertical" if first_axis == "horizontal" else "horizontal"
#         if second_axis == "horizontal":
#             sub_imgs = _separate_areas_by_horizontal_lines(
#                 cur_img, cur_name,
#                 horizontal_line_ratio, horizontal_min_gap, iter_save
#             )
#         else:
#             sub_imgs = _separate_areas_by_vertical_lines(
#                 cur_img, cur_name,
#                 vertical_line_ratio, vertical_min_gap, iter_save
#             )

#         if len(sub_imgs) > 1:
#             _update_parent_json(cur_name)
#             for i, (img, name) in enumerate(sub_imgs):
#                 image_queue.append((img, name))
#         else: # 더 이상 분할되지 않으면 최종 결과로 간주
#             final_sub_images.append((cur_img, cur_name))
#         iteration += 1

#     print(f"[완료] '{area_name}'에 대해 반복 분할 완료 (총 {iteration}회 루프 실행됨)")
#     return img_np_bgr

# def _separate_areas_by_horizontal_lines(
#     img_np_bgr: np.ndarray,
#     area_name: str,
#     horizontal_line_ratio: float = 0.7,
#     horizontal_min_gap: int = 15,
#     iter_save: bool = False,
#     **kwargs
# ) -> List[Tuple[np.ndarray, str]]:
#     gray = cv2.cvtColor(img_np_bgr, cv2.COLOR_BGR2GRAY)
#     binary = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)[1]
#     dilated_binary = cv2.dilate(binary, cv2.getStructuringElement(cv2.MORPH_RECT, (3, 3)), iterations=1)
#     h, w = dilated_binary.shape

#     horizontal_kernel_size = int(w * horizontal_line_ratio)
#     if horizontal_kernel_size < 1:
#         return []

#     detected_lines = cv2.morphologyEx(dilated_binary, cv2.MORPH_OPEN,
#                                       cv2.getStructuringElement(cv2.MORPH_RECT, (horizontal_kernel_size, 1)),
#                                       iterations=2)

#     contours, _ = cv2.findContours(detected_lines, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
#     y_coords = [cv2.boundingRect(c)[1] + cv2.boundingRect(c)[3] // 2 for c in contours]
#     split_points = sorted(set([0] + y_coords + [h]))

#     meta_path = Path(TEMP_FOLDER) / result_map["folder_path"] / f"{area_name}_set2.json"
#     if not meta_path.exists():  # 최초 분할이라면 set1.json 참조
#         meta_path = Path(TEMP_FOLDER) / result_map["folder_path"] / f"{area_name}_set1.json"

#     meta = json_util.load(meta_path) or {}
#     orig_x = meta.get("area_pos", [0, 0, 0, 0])[0]
#     orig_y = meta.get("area_pos", [0, 0, 0, 0])[1]

#     results = []
#     for i in range(len(split_points) - 1):
#         y1, y2 = split_points[i], split_points[i + 1]
#         if y2 - y1 < horizontal_min_gap:
#             continue
#         sub_img = img_np_bgr[y1:y2, :]
#         sub_name = f"{area_name}_sub_h_{i}"
#         save(type_convert_util.convert_type(sub_img, "np_bgr", "file_path"), sub_name)

#         sub_meta = {
#             "area_name": sub_name,
#             "area_pos": [orig_x, orig_y + y1, w, y2 - y1],
#             "child": 0  # 분할된 자식은 우선 child=0으로 설정
#         }
#         sub_meta_path = Path(TEMP_FOLDER) / result_map["folder_path"] / f"{sub_name}_set2.json"
#         json_util.save(sub_meta_path, sub_meta)

#         results.append((sub_img, sub_name))

#     return results

# def _separate_areas_by_vertical_lines(
#     img_np_bgr: np.ndarray,
#     area_name: str,
#     vertical_line_ratio: float = 0.7,
#     vertical_min_gap: int = 15,
#     iter_save: bool = False,
#     **kwargs
# ) -> List[Tuple[np.ndarray, str]]:
#     gray = cv2.cvtColor(img_np_bgr, cv2.COLOR_BGR2GRAY)
#     binary = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)[1]
#     dilated_binary = cv2.dilate(binary, cv2.getStructuringElement(cv2.MORPH_RECT, (3, 3)), iterations=1)
#     h, w = dilated_binary.shape

#     vertical_kernel_size = int(h * vertical_line_ratio)
#     if vertical_kernel_size < 1:
#         return []

#     detected_lines = cv2.morphologyEx(dilated_binary, cv2.MORPH_OPEN,
#                                       cv2.getStructuringElement(cv2.MORPH_RECT, (1, vertical_kernel_size)),
#                                       iterations=2)

#     contours, _ = cv2.findContours(detected_lines, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
#     x_coords = [cv2.boundingRect(c)[0] + cv2.boundingRect(c)[2] // 2 for c in contours]
#     split_points = sorted(set([0] + x_coords + [w]))

#     meta_path = Path(TEMP_FOLDER) / result_map["folder_path"] / f"{area_name}_set2.json"
#     if not meta_path.exists():  # 최초 분할이라면 set1.json 참조
#         meta_path = Path(TEMP_FOLDER) / result_map["folder_path"] / f"{area_name}_set1.json"
#     meta = json_util.load(meta_path) or {}
#     orig_x = meta.get("area_pos", [0, 0, 0, 0])[0]
#     orig_y = meta.get("area_pos", [0, 0, 0, 0])[1]

#     results = []
#     for i in range(len(split_points) - 1):
#         x1, x2 = split_points[i], split_points[i + 1]
#         if x2 - x1 < vertical_min_gap:
#             continue
#         sub_img = img_np_bgr[:, x1:x2]
#         sub_name = f"{area_name}_sub_v_{i}"
#         save(type_convert_util.convert_type(sub_img, "np_bgr", "file_path"), sub_name)

#         sub_meta = {
#             "area_name": sub_name,
#             "area_pos": [orig_x + x1, orig_y, x2 - x1, h],
#             "child": 0  # 분할된 자식은 우선 child=0으로 설정
#         }
#         sub_meta_path = Path(TEMP_FOLDER) / result_map["folder_path"] / f"{sub_name}_set2.json"
#         json_util.save(sub_meta_path, sub_meta)

#         results.append((sub_img, sub_name))

#     return results





# # child 없는 함수
# def separate_areas_set2(
#     img_np_bgr: np.ndarray,
#     area_name: str,
#     first_axis: str = "horizontal",  # or "vertical"
#     horizontal_line_ratio: float = 0.7,
#     horizontal_min_gap: int = 15,
#     vertical_line_ratio: float = 0.7,
#     vertical_min_gap: int = 15,
#     iter_save: bool = False,
#     **kwargs
# ) -> np.ndarray:
#     """
#     이미지에서 수평선 또는 수직선을 기준으로 반복적으로 영역을 분리합니다.
#     각 분할 방향은 사용자가 지정한 순서에 따라 while문을 통해 반복적으로 수행되며,
#     더 이상 선이 검출되지 않을 때까지 분할됩니다.

#     :param img_np_bgr: 입력 BGR 이미지 (np.ndarray).
#     :param area_name: 현재 영역 이름 (저장 시 접두사로 사용).
#     :param first_axis: 먼저 분리할 축 ("horizontal" 또는 "vertical").
#     :param horizontal_line_ratio: 수평선 길이 비율.
#     :param horizontal_min_gap: 수평선 기반 최소 영역 높이.
#     :param vertical_line_ratio: 수직선 길이 비율.
#     :param vertical_min_gap: 수직선 기반 최소 영역 너비.
#     :param iter_save: 디버깅 이미지 저장 여부.
#     :return: 원본 이미지 그대로 반환.
#     """
#     from collections import deque
#     image_queue = deque()
#     image_queue.append((img_np_bgr, area_name))
#     final_sub_images = []
#     iteration = 0

#     while image_queue:
#         cur_img, cur_name = image_queue.popleft()

#         # 1. 우선 축 분할
#         if first_axis == "horizontal":
#             sub_imgs = _separate_areas_by_horizontal_lines(
#                 cur_img, cur_name,
#                 horizontal_line_ratio, horizontal_min_gap, iter_save
#             )
#         else:
#             sub_imgs = _separate_areas_by_vertical_lines(
#                 cur_img, cur_name,
#                 vertical_line_ratio, vertical_min_gap, iter_save
#             )

#         if len(sub_imgs) > 1:
#             for i, (img, name) in enumerate(sub_imgs):
#                 image_queue.append((img, name))

#             continue  # 새로 생긴 항목부터 다시 반복

#         # 2. 보조 축 분할
#         second_axis = "vertical" if first_axis == "horizontal" else "horizontal"
#         if second_axis == "horizontal":
#             sub_imgs = _separate_areas_by_horizontal_lines(
#                 cur_img, cur_name,
#                 horizontal_line_ratio, horizontal_min_gap, iter_save
#             )
#         else:
#             sub_imgs = _separate_areas_by_vertical_lines(
#                 cur_img, cur_name,
#                 vertical_line_ratio, vertical_min_gap, iter_save
#             )

#         if len(sub_imgs) > 1:
#             for i, (img, name) in enumerate(sub_imgs):
#                 image_queue.append((img, name))
#         else: # 더 이상 분할되지 않으면 최종 결과로 간주
#             final_sub_images.append((cur_img, cur_name))
#         iteration += 1

#     print(f"[완료] '{area_name}'에 대해 반복 분할 완료 (총 {iteration}회 루프 실행됨)")
#     return img_np_bgr

# def _separate_areas_by_horizontal_lines(
#     img_np_bgr: np.ndarray,
#     area_name: str,
#     horizontal_line_ratio: float = 0.7,
#     horizontal_min_gap: int = 15,
#     iter_save: bool = False,
#     **kwargs
# ) -> List[Tuple[np.ndarray, str]]:
#     gray = cv2.cvtColor(img_np_bgr, cv2.COLOR_BGR2GRAY)
#     binary = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)[1]
#     dilated_binary = cv2.dilate(binary, cv2.getStructuringElement(cv2.MORPH_RECT, (3, 3)), iterations=1)
#     h, w = dilated_binary.shape

#     horizontal_kernel_size = int(w * horizontal_line_ratio)
#     if horizontal_kernel_size < 1:
#         return []

#     detected_lines = cv2.morphologyEx(dilated_binary, cv2.MORPH_OPEN,
#                                       cv2.getStructuringElement(cv2.MORPH_RECT, (horizontal_kernel_size, 1)),
#                                       iterations=2)

#     contours, _ = cv2.findContours(detected_lines, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
#     y_coords = [cv2.boundingRect(c)[1] + cv2.boundingRect(c)[3] // 2 for c in contours]
#     split_points = sorted(set([0] + y_coords + [h]))

#     meta_path = Path(TEMP_FOLDER) / result_map["folder_path"] / f"{area_name}_set2.json"
#     if not meta_path.exists():  # 최초 분할이라면 set1.json 참조
#         meta_path = Path(TEMP_FOLDER) / result_map["folder_path"] / f"{area_name}_set1.json"

#     meta = json_util.load(meta_path) or {}
#     orig_x = meta.get("area_pos", [0, 0, 0, 0])[0]
#     orig_y = meta.get("area_pos", [0, 0, 0, 0])[1]

#     results = []
#     for i in range(len(split_points) - 1):
#         y1, y2 = split_points[i], split_points[i + 1]
#         if y2 - y1 < horizontal_min_gap:
#             continue
#         sub_img = img_np_bgr[y1:y2, :]
#         sub_name = f"{area_name}_sub_h_{i}"
#         save(type_convert_util.convert_type(sub_img, "np_bgr", "file_path"), sub_name)

#         sub_meta = {
#             "area_name": sub_name,
#             "area_pos": [orig_x, orig_y + y1, w, y2 - y1]
#         }
#         sub_meta_path = Path(TEMP_FOLDER) / result_map["folder_path"] / f"{sub_name}_set2.json"
#         json_util.save(sub_meta_path, sub_meta)

#         results.append((sub_img, sub_name))

#     return results

# def _separate_areas_by_vertical_lines(
#     img_np_bgr: np.ndarray,
#     area_name: str,
#     vertical_line_ratio: float = 0.7,
#     vertical_min_gap: int = 15,
#     iter_save: bool = False,
#     **kwargs
# ) -> List[Tuple[np.ndarray, str]]:
#     gray = cv2.cvtColor(img_np_bgr, cv2.COLOR_BGR2GRAY)
#     binary = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)[1]
#     dilated_binary = cv2.dilate(binary, cv2.getStructuringElement(cv2.MORPH_RECT, (3, 3)), iterations=1)
#     h, w = dilated_binary.shape

#     vertical_kernel_size = int(h * vertical_line_ratio)
#     if vertical_kernel_size < 1:
#         return []

#     detected_lines = cv2.morphologyEx(dilated_binary, cv2.MORPH_OPEN,
#                                       cv2.getStructuringElement(cv2.MORPH_RECT, (1, vertical_kernel_size)),
#                                       iterations=2)

#     contours, _ = cv2.findContours(detected_lines, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
#     x_coords = [cv2.boundingRect(c)[0] + cv2.boundingRect(c)[2] // 2 for c in contours]
#     split_points = sorted(set([0] + x_coords + [w]))

#     meta_path = Path(TEMP_FOLDER) / result_map["folder_path"] / f"{area_name}_set2.json"
#     if not meta_path.exists():  # 최초 분할이라면 set1.json 참조
#         meta_path = Path(TEMP_FOLDER) / result_map["folder_path"] / f"{area_name}_set1.json"
#     meta = json_util.load(meta_path) or {}
#     orig_x = meta.get("area_pos", [0, 0, 0, 0])[0]
#     orig_y = meta.get("area_pos", [0, 0, 0, 0])[1]

#     results = []
#     for i in range(len(split_points) - 1):
#         x1, x2 = split_points[i], split_points[i + 1]
#         if x2 - x1 < vertical_min_gap:
#             continue
#         sub_img = img_np_bgr[:, x1:x2]
#         sub_name = f"{area_name}_sub_v_{i}"
#         save(type_convert_util.convert_type(sub_img, "np_bgr", "file_path"), sub_name)

#         sub_meta = {
#             "area_name": sub_name,
#             "area_pos": [orig_x + x1, orig_y, x2 - x1, h]
#         }
#         sub_meta_path = Path(TEMP_FOLDER) / result_map["folder_path"] / f"{sub_name}_set2.json"
#         json_util.save(sub_meta_path, sub_meta)

#         results.append((sub_img, sub_name))

#     return results




def del_blank_set2( # del_blank_set2만 수정
    img_np_bgr: np.ndarray,
    line_ratios: List[float] = [0.05, 0.05], # [length_ratio, height_ratio] 순서
    padding_ratios: List[float] = [0.01, 0.01, 0.01, 0.01], # [top, bottom, left, right] 순서
    iter_save: bool = False
) -> np.ndarray:
    """
    이미지 내의 긴 수평/수직선을 기준으로 상하좌우 공백을 제거합니다.
    패딩은 이미지 크기에 대한 백분율로 적용됩니다.

    :param img_np_bgr: BGR 채널을 가진 numpy 배열(OpenCV 이미지)
    :param line_ratios: [이미지 너비 대비 '긴 선' 길이 비율, 이미지 높이 대비 '긴 선' 길이 비율] 순서의 리스트.
    :param padding_ratios: [상단, 하단, 좌측, 우측] 순서의 패딩 비율 리스트 (이미지 콘텐츠 영역 크기 대비).
    :param iter_save: 중간 결과물(긴 선 이미지) 저장 여부
    :return: 상하좌우 공백이 제거된 BGR numpy 배열
    """
    # 1. 전처리
    gray = cv2.cvtColor(img_np_bgr, cv2.COLOR_BGR2GRAY)
    binary = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)[1]
    dilated = cv2.dilate(binary, np.ones((3, 3), np.uint8), iterations=1)
    h, w = dilated.shape

    # line_ratios 언팩
    if len(line_ratios) != 2:
        raise ValueError("line_ratios는 [length_ratio, height_ratio] 순서의 2개 float 값을 포함해야 합니다.")
    line_length_ratio, line_height_ratio = line_ratios

    # --- 수평선 기준 상/하단 경계 찾기 ---
    horizontal_size = int(w * line_length_ratio)
    y_start, y_end = 0, h

    if horizontal_size >= 2:
        horizontal_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (horizontal_size, 1))
        horizontal_lines = cv2.morphologyEx(dilated, cv2.MORPH_OPEN, horizontal_kernel, iterations=2)
        if iter_save:
            file_path = type_convert_util.convert_type(horizontal_lines, "np_gray", "file_path")
            save(file_path, "del_blank_set2_horizontal_lines")
        rows, _ = np.where(horizontal_lines > 0)
        if rows.size > 0:
            y_start, y_end = np.min(rows), np.max(rows)
        else:
            print("del_blank_set2: 긴 수평선을 찾을 수 없습니다.")

    # --- 수직선 기준 좌/우측 경계 찾기 ---
    vertical_size = int(h * line_height_ratio)
    x_start, x_end = 0, w

    if vertical_size >= 2:
        vertical_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (1, vertical_size))
        vertical_lines = cv2.morphologyEx(dilated, cv2.MORPH_OPEN, vertical_kernel, iterations=2)
        if iter_save:
            file_path = type_convert_util.convert_type(vertical_lines, "np_gray", "file_path")
            save(file_path, "del_blank_set2_vertical_lines")
        _, cols = np.where(vertical_lines > 0)
        if cols.size > 0:
            x_start, x_end = np.min(cols), np.max(cols)
        else:
            print("del_blank_set2: 긴 수직선을 찾을 수 없습니다.")

    # padding_ratios 언팩
    if len(padding_ratios) != 4:
        raise ValueError("padding_ratios는 [top, bottom, left, right] 순서의 4개 float 값을 포함해야 합니다.")
    padding_top_ratio, padding_bottom_ratio, padding_left_ratio, padding_right_ratio = padding_ratios
    # 4. 백분율 패딩을 픽셀 값으로 변환
    # 긴 수평선이 감지된 영역의 높이
    content_h = y_end - y_start
    # 긴 수직선이 감지된 영역의 너비
    content_w = x_end - x_start

    pad_top_px = int(content_h * padding_top_ratio)
    pad_bottom_px = int(content_h * padding_bottom_ratio)
    pad_left_px = int(content_w * padding_left_ratio)
    pad_right_px = int(content_w * padding_right_ratio)
    # pad_top_px = int(h * padding_top_ratio)
    # pad_bottom_px = int(h * padding_bottom_ratio)
    # pad_left_px = int(w * padding_left_ratio)
    # pad_right_px = int(w * padding_right_ratio)

    # 5. 패딩 적용 및 최종 좌표 계산
    target_y_start = y_start - pad_top_px
    target_y_end = y_end + 1 + pad_bottom_px
    target_x_start = x_start - pad_left_px
    target_x_end = x_end + 1 + pad_right_px

    # 6. 새 캔버스(배경) 생성
    new_h = target_y_end - target_y_start
    new_w = target_x_end - target_x_start

    if new_h <= 0 or new_w <= 0:
        print("결과 이미지 크기가 0 또는 음수이므로 원본을 반환합니다.")
        return img_np_bgr

        # 이미지 테두리에서 가장 많이 나타나는 색상(배경색)을 추정
    top_border = img_np_bgr[0, :]
    bottom_border = img_np_bgr[-1, :]
    left_border = img_np_bgr[1:-1, 0]
    right_border = img_np_bgr[1:-1, -1]
    border_pixels = np.concatenate([top_border, bottom_border, left_border, right_border], axis=0)
    
    colors, counts = np.unique(border_pixels.reshape(-1, 3), axis=0, return_counts=True)
    background_color = tuple(colors[counts.argmax()].tolist())

    background = np.full((new_h, new_w, 3), background_color, dtype=np.uint8)

    # 7. 원본 이미지에서 복사할 영역과 새 캔버스에 붙여넣을 영역 계산
    src_y_start = max(0, target_y_start)
    src_y_end = min(h, target_y_end)
    src_x_start = max(0, target_x_start)
    src_x_end = min(w, target_x_end)

    dest_y_start = max(0, -target_y_start)
    dest_x_start = max(0, -target_x_start)

    copy_h = src_y_end - src_y_start
    copy_w = src_x_end - src_x_start

    if copy_h > 0 and copy_w > 0:
        background[dest_y_start : dest_y_start + copy_h, dest_x_start : dest_x_start + copy_w] = \
            img_np_bgr[src_y_start:src_y_end, src_x_start:src_x_end]
    print('공백 제거 및 패딩 적용 완료')
    return background


def calc_angle_set1(img_np_bgr: np.ndarray,key:str,iterations:int=3,iter_save:bool=False) -> np.ndarray:
    """
    다각형 근사화를 활용한 표 인식 및 미세회전
    문서 방향 조정을 위해 text_orientation_set과 함께 사용 추천
    """
    target_img=img_np_bgr
    total_angle=0
    idx=1
    while idx<=iterations:
        # 1. 회전을 위한 전처리
        # 1-1. 그레이스케일
        gray = cv2.cvtColor(target_img, cv2.COLOR_BGR2GRAY)
        # 1-2. 이진화 (표 경계를 명확하게)
        _, thresh = cv2.threshold(gray, 128, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)
        # # 임시 저장
        # if iter_save:
        #     file = type_convert_util.convert_type(thresh,"np_bgr","file_path")
        #     save(file,f"rotate1_{idx}")
        
        # 2. 보정각도 추출
        # 2-1. 윤곽선 검출
        contours, _ = cv2.findContours(thresh, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        # 2-2. 가장 큰 윤곽선 선택
        largest_contour = max(contours, key=cv2.contourArea)
        # 2-3. 다각형 근사화 (4꼭지점 추출)
        epsilon = 0.02 * cv2.arcLength(largest_contour, True)
        approx = cv2.approxPolyDP(largest_contour, epsilon, True)
        if not len(approx) == 4:
            break
        else:
            # 2-4. 꼭지점 기준 각도 계산(상단만 체크)
            corners = approx.reshape(4, 2)
            # 꼭지점 정렬 (x+y 기준으로 좌상단, 우상단, 좌하단, 우하단)
            sorted_corners = sorted(corners, key=lambda pt: (pt[0] + pt[1]))
            top_left, top_right, bottom_left, bottom_right = sorted_corners
            # 상단 벡터 (우상단 - 좌상단)
            dx_top = top_right[0] - top_left[0]
            dy_top = top_right[1] - top_left[1]
            angle_top = np.degrees(np.arctan2(dy_top, dx_top))
            # 하단 벡터 (우상단 - 좌상단)
            dx_top = bottom_right[0] - bottom_left[0]
            dy_top = bottom_right[1] - bottom_left[1]
            angle_bottom = np.degrees(np.arctan2(dy_top, dx_top))
            # 좌측 벡터 (좌하단 - 좌상단)
            dx_left = bottom_left[0] - top_left[0]
            dy_left = bottom_left[1] - top_left[1]
            angle_left = np.degrees(np.arctan2(dy_left, dx_left)) + 90
            # 우측 벡터 (좌하단 - 좌상단)
            dx_left = bottom_right[0] - top_right[0]
            dy_left = bottom_right[1] - top_right[1]
            angle_right = np.degrees(np.arctan2(dy_left, dx_left)) + 90
            # 평균
            avg_angle = (angle_top+angle_bottom+angle_left+angle_right)/4
            print(f"angle{idx} : ",total_angle,avg_angle, angle_left, angle_right, angle_top, angle_bottom)
            if avg_angle < 0.1:
                break
            
            # 3. 타겟이미지를 보정 각도만큼 회전
            rotated = _rotate(target_img,avg_angle)
            # 4. 반복 처리를 위한 작업
            total_angle+=avg_angle
            target_img = rotated
            idx+=1
    
    result_map["cache"][f"angle_{key}"] = total_angle
    return target_img


def before_angle1(img_np_bgr: np.ndarray) -> np.ndarray:
    # 1. 그레이스케일
    gray = cv2.cvtColor(img_np_bgr, cv2.COLOR_BGR2GRAY)
    # 2. 이진화 (표 경계를 명확하게)
    _, thresh = cv2.threshold(gray, 128, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)
    return thresh

def calc_angle1(img_np_gray: np.ndarray, angle_key: str) -> float:
    # 윤곽선 검출
    contours, _ = cv2.findContours(img_np_gray, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    # 가장 큰 윤곽선 선택
    largest_contour = max(contours, key=cv2.contourArea)
    # 다각형 근사화 (4꼭지점 추출)
    epsilon = 0.02 * cv2.arcLength(largest_contour, True)
    approx = cv2.approxPolyDP(largest_contour, epsilon, True)
    
    if not len(approx) == 4:
        print("표의 4꼭지점을 찾을 수 없습니다.")
        result_map["cache"][f"angle_{angle_key}"] = 0
        return img_np_gray
    else:
        corners = approx.reshape(4, 2)
        # 꼭지점 정렬 (x+y 기준으로 좌상단, 우상단, 좌하단, 우하단)
        sorted_corners = sorted(corners, key=lambda pt: (pt[0] + pt[1]))
        top_left, top_right, bottom_left, bottom_right = sorted_corners

        # 상단 벡터 (우상단 - 좌상단)
        dx_top = top_right[0] - top_left[0]
        dy_top = top_right[1] - top_left[1]
        angle_top = np.degrees(np.arctan2(dy_top, dx_top))
        # 가로선을 0도로 맞추는 보정 각도
        correction_angle_top = -angle_top

        # 좌측 벡터 (좌하단 - 좌상단)
        dx_left = bottom_left[0] - top_left[0]
        dy_left = bottom_left[1] - top_left[1]
        angle_left = np.degrees(np.arctan2(dy_left, dx_left))
        # 세로선을 90도로 맞추는 보정 각도
        correction_angle_left = 90 - angle_left

        # 보정 각도 출력 및 반환 (가로선 기준 또는 평균, 필요에 따라 선택)
        print(f"가로선 보정 각도: {correction_angle_top:.2f}도")
        print(f"세로선 보정 각도: {correction_angle_left:.2f}도")
        result_map["cache"][f"angle_{angle_key}"] = (correction_angle_top*correction_angle_left)/2 * -1
        return img_np_gray

def calc_angle_set2(img_np_bgr:np.ndarray,angle_key:str,delta:float=0.25,limit:int=5,iterations:int=3,iter_save:bool=False) -> np.ndarray:
    """
    각도별 커널 탐색을 활용한 수평선/수직선 인식 및 미세회전
    문서 방향 조정을 위해 text_orientation_set과 함께 사용 추천
    """
    target_img=img_np_bgr
    total_angle=0
    idx=1
    
    while idx<=iterations:
        # delta 간격으로 limit값의 ± 범위를 돌려보며 적절한 각도 탐색 
        angles = np.arange(-limit, limit + delta, delta)
        scores = []
        
        (h, w) = target_img.shape[:2]
        min_length = int(min(w,h) * 0.1)
        min_length2 = int(min(w,h) * 0.4)

        def long_kernal_score(arr, angle):
            i=0
            #짧은선
            horizon_kernel = np.ones((min_length, 1), np.uint8)
            vertical_kernel = np.ones((1, min_length), np.uint8)
            #긴선
            horizon_kernel2 = np.ones((min_length2, 1), np.uint8)
            vertical_kernel2 = np.ones((1, min_length2), np.uint8)
            
            # # 1-3. 작은 객체 제거
            # kernel_horiz = np.ones((1, line_kernel), np.uint8)  # 가로 방향 커널 (길이 조정 필요)
            # horizontal = cv2.morphologyEx(binary, cv2.MORPH_OPEN, kernel_horiz)

            # # 수직선 강조 (글자 제거, 수직선만 남기기)
            # kernel_vert = np.ones((line_kernel, 1), np.uint8)   # 세로 방향 커널 (길이 조정 필요)
            # vertical = cv2.morphologyEx(binary, cv2.MORPH_OPEN, kernel_vert)
            
            # # 수평+수직 합치기
            # line_filtered = cv2.add(horizontal, vertical)
            
            # # 1.3. 반전
            # lines_inv = cv2.bitwise_not(line_filtered)
            
            # # 1-4. 에지 검출 (Canny)
            # edges = cv2.Canny(lines_inv, 50, 150, apertureSize=3)
        
            #3,3 커널을 사용하여 침식(검정 늘어남)
            eroded = cv2.erode(arr, np.ones((3, 3), np.uint8), iterations=1)

            # data = inter.rotate(arr, angle, reshape=False, order=0)
            data = _rotate(eroded,angle)
            
            # 1-1. 그레이스케일
            gray = cv2.cvtColor(data, cv2.COLOR_BGR2GRAY)
            # 1-2. 이진화 (표 경계를 명확하게)
            thresh = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)[1]
                        
            # 임시 저장
            if iter_save:
                file = type_convert_util.convert_type(thresh,"np_bgr","file_path")
                save(file,f"rotate21_{idx}_{angle}")

            #짧은 선과 긴 선을 기준으로 침식(검정 늘어남)=>
            #1-2이진화에서 반전한 이미지를 침식했으므로 실제로는 검정 수직수평선을 찾아 길이를 1 줄임
            #긴 선일수록 픽셀이 많이 남음
            horizon_eroded = cv2.erode(thresh, horizon_kernel)
            vertical_eroded = cv2.erode(thresh, vertical_kernel)
            horizon_eroded2 = cv2.erode(thresh, horizon_kernel2)
            vertical_eroded2 = cv2.erode(thresh, vertical_kernel2)
            
            #각 작업의 픽셀개수를 세어 점수 계산
            score = cv2.countNonZero(horizon_eroded) + cv2.countNonZero(vertical_eroded) + cv2.countNonZero(horizon_eroded2) + cv2.countNonZero(vertical_eroded2)
            # 임시 저장 
            if iter_save :
                tmp=cv2.add(horizon_eroded,vertical_eroded)
                tmp2=cv2.add(horizon_eroded2,vertical_eroded2)
                tmp3=cv2.add(tmp,tmp2)
                i+=1
                file = type_convert_util.convert_type(tmp3,"np_bgr","file_path")
                save(file,f"rotate22_{idx}_{angle}_{score}")
            
            return score
        
        for angle in angles:
            score = long_kernal_score(target_img, angle)
            scores.append(score)
        
        threshold_val = 50
        best_angle = angles[scores.index(max(scores))]
        if max(scores) <= threshold_val:
            best_angle = 0
        total_angle+=best_angle
        print(f"osd {idx}",total_angle,best_angle,scores.index(max(scores)), max(scores), scores)
        
        # 3. 타겟이미지를 보정 각도만큼 회전
        rotated = _rotate(target_img,best_angle)
    
        # 4. 반복 처리를 위한 작업
        target_img = rotated
        idx+=1
    result_map["cache"][f"angle_{angle_key}"] = total_angle
    return target_img 

def calc_angle_set3(img_np_bgr:np.ndarray,angle_key:str,iterations:int=3,iter_save:bool=False) -> np.ndarray:
    """
    허프변환을 활용한 직선 인식 및 미세회전
    문서에 따른 수치 조정이 많이 필요함(미완)
    문서 방향 조정을 위해 text_orientation_set과 함께 사용 추천
    """
    target_img=img_np_bgr
    total_angle=0
    idx=1
    tolerance = 2
    
    height, width = img_np_bgr.shape[:2]
    line_kernel = int(min(width,height) * 0.2)  # 전체 크기 10% 이상 길이의 선을 기준으로 문자 제거
    while idx<=iterations:
        # 1-1. 그레이스케일
        gray = cv2.cvtColor(target_img, cv2.COLOR_BGR2GRAY)
        # 1-2. 이진화 (표 경계를 명확하게)
        _, binary = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)
        # 1-3. 작은 객체 제거
        kernel_horiz = np.ones((1, line_kernel), np.uint8)  # 가로 방향 커널 (길이 조정 필요)
        horizontal = cv2.morphologyEx(binary, cv2.MORPH_OPEN, kernel_horiz)

        # 수직선 강조 (글자 제거, 수직선만 남기기)
        kernel_vert = np.ones((line_kernel, 1), np.uint8)   # 세로 방향 커널 (길이 조정 필요)
        vertical = cv2.morphologyEx(binary, cv2.MORPH_OPEN, kernel_vert)
        
        # 수평+수직 합치기
        line_filtered = cv2.add(horizontal, vertical)
        
        # 1.3. 반전
        lines_inv = cv2.bitwise_not(line_filtered)
        
        # 1-4. 에지 검출 (Canny)
        edges = cv2.Canny(lines_inv, 50, 150, apertureSize=3)
        
        # 임시 저장
        if iter_save:
            file = type_convert_util.convert_type(edges,"np_bgr","file_path")
            save(file,f"rotate3_{idx}")
        
        # 2. Hough Line Transform으로 선 검출
        lines = cv2.HoughLines(edges, 1, np.pi/180, threshold=150)
        angles = []
        if lines is not None:
            for line in lines:
                rho, theta = line[0]
                # 각도를 도 단위로 변환
                angle = np.degrees(theta)
                # 0~179도 범위로 정규화
                angle = angle % 180
                if angle < 0:
                    angle += 180
                angles.append(angle)
        
        if not angles:
            print("선을 찾을 수 없습니다.")
            break
        grouped_angles = []
        for angle in angles:
            grouped_angle = round(angle / tolerance) * tolerance
            grouped_angles.append(grouped_angle)
        tolerance = tolerance/2
        # 가장 많이 나타나는 각도 찾기
        angle_counter = Counter(grouped_angles)
        dominant_angle = angle_counter.most_common(1)[0][0]
        
        # 후보군 중 가장 적게 회전하는 각도 탐색
        candidates = [0, 90, -90]
        differences = [abs(dominant_angle - candidate) for candidate in candidates]
        target_angle = candidates[differences.index(min(differences))]
        rotation_angle = target_angle - dominant_angle

        total_angle+=rotation_angle
        print(f"osd {idx}",total_angle,rotation_angle, angles)
            
        # 3. 타겟이미지를 보정 각도만큼 회전
        rotated = _rotate(target_img,rotation_angle)
        
        # 4. 반복 처리를 위한 작업
        target_img = rotated
        idx+=1
    result_map["cache"][f"angle_{angle_key}"] = total_angle
    return target_img

def calc_angle_set4(img_np_bgr:np.ndarray,angle_key:str,iterations:int=3,iter_save:bool=False) -> np.ndarray:
    """
    각도별 수평/수직 픽셀들의 변화 활용해 미세회전
    문서 방향 조정을 위해 text_orientation_set과 함께 사용 추천
    """
    target_img=img_np_bgr
    total_angle=0
    idx=1
    
    while idx<=iterations:
        # 1-1. 그레이스케일
        gray = cv2.cvtColor(target_img, cv2.COLOR_BGR2GRAY)
        # 1-2. 이진화 (표 경계를 명확하게)
        thresh = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)[1]

        delta = 0.25
        limit = 5
        angles = np.arange(-limit, limit + delta, delta)
        scores = []

        def histogram_score(arr, angle):
            data = inter.rotate(arr, angle, reshape=False, order=0)
            histogram = np.sum(data, axis=1, dtype=float)
            score = np.sum((histogram[1:] - histogram[:-1]) ** 2, dtype=float)
            return score
        
        for angle in angles:
            score = histogram_score(thresh, angle)
            scores.append(score)

        best_angle = angles[scores.index(max(scores))]
        total_angle+=best_angle
        print(f"osd {idx}",total_angle,best_angle,scores)
        
        # 3. 타겟이미지를 보정 각도만큼 회전
        rotated = _rotate(target_img,best_angle)
        
        # 4. 반복 처리를 위한 작업
        target_img = rotated
        idx+=1
    result_map["cache"][f"angle_{angle_key}"] = total_angle
    return target_img

def text_orientation_set(img_np_bgr:np.ndarray,angle_key:str,iterations:int=2,iter_save:bool=False) -> np.ndarray:
    """
    테서랙트의 텍스트 방향과 문자 종류 감지를 활용한 90도 단위 회전
    미세조정을 위해 calc_angle_set1,3,5 등과 함께 사용 추천
    """
    target_img=img_np_bgr
    total_angle=0
    idx=1
    while idx<=iterations:
        # 1. 회전을 위한 전처리
        # 1-1. 노이즈 제거
        denoised = cv2.fastNlMeansDenoisingColored(
            src=target_img,
            h=3,                  # 밝기 성분 강도
            hColor=3,             # 색상 성분 강도
            templateWindowSize=7, # 검사 패치 크기
            searchWindowSize=21   # 검색 윈도우 크기
        )
        # 1-2. 그레이스케일
        gray = cv2.cvtColor(denoised, cv2.COLOR_BGR2GRAY)
        # 1-3. 이진화 (표 경계를 명확하게)
        _, binary = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
        # 1-4. 태서렉트 입력을 위해 rgb로 변경
        rgb = cv2.cvtColor(binary, cv2.COLOR_GRAY2RGB)
        # 임시 저장
        if iter_save:
            file = type_convert_util.convert_type(rgb,"np_bgr","file_path")
            save(file,f"rotate2_{idx}")
        
        # 2. ocr을 이용한 보정 각도 추출
        try:
            osd = pytesseract.image_to_osd(rgb)
        except pytesseract.TesseractError as e:
            print(f"Tesseract OSD Error: {e}")
            break
    
        rotation = 0
        for info in osd.split('\n'):
            if 'Rotate: ' in info:
                rotation = int(info.split(': ')[1])
            if 'Orientation confidence:' in info:
                orientation_confidence = float(info.split(': ')[1])
            if 'Script: ' in info:
                script_name = info.split(': ')[1]
            if 'Script confidence:' in info:
                script_confidence = float(info.split(': ')[1])
        if rotation == 0:
            print(f"osd {idx} break ",total_angle,rotation, orientation_confidence, script_name, script_confidence)
            break
        total_angle+=rotation
        print(f"osd {idx}",total_angle,rotation, orientation_confidence, script_name, script_confidence)
        
        # 3. 타겟이미지를 보정 각도만큼 회전
        rotated = _rotate(target_img,rotation)
        
        # 4. 반복 처리를 위한 작업
        target_img = rotated
        idx+=1
    result_map["cache"][f"angle_{angle_key}"] = total_angle
    return target_img

def before_orientation(img_np_bgr: np.ndarray) -> np.ndarray:
    # 1. 그레이스케일
    gray = cv2.cvtColor(img_np_bgr, cv2.COLOR_BGR2GRAY)
    # 2. 이진화 (표 경계를 명확하게)
    _, thresh = cv2.threshold(gray, 128, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)
    return thresh

def calc_orientation(img_np_bgr: np.ndarray,angle_key:str) -> np.ndarray:
    """테서랙트 OSD를 이용한 방향 보정"""
    try:
        osd = pytesseract.image_to_osd(img_np_bgr)
        rotation = 0
        for info in osd.split('\n'):
            if 'Rotate: ' in info:
                rotation = int(info.split(': ')[1])
    except pytesseract.TesseractError as e:
        print(f"Tesseract OSD Error: {e}")
        rotation = 0            
    print(f"가로선 보정 각도: {rotation:.2f}도")
    result_map["cache"][f"angle_{angle_key}"] = rotation
    return img_np_bgr


def rotate(img_np_bgr: np.ndarray, angle_key: str = None, angle_keys: List = []) -> np.ndarray:
    """이미지 회전 함수"""
    if len(angle_keys) > 0:
        angle = 0.0
        for key in angle_keys:
            angle += result_map["cache"].get(f"angle_{key}", 0)
    else:
        angle = result_map["cache"].get(f"angle_{angle_key}", 0)
    if angle == 0:
        return img_np_bgr
    rotated = _rotate(img_np_bgr, angle)
    return rotated

def line_tracking(img_np_gray:np.ndarray, iter_save:bool=False) -> np.ndarray:
    _, binary = cv2.threshold(img_np_gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)
    # 임시 저장
    if iter_save:
        print("binary:", binary.shape)
        file = type_convert_util.convert_type(binary,"np_gray","file_path")
        save(file,f"line_binary")
    
    # 2. 수평/수직 라인 강조 (모폴로지 연산)
    horizontal = binary.copy()
    vertical = binary.copy()

    # 수평 라인 검출
    cols = horizontal.shape[1]  #가로픽셀수
    horizontal_size = cols // 30  # 표 구조에 따라 조정
    horizontal_structure = cv2.getStructuringElement(cv2.MORPH_RECT, (horizontal_size, 1))
    horizontal = cv2.erode(horizontal, horizontal_structure)
    horizontal = cv2.dilate(horizontal, horizontal_structure)

    # 수직 라인 검출
    rows = vertical.shape[0]  #세로픽셀수
    vertical_size = rows // 30  # 표 구조에 따라 조정
    vertical_structure = cv2.getStructuringElement(cv2.MORPH_RECT, (1, vertical_size))
    vertical = cv2.erode(vertical, vertical_structure)
    vertical = cv2.dilate(vertical, vertical_structure)

    # 3. 라인 합치기 (표 전체 구조)
    table_mask = cv2.add(horizontal, vertical)

    # 4. 라인 추적 (연결성 보완)
    # 라벨링으로 연결된 라인 추출
    num_labels, labels, stats, centroids = cv2.connectedComponentsWithStats(table_mask, connectivity=8)

    # 결과 시각화
    output = cv2.cvtColor(img_np_gray, cv2.COLOR_GRAY2BGR)
    for i in range(1, num_labels):
        x, y, w, h, area = stats[i]
        if area > 50:  # 작은 잡음 제거
            cv2.rectangle(output, (x, y), (x + w, y + h), (0, 255, 0), 2)

    # 임시 저장
    if iter_save:
        print("output:", output.shape)
        file = type_convert_util.convert_type(output,"np_bgr","file_path")
        save(file,f"detected_lines")
    return output
    
#내부 함수
def _rotate(img_np_bgr: np.ndarray, angle:float) -> np.ndarray:
    """이미지 회전 내부 함수"""
    # 90도 단위 회전은 손실없이 회전
    if angle % 90 == 0:
        k = int(angle // 90) % 4  # 90도 단위 회전 횟수
        if k < 0:
            k = 4 + k
        return np.rot90(img_np_bgr, k=k)
    
    h, w = img_np_bgr.shape[:2]
    center = (w//2, h//2)
    M = cv2.getRotationMatrix2D(center, angle, 1.0)

    # 회전 후 이미지 크기 계산
    cos = np.abs(M[0, 0])
    sin = np.abs(M[0, 1])
    new_w = int((h * sin) + (w * cos))
    new_h = int((h * cos) + (w * sin))

    # 회전 중심 조정 (중심 이동)
    M[0, 2] += (new_w - w) / 2
    M[1, 2] += (new_h - h) / 2

    # 이미지 테두리에서 가장 많은 색(최빈값) 계산 (예: 흰색/검정)
    def get_most_common_border_color(img):
        top = img[0, :]
        bottom = img[-1, :]
        left = img[1:-1, 0]
        right = img[1:-1, -1]
        border = np.concatenate([top, bottom, left, right], axis=0)
        colors, counts = np.unique(border, axis=0, return_counts=True)
        return colors[counts.argmax()]
    most_common = get_most_common_border_color(img_np_bgr)

    rotated = cv2.warpAffine(
        img_np_bgr, M, (new_w, new_h),
        flags=cv2.INTER_CUBIC,
        borderMode=cv2.BORDER_CONSTANT,
        borderValue=tuple(most_common.tolist())
    )
    return rotated

