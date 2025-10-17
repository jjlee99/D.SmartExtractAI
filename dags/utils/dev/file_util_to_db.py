import pymysql
import json
if __name__ == "__main__":
    import sys, os
    sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

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
def insert_a_class():
    # class_id = config['class_id']
    doc_name = 'general_building_register'
    layout_name = 'a_class'
    config = get_config(doc_name,layout_name)
    
    # config가 None인 경우 예외 처리
    if config is None:
        raise ValueError("설정을 가져올 수 없습니다. 'a_class' 설정을 확인해주세요.")
    
    
    doc_class_pass = False
    if doc_class_pass:
        # TB_DI_DOC_CLASS
        sql = """
        INSERT INTO TB_DI_DOC_CLASS (DOC_NAME)
        VALUES (%s)
        """
        cursor.execute(sql, (doc_name,))
        doc_class_id = cursor.lastrowid
        print('1')
    else : 
        sql = """
        SELECT DOC_CLASS_ID FROM TB_DI_DOC_CLASS WHERE DOC_NAME = %s
        """
        cursor.execute(sql, (doc_name,))
        row = cursor.fetchone()
        print("row  ",row)
        doc_class_id = row[0] if row else None
        print("doc_class_id  ",doc_class_id, row)

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
        cleansing = json.dumps(area['cleansing'], ensure_ascii=False)
        structuring = json.dumps(area['structuring'], ensure_ascii=False)

        sql = """
        INSERT INTO TB_DI_SECTION_CLASS (LAYOUT_CLASS_ID,
        SECTION_NAME,SEPARATE_AREA_INFO, 
        SEPARATE_BLOCK_INFO, OCR_INFO, CLEANSING_INFO, STRUCTURING_INFO
        )
        VALUES (%s,%s, %s, %s, %s, %s, %s)
        """
        cursor.execute(sql, (layout_class_id ,section_name, separate_area, separate_block, ocr, cleansing, structuring))


def get_section_class_id_map():
    sql = "SELECT SECTION_NAME, SECTION_CLASS_ID FROM TB_DI_SECTION_CLASS"
    cursor.execute(sql)
    return {row[0]: row[1] for row in cursor.fetchall()}

def migrate_blocks_from_json():
    base_dirs = [
        '/opt/airflow/data/temp/building_detail',
        '/opt/airflow/data/temp/building_info',
        '/opt/airflow/data/temp/building_status'
        '/opt/airflow/data/temp/owner_status'
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
                if section_name in ['building_detail', 'building_info', 'owner_status', 'building_status']:
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

# 1. a_class 정보 추출
def delete_class_info():
    # TB_DI_DOC_CLASS
    # sql = """
    # delete FROM TB_DI_DOC_CLASS
    # """
    # cursor.execute(sql)
    
    sql = """
    delete FROM TB_DI_LAYOUT_CLASS
    """
    cursor.execute(sql)

    sql = """
    delete FROM TB_DI_SECTION_CLASS
    """
    cursor.execute(sql)

if __name__ == "__main__":
    try:
        delete_class_info()
        insert_a_class()
        print("데이터 이관이 완료되었습니다.")
        # migrate_blocks_from_json()
        # print("블록 데이터 이관이 완료되었습니다.")
    except Exception as e:
        print(f"오류 발생: {e}")
    finally:
        cursor.close()
        conn.close()
