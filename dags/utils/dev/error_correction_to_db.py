import json
import os
import pymysql  # MySQL용, 환경에 맞게 변경

# DB 연결 설정 (환경에 맞게 수정)
conn = pymysql.connect(
    host='192.168.10.18',
    user='digitalflow',
    password='digital10',
    db='dococr',
    charset='utf8mb4',
    autocommit=True
)



def insert_block_correction(cursor, section_class_id, block_row_num, block_col_num, error_text, crct_text):
    sql = """
    INSERT INTO TB_DI_BLOCK_CRCTN (BLOCK_CLASS_ID, ERROR_TEXT, CRRCT_TEXT, CRCTN_CNT) 
    VALUES (
      (SELECT BLOCK_CLASS_ID 
       FROM TB_DI_BLOCK_CLASS 
       WHERE SECTION_CLASS_ID = %s
       AND BLOCK_ROW_NUM = %s AND BLOCK_COL_NUM = %s
      ),
      %s, %s, 0
    )
    """
    cursor.execute(sql, (section_class_id, block_row_num, block_col_num, error_text, crct_text))

def main():
    folder_path = "/opt/airflow/data/class/a_class/add_block_dictionary"  # JSON 파일들이 있는 폴더
    with conn.cursor() as cursor:
        for filename in os.listdir(folder_path):
            if filename.endswith('.json'):
                file_path = os.path.join(folder_path, filename)
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                ac = data.get('add_correction', {})
                if not ac:
                    print(f"Skip {filename}, no 'add_correction' key")
                    continue

                section_class_id = ac.get('section_class_id')
                block_row_num = ac.get('block_row_num')
                block_col_num = ac.get('block_col_num')
                default_text = ac.get('default_text')
                error_text = ac.get('error_text')
                if None in (section_class_id, block_row_num, block_col_num, default_text, error_text):
                    print(f"Skip {filename}, missing required fields")
                    continue

                try:
                    insert_block_correction(cursor,
                                            section_class_id, 
                                            block_row_num, block_col_num,
                                            error_text, default_text)
                    print(f"Inserted correction from {filename}")
                    os.remove(file_path)
                    print(f"Deleted file: {filename}")
                except Exception as e:
                    print(f"Error inserting from {filename}: {e}")

        conn.commit()
    conn.close()

if __name__ == "__main__":
    main()
