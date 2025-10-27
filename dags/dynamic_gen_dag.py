from datetime import datetime
import json
import os
from pathlib import Path
from utils.com import json_util
from tasks.common.setup_task import final_cleanup
from dag_factory.factory import create_dag
from dag_factory.task_definitions import get_task_definitions
from utils.db import dococr_query_util
from airflow.models import Variable

CLASS_FOLDER = Variable.get("CLASS_FOLDER", default_var="/opt/airflow/data/class")
# 기본 인자 설정
default_args = {
    "owner": "airflow", 
    "start_date": datetime(2024, 1, 1),
    "on_failure_callback": final_cleanup,
}

file_path = os.path.join(CLASS_FOLDER,"doc_list.json")
with open(file_path, 'r') as file:
    content = file.read()
doc_list = json.loads(content)
for doc_info in doc_list:
    # DAG ID 동적 생성
    dag_id = f"dococr_{doc_info["doc_class_id"]}"
    dag_name = f"[문서분류] {doc_info["doc_name"]} <{doc_info["doc_class_id"]}>"
    doc_class_id = doc_info["doc_class_id"]
    
    # 모든 DAG의 스케줄 동일
    # @daily:매일 0시 실행
    schedule = "@daily"

    # 생성된 DAG 객체를 globals()에 추가하여 Airflow가 인식하게 합니다.
    
    # dag 자동생성 시 오류로 사용중지
    # globals()[dag_id] = create_dag(
    #     dag_id=dag_id,
    #     description=dag_name,
    #     schedule=schedule,
    #     default_args=default_args,
    #     tags=[],
    #     task_definition_func=get_task_definitions, # 태스크 정의 함수 전달
    #     doc_class_id=doc_class_id # DOC_CLASS_ID를 태스크 정의 함수에 전달
    # )


# # -------------------------------------------------------------
# # 1. INFO 유효성 검사 함수
# def is_info_valid(info_value):
#     """
#     INFO 컬럼 값이 유효한지 검사합니다.
#     """
#     if info_value is None:
#         return False
    
#     info_str = str(info_value).strip()
#     if not info_str: # 빈 문자열
#         return False
        
#     # JSON 객체 형태의 빈 값 체크
#     if info_str in ('{}', '{{}}'):
#         return False
        
#     return True

# # -------------------------------------------------------------
# # 2. DB에서 모든 섹션/레이아웃 INFO 정보를 조회하고, INFO 컬럼 목록을 가져옵니다.
# all_dag_info_records, info_columns_list = dococr_query_util.select_infos_for_dag_generation()

# # 3. DOC_CLASS_ID별로 그룹화하고, 유효한 DAG ID 목록을 생성합니다.
# grouped_info = {}
# invalid_doc_ids = set()

# # DOC_CLASS_ID 별로 레코드를 그룹화
# temp_grouped_info = {}
# for record in all_dag_info_records:
#     doc_id = record['DOC_CLASS_ID']
#     if doc_id not in grouped_info:
#         grouped_info[doc_id] = []
#     grouped_info[doc_id].append(record)
    
# # 그룹화된 DOC_CLASS_ID를 순회하며 유효성 검사
# for doc_id, records in temp_grouped_info.items():
#     is_valid_dag = True
    
#     # 해당 DOC_ID의 모든 섹션/레이아웃 레코드를 검사합니다.
#     for record in records:
#         # DB 쿼리 함수에서 가져온 INFO 컬럼 목록을 사용합니다.
#         for info_col in info_columns_list: 
#             info_data = record.get(info_col)
#             if not is_info_valid(info_data):
#                 is_valid_dag = False
#                 break
#         if not is_valid_dag:
#             break
            
#     if is_valid_dag:
#         grouped_info[doc_id] = records # 
#     else:
#         invalid_doc_ids.add(doc_id)
#         print(f"⚠️ Skipping DAG for DOC_CLASS_ID {doc_id}: Missing or empty INFO data.")

# # -------------------------------------------------------------
# # 4. 유효한 ID 목록을 순회하며 DAG를 생성합니다.
# for doc_id, doc_info_records in grouped_info.items():
#     # DAG ID 동적 생성
#     dag_id = f"문서서식_{doc_id}"
    
#     # 모든 DAG의 스케줄 동일
#     schedule = "@daily"

#     # 생성된 DAG 객체를 globals()에 추가하여 Airflow가 인식하게 합니다.
#     globals()[dag_id] = create_dag(
#         dag_id=dag_id,
#         schedule=schedule,
#         default_args=default_args,
#         task_definition_func=get_task_definitions, # 태스크 정의 함수 전달
#         doc_class_id=doc_id # DOC_CLASS_ID를 태스크 정의 함수에 전달
#     )

#     # print(f"✅ Created dynamic DAG: {dag_id}") # 디버깅용 로

