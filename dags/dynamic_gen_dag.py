from datetime import datetime
from dag_factory.factory import create_dag
from dag_factory.task_definitions import get_task_definitions
from utils.db import dococr_query_util

# 기본 인자 설정
default_args = {
    "owner": "airflow", 
    "start_date": datetime(2024, 1, 1),
    # "catchup": False,
}


# -------------------------------------------------------------
# 1. DB에서 모든 섹션/레이아웃 INFO 정보를 조회하고, INFO 컬럼 목록을 가져옵니다.
all_dag_info_records, info_columns_list = dococr_query_util.select_doc_class_id_list()

# 2. DOC_CLASS_ID별로 그룹화합니다.
grouped_info = {}

# DOC_CLASS_ID 별로 레코드를 그룹화 (유효성 검사된 레코드만 포함됨)
for record in all_dag_info_records:
    doc_id = record['DOC_CLASS_ID']
    if doc_id not in grouped_info:
        grouped_info[doc_id] = []
    grouped_info[doc_id].append(record)
    
# DB 쿼리에서 유효성 검사가 완료되었으므로, 
# 'invalid_doc_ids' 로직 및 'is_info_valid' 함수는 제거합니다.

# -------------------------------------------------------------
# 3. 유효한 ID 목록을 순회하며 DAG를 생성합니다.
for doc_id, doc_info_records in grouped_info.items():
    # DAG ID 동적 생성
    dag_id = f"문서서식_{doc_id}"
    
    # 모든 DAG의 스케줄 동일
    schedule = None

    # 생성된 DAG 객체를 globals()에 추가하여 Airflow가 인식하게 합니다.
    globals()[dag_id] = create_dag(
        dag_id=dag_id,
        schedule=schedule,
        default_args=default_args,
        task_definition_func=get_task_definitions, # 태스크 정의 함수 전달
        doc_class_id=doc_id,
        #  doc_info_records=doc_info_records # DOC_CLASS_ID를 태스크 정의 함수에 전달
        doc_info_records=None
    )
    
    # print(f"✅ Created dynamic DAG: {dag_id}") # 디버깅용 로그