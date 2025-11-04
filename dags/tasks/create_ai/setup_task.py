from airflow.decorators import task
from pathlib import Path
import shutil, os
from airflow.models import Variable
import uuid, json
from utils.com import json_util
from pdf2image import convert_from_path
from datetime import datetime
from utils.db import dococr_query_util
from utils.com import file_util

TEMP_FOLDER = Variable.get("TEMP_FOLDER", default_var="/opt/airflow/data/temp")
RESULT_FOLDER = Variable.get("RESULT_FOLDER", default_var="/opt/airflow/data/result")
FAILED_FOLDER = Variable.get("FAILED_FOLDER", default_var="/opt/airflow/data/failed")
COMPLETE_FOLDER = Variable.get("COMPLETE_FOLDER", default_var="/opt/airflow/data/complete")
OCR_RESULT_FOLDER = Variable.get("OCR_RESULT_FOLDER", default_var="/opt/airflow/data/class/a_class/ocr")

#dag_id = 순환하지 않는 방향성있는 그래프로 표현한 워크플로우의 관리번호(속성: dag소스, 스케쥴 등)
#run_id = dag의 워크플로우에 따른 실행 인스턴스 관리번호(속성: 시작일시, 종료일시, 실행상태 등)
#task_id = 워크플로우의 개별 작업 단위(속성: task소스, 파라미터, 로그 등)
#target_id = 실행 인스턴스에서 동적으로 생성하여 작업할 대상 관리번호(속성: 파일경로, 클래스명, 전처리결과 등)

#학습 대상이 있는지 확인, 있으면 setup_create_ai, 없으면 end_runtime
@task.branch
def check_next_layout_info(**context):
    layout_info = dococr_query_util.select_row_map("selectNextLayoutInfo")
    if layout_info:
        return "setup_next_layout_info"
    return "end_runtime"

@task(pool='ocr_pool') 
def setup_next_layout_info(**context):
    run_id = context['dag_run'].run_id
    layout_info = dococr_query_util.select_row_map("selectNextLayoutInfo")
    id = str(uuid.uuid4())
    param = (run_id,id,json.dumps(layout_info,ensure_ascii=False))
    dococr_query_util.insert_map("insertTargetFile", params=param)
    param = (layout_info["create_id"],)
    dococr_query_util.update_map("updateCreateStart", params=param)
    return layout_info



def final_cleanup(**context):
    dag_id = context['dag'].dag_id
    run_id = context['dag_run'].run_id
    # 완료되지 않은 런 상태를 E로 업데이트
    result = dococr_query_util.select_list_map("selectBreakRun",(dag_id,run_id,))
    if result:
        dococr_query_util.update_map("updateRunEnd",params=('E',dag_id,run_id,))
    
    # 완료되지 않은 크리에이트 상태를 E로 업데이트
    # 다음 크리에이트 정보 등록
    result = dococr_query_util.select_list_map("selectBreakCreate")
    for create_info in result:
        dococr_query_util.update_map("updateCreateEnd",params=('E',str(create_info.get("create_id")),))
        dococr_query_util.insert_map("insertCreateUnready",params=(str(create_info.get("doc_class_id")), str(create_info.get("layout_class_id")),))
    dococr_query_util.update_map("updateCreateReady")
     
