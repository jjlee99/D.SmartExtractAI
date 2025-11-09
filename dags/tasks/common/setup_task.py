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

#dag_id = 순환하지 않는 방향성있는 그래프로 표현한 워크플로우의 관리번호(속성: dag소스, 스케쥴 등)
#run_id = dag의 워크플로우에 따른 실행 인스턴스 관리번호(속성: 시작일시, 종료일시, 실행상태 등)
#task_id = 워크플로우의 개별 작업 단위(속성: task소스, 파라미터, 로그 등)
#target_id = 실행 인스턴스에서 동적으로 생성하여 작업할 대상 관리번호(속성: 파일경로, 클래스명, 전처리결과 등)
@task(pool='ocr_pool') 
def setup_runtime(**context):
    dag_id = context['dag'].dag_id
    run_id = context['dag_run'].run_id
    print(f"dag_id: {dag_id}")
    print(f"run_id: {run_id}")
    dococr_query_util.insert_map("insertRun",params=(dag_id,run_id))
    folder = Path(TEMP_FOLDER) / run_id
    folder.mkdir(parents=True, exist_ok=True)

# 이전 타스크가 일부만 성공한 경우엔 중단된 파일정보를 제거 후 다음 타스크로 넘김.
# 반복 사용을 위해 status는 제거.
@task(pool='ocr_pool',trigger_rule="all_done")
def get_success_results(preprocess_results:list[dict],**context):
    # 성공한 결과만 필터링하여 다음 단계로 넘김
    if preprocess_results is None:
        return []
    result_only_success = []
    for r in preprocess_results:
        if r and r.get("status") == "success":
            r.pop("status", None)  # status 키 제거
            result_only_success.append(r)
    return result_only_success

@task(pool='ocr_pool', trigger_rule="all_done")
def get_failed_results(preprocess_results:list[dict],**context):
    # 성공한 결과만 필터링하여 다음 단계로 넘김
    if preprocess_results is None:
        return []
    result_only_failed = []
    for r in preprocess_results:
        if r and r.get("status","") == "success":
            pass
        else:
            result_only_failed.append(r)
    return result_only_failed

@task(pool='ocr_pool') 
def end_runtime(msg="dag을 종료합니다.",**context):
    print(msg)
    dag_id = context['dag'].dag_id
    run_id = context['dag_run'].run_id
    dococr_query_util.update_map("updateRunEnd",params=("N",dag_id,run_id)) # N 대상없음
    return


@task(pool='ocr_pool') 
def complete_runtime(msg="완료되었습니다.",**context):
    print(msg)
    dag_id = context['dag'].dag_id
    run_id = context['dag_run'].run_id
    dococr_query_util.update_map("updateRunEnd",params=("C",dag_id,run_id)) # C 정상완료
    

def final_cleanup(**context):
    print("final_cleanup 호출됨")
    dag_id = context['dag'].dag_id
    run_id = context['dag_run'].run_id
    result = dococr_query_util.select_list_map("selectBreakRun",(dag_id,run_id,))
    if result:
        dococr_query_util.update_map("updateRunEnd",params=('E',dag_id,run_id,))
     
