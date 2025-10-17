from airflow.decorators import task
from pathlib import Path
import shutil, os
from airflow.models import Variable
import uuid, json

from utils.db import dococr_query_util

TEMP_FOLDER = Variable.get("TEMP_FOLDER", default_var="/opt/airflow/data/temp")
RESULT_FOLDER = Variable.get("RESULT_FOLDER", default_var="/opt/airflow/data/result")

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

#폴더 안에 파일이 있는지 분기 있으면 setup_target_file_list, 없으면 no_file_task
@task.branch
def check_file_exists(folder_path):
    p = Path(folder_path)
    for f in p.rglob("*"):
        if f.is_file():
            return "setup_target_file_list"
    return "end_runtime"

@task(pool='ocr_pool') 
def setup_target_file_list(folder_path:str,**context):
    run_id = context['dag_run'].run_id
    p = Path(folder_path)
    files = [str(f) for f in p.rglob("*") if f.is_file()]
    file_info_list = []
    db_params_list = []
    for path in files:
        id = str(uuid.uuid4())
        content = {"file_id":id, "file_path":{"_origin":path}}
        file_info_list.append(content)
        db_params_list.append( (run_id,id,json.dumps(content)) )
    dococr_query_util.insert_map("insertTargetFile", params=db_params_list)
    return file_info_list

@task(pool='ocr_pool') 
def end_runtime(msg="dag을 종료합니다.",**context):
    print(msg)
    dag_id = context['dag'].dag_id
    run_id = context['dag_run'].run_id
    dococr_query_util.update_map("updateRunEnd",params=(dag_id,run_id))
    return