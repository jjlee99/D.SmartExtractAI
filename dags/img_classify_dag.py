import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task, task_group
from datetime import datetime
from pathlib import Path
from airflow.models import Variable,XCom
# utils 모듈 임포트 (PYTHONPATH에 proj
# ect_root가 잡혀있다고 가정)
# 만약 utils 모듈이 DAG 파일과 같은 디렉토리 내에 있다면, 상대 경로 임포트를 고려하거나
# Airflow DAGs 폴더 구조에 맞게 배치해야 합니다.
# 예: dags/your_dag_file.py, dags/utils/file_util.py
from tasks.export_output_task import export_output_task
from tasks.translate_output_task import translate_output_task
from utils.db import dococr_query_util
from utils.com import file_util
from tasks.ocr_task import ocr_task, aggregate_ocr_results_task
from tasks.file_task import get_file_info_list_task,copy_results_folder_task, clear_temp_folder_task, save_file_info_task
from tasks.img_preprocess_task import img_preprocess_task
from tasks.setup_task import failed_result_task, get_failed_results, setup_runtime, check_file_exists, setup_target_file_list, get_success_results, end_runtime, complete_runtime
from tasks.img_classify_task import img_classify_task, aggregate_classify_results_task


TEMP_FOLDER = Variable.get("TEMP_FOLDER", default_var="/opt/airflow/data/temp")
RESULT_FOLDER = Variable.get("RESULT_FOLDER", default_var="/opt/airflow/data/upload")
UPLOAD_FOLDER = Variable.get("UPLOAD_FOLDER", default_var="/opt/airflow/data/upload")

# DAG 정의 (DAG 클래스 직접 사용)
with DAG(
    dag_id="image_classify_v1.1", # 이전 DAG ID와 충돌 방지를 위해 변경
    start_date=datetime(2024, 1, 1),
    schedule=None, # None으로 설정하면 수동 트리거만 가능
    catchup=False,
    tags=['image', 'batch']
) as dag:
    t_img_classify_runtime_setup = setup_runtime()
    doc_class_id = "5"
    # 1. 작업 대상 파일 로드
    b_check_file_exists = check_file_exists(UPLOAD_FOLDER)
    t_no_file_end = end_runtime("폴더 안에 파일이 존재하지 않습니다.")
    t_get_file_info_list = setup_target_file_list(UPLOAD_FOLDER)
    # 1-z. 실행순서
    t_img_classify_runtime_setup >> b_check_file_exists >> [t_no_file_end,t_get_file_info_list]


    # 2. 클래스 분류를 위한 각 클래스 전처리 작업
    # 2-0. 클래스 지정. (나중에 클래스 목록 가져오는 함수로 변경)
    layout_list = dococr_query_util.select_list_map("selectLayoutList",doc_class_id)
    class_classify_preprocess_infos = [json.loads(item["img_preprocess_info"]) for item in layout_list]
    
    # 2-2. 분류 전처리 작업 실행
    t_classify_preprocess_task = img_preprocess_task.partial(step_infos=class_classify_preprocess_infos,target_key="_origin").expand(file_info=t_get_file_info_list)
    t_classify_preprc_failed_results = get_failed_results(t_classify_preprocess_task)
    t_classify_preprc_success_results = get_success_results(t_classify_preprocess_task)
    
    # 2-3. 실패 작업 처리
    t_fail_classify_preprc = failed_result_task.expand(file_info=t_classify_preprc_failed_results)

    # 2-y. 분류 전처리 후 결과 이미지 취합용(분류 작업 확인용)
    t_classify_preprc_result = copy_results_folder_task(t_classify_preprc_success_results, dest_folder=RESULT_FOLDER, last_folder=doc_class_id)

    # 2-z. 실행순서
    t_get_file_info_list >> t_classify_preprocess_task >> [t_classify_preprc_success_results, t_classify_preprc_failed_results]
    t_classify_preprc_failed_results >> t_fail_classify_preprc
    t_classify_preprc_success_results >> t_classify_preprc_result

    # 3. AI를 통한 분류
    # 3-2. 분류 AI 작업 실행
    t_img_classify_task = img_classify_task.partial(layout_list=layout_list,target_key=f"_result").expand(file_info=t_classify_preprc_success_results) #_result : 이전 작업 결과
    t_img_classify_failed_results = get_failed_results(t_img_classify_task)
    t_img_classify_success_results = get_success_results(t_img_classify_task)

    # 3-3. 실패한 작업 처리
    t_fail_classify = failed_result_task.expand(file_info=t_img_classify_failed_results)
    
    # 3-4. 분류 AI 작업 취합 및 분석 후 클래스 결정
    t_classify_result_task = aggregate_classify_results_task(t_img_classify_success_results, layout_list=layout_list)
    
    
    # 3-z. 실행순서
    t_classify_preprc_success_results >> t_img_classify_task >> [t_img_classify_success_results, t_img_classify_failed_results]
    t_img_classify_failed_results >> t_fail_classify
    t_img_classify_success_results >> t_classify_result_task

    # 4. OCR
    t_ocr_dispatcher_task = ocr_task.partial(target_key="_normalize").expand(file_info=t_classify_result_task)
    t_ocr_failed_results = get_failed_results(t_ocr_dispatcher_task)
    t_ocr_success_results = get_success_results(t_ocr_dispatcher_task)
    
    # 4-2. 실패한 작업 처리
    t_fail_ocr = failed_result_task.expand(file_info=t_ocr_failed_results)

    # 4-3. OCR 후 취합 작업
    t_ocr_result_task = aggregate_ocr_results_task(t_ocr_success_results)

    
    
    t_classify_result_task >> t_ocr_dispatcher_task >> [t_ocr_success_results,t_ocr_failed_results]
    t_ocr_failed_results >> t_fail_ocr
    t_ocr_success_results >> t_ocr_result_task

    # 5. 작업 완료
    # 5-1. 완료처리
    t_complete_doc = complete_runtime.expand(doc_info=t_ocr_result_task)
    
    # 5-2. 내보내기(DB화)
    t_export_output = export_output_task.expand(doc_info=t_complete_doc)
    
    # 5-z. 실행순서
    t_ocr_result_task >>  t_complete_doc >> t_export_output
    
    # 6. 완료 작업에 대해 번역 작업

    table_info = [
        {"table_name": "TB_OCR_BILD_BASIC_INFO","id_col_name": "BILD_SEQ_NUM"},
        {"table_name": "TB_OCR_FLR_STATUS","id_col_name": "FLR_SEQ_NUM"},
        {"table_name": "TB_OCR_OWN_STATUS","id_col_name": "OWNR_SEQ_NUM"},
    ]
    t_translate_output = translate_output_task.expand(table_info=table_info)

    t_export_output >> t_translate_output


    # Y. 저장
    t_save_file_info_task = save_file_info_task.partial(save_type="result").expand(file_info=t_ocr_failed_results)
    
    # Y-z. 실행순서
    #t_export_output_task >> t_save_file_info_task


    # Z. 템프폴더 제거
    all_clear_temp_folder_task = clear_temp_folder_task()
    
    # Z-z. 실행순서
    t_export_output >> all_clear_temp_folder_task

# Airflow 2.x에서 Python 스크립트 직접 실행 시에는 DAG가 파싱만 됩니다.
# 실제 테스트는 Airflow CLI를 통해 실행해야 합니다.
if __name__ == "__main__":
    print("이 스크립트를 직접 실행하면 DAG가 Airflow에 로드됩니다.")
    print("DAG를 테스트하려면 Airflow CLI 명령을 사용하세요.")
    print("예: airflow dags test image_processing_per_file_v2 2024-01-01")
    # 주의: 이 부분은 Airflow 2.x에서는 dag_instance를 직접 호출하여 테스트하는 방식이 아닙니다.
    # DAG 파일은 단순히 Airflow 스케줄러/웹서버가 읽고 파싱할 수 있도록 존재합니다.
    