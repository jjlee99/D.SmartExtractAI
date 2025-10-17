# def sample_task():
#     print("Hello, Airflow!")

# # 여러 DAG에서 동일한 태스크를 사용
# task_configs = {
#     "task_1": sample_task,
#     "task_2": sample_task
# }


import json
from airflow.operators.python import PythonOperator
from airflow.decorators import task, task_group
from airflow.models import Variable
from pathlib import Path

# --- 필요한 모듈 임포트 (기존 DAG 파일 참조) ---
# tasks/와 utils/db/ 경로가 Airflow PYTHONPATH에 설정되어 있어야 합니다.
# 경로는 사용자 환경에 맞게 조정해야 합니다.
from tasks.export_output_task import export_output_task
from tasks.translate_output_task import translate_output_task
# **주의:** dococr_query_util 임포트 경로가 맞는지 확인해야 합니다.
# 기존 DAG 파일 경로: from utils.db import dococr_query_util
import utils.db.dococr_query_util as dococr_query_util 
from tasks.file_task import get_file_info_list_task, copy_results_folder_task, clear_temp_folder_task, save_file_info_task
from tasks.ocr_task import ocr_task, aggregate_ocr_results_task
from tasks.img_preprocess_task import img_preprocess_task
from tasks.setup_task import failed_result_task, get_failed_results, setup_runtime, check_file_exists, setup_target_file_list, get_success_results, end_runtime, complete_runtime
from tasks.img_classify_task import img_classify_task, aggregate_classify_results_task


# Airflow Variable은 최상단에 정의합니다.
TEMP_FOLDER = Variable.get("TEMP_FOLDER", default_var="/opt/airflow/data/temp")
RESULT_FOLDER = Variable.get("RESULT_FOLDER", default_var="/opt/airflow/data/upload")
UPLOAD_FOLDER = Variable.get("UPLOAD_FOLDER", default_var="/opt/airflow/data/upload")


def get_task_definitions(doc_class_id, doc_info_records):
    """
    특정 DOC_CLASS_ID에 대한 전체 태스크 플로우를 정의하고 의존성을 설정합니다.
    이 함수는 factory.py의 with DAG(..) as dag: 컨텍스트 내에서 호출됩니다.
    """
    
    # doc_class_id는 문자열로 전달되므로, dococr_query_util 호출을 위해 튜플로 변환합니다.
    doc_class_id = (str(doc_class_id),)
    
    # 1. 초기 셋업
    t_img_classify_runtime_setup = setup_runtime()
    
    # 1. 작업 대상 파일 로드
    b_check_file_exists = check_file_exists(UPLOAD_FOLDER)
    t_no_file_end = end_runtime(f"[{doc_class_id}] 폴더 안에 파일이 존재하지 않습니다.")
    t_get_file_info_list = setup_target_file_list(UPLOAD_FOLDER)
    
    # 1-z. 실행순서
    t_img_classify_runtime_setup >> b_check_file_exists >> [t_no_file_end, t_get_file_info_list]


    # 2. 클래스 분류를 위한 각 클래스 전처리 작업
    # **수정: 쿼리에서 유효성 검사가 되었으므로, DB에서 LAYOUT 목록을 가져올 때 필터링 조건 추가 (선택적)
    # *참고: 여기서는 layout_list를 별도 쿼리 대신 doc_info_records에서 추출하여 사용합니다.
    # doc_info_records는 dynamic_gen_dag.py에서 유효성 검사된 레코드만 넘어왔다고 가정
    layout_list = dococr_query_util.select_list_map("selectLayoutList", doc_class_id)
    print( layout_list )
    class_classify_preprocess_infos = [json.loads(item["img_preprocess_info"]) for item in layout_list]
    print( class_classify_preprocess_infos )
    
    # processed_layout_list = []
    # class_classify_preprocess_infos = []
    
    # for item in layout_list:
    #     processed_item = item.copy() # 원본 보존을 위해 복사
    #     layout_class_id = item.get('LAYOUT_CLASS_ID', 'UNKNOWN')

    #     # 쿼리에서 이미 NULL 및 빈 문자열을 제외했으므로, 
    #     # 여기서는 JSON 로드만 수행합니다. (빈 값에 대한 예외 처리만 남김)
    #     preprocess_info = processed_item.get("IMG_PREPROCESS_INFO")
        
    #     # JSONDecodeError는 대비
    #     if preprocess_info and isinstance(preprocess_info, str):
    #         try:
    #             # 유효한 JSON 문자열만 로드하여 전처리 정보 리스트에 추가
    #             class_classify_preprocess_infos.append(json.loads(preprocess_info))
    #         except json.JSONDecodeError as e:
    #             # 쿼리 유효성 검사를 통과했더라도, 형식상의 오류가 있을 수 있으므로 경고만 출력하고 건너뜁니다.
    #             print(f"❌ 오류: LAYOUT_CLASS_ID={layout_class_id}의 전처리 정보가 유효한 JSON이 아닙니다. 이 정보를 건너뜁니다. 오류: {e}")
                
    #     processed_layout_list.append(processed_item) # 최종 리스트에 추가

    # # 최종적으로 처리된 리스트를 사용합니다.
    
    # 2-2. 분류 전처리 작업 실행
    t_classify_preprocess_task = img_preprocess_task.partial(
        step_infos=class_classify_preprocess_infos,
        target_key="_origin"
    ).expand(file_info=t_get_file_info_list)
    
    t_classify_preprc_failed_results = get_failed_results(t_classify_preprocess_task)
    t_classify_preprc_success_results = get_success_results(t_classify_preprocess_task)
    
    # 2-3. 실패 작업 처리
    t_fail_classify_preprc = failed_result_task.expand(file_info=t_classify_preprc_failed_results)

    # 2-y. 분류 전처리 후 결과 이미지 취합용
    t_classify_preprc_result = copy_results_folder_task(
        t_classify_preprc_success_results, 
        dest_folder=RESULT_FOLDER, 
        last_folder=str(doc_class_id) # doc_class_id를 폴더명으로 사용
    )

    # 2-z. 실행순서
    t_get_file_info_list >> t_classify_preprocess_task >> [t_classify_preprc_success_results, t_classify_preprc_failed_results]
    t_classify_preprc_failed_results >> t_fail_classify_preprc
    t_classify_preprc_success_results >> t_classify_preprc_result

    # 3. AI를 통한 분류
    t_img_classify_task = img_classify_task.partial(
        layout_list=layout_list,
        target_key=f"_result"
    ).expand(file_info=t_classify_preprc_success_results)
    
    t_img_classify_failed_results = get_failed_results(t_img_classify_task)
    t_img_classify_success_results = get_success_results(t_img_classify_task)

    # 3-3. 실패한 작업 처리
    t_fail_classify = failed_result_task.expand(file_info=t_img_classify_failed_results)
    
    # 3-4. 분류 AI 작업 취합 및 분석 후 클래스 결정
    t_classify_result_task = aggregate_classify_results_task(
        t_img_classify_success_results, 
        layout_list=layout_list
    )
    
    # 3-z. 실행순서
    t_classify_preprc_success_results >> t_img_classify_task >> [t_img_classify_success_results, t_img_classify_failed_results]
    t_img_classify_failed_results >> t_fail_classify
    t_img_classify_success_results >> t_classify_result_task

    # 4. OCR
    # OCR 이후 태스크들은 doc_class_id를 내부적으로 사용하지 않으므로 그대로 사용 가능
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
    
    # 5-2. 내보내기(DB화) - 이 태스크도 doc_class_id를 인자로 받아야 할 가능성이 높습니다.
    # export_output_task 함수 시그니처에 따라 수정이 필요할 수 있습니다. 
    # 현재는 doc_info (이전 태스크 결과)만 받도록 되어 있으므로, doc_info 내에 doc_class_id가 포함되어 있다고 가정합니다.
    t_export_output = export_output_task.expand(doc_info=t_complete_doc)
    
    # 5-z. 실행순서
    t_ocr_result_task >>  t_complete_doc >> t_export_output
    
    # 6. 완료 작업에 대해 번역 작업 (테이블 정보 고정)
    table_info = [
        {"table_name": "TB_OCR_BILD_BASIC_INFO","id_col_name": "BILD_SEQ_NUM"},
        {"table_name": "TB_OCR_FLR_STATUS","id_col_name": "FLR_SEQ_NUM"},
        {"table_name": "TB_OCR_OWN_STATUS","id_col_name": "OWNR_SEQ_NUM"},
    ]
    t_translate_output = translate_output_task.expand(table_info=table_info)

    t_export_output >> t_translate_output


    # Z. 템프폴더 제거
    all_clear_temp_folder_task = clear_temp_folder_task()
    
    # Z-z. 실행순서
    t_export_output >> all_clear_temp_folder_task
    
    # (생략된 태스크: t_save_file_info_task - 최종 완료 후 실행되는 것으로 간주하여 임시 제거)
    # t_export_output >> t_save_file_info_task