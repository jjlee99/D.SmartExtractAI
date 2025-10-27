from airflow import DAG
from datetime import datetime
from tasks.common.file_task import clear_temp_folder_task, save_file_info_task
from tasks.common.setup_task import final_cleanup, setup_runtime, get_success_results, get_failed_results, end_runtime
from tasks.doc_ocr.export_output_task import export_output_task
from tasks.doc_ocr.translate_output_task import translate_output_task
from tasks.doc_ocr.ocr_task import ocr_task, aggregate_ocr_results_task
from tasks.doc_ocr.setup_task import check_file_exists, setup_target_file_list, setup_layout_list_task, failed_result_task, setup_table_list, complete_runtime
from tasks.doc_ocr.img_preprocess_task import img_preprocess_task 
from tasks.doc_ocr.img_classify_task import img_classify_task, aggregate_classify_results_task

# DAG 정의 (DAG 클래스 직접 사용)
with DAG(
    dag_id="dococr_5", # 이전 DAG ID와 충돌 방지를 위해 변경
    description="일반건축물대장<5> 데이터 추출 프로세스",
    start_date=datetime(2024, 1, 1),
    schedule=None, # None으로 설정하면 수동 트리거만 가능
    catchup=False,
    on_failure_callback= final_cleanup,
    tags=[]
) as dag:
    t_img_classify_runtime_setup = setup_runtime()
    doc_class_id = "5"
    # 1. 작업 대상 파일 로드
    b_check_file_exists = check_file_exists()
    t_no_file_end = end_runtime("폴더 안에 파일이 존재하지 않습니다.")
    t_get_file_info_list = setup_target_file_list()
    # 1-z. 실행순서
    t_img_classify_runtime_setup >> b_check_file_exists >> [t_no_file_end,t_get_file_info_list]
    
    # 2. 클래스 분류를 위한 각 클래스 전처리 작업
    # 2-1. 클래스 지정. (나중에 클래스 목록 가져오는 함수로 변경)
    t_layout_list = setup_layout_list_task(doc_class_id)
    
    # 2-2. 분류 전처리 작업 실행
    t_classify_preprocess_task = img_preprocess_task.partial(layout_list=t_layout_list,target_key="_origin").expand(file_info=t_get_file_info_list)
    t_classify_preprc_failed_results = get_failed_results.override(task_id="classify_preprc_failed_results")(t_classify_preprocess_task)
    t_classify_preprc_success_results = get_success_results.override(task_id="classify_preprc_success_results")(t_classify_preprocess_task)
    
    # 2-3. 실패 작업 처리
    t_fail_classify_preprc = failed_result_task.override(task_id="fail_classify_preprc").expand(file_info=t_classify_preprc_failed_results)

    # 2-y. 분류 전처리 후 결과 이미지 취합용(분류 작업 확인용)
    #t_classify_preprc_result = copy_results_folder_task(t_classify_preprc_success_results, dest_folder=RESULT_FOLDER, last_folder=doc_class_id)

    # 2-z. 실행순서
    t_get_file_info_list >> t_layout_list >> t_classify_preprocess_task >> [t_classify_preprc_success_results, t_classify_preprc_failed_results]
    t_classify_preprc_failed_results >> t_fail_classify_preprc
    #t_classify_preprc_success_results >> t_classify_preprc_result

    # 3. AI를 통한 분류
    # 3-2. 분류 AI 작업 실행
    t_img_classify_task = img_classify_task.partial(layout_list=t_layout_list,target_key=f"_result").expand(file_info=t_classify_preprc_success_results) #_result : 이전 작업 결과
    t_img_classify_failed_results = get_failed_results.override(task_id="img_classify_failed_results")(t_img_classify_task)
    t_img_classify_success_results = get_success_results.override(task_id="img_classify_success_results")(t_img_classify_task)

    # 3-3. 실패한 작업 처리
    t_fail_classify = failed_result_task.override(task_id="fail_classify").expand(file_info=t_img_classify_failed_results)
    
    # 3-4. 분류 AI 작업 취합 및 분석 후 클래스 결정
    t_classify_result_task = aggregate_classify_results_task(t_img_classify_success_results)
    
    
    # 3-z. 실행순서
    t_classify_preprc_success_results >> t_img_classify_task >> [t_img_classify_success_results, t_img_classify_failed_results]
    t_img_classify_failed_results >> t_fail_classify
    t_img_classify_success_results >> t_classify_result_task

    # 4. OCR
    t_ocr_task = ocr_task.partial(target_key="_normalize").expand(file_info=t_classify_result_task)
    t_ocr_failed_results = get_failed_results.override(task_id="ocr_failed_results")(t_ocr_task)
    t_ocr_success_results = get_success_results.override(task_id="ocr_success_results")(t_ocr_task)
    
    # 4-2. 실패한 작업 처리
    t_fail_ocr = failed_result_task.override(task_id="fail_ocr").expand(file_info=t_ocr_failed_results)

    # 4-3. OCR 후 취합 작업
    t_ocr_result_task = aggregate_ocr_results_task(t_ocr_success_results)

    t_classify_result_task >> t_ocr_task >> [t_ocr_success_results,t_ocr_failed_results]
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
    t_setup_table_list = setup_table_list.expand(doc_info=t_export_output)
    t_translate_output = translate_output_task.expand(table_list=t_setup_table_list)

    t_export_output >> t_setup_table_list >> t_translate_output

    # Y. 저장
    t_save_file_info_task = save_file_info_task.partial(save_type="result").expand(file_info=t_ocr_failed_results)
    
    # Y-z. 실행순서
    #t_export_output_task >> t_save_file_info_task


    # Z. 템프폴더 제거
    all_clear_temp_folder_task = clear_temp_folder_task()
    
    # Z-z. 실행순서
    t_export_output >> all_clear_temp_folder_task
