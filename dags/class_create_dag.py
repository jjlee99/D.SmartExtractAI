import json
from airflow import DAG
from airflow.decorators import task, task_group
from datetime import datetime
from utils.com import json_util
from utils.db import dococr_query_util
from tasks.class_create_task import balance_false_images, build_balanced_dataset, train, image_data_augment, generating_masking_image
from tasks.file_task import get_file_info_list_task, copy_results_folder_task
from tasks.setup_task import setup_runtime, check_file_exists, setup_target_file_list, end_runtime
from tasks.img_preprocess_task import img_preprocess_task
from airflow.models import Variable,XCom
from utils.com import file_util


NONE_DOC_IMAGE_DIR = Variable.get("NONE_CLASS_FOLDER", default_var="/opt/airflow/data/common/none_class") # 비서식 일반 문서 이미지
# 경로 설정 (DAG 파라미터로 받거나 환경변수로 설정 가능)
DATA_DIR = "/opt/airflow/data"   # 루트

with DAG(
    dag_id='create_document_classifier_balanced_V0.1',
    start_date=datetime(2024, 1, 1),
    schedule=None,  # None으로 설정하면 수동 트리거만 가능
    catchup=False,
    tags=['document', 'classification', 'balanced']
) as dag:
    # 1. 작업 준비
    # 1-0. 초기화
    t_class_create_setup_runtime = setup_runtime()
    
    # 1-1. AI 생성 작업 정보 로드
    target_layout_class_id = "9"
    d_layout_info = dococr_query_util.select_row_map("selectLayoutInfo",(target_layout_class_id,))
    
    # for d_layout_list_item in d_layout_list:
    #     if target_method == "in": # 목록 안의 대상만 작업
    #         if not(d_doc_class_id in target_doc_class_ids and d_layout_class_id in target_layout_class_ids): 
    #             continue
    #     elif target_method == "not in": # 목록에 없는 대상만 작업
    #         if not(d_doc_class_id not in target_doc_class_ids and d_layout_class_id not in target_layout_class_ids):
    #             continue
    #     elif target_method == "all": # 모든 대상 작업
    #         pass
    #     d_layout_info = d_layout_list_item
    d_doc_class_id = d_layout_info.get("doc_class_id")
    d_layout_class_id = d_layout_info.get("layout_class_id")
    d_template_file_path = d_layout_info.get("template_file_path")
    d_classify_preprocess_step_info = json.loads(d_layout_info["img_preprocess_info"])
    d_layout_dir = f"{DATA_DIR}/class/{d_doc_class_id}/{d_layout_class_id}"
    #json_util.save(f"{d_layout_dir}/layout_list.json", d_layout_info, duplicate_policy="skip")  # 레이아웃 정보 저장
    d_classify_dir = f"{d_layout_dir}/classify"  # 클래스 디렉토리
    d_origin_true_imgae_dir = f"{d_classify_dir}/origin/true"   # 원본 문서 이미지
    d_origin_false_imgae_dir = f"{d_classify_dir}/origin/false"   # 일반 문서 이미지
    d_ready_image_dir = f"{d_classify_dir}/ready"
    d_ready_true_image_dir = f"{d_ready_image_dir}/true"   # 특정 서식 증강된 문서 이미지
    d_ready_false_image_dir = f"{d_ready_image_dir}/false" # 일반 증강된 문서 이미지
    d_preprc_true_image_dir = f"{d_classify_dir}/preprc/true"   # 특정 서식 전처리된 문서 이미지
    d_preprc_false_image_dir = f"{d_classify_dir}/preprc/false" # 일반 전처리된 문서 이미지
    
    augskip = False
    if augskip:
        augment_threshold = 1
    else:
        augment_threshold = 10
    
    # 1-1. 증강 처리를 통해 파일 개수 증가
    t_true_image_data_augment = image_data_augment(d_origin_true_imgae_dir,d_ready_true_image_dir, threshold=augment_threshold, aug_limit=augment_threshold)
    t_false_image_data_augment = image_data_augment(d_origin_false_imgae_dir,d_ready_false_image_dir, threshold=augment_threshold, aug_limit=3)
    
    # 1-2. true 파일 개수에 맞춰 부족한 false 파일 채우기
    t_balance_false_images = balance_false_images(d_ready_image_dir)
    
    # 1.z. 실행순서
    t_class_create_setup_runtime >> [t_true_image_data_augment,t_false_image_data_augment] >> t_balance_false_images


    # 2. AI에게 학습할 이미지 전처리 작업
    # 2-1. True 이미지 전처리
    true_file_info_list_task = setup_target_file_list(d_ready_true_image_dir)
    true_file_list_task = img_preprocess_task.partial(step_info=d_classify_preprocess_step_info,target_key="_origin").expand(file_info=true_file_info_list_task)
    true_copy_results_task = copy_results_folder_task(true_file_list_task,dest_folder=d_preprc_true_image_dir,target_key="_result")
    
    # 2-2. False 이미지 전처리
    false_file_info_list_task = get_file_info_list_task(d_ready_false_image_dir)
    false_file_list_task = img_preprocess_task.partial(step_info=d_classify_preprocess_step_info,target_key="_origin").expand(file_info=false_file_info_list_task)
    false_copy_results_task = copy_results_folder_task(false_file_list_task,dest_folder=d_preprc_false_image_dir,target_key="_result")

    # 2-z. 실행순서
    t_balance_false_images >> [true_file_info_list_task, false_file_info_list_task]
    true_file_info_list_task >> true_file_list_task >> true_copy_results_task
    false_file_info_list_task >> false_file_list_task >> false_copy_results_task
    
    t_mask_extract = generating_masking_image(d_preprc_true_image_dir,layout_info=d_layout_info)
    # 3. 균형이 맞춰진 데이터셋 구성
    t_build_dataset = build_balanced_dataset(d_preprc_true_image_dir,d_preprc_false_image_dir)
    
    # 3-z. 실행순서
    [true_copy_results_task,false_copy_results_task] >> t_build_dataset
    true_copy_results_task >> t_mask_extract


    # 4. 모델 학습
    t_train = train(t_build_dataset,layout_info=d_layout_info)

    # 4-z. 실행순서
    t_build_dataset >> t_train