import json
from airflow import DAG
from airflow.decorators import task, task_group
from datetime import datetime
from utils.com import json_util
from utils.db import dococr_query_util
from tasks.create_ai.class_create_task import build_balanced_dataset, train, image_data_augment, generating_masking_image, complete_runtime
from tasks.create_ai.img_preprocess_task import img_normalize_task
from tasks.create_ai.setup_task import check_next_layout_info, final_cleanup, setup_next_layout_info
from tasks.common.setup_task import setup_runtime, end_runtime
from airflow.models import Variable,XCom
from utils.com import file_util


NONE_DOC_IMAGE_DIR = Variable.get("NONE_CLASS_FOLDER", default_var="/opt/airflow/data/common/none_class") # 비서식 일반 문서 이미지
# 경로 설정 (DAG 파라미터로 받거나 환경변수로 설정 가능)
DATA_DIR = "/opt/airflow/data"   # 루트

with DAG(
    dag_id='create_classify_ai_V0.1',
    description="문서 분류 AI 생성 프로세스",
    start_date=datetime(2024, 1, 1),
    schedule="@weekly",  # None으로 설정하면 수동 트리거만 가능
    catchup=False,
    on_failure_callback=final_cleanup,
    tags=['document', 'classify', 'ai', 'augmentation', 'weekly']
) as dag:
    # 1. 작업 준비
    # 1-0. 초기화
    t_class_create_setup_runtime = setup_runtime()
    
    # 1. 작업 대상 정보 로드
    t_check_next_layout_info = check_next_layout_info()
    t_no_target_end = end_runtime("생성할 AI 정보가 존재하지 않습니다.")
    t_layout_info = setup_next_layout_info()
    # 1-z. 실행순서
    t_class_create_setup_runtime >> t_check_next_layout_info >> [t_no_target_end,t_layout_info]
    

    # 1-1. 증강 처리를 통해 데이터셋 생성
    t_image_data_augment = image_data_augment(t_layout_info)

    # 1.z. 실행순서
    t_class_create_setup_runtime >> t_image_data_augment

    # 2. AI에게 학습할 이미지 전처리 작업
    # 2-1. True 이미지 전처리
    t_img_normalize_task = img_normalize_task(t_layout_info)
    
    
    t_mask_extract = generating_masking_image(layout_info=t_layout_info)
    # 3. 균형이 맞춰진 데이터셋 구성
    t_build_dataset = build_balanced_dataset(layout_info=t_layout_info)
    
    # 3-z. 실행순서
    t_image_data_augment >> t_img_normalize_task >> [t_build_dataset,t_mask_extract]
    
    # 4. 모델 학습
    t_train = train(t_build_dataset,layout_info=t_layout_info)

    # 4-z. 실행순서
    t_build_dataset >> t_train

    # 5. 완료처리
    t_complete = complete_runtime(t_train,layout_info=t_layout_info)

    # 4-z. 실행순서
    t_train >> t_complete