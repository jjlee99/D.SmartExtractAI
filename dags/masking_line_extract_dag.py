import json
import os,cv2
from airflow import DAG
from datetime import datetime
from airflow.models import Variable,XCom
from airflow.decorators import task
from utils.dev import draw_block_box_util
from utils.db import dococr_query_util
import pytesseract
import uuid
import shutil
from pathlib import Path
import numpy as np
from PIL import Image
from utils.ocr import separate_area_util
from utils.img.img_preprocess_util import img_preprocess_step_list
from tasks.img_preprocess_task import img_preprocess_task
from tasks.setup_task import failed_result_task, get_failed_results, setup_runtime, check_file_exists, setup_target_file_list, get_success_results, end_runtime, complete_runtime
from tasks.file_task import get_file_info_list_task,copy_results_folder_task, clear_temp_folder_task, save_file_info_task
from utils.com import file_util

BASE_DIR = Variable.get("DATA_FOLDER", default_var="/opt/airflow/data")
TEMP_FOLDER = Variable.get("TEMP_FOLDER", default_var="/opt/airflow/data/temp")
RESULT_FOLDER = Variable.get("RESULT_FOLDER", default_var="/opt/airflow/data/upload")


# 서식에 다른 분류 번호 지정(db로 입력?)
class_num_first = '5'
class_num_last = '11'

# TEMPLATE_FOLDER = Variable.get("TEMPLATE_FOLDER", default_var=f'opt/airflow/data/class/{class_num_first}/{class_num_last}/classify/template')

TEMPLATE_FOLDER = f'/opt/airflow/data/class/{class_num_first}/{class_num_last}/classify/template'

# 이미지 전처리(헤더 ocr, 표 선 검출) 후, 피처별 통계정보 추출하여 dict 형태로 데이터 생성
@task
def generating_masking_image(file_info: dict):
    """
    이미지에서 수평선과 수직선을 감지하고 해당 바운딩 박스를 반환합니다.

    Args:
        file_info (dict): Airflow XCom에서 전달받은 파일 정보 사전.

    Returns:
        tuple: (수평선 바운딩 박스 리스트, 수직선 바운딩 박스 리스트)
    """
    # 파일 경로 딕셔너리에서 실제 경로(문자열)를 추출. 이중 딕셔너리 형태.
    image_path = file_info['file_path']['_result'] 
    print(image_path)
    iter_save = True
    image = Image.open(image_path).convert("RGB")
    image_width, image_height = image.size

    cv_image = np.array(image)
    if len(cv_image.shape) == 3:
        cv_image = cv2.cvtColor(cv_image, cv2.COLOR_RGB2GRAY)
    _, binary = cv2.threshold(cv_image, 180, 255, cv2.THRESH_BINARY_INV)

        
    horizontal_kernel_ratio = 0.07
    vertical_kernel_ratio = 0.03
    horizontal_kernel_size = max(1, int(image_width * horizontal_kernel_ratio))
    horizontal_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (horizontal_kernel_size, 1))
    detect_horizontal = cv2.morphologyEx(binary, cv2.MORPH_OPEN, horizontal_kernel, iterations=1)
    # 코드 실행 시, 수평선이 매우 길게 확장됨
    # detect_horizontal = cv2.morphologyEx(detect_horizontal, cv2.MORPH_CLOSE, horizontal_kernel, iterations=1)

    vertical_kernel_size = max(1, int(image_height * vertical_kernel_ratio))
    vertical_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (1, vertical_kernel_size))
    detect_vertical = cv2.morphologyEx(binary, cv2.MORPH_OPEN, vertical_kernel, iterations=1)

    combined_lines = cv2.bitwise_or(detect_horizontal, detect_vertical)
    # 박스 정규화
    # contours_combined, _ = cv2.findContours(combined_lines, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    # for cnt in contours_combined:
    #     x, y, w, h = cv2.boundingRect(cnt)
    #     bbox = (x, y, x + w, y + h)
    #     norm_bbox3 = normalize_bbox(bbox, image_width, image_height)
    if iter_save:
        inverted_combined = cv2.bitwise_not(combined_lines)
        separate_area_util.separate_area_step_list(inverted_combined, data_type='np_bgr', output_type='np_bgr',
            step_list=[{"name":"save","param":{"save_key":"_combined_contour","tmp_save":True}}], result_map={"folder_path":"line_export"}) 
        save_folder = Path(TEMPLATE_FOLDER)
        save_folder.mkdir(parents=True, exist_ok=True) # 폴더가 없으면 생성
        save_path = save_folder / f"{class_num_first}_{class_num_last}_masking_image.png"
        cv2.imwrite(str(save_path), inverted_combined)

        # file_util.file_copy(image_path, Path(TEMPLATE_FOLDER) / "line_export" / "original.png")
    return file_info

# DAG 정의 (DAG 클래스 직접 사용)
with DAG(
    dag_id="image_masking_v1", # 이전 DAG ID와 충돌 방지를 위해 변경
    start_date=datetime(2024, 1, 1),
    schedule=None, # None으로 설정하면 수동 트리거만 가능
    catchup=False,
    tags=['image', 'masking']
) as dag:
    img_masking_runtime_setup = setup_runtime()
    b_check_file_exists = check_file_exists(TEMPLATE_FOLDER)
    no_file_end = end_runtime("폴더 안에 파일이 존재하지 않습니다.")

    
    get_file_info_list = setup_target_file_list(TEMPLATE_FOLDER)
    # 1-z. 실행순서
    img_masking_runtime_setup >> b_check_file_exists >> [no_file_end,get_file_info_list]
    
    # 2. 클래스 분류를 위한 각 클래스 전처리 작업
    # 2-0. 클래스 지정. (나중에 클래스 목록 가져오는 함수로 변경)
    layout_list = dococr_query_util.select_list_map("selectLayoutList", "5")
    class_keys = [str(item["layout_class_id"]) for item in layout_list]
    #for layout_info in layout_list:
    # if layout_list:
    layout_info = layout_list[0]
    layout_class_id = layout_info["layout_class_id"]
    layout_name = layout_info["layout_name"]
    doc_class_id = layout_info["doc_class_id"]
    class_preprocess_info = json.loads(layout_info["img_preprocess_info"])
    class_ai_info = json.loads(layout_info["classify_ai_info"])

    # 2-2. 분류 전처리 작업 실행
    preprocess_task = img_preprocess_task.partial(step_info=class_preprocess_info,target_key="_origin").expand(file_info=get_file_info_list)
    preprc_success_results = get_success_results(preprocess_task)

     # 2-y. 분류 전처리 후 결과 이미지 취합용(분류 작업 확인용)
    preprc_result = copy_results_folder_task(preprc_success_results, dest_folder=RESULT_FOLDER, last_folder=layout_name)


    #실행순서
    get_file_info_list >> preprocess_task >> [preprc_success_results]
    preprc_success_results >> preprc_result


    # 이미지 상의 선 그리기 (마스킹 이미지 생성)
    masking_task = generating_masking_image.partial().expand(file_info=preprc_success_results)
    # masking_success_results = get_success_results(masking_task)


    preprc_result >> masking_task
