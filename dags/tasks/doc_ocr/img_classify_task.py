import os
from airflow.decorators import task
from collections import Counter
from pathlib import Path
from PIL import Image
import cv2
import numpy as np
import json
from typing import Any,List
import uuid
from utils.ai import classify_ai_util
from utils.ai import classify_string_util
from utils.com import create_step_info_util, file_util, json_util

from utils.ocr import separate_area_util, ocr_cleansing_util, ocr_util
from utils.db import dococr_query_util
from utils.img import type_convert_util
from airflow.models import Variable,XCom
from datetime import datetime
import pytesseract
    
RESULT_FOLDER = Variable.get("RESULT_FOLDER", default_var="/opt/airflow/data/result")
TEMP_FOLDER = Variable.get("TEMP_FOLDER", default_var="/opt/airflow/data/temp")
CLASS_FOLDER = Variable.get("CLASS_FOLDER", default_var="/opt/airflow/data/class")

@task(pool='ocr_pool') 
def img_classify_task(file_info: dict, layout_list:list, target_key: str, **context) -> dict:
    """ai_info로 LiLT 모델로 이미지 분류 결과를 file_info에 저장하여 반환"""
    data = file_info["file_path"][target_key]
    classify_map = {}
    if isinstance(data,list):
        if len(data) != len(layout_list):
            file_info["status"] = "error"
            file_info["status_msg"] = f"[img_classify_task]'{target_key}'의 길이({len(data)})가 layout_list의 길이({len(layout_list)})와 일치하지 않습니다."
        else:
            for i, layout_info in enumerate(layout_list):
                target_file_path = data[i]
                mask_file_path = Path(CLASS_FOLDER)/str(layout_info.get('doc_class_id'))/str(layout_info.get('layout_class_id'))/"classify"/"asset"/"mask_image.png"
                if mask_file_path.exists():
                    mask = cv2.imread(mask_file_path, cv2.IMREAD_GRAYSCALE)
                    mask_inv = cv2.bitwise_not(mask)
                    mask_h, mask_w = mask_inv.shape
                    original_img = cv2.imread(target_file_path)
                    target_h, target_w = original_img.shape[:2]
                    new_mask = np.zeros((target_h, target_w), dtype=np.uint8)
                    
                    # y축 처리
                    if mask_h > target_h:
                        y_start = (mask_h - target_h) // 2
                        y_end = y_start + target_h
                        mask_aligned_y = mask_inv[y_start:y_end, :]
                        y_offset = 0
                    else:
                        mask_aligned_y = mask_inv
                        y_offset = (target_h - mask_h) // 2

                    # x축 처리
                    if mask_w > target_w:
                        x_start = (mask_w - target_w) // 2
                        x_end = x_start + target_w
                        mask_aligned = mask_aligned_y[:, x_start:x_end]
                        x_offset = 0
                    else:
                        mask_aligned = mask_aligned_y
                        x_offset = (target_w - mask_w) // 2
                    # 최종 삽입
                    new_mask[y_offset:y_offset + mask_aligned.shape[0], x_offset:x_offset + mask_aligned.shape[1]] = mask_aligned

                    kernel = np.ones((11,11), np.uint8)
                    dilated_mask = cv2.dilate(new_mask, kernel, iterations=1)
                    final_mask = cv2.bitwise_not(dilated_mask)
                    final_mask_path = type_convert_util.convert_type(final_mask,"np_gray","file_path")
                    file_util.file_copy(final_mask_path,dest_folder=str(Path(TEMP_FOLDER)/"mask1")) # 팽창된 마스크
                    
                    # bitwise_and에 맞게 다시 반전 (흰색 영역이 남김 부분)
                    mask_for_bitwise = cv2.bitwise_not(final_mask)
                    # 마스크로 원본 이미지에서 해당 영역 남기기
                    masked_img = cv2.bitwise_and(original_img, original_img, mask=mask_for_bitwise)

                    masked_img_path = type_convert_util.convert_type(masked_img,"np_gray","file_path")
                    file_util.file_copy(masked_img_path,dest_folder=str(Path(TEMP_FOLDER)/"mask2")) # 검정색으로 지워짐

                    white_background = np.ones_like(original_img, dtype=np.uint8) * 255
                    mask_inv_for_bg = cv2.bitwise_not(mask_for_bitwise)
                    background_part = cv2.bitwise_and(white_background, white_background, mask=mask_inv_for_bg)
                    white_masked_img = cv2.add(masked_img, background_part)

                    masked_file_path = type_convert_util.convert_type(white_masked_img,"np_gray","file_path")
                    file_util.file_copy(masked_file_path,dest_folder=str(Path(TEMP_FOLDER)/"mask3")) # 하얀색으로 지워짐
                else:
                    masked_file_path = target_file_path
                classify_ai_step_info = json.loads(layout_info["classify_ai_info"])
                layout_class_id = layout_info["layout_class_id"]
                result_map = {}
                result_map["model_dir"] = str(Path(CLASS_FOLDER)/str(layout_info.get('doc_class_id'))/str(layout_info.get('layout_class_id'))/"classify"/"model")
                result_map["folder_path"] = file_info["file_id"]
                result_map = classify_ai_util.classify_ai(masked_file_path, step_info=classify_ai_step_info, result_map=result_map)
                
                # 레이아웃이 true인 경우 텍스트 체크
                if result_map["_classify"]["pred"] == 1:
                    # 문자열 체크로직
                    param = (layout_info.get("layout_class_id"),)
                    static_block_info = dococr_query_util.select_row_map("selectStaticBlockInfo",param)
                    param = {"area_box":json.loads(static_block_info.get("block_box",None))}
                    separate_section_step_info = create_step_info_util.create_step("ss_bounding_box",param=param, result_map=result_map)
                    static_block_img,result_map = separate_area_util.separate_area(target_file_path,data_type="file_path",output_type="np_bgr",step_info=separate_section_step_info, result_map=result_map)
                    
                    static_block_info["block_id"] = "static_string"
                    static_block_info["page_num"] = "0"
                    param = {}
                    ocr_step_info = create_step_info_util.create_step("or_tesseract",param=param, result_map=result_map)
                    block_map = ocr_util.ocr((static_block_img, static_block_info), input_img_type="np_bgr", step_info=ocr_step_info, result_map={"folder_path":"static_string"}) # ocr결과가 추가된 block_map 반환
                    param = {"ocr_type":"tesseract"}
                    cleansing_step_info = create_step_info_util.create_step("cl_general_cleansing",param=param, result_map=result_map)
                    _, block_map  = ocr_cleansing_util.ocr_cleansing((target_file_path, block_map), step_info=cleansing_step_info, result_map={"folder_path":"static_string"}) # 정제 데이터가 추가된 block_map 반환
                    
                    ocr_text = block_map.get("ocr",{})["text"]
                    param = {"correct_text": static_block_info.get("default_text",""), "target_key": "_strcomp"}
                    classify_string_step_info = create_step_info_util.create_step("cs_contain_check",param=param, result_map=result_map)
                    result_map = classify_string_util.classify_string(ocr_text, step_info=classify_string_step_info, result_map=result_map)                

                    ai_pred_confidence = result_map["_classify"]["confidence"]
                    ai_weight = 0.9
                    strcomp_confidence = result_map["_strcomp"]
                    strcomp_weight = 0.1
                    result_map["_classify"]["ai_confidence"] = ai_pred_confidence
                    result_map["_classify"]["str_confidence"] = strcomp_confidence
                    result_map["_classify"]["confidence"] = (ai_pred_confidence*ai_weight + strcomp_confidence*strcomp_weight)
                result_map["_classify"]["normalize_img"] = target_file_path
                classify_map[layout_class_id] = result_map["_classify"]
    file_info["classify"] = classify_map
    # 결과 폴더에 파일 복사
    run_id = context['dag_run'].run_id
    target_id = file_info.get("layout_id",file_info["file_id"])
    dococr_query_util.update_map("updateTargetContent",(json.dumps(file_info,ensure_ascii=False),run_id,target_id))
    file_info["status"] = "success"
    return file_info
    
    
@task(pool='ocr_pool')
def aggregate_classify_results_task(file_infos:List, **context):
    """
    파일 정보 리스트에서 각 파일별로 분류 결과를 종합하고, 가장 신뢰도가 높은 클래스로 최종 분류 결과를 저장하는 함수.
    결과는 파일 정보에 추가되고, 분류 결과 및 파일 복사, DB 저장 등의 후처리를 수행한다.

    Args:
        file_infos (list): 각 파일의 정보(분류 결과 포함)가 담긴 딕셔너리 리스트
        class_keys (list): 분류 기준이 되는 클래스 키 리스트
        context (dict): Airflow 등에서 전달되는 context 정보(예: dag_run 등)
    Returns:
        list: 최종 분류 결과가 추가된 파일 정보 리스트
    """
    # 1. 같은 file_id, page_num별로 classify 합치기
    merged_info_map = {}  # key: (file_id, page_num), value: 합친 file_info
    for info in file_infos:
        key = (info['file_id'], info['page_num'])
        if key not in merged_info_map:
            # 새 객체 생성 (deepcopy 필요시 import copy 후 copy.deepcopy 사용)
            merged_info_map[key] = {k: v for k, v in info.items() if k != 'classify'}
            merged_info_map[key]['classify'] = {}
        # 기존 classify에 추가/업데이트
        merged_info_map[key]['classify'].update(info.get('classify', {}))

    # 합쳐진 file_info 리스트로 변환
    file_infos = list(merged_info_map.values())
    
    for file_info in file_infos:
        # 각 파일별로 최대 신뢰도와 해당 클래스를 찾기 위한 초기화
        max_conf = -1
        best_class = None
        classify_result = file_info.get("classify", {})
        print(classify_result)

        for class_key, class_result in classify_result.items():
            pred = class_result.get("pred", 0)
            conf = class_result.get("confidence", 0)
            print(class_key, " - ", pred, " ", conf)
            if pred == 1:
                if conf > max_conf:
                    max_conf = conf
                    best_class = class_key # layout_class_id
                    normalize_path = class_result.get("normalize_img", "")
                    print("max_conf:", max_conf, "best_class:", best_class, "pred", pred)
        # 신뢰도가 0.5을 초과하면 최종 클래스로 설정, 아니면 "None"으로 처리
        if max_conf>0.5:
            file_info["layout_class_id"] = best_class
            file_info["doc_class_id"] = dococr_query_util.select_doc_class_id([best_class])
            file_info["confidence"] = max_conf
            file_info["file_path"]["_normalize"] = normalize_path
        else:
            file_info["layout_class_id"] = "None"
            file_info["doc_class_id"] = "None"
            file_info["confidence"] = max_conf
            file_info["file_path"]["_normalize"] = file_info["file_path"]["_origin"]
        
        # 결과 폴더에 파일 복사
        run_id = context['dag_run'].run_id
        target_id = file_info.get("layout_id",file_info["file_id"])
        #temp_folder = Path(TEMP_FOLDER)/run_id
        #file_util.file_copy(file_info["file_path"]["_origin"],temp_folder/Path(file_info["file_path"]["_origin"]).name)
        dococr_query_util.update_map("updateTargetContent",(json.dumps(file_info, ensure_ascii=False),run_id,target_id))

    return file_infos