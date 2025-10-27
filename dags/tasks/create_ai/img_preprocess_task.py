from collections import Counter
import json
from pathlib import Path
import uuid
from typing import Any, Union
import cv2
import numpy as np
import pytesseract
from scipy.ndimage import interpolation as inter
from airflow.decorators import task
from airflow.models import Variable, XCom
from utils.com import file_util
from utils.img import type_convert_util,img_preprocess_util

RESULT_FOLDER = Variable.get("RESULT_FOLDER", default_var="/opt/airflow/data/result")
TEMP_FOLDER = Variable.get("TEMP_FOLDER", default_var="/opt/airflow/data/temp")
DATA_FOLDER = Variable.get("DATA_FOLDER", default_var="/opt/airflow/data")
CREATE_NORMALIZE_POLICY = Variable.get("CREATE_NORMALIZE_POLICY", default_var="default") # default,always,skip

@task(pool='ocr_pool') 
def img_preprocess_task(file_info:dict, layout_info:dict, target_key:str="_origin")->dict:
    step_info = json.loads(layout_info["img_preprocess_info"])
    #step_info 단일인 경우
    data = file_info["file_path"][target_key]
    rslt_map = {}
    rslt_map["folder_path"] = file_info["file_id"]
    result,rslt_map = img_preprocess_util.img_preprocess(data,data_type="file_path",output_type="file_path",step_info=step_info, result_map=rslt_map)
    file_info["file_path"].update(rslt_map["save_path"])
    file_info["file_path"]["_result"] = result
    file_info["status"] = "success"
    return file_info


@task(pool='ocr_pool') 
def img_normalize_task(layout_info:dict, target_key:str="_origin")->dict:
    step_info = json.loads(layout_info["img_preprocess_info"])
    #step_info 단일인 경우
    doc_class_id = layout_info.get("doc_class_id")
    layout_class_id = layout_info.get("layout_class_id")
    layout_dir = f"{DATA_FOLDER}/class/{doc_class_id}/{layout_class_id}"

    ready_true_image_dir = f"{layout_dir}/classify/ready/true"   # 특정 서식 증강된 문서 이미지
    ready_false_image_dir = f"{layout_dir}/classify/ready/false" # 일반 증강된 문서 이미지
    preprc_true_imgae_dir = f"{layout_dir}/classify/preprc/true"   # 표준화된 특정 서식 문서 이미지
    preprc_false_imgae_dir = f"{layout_dir}/classify/preprc/false"   # 표준화된 일반 문서 이미지
    
    if CREATE_NORMALIZE_POLICY == "default": # 목표치보다 데이터가 부족할 때만 전처리
        preprc_true_image_paths = file_util.get_image_paths(preprc_true_imgae_dir)
        ready_true_image_paths = file_util.get_image_paths(ready_true_image_dir)
        if len(preprc_true_image_paths) < len(ready_true_image_paths):
            _img_normalize(ready_true_image_dir,preprc_true_imgae_dir,step_info)
        preprc_false_imgae_dir_paths = file_util.get_image_paths(preprc_false_imgae_dir)
        ready_false_image_paths = file_util.get_image_paths(ready_false_image_dir)
        if len(preprc_false_imgae_dir_paths) < len(ready_false_image_paths):               
            _img_normalize(ready_false_image_dir,preprc_false_imgae_dir,step_info)
    if CREATE_NORMALIZE_POLICY == "always": # 무조건 전처리
        _img_normalize(ready_true_image_dir,preprc_true_imgae_dir,step_info)
        _img_normalize(ready_false_image_dir,preprc_false_imgae_dir,step_info)
    if CREATE_NORMALIZE_POLICY == "skip":
        pass

def _img_normalize(ready_dir,preprc_dir,step_info):
    image_paths = file_util.get_image_paths(ready_dir)
    for image_path in image_paths:
        result,rslt_map = img_preprocess_util.img_preprocess(image_path,data_type="file_path",output_type="file_path",step_info=step_info)
        file_util.file_copy(result,dest_file=str(Path(preprc_dir)/Path(image_path).name),duplicate_policy="overwrite")    