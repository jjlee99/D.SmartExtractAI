from airflow.models import Variable
from typing import Any, List, Dict
from pathlib import Path
from PIL import Image
import uuid
import json
import os
import cv2
import numpy as np
from utils.com import file_util
import pytesseract
import torch
from transformers import AutoProcessor, AutoModelForSequenceClassification
import torch.nn.functional as F
from utils.ai.machine_learning_dataset import extract_feature_for_table_doc_util
from utils.com import json_util
from utils.db import dococr_query_util
from utils.ocr import separate_area_util
from collections import defaultdict
import csv
import pickle
import joblib
import sklearn
import pandas as pd
# from pycaret.classification import *

RESULT_FOLDER = Variable.get("RESULT_FOLDER", default_var="/opt/airflow/data/result")
TEMP_FOLDER = Variable.get("TEMP_FOLDER", default_var="/opt/airflow/data/temp")
STEP_INFO_DEFAULT = {
    "name":"create_ai default",
    "type":"step_list",
    "step_list":[
        {"name":"save","param":{"save_key":"tmp_save"}}
    ]
}

def classify_ai(file_path:list, step_info:Dict=None, result_map:dict=None) -> Any:
    """
    ai 생성 함수
    :param data: 이미지 파일 경로 또는 numpy 배열
    :param step_info: 생성 단계 정보 (기본값은 STEP_INFO_DEFAULT)
    :param result_map: 결과를 저장할 맵 (기본값은 빈 딕셔너리)
    :return: 결과
    """
    if step_info is None:
        step_info = STEP_INFO_DEFAULT
    if result_map is None:
        result_map = {}
    step_list = step_info.get("step_list", STEP_INFO_DEFAULT["step_list"])
    return classify_ai_step_list(file_path=file_path, step_list=step_list, result_map=result_map)

def classify_ai_step_list(file_path:str, step_list:List=None, result_map:dict=None) -> Any:
    """
    이미지 전처리 함수
    :param data: 이미지 파일 경로 또는 numpy 배열
    :param data_type: 입력 데이터의 타입 ("file_path", "np_bgr", "np_gray" 등)
    :param output_type: 출력 데이터의 타입 ("file_path", "np_bgr", "np_gray" 등)
    :param step_info: 전처리 단계 정보 (기본값은 STEP_INFO_DEFAULT)
    :param result_map: 결과를 저장할 맵 (기본값은 빈 딕셔너리)
    :return: 전처리된 이미지 또는 결과
    """
    if step_list is None:
        step_list = STEP_INFO_DEFAULT["step_list"]
    if result_map is None:
        result_map = {}
    process_id = f"_pre_{str(uuid.uuid4())}"
    result_map["process_id"] = process_id
    result_map["folder_path"] = result_map.get("folder_path",f"{TEMP_FOLDER}/{process_id}")
    result_map["cache"] = {}
    result_map["save_path"] = {}
    
    output = file_path
    
    for stepinfo in step_list:
        print("step :",stepinfo["name"])
        if stepinfo["name"] not in function_map:
            print(f"경고: '{stepinfo['name']}' 함수가 정의되지 않아 다음 단계를 진행합니다.")
            continue  # 정의되지 않은 함수는 건너뜀
        function_info = function_map[stepinfo["name"]]
        input = output
        output = function_info["function"](input,**stepinfo["param"],result_map=result_map)
    return result_map

def cache(file_path:str,cache_key:str,result_map:dict)->str:
    result_map["cache"][f"filepath_{cache_key}"] = file_path
    return file_path

def load(_,cache_key:str,result_map:dict)->str:
    return result_map["cache"][f"filepath_{cache_key}"]

def save(file_path:str,save_key:str="tmp",tmp_save:bool=False,result_map:dict=None)->str:
    if not result_map:
        result_map = {}
    if tmp_save:
        if result_map.get("folder_path", "temp").startswith(TEMP_FOLDER) or result_map.get("folder_path", "temp").startswith(RESULT_FOLDER) :
            save_path = Path(result_map.get("folder_path","temp")) / f"{save_key}.png"
        else : 
            save_path = Path(TEMP_FOLDER) / result_map.get("folder_path","temp") / f"{save_key}.png"
        file_util.file_copy(file_path,save_path)
    result_map["save_path"][save_key]=file_path
    return file_path

def ml_line_classify(file_path:str, model_name:str, save_input:bool=False, model_dir:str=None, model_path:str=None,result_map:dict=None,**context):
    # 프로세서
    if not model_path:
        if not model_dir:
            if result_map["model_dir"]:
                model_dir = result_map["model_dir"]
            else:
                CLASS_FOLDER = Variable.get("CLASS_FOLDER", default_var="/opt/airflow/data/class/")
                model_dir = f"{CLASS_FOLDER}/none/classify/model"
        model_path = f'{model_dir}/{model_name}.pkl'
        
    with open(f'{model_path}','rb') as file:
        model = pickle.load(file)
    print("1. 모델 로드 완료")

    image_path = file_path
    print("3. 이미지 전처리 및 특성 추출 시작")
    
    row_data = extract_feature_for_table_doc_util.preprocess_image_and_extract_features(image_path)
    
    # 단일 행의 데이터프레임으로 변환
    input_df = pd.DataFrame([row_data])
    
    if input_df.empty:
        print("경고: 모델 입력 데이터가 비어있습니다. 기본값 반환")
        return -1, 0.0, {} 
    print("4-2. 모델 예측 중...")
    
    # 이 메서드는 각 클래스에 대한 확률을 반환
    probabilities = model.predict_proba(input_df)
    print(f'각 클래스에 대한 예측 확률{probabilities}')
    pred = model.predict(input_df)[0]
    pred = int(pred)
    print(f'분류 결과 : {pred}')
    confidence = float(probabilities[0, pred])
    print(f'신뢰도 : {confidence}')
    print("4-3. 모델 예측 완료")
    
    input_kwargs = input_df.to_dict('records')
    if save_input:
        classify_result = {"pred": pred, "confidence": confidence, "input_kwargs":input_kwargs}
    else :
        classify_result = {"pred": pred, "confidence": confidence}
    result_map["_classify"] = classify_result
    return classify_result

#이후
function_map = {
    # 공통
    "cache": {"function": cache, "param": "cache_key"},
    "load": {"function": load, "param": "cache_key"},
    "save": {"function": save, "param": "save_key,tmp_save"},
    "ml_line_classify": {"function": ml_line_classify, "param": "model_dir,model_name,save_input,model_path"},
}
