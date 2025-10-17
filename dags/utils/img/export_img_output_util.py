from collections import Counter
from pathlib import Path
from airflow.models import Variable, XCom
from typing import Any, List, Dict
import uuid
import cv2
import numpy as np
import pytesseract
from scipy.ndimage import interpolation as inter
from utils.com import file_util
from utils.img import type_convert_util

RESULT_FOLDER = Variable.get("RESULT_FOLDER", default_var="/opt/airflow/data/result")
TEMP_FOLDER = Variable.get("TEMP_FOLDER", default_var="/opt/airflow/data/temp")
STEP_INFO_DEFAULT = {
    "name":"img preprocess default",
    "type":"step_list",
    "step_list":[
        {"name":"save","param":{"save_key":"tmp_save"}}
    ]
}

def export_img_output(data:Any, data_type:str="file_path", output_type:str="file_path", step_info:Dict=None, result_map:dict=None) -> Any:
    """
    이미지 전처리 함수
    :param data: 이미지 파일 경로 또는 numpy 배열
    :param data_type: 입력 데이터의 타입 ("file_path", "np_bgr", "np_gray" 등)
    :param output_type: 출력 데이터의 타입 ("file_path", "np_bgr", "np_gray" 등)
    :param step_info: 전처리 단계 정보 (기본값은 STEP_INFO_DEFAULT)
    :param result_map: 결과를 저장할 맵 (기본값은 빈 딕셔너리)
    :return: 전처리된 이미지 또는 결과
    """
    if step_info is None:
        step_info = STEP_INFO_DEFAULT
    if result_map is None:
        result_map = {}
    step_list = step_info.get("step_list", STEP_INFO_DEFAULT["step_list"])
    return export_img_output_step_list(data=data, data_type=data_type, output_type=output_type, step_list=step_list, result_map=result_map)

def export_img_output_step_list(data:Any, data_type:str="file_path", output_type:str="file_path", step_list:List=None, result_map:dict=None) -> Any:
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
    
    output = data
    before_output_type = data_type

    for stepinfo in step_list:
        print("step :",stepinfo["name"])
        if stepinfo["name"] not in function_map:
            print(f"경고: '{stepinfo['name']}' 함수가 정의되지 않아 다음 단계를 진행합니다.")
            continue  # 정의되지 않은 함수는 건너뜀
        function_info = function_map[stepinfo["name"]]
        convert_param = stepinfo.get("convert_param", {})
        input = type_convert_util.convert_type(output,before_output_type,function_info["input_type"],params=convert_param)
        output = function_info["function"](input,**stepinfo["param"],result_map=result_map)
        before_output_type = function_info["output_type"]
    
    result = type_convert_util.convert_type(output, before_output_type, output_type)
    return result, result_map

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


function_map = {
    # 공통
    "cache": {"function": cache,"input_type": "file_path","output_type": "file_path","param": "cache_key"},
    "load": {"function": load,"input_type": "any","output_type": "file_path","param": "cache_key"},
    "save": {"function": save,"input_type": "file_path","output_type": "file_path","param": "save_key,tmp_save"},
}
