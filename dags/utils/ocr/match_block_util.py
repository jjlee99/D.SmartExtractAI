from collections import defaultdict
from pathlib import Path
from airflow.models import Variable
from typing import Any
import uuid
import numpy as np
from utils.com import json_util

RESULT_FOLDER = Variable.get("RESULT_FOLDER", default_var="/opt/airflow/data/result")
TEMP_FOLDER = Variable.get("TEMP_FOLDER", default_var="/opt/airflow/data/temp")
STEP_INFO_DEFAULT = {
    "name":"match block default",
    "type":"match_block_step_list",
    "step_list":[
        {"name":"save","param":{"save_key":"tmp_save"}}
    ]
}

def match_block(data:list[tuple[Any,dict]], step_info:dict=None, result_map:dict=None) -> Any:
    """
    블록 매칭 함수
    :param data: 이미지(Any)와 파일정보(dict)가 담긴 목록
    :param step_info: 전처리 단계 정보 (기본값은 STEP_INFO_DEFAULT)
    :param result_map: 결과를 저장할 맵 (기본값은 빈 딕셔너리)
    :return: 전처리된 이미지 또는 결과
    """
    if step_info is None:
        step_info = STEP_INFO_DEFAULT
    if result_map is None:
        result_map = {}
    step_list = step_info.get("step_list", STEP_INFO_DEFAULT["step_list"])
    return match_block_step_list(data=data, step_list=step_list, result_map=result_map)

def match_block_step_list(data:list[tuple[Any,dict]], step_list:dict=None, result_map:dict=None) -> Any:
    """
    블록 매칭 함수
    :param data: 이미지(Any)와 파일정보(dict)가 담긴 목록
    :param step_list: 전처리 단계 정보 (기본값은 STEP_INFO_DEFAULT["step_list"])
    :param result_map: 결과를 저장할 맵 (기본값은 빈 딕셔너리)
    :return: 전처리된 이미지 또는 결과
    """
    if step_list is None:
        step_list = STEP_INFO_DEFAULT["step_list"]
    if result_map is None:
        result_map = {}
    process_id = f"_mch_{str(uuid.uuid4())}"
    result_map["process_id"] = process_id
    result_map["folder_path"] = result_map.get("folder_path",f"{TEMP_FOLDER}/{process_id}")
    result_map["cache"] = {}
    result_map["save_path"] = {}
    
    output = data

    for stepinfo in step_list:
        print("step :",stepinfo["name"])
        if stepinfo["name"] not in function_map:
            print(f"경고: '{stepinfo['name']}' 함수가 정의되지 않아 다음 단계를 진행합니다.")
            continue  # 정의되지 않은 함수는 건너뜀
        function_info = function_map[stepinfo["name"]]
        input = output
        output = function_info["function"](input,**stepinfo["param"],result_map=result_map)

    return output

def save(json_list: list[tuple[Any,dict]],save_key:str="tmp",result_map:dict=None)->tuple[Any,dict]:
    if not result_map:
        result_map = {}
    if result_map.get("folder_path", "temp").startswith(TEMP_FOLDER) or result_map.get("folder_path", "temp").startswith(RESULT_FOLDER) :
        json_save_path = Path(result_map.get("folder_path","temp")) / f"{save_key}.json"
    else : 
        json_save_path = Path(TEMP_FOLDER) / result_map.get("folder_path","temp") / f"{save_key}.json"
    dict_list = [item[1] for item in json_list]
    json_util.save(str(json_save_path),dict_list)
    return json_list


def detect_row_col_set1(
    json_list: list[tuple[Any,dict]],
    gap_threshold:int,
    result_map: dict = None,
    **kwargs
) -> list[tuple[Any,dict]]:
    
    if len(json_list) == 1:
        # 데이터 1개면 바로 row=1, col=1 부여하고 반환
        _, block_data = json_list[0]
        block_data["row"] = 1
        block_data["col"] = 1
        print(block_data["block_id"], block_data["row"], block_data["col"])
        return json_list
    
    # 1. row 설정
    json_list_sorted = sorted(json_list, key=lambda item: item[1]["block_box"][1])  #item:(img,block_data), block_map:[x,y,w,h] 
    # 1-1. row 분리 임계값 계산
    if gap_threshold is None:
        y_values = [item[1]["block_box"][1] for item in json_list_sorted]
        gaps = np.diff(y_values)
        if len(gaps) == 0:
            gap_threshold = 100
        else:    
            gap_threshold = np.mean(gaps) + np.std(gaps)
        print(gap_threshold)

    
    # 1-2. row 값 지정(1부터 시작)
    current_row = 0
    prev_y = -gap_threshold
    row_groups = defaultdict(list)
    for _, block_data in json_list_sorted:
        y = block_data["block_box"][1]
        # 이전 y와의 차이가 threshold 초과되면 row 증가
        if y - prev_y > gap_threshold:
            current_row += 1
        block_data["row"] = current_row
        row_groups[current_row].append(block_data)
        prev_y = y
    
    # 각 row 그룹에서 x 기준 정렬 후 col 번호 부여
    for row_items in row_groups.values():
        # x 기준 정렬해서 col 부여
        row_items.sort(key=lambda item: item["block_box"][0])  #item["block_box"] = [x,y,w,h]
        for col_idx, block_data in enumerate(row_items, start=1):
            block_data["col"] = col_idx
    
    for _,block_data in json_list:
        print(block_data["block_id"],block_data["row"],block_data["col"])

    return json_list


function_map = {
    "save": {"function": save, "param": ""},
    "detect_row_col_set1": {"function": detect_row_col_set1, "input_type": "np_bgr", "output_type": "np_bgr", "param": ""},
}