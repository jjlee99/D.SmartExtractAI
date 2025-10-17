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
import re
from collections import defaultdict
from math import log2, ceil
import pandas as pd

RESULT_FOLDER = Variable.get("RESULT_FOLDER", default_var="/opt/airflow/data/result")
TEMP_FOLDER = Variable.get("TEMP_FOLDER", default_var="/opt/airflow/data/temp")
STEP_INFO_DEFAULT = {
    "name":"create_ai default",
    "type":"step_list",
    "step_list":[
        {"name":"save","param":{"save_key":"tmp_save"}}
    ]
}

def classify_string(data:dict, step_info:Dict=None, result_map:dict=None) -> Any:
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
    return classify_string_step_list(data=data, step_list=step_list, result_map=result_map)

def classify_string_step_list(data:dict, step_list:List=None, result_map:dict=None) -> Any:
    """
    이미지 전처리 함수
    :param data: 대상 텍스트
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
    
    for stepinfo in step_list:
        print("step :",stepinfo["name"])
        if stepinfo["name"] not in function_map:
            print(f"경고: '{stepinfo['name']}' 함수가 정의되지 않아 다음 단계를 진행합니다.")
            continue  # 정의되지 않은 함수는 건너뜀
        function_info = function_map[stepinfo["name"]]
        input = output
        output = function_info["function"](input,**stepinfo["param"],result_map=result_map)
    return result_map

def contain_check(data:str, correct_text:str="",target_key:str="_strcomp", result_map:dict=None,**context):
    full_term = correct_text
    # 1. 2의 거듭제곱 단위 부분 문자열 생성
    def generate_substrings_by_powers_of_two(text):
        substrings = set()
        n = len(text)
        length = 1
        while length <= n:
            for i in range(n - length + 1):
                substr = text[i:i+length]
                substrings.add(substr)
            length *= 2
        return list(substrings)

    # 2. 패턴별로 위치 및 길이 매칭
    def find_matches_with_positions(ocr_text, patterns):
        matches = []
        for pattern in patterns:
            try:
                regex = re.compile(re.escape(pattern))
            except re.error:
                continue
            for m in regex.finditer(ocr_text):
                start = m.start()
                end = m.end()
                matches.append({
                    'pattern': pattern,
                    'start': start,
                    'end': end,
                    'length': end - start
                })
        return matches

    # 3. 그룹화 및 점수 계산
    def group_and_score(matches, full_term_length, text_length):
        groups = defaultdict(list)
        for match in matches:
            groups[match['start']].append(match)
        max_power = ceil(log2(full_term_length)) if full_term_length > 0 else 1
        max_count_limit = sum(range(1, max_power + 1))
        length_groups = defaultdict(int)
        results = []
        for start, group in groups.items():
            length_groups[len(group)] += 1
        for group_num, cnt in length_groups.items():
            remaining_limit = group_num
            weighted_limited_sum = 0
            for i in range(0, cnt+1):
                limit = (remaining_limit / max_power) * (group_num)
                weighted_limited_sum += limit
                remaining_limit -= limit
            group_score = min(weighted_limited_sum, group_num)
            confidence = group_score / max_count_limit
            results.append({
                'group_num': group_num,
                'count': weighted_limited_sum,
                'confidence': confidence
            })
        results.sort(key=lambda x: x['confidence'], reverse=True)
        match_weight = 0.6
        match_score = sum([i['confidence'] for i in results])
        print("match_score:", match_score)
        # 밀도 계산
        position_counts = [0] * text_length
        for match in matches:
            start_pos = match['start']
            if 0 <= start_pos < text_length:
                position_counts[start_pos] += max_power - log2(match['length'])
        window_size = full_term_length
        max_density = 0
        for start in range(0, text_length - window_size + 1):
            window_sum = sum(position_counts[start:start+window_size])
            if window_sum > max_density:
                max_density = window_sum
        norm_denominator = ((full_term_length * max_count_limit) - ((max_count_limit-max_power-1)*(max_power-1)))
        density_weight = 0.6
        norm_density = max_density / norm_denominator if norm_denominator else 0
        print("norm_density:", norm_density)
        #가중치를 추가하여 최종 점수 계산
        final_score = min((match_score*match_weight + norm_density*density_weight),1)
        print("final_score:", final_score)
        return final_score
    # 분석
    patterns = generate_substrings_by_powers_of_two(full_term) # 거듭제곱 단위의 부분 문자열 생성
    matches = find_matches_with_positions(data, patterns)  # 부분 문자열 매칭 탐색
    total_confidence = group_and_score(matches, len(full_term), len(data)) # 그룹화 및 점수 계산
    result_map[target_key] = total_confidence
    return data    
    
#이후
function_map = {
    # 공통
    "contain_check": {"function": contain_check, "param": "correct_text,target_key"},
}
