from collections import defaultdict
from dateutil import parser
from datetime import datetime
import json
import os
from pathlib import Path
import re
from airflow.models import Variable
from typing import Any
import uuid
import numpy as np
from utils.db import dococr_query_util
from utils.com import json_util
from kiwipiepy import Kiwi
import re

RESULT_FOLDER = Variable.get("RESULT_FOLDER", default_var="/opt/airflow/data/result")
TEMP_FOLDER = Variable.get("TEMP_FOLDER", default_var="/opt/airflow/data/temp")
STEP_INFO_DEFAULT = {
    "name":"match block default",
    "type":"match_block_step_list",
    "step_list":[
        {"name":"save","param":{"save_key":"tmp_save"}}
    ]
}

def ocr_cleansing(block_data:tuple[Any,dict], step_info:dict=None, result_map:dict=None) -> Any:
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
    return ocr_cleansing_step_list(block_data=block_data, step_list=step_list, result_map=result_map)

def ocr_cleansing_step_list(block_data:tuple[Any,dict], step_list:dict=None, result_map:dict=None) -> Any:
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
    process_id = f"_cln_{str(uuid.uuid4())}"
    result_map["process_id"] = process_id
    result_map["folder_path"] = result_map.get("folder_path",f"{TEMP_FOLDER}/{process_id}")
    result_map["cache"] = {}
    result_map["save_path"] = {}
    
    output = block_data

    for stepinfo in step_list:
        print("step :",stepinfo["name"])
        if stepinfo["name"] not in function_map:
            print(f"경고: '{stepinfo['name']}' 함수가 정의되지 않아 다음 단계를 진행합니다.")
            continue  # 정의되지 않은 함수는 건너뜀
        function_info = function_map[stepinfo["name"]]
        input = output
        output = function_info["function"](input,**stepinfo["param"],result_map=result_map)

    return output

def save(block_data: tuple[Any,dict],save_key:str="tmp",result_map:dict=None)->tuple[Any,dict]:
    if not result_map:
        result_map = {}
    if result_map.get("folder_path", "temp").startswith(TEMP_FOLDER) or result_map.get("folder_path", "temp").startswith(RESULT_FOLDER) :
        json_save_path = Path(result_map.get("folder_path","temp")) / f"{save_key}.json"
    else : 
        json_save_path = Path(TEMP_FOLDER) / result_map.get("folder_path","temp") / f"{save_key}.json"
    json_util.save(str(json_save_path),block_data[1])
    return block_data

import re
from typing import Any, Tuple

def remove_whitespace(
    block_data: Tuple[Any, dict],
    ocr_type: str = "tesseract",
    result_map: dict = None,
    **kwargs
) -> Tuple[Any, dict]:
    """
    block_data 내 ocr[ocr_type]['text'] 문자열에서 모든 공백 문자(스페이스, 탭, 엔터 등)를 제거하고,
    변경된 문자를 원래 위치에 덮어씌워서 반환합니다.
    """
    _WHITESPACE_PATTERN = re.compile(r"\s+", flags=re.UNICODE)

    _, data = block_data
    text = data.get("ocr", {}).get(ocr_type, {}).get("text", "")
    if not isinstance(text, str):
        text = ""

    new_text = _WHITESPACE_PATTERN.sub("", text)  # 모든 공백 문자 제거

    # 변경된 텍스트를 원래 위치에 저장
    data.setdefault("ocr", {}).setdefault(ocr_type, {})["text"] = new_text
    # 필요 시 최상위 "ocr" 딕셔너리에도 반영 (주어진 sanitize_text 함수 참고)
    data.setdefault("ocr", {})["text"] = new_text  
    print(f"remove_whitespace: '{text}' -> '{new_text}'")

    return block_data

def sanitize_text(
    block_data: tuple[Any,dict],
    ocr_type: str = "tesseract",
    keep_chars: str = ":().",
    extra_chars: str = "_ㆍ",
    result_map: dict = None,
    **kwargs
) -> tuple[Any,dict]:
    """
    특수문자 제거 및 공백 정리 후, json_list의 각 dict['ocr'][ocr_type]['text'] 값을 덮어쓰기합니다.
    (각 튜플의 첫 값(img 등)은 그대로 두고 dict만 수정, 전체 list를 반환)
    """
    _PUNCT_PATTERN = re.compile(rf"[^\w{keep_chars}]", flags=re.UNICODE) # 제거 예외 문자(제거 안함)
    _EXTRA_PATTERN = re.compile(rf"[{extra_chars}]", flags=re.UNICODE) #추가로 더 제거할 문자()

    _, data = block_data
    text = data.get("ocr", {}).get(ocr_type, {}).get("text", "")
    if not isinstance(text, str):  # 혹시 text가 None이거나 비정상적이면 빈 문자열 처리
        text = ""
    new_text = _PUNCT_PATTERN.sub("", text)
    new_text = _EXTRA_PATTERN.sub("", new_text)

    # 원래 위치에 넣기
    data.setdefault("ocr", {}).setdefault(ocr_type, {})["text"] = new_text
    data.setdefault("ocr", {})["text"] = new_text
    print(f"sanitize: '{text}' -> '{new_text}'")
    return block_data

def apply_common_dictionary(
    block_data: tuple[Any,dict],
    ocr_type: str = "tesseract",
    result_map: dict = None,
    **kwargs
) -> tuple[Any,dict]:
    """딕셔너리 기반 치환. 긴 키를 먼저 처리하기 위해 길이순 정렬."""
    dic_prc = "db"  # 'file' or 'db'
    if dic_prc == "file":
        dictionary_path="/opt/airflow/data/common/dictionary/common_dictionary.json"    
        dictionary = json_util.load(dictionary_path)
        if not dictionary:
            return block_data
        sorted_dict = dict(sorted(dictionary.items(), key=lambda item: len(item[0]), reverse=True))
    elif dic_prc == "db":
        dictionary = dococr_query_util.select_list_map("selectCommonCrctnList",None)
        if not dictionary:
            return block_data
        sorted_list = sorted(dictionary, key=lambda x: len(x["error_text"]), reverse=True)
        sorted_dict = {item["error_text"]: item["crrct_text"] for item in sorted_list}
    _, data = block_data
    text = data.get("ocr", {}).get(ocr_type, {}).get("text", "")
    new_text = text
    if not isinstance(text, str):  # 혹시 text가 None이거나 비정상적이면 빈 문자열 처리
        new_text = ""
    for wrong, correct in sorted_dict.items():
        new_text = text.replace(wrong, correct)
    data.setdefault("ocr", {}).setdefault(ocr_type, {})["text"] = new_text
    data.setdefault("ocr", {})["text"] = new_text
    print(f"comdict: '{text}' -> '{new_text}'")
    return block_data


# def apply_block_dictionary(
#     block_data: tuple[Any,dict],
#     ocr_type: str = "tesseract",
#     result_map: dict = None,
#     **kwargs
# ) -> tuple[Any,dict]:
#     """블록 별 딕셔너리 기반 치환. 블록 텍스트가 완전 일치할 경우에만 치환."""
#     _, data = block_data
#     block_row = data.get("row",0)
#     block_col = data.get("col",0)
#     section_class_id = data.get("section_class_id",None)
#     section_name = data.get("section_name",None)
#     dic_prc = "db"
#     text = data.get("ocr", {}).get(ocr_type, {}).get("text", "")
#     if not isinstance(text, str):  # 혹시 text가 None이거나 비정상적이면 빈 문자열 처리
#         text = ""
    
#     if dic_prc == "file":
#         dictionary_path = f"/opt/airflow/data/class/a_class/ocr/dictionary/{section_name}_{block_row}_{block_col}.json"
#         dictionary = json_util.load(dictionary_path)
#         if not dictionary:
#             return block_data
#         if text in dictionary:
#             text = dictionary[text]
#     elif dic_prc == "db":
#         result = dococr_query_util.select_one_map("selectBlockCrctnMatched",(section_class_id,block_row,block_col,text))
#         if result:
#             text = result
#     # 원래 위치에 넣기
#     data.setdefault("ocr", {}).setdefault(ocr_type, {})["text"] = text
#     data.setdefault("ocr", {})["text"] = text
#     return block_data

def apply_block_dictionary(
    block_data: tuple[Any,dict],
    ocr_type: str = "tesseract",
    result_map: dict = None,
    **kwargs
) -> tuple[Any,dict]:
    """블록 별 딕셔너리 기반 치환. 블록 텍스트가 완전 일치할 경우에만 치환."""
    _, data = block_data
    block_row = data.get("row",0)
    block_col = data.get("col",0)
    section_class_id = data.get("section_class_id",None)
    dic_prc = "db"
    text = data.get("ocr", {}).get(ocr_type, {}).get("text", "")
    new_text = text
    if not isinstance(text, str):  # 혹시 text가 None이거나 비정상적이면 빈 문자열 처리
        new_text = ""
    elif dic_prc == "file":
        dictionary_path = f"/opt/airflow/data/class/a_class/ocr/dictionary/{section_class_id}_{block_row}_{block_col}.json"
        dictionary = json_util.load(dictionary_path)
        if not dictionary:
            return block_data
        if text in dictionary:
            new_text = dictionary[text]
    elif dic_prc == "db":
        result = dococr_query_util.select_one_map("selectBlockCrctnMatched",(text,text,section_class_id,block_row,block_col))
        if result:
            new_text = result
        else:
            repeat_row_info = dococr_query_util.select_row_map("selectMultiRowInfo",(section_class_id,))
            if repeat_row_info:
                min_row_num = repeat_row_info.get("minnum",None)
                max_row_num = repeat_row_info.get("maxnum",None)
                # 멀티로우 레이아웃인 경우 반복 블록을 계산하여 다시 치환
                if min_row_num is not None and max_row_num is not None:
                    repeat_row_cnt = max_row_num - min_row_num + 1  # 반복 블록의 행 개수 계산
                    index_num = (block_row - min_row_num)%repeat_row_cnt  # 현재 행이 반복 블록의 몇번째 행인지 계산
                    final_row = index_num + min_row_num  # 실제 검증할 행 번호 계산
                    result = dococr_query_util.select_one_map("selectBlockCrctnMatched",(text,text,section_class_id,final_row,block_col))
                    if result:
                        new_text = result
    # 원래 위치에 넣기  
    data.setdefault("ocr", {}).setdefault(ocr_type, {})["text"] = new_text
    data.setdefault("ocr", {})["text"] = new_text
    print(f"blockdict: '{text}' -> '{new_text}'")
    return block_data


kiwi = Kiwi()
def pattern_check(
    block_data: tuple[Any,dict],
    ocr_type: str = "tesseract",
    result_map: dict = None,
    **kwargs
) -> tuple[Any,dict]:
    """딕셔너리 기반 치환. 긴 키를 먼저 처리하기 위해 길이순 정렬."""
    _, data = block_data
    block_row = data.get("row",0)
    block_col = data.get("col",0)
    section_class_id = data.get("section_class_id",None)
    result = dococr_query_util.select_one_map("selectPatternInfo",(section_class_id,block_row,block_col))
    text = data.get("ocr", {}).get(ocr_type, {}).get("text", "")
    if result:
        pattern_info = json.loads(result)
        dtype = pattern_info["type"]
        if dtype == "decimal": #DB 저장 필수 타입
            trimtext = str(text).replace(',', '').replace(' ', '') # 공백,콤마 제거
            pattern = pattern_info["pattern"]
            if pattern and not re.match(pattern, trimtext):
                # 숫자 및 점(.) 문자만 남기기
                numtext = ''.join(ch for ch in trimtext if ch.isdigit() or ch == '.')
                if numtext.count('.') > 1:
                    # 소수점이 두 개 이상이면 마지막 점만 살리고 나머지는 제거
                    parts = numtext.split('.')
                    new_text = ''.join(parts[:-1]) + '.' + parts[-1]
                else:
                    new_text = numtext
            else:
                new_text = trimtext
        elif dtype == "date": 
            pattern = pattern_info.get("pattern",None)
            error_occurred, dt = False, None
            try:
                dt = datetime.strptime(text, pattern)
            except (ValueError, OverflowError): 
                try:
                    dt = parser.parse(text)
                except (ValueError, OverflowError):
                    data.setdefault("error_list",[]).append({"type":"pattern","msg":f"'{pattern}' 패턴에 맞지 않음."})
                    error_occurred = True
            if not error_occurred and dt is not None:
                new_text = dt.strftime(pattern)
            else:
                new_text = text
        else:
            #TODO: 띄어쓰기 처리
            #new_text = spacing(text) # pykospacing 사용시
            #new_text = text
            tokens = kiwi.tokenize(text)
            token_forms = [token.form for token in tokens]  # 각 토큰의 텍스트 추출
            new_text = " ".join(token_forms)              # 띄어쓰기로 결합
    else:
        #TODO: 띄어쓰기 처리
        #new_text = spacing(text)
        #new_text = text
        tokens = kiwi.tokenize(text)
        token_forms = [token.form for token in tokens]  # 각 토큰의 텍스트 추출
        new_text = " ".join(token_forms)              # 띄어쓰기로 결합
    # 원래 위치에 넣기
    data.setdefault("ocr", {}).setdefault(ocr_type, {})["text"] = new_text
    data.setdefault("ocr", {})["text"] = new_text   
    print(f"pattern: '{text}' -> '{new_text}'")
    return block_data


function_map = {
    "save": {"function": save, "param": ""},
    "remove_whitespace": {"function": remove_whitespace, "param": "ocr_type"},
    "sanitize_text": {"function": sanitize_text, "param": "ocr_type,keep_chars"},
    "apply_common_dictionary": {"function": apply_common_dictionary, "param": "ocr_type"},
    "apply_block_dictionary": {"function": apply_block_dictionary, "param": "ocr_type"},
    "pattern_check": {"function": pattern_check, "param": "ocr_type"},
}