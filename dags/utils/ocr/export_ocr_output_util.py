import pandas as pd
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

def export_ocr_output(doc_info:dict, step_info:dict=None, result_map:dict=None) -> Any:
    """
    이미지 결과 저장 함수
    :param data: 이미지(Any) 데이터
    :param step_info: 전처리 단계 정보 (기본값은 STEP_INFO_DEFAULT)
    :param result_map: 결과를 저장할 맵 (기본값은 빈 딕셔너리)
    :return: 전처리된 이미지 또는 결과
    """
    if step_info is None:
        step_info = STEP_INFO_DEFAULT
    if result_map is None:
        result_map = {}
    step_list = step_info.get("step_list", STEP_INFO_DEFAULT["step_list"])
    return export_ocr_output_step_list(doc_info=doc_info, step_list=step_list, result_map=result_map)

def export_ocr_output_step_list(doc_info:dict, step_list:dict=None, result_map:dict=None) -> Any:
    """
    이미지 결과 저장 함수
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
    
    for stepinfo in step_list:
        print("step :",stepinfo["name"])
        if stepinfo["name"] not in function_map:
            print(f"경고: '{stepinfo['name']}' 함수가 정의되지 않아 다음 단계를 진행합니다.")
            continue  # 정의되지 않은 함수는 건너뜀
        function_info = function_map[stepinfo["name"]]
        function_info["function"](doc_info,**stepinfo["param"],result_map=result_map)

    return result_map

def insert_ocr_result(
    doc_info: dict,
    result_map: dict = None,
    **kwargs
) -> dict:
    doc_class_id = str(doc_info.get("doc_class_id", ""))
    structed_doc = doc_info.get("structed_doc", {})
    complete_id = doc_info.get("complete_id", None)
    dococr_query_util.insert_structed_ocr_result(doc_class_id,structed_doc,complete_id=complete_id)
    return doc_info

def db_to_excel(
    doc_info: dict,
    result_map: dict = None,
    **kwargs
) -> dict:
    complete_id = doc_info.get("complete_id", None)
    if not complete_id:
        print("[ERROR] complete_id가 없습니다.")
        return doc_info
    complete_map_data_list = dococr_query_util.select_complete_map_data(complete_id)
    if not complete_map_data_list:
        print(f"경고: {complete_id}작업으로 등록된 결과 없습니다.")
        return doc_info
    
    origin_file_path = doc_info["doc_path"]["_originfile"]
    output_path = Path(origin_file_path).with_suffix(".xlsx")
    
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for table_name, rows in complete_map_data_list.items():
            if not rows:
                print(f"[INFO] {table_name} 데이터 없음 → 스킵")
                continue

            # dict 리스트 → DataFrame 변환
            df = pd.DataFrame(rows)

            # 시트명은 31자 이하여야 함
            sheet_name = table_name[:31]
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            print(f"[OK] 시트 생성: {sheet_name} ({len(df)} 건)")
    print(f"[DONE] Excel 저장 완료: {output_path}")
    return doc_info


def export_to_excel(
    doc_info: dict,
    result_map: dict = None,
    **kwargs
) -> dict:
    structed_doc = doc_info.get("structed_doc", {})
    
    origin_file_path = doc_info["doc_path"]["_originfile"]
    output_path = Path(origin_file_path).with_suffix(".xlsx")
    
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for table_name, rows in structed_doc.items():
            if not rows:
                print(f"[INFO] {table_name} 데이터 없음 → 스킵")
                continue
            # 컬럼명 추출
            col_keys = []
            for row in rows:
                col_keys.extend(list(row.keys()))
            col_keys = sorted(list(set(col_keys)))

            # structed_text 값 추출
            excel_rows = []
            for row in rows:
                excel_row = []
                for k in col_keys:
                    val = row.get(k)
                    if val is not None and isinstance(val, dict):
                        val = val.get("structed_text", "")
                    else:
                        val = ""
                    excel_row.append(val)
                excel_rows.append(excel_row)
            # 데이터프레임 생성
            df = pd.DataFrame(excel_rows, columns=col_keys)

            # 시트명은 31자 이하여야 함
            sheet_name = table_name[:31]
            # 엑셀 시트로 저장
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            print(f"[OK] 시트 생성: {sheet_name} ({len(df)} 건)")
    print(f"[DONE] Excel 저장 완료: {output_path}")
    return doc_info



def _save(file_path:str,save_key:str="tmp",result_map:dict=None):
    if not result_map:
        result_map = {}
    result_map["save_path"][save_key]=file_path 

    
function_map = {
    "insert_ocr_result": {"function": insert_ocr_result, "param": "ocr_type,keep_chars"},
    "db_to_excel": {"function": db_to_excel, "param": ""},
    "export_to_excel": {"function": export_to_excel, "param": ""},
}