from collections import defaultdict
import copy
from pathlib import Path
from airflow.models import Variable
from typing import Any
import uuid
from utils.img import type_convert_util
from utils.com import file_util
from utils.db import dococr_query_util
import numpy as np
from utils.com import json_util

RESULT_FOLDER = Variable.get("RESULT_FOLDER", default_var="/opt/airflow/data/result")
TEMP_FOLDER = Variable.get("TEMP_FOLDER", default_var="/opt/airflow/data/temp")
CLASS_FOLDER = Variable.get("CLASS_FOLDER", default_var="/opt/airflow/data/class")


STEP_INFO_DEFAULT = {
    "name":"structuring default",
    "type":"structuring_step_list",
    "step_list":[
        {"name":"save","param":{"save_key":"tmp_save"}}
    ]
}

def structuring(block_list: list[tuple[Any,dict]], step_info:dict=None, result_map:dict=None) -> dict:
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
    return structuring_step_list(block_list=block_list, step_list=step_list, result_map=result_map)

def structuring_step_list(block_list: list[tuple[Any,dict]], step_list:dict=None, result_map:dict=None) -> dict:
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
    process_id = f"_srt_{str(uuid.uuid4())}"
    result_map["process_id"] = process_id
    result_map["folder_path"] = result_map.get("folder_path",f"{TEMP_FOLDER}/{process_id}")
    result_map["result"] = defaultdict(list)
    
    output = block_list

    for stepinfo in step_list:
        print("step :",stepinfo["name"])
        if stepinfo["name"] not in function_map:
            print(f"srt경고: '{stepinfo['name']}' 함수가 정의되지 않아 다음 단계를 진행합니다.")
            continue  # 정의되지 않은 함수는 건너뜀
        function_info = function_map[stepinfo["name"]]
        input = output
        output = function_info["function"](input,**stepinfo["param"],result_map=result_map)
    return result_map["result"]

def structuring_by_type(
    block_list: list[tuple[Any,dict]],
    # section_class_id: int,
    # section_name: str,
    # section_type: str,
    result_map: dict = None,
    **kwargs
) :
    section_class_id = block_list[0][1].get("section_class_id",-1)
    section_name = block_list[0][1].get("section_name","unknown_section")
    section_type = block_list[0][1].get("section_type","MULTI_ROW")

    print("kkkkkkkkkkkkkkkkkkkkkkkkkdd",block_list[0][1].get("page_num",-1))
    templete_block_list = dococr_query_util.select_list_map("selectBlockList",(section_class_id,))
        
    #구역 타입에 따라 처리(STATIC_TEXT,ONE_ROW,MULTI_ROW)
    if section_type == "STATIC_TEXT": # 구조화할 정보 없음
        return block_list
    elif section_type == "ONE_ROW": # 1건의 row로 구성된 구역
        tmp_map = defaultdict(dict) 
        
        table_map = defaultdict(dict) 
        for block_data in block_list:
            table_map[block_data[1]["row"]][block_data[1]["col"]] = block_data
        table_name = ""
        result_list = defaultdict(list) # dict[list[dict]]
        for template_block in templete_block_list:
            block_row_num = int(template_block["block_row_num"])
            block_col_num = int(template_block["block_col_num"])
            block_type = template_block["block_type"]
            table_name = template_block["table_name"]
            column_name = template_block["column_name"]
            default_text = template_block["default_text"]

            block_data = table_map.get(block_row_num,{}).get(block_col_num,None)
            if block_data is None:
                _save_no_block_error(section_class_id,section_name,block_list,template_block,result_map)
            # 실제 저장 및 출력되는 데이터 맵
            text_info = _get_text_info(block_data[1])
            
            block_text = block_data[1].get("ocr",{}).get("text","")
            if block_type == "key":
                if block_text != default_text:
                    _save_block_check_error(section_class_id,section_name,block_data,template_block,block_text,result_map)
                else:
                    print(";;;;;;;;;;데이터일치",block_text, default_text)
            elif block_type == "val":
                tmp_map[table_name][column_name] = text_info
                print("값출력*******", column_name," : ",text_info)
        for table_name, data_map in tmp_map.items():
            if data_map:  # 빈 dict가 아니라면
                result_list[table_name].append(data_map)
        print(section_class_id,section_name,":",result_list)
        result_map["result"] = result_list
        print("...................",result_list)
        return block_list
    elif section_type == "MULTI_ROW": # 다중 row로 구성된 구역(목록)
        # 반복할 템플릿 블록 목록 생성
        max_rownum, loop_min_rownum, loop_max_rownum = 0, 999999, 0
        loop_templete_block_list = []
        for template_block in templete_block_list:
            block_row_num = int(template_block["block_row_num"])
            if template_block["block_type"] == "val":
                loop_templete_block_list.append(template_block)
                loop_min_rownum = min(loop_min_rownum, block_row_num)
                loop_max_rownum = max(loop_max_rownum, block_row_num)
            max_rownum = max(max_rownum, block_row_num)
        
        table_map = defaultdict(dict) 
        for block_data in block_list:
            table_map[block_data[1]["row"]][block_data[1]["col"]] = block_data
        
        # table_map의 row수에 맞춰 val 블록 목록을 반복하여 템플릿 블록 목록을 증가시킴
        loop_rownum = loop_max_rownum - loop_min_rownum + 1
        loop_cnt=1
        while max_rownum < len(table_map):
            new_block_list = []
            for template_block in loop_templete_block_list:
                new_block = copy.deepcopy(template_block)
                new_block["block_row_num"] = new_block["block_row_num"]+(loop_rownum*loop_cnt)
                new_block_list.append(new_block)
            templete_block_list.extend(new_block_list)
            max_rownum = max_rownum + loop_rownum
            loop_cnt = loop_cnt + 1
        print(loop_rownum)

        
        # 템플릿 블록 목록 기준 table_map을 구조화
        tmp_map = defaultdict(dict) # dict[dict]
        table_name = ""
        result_list = defaultdict(list) # dict[list[dict]]
        for template_block in templete_block_list:
            block_row_num = int(template_block["block_row_num"])
            block_col_num = int(template_block["block_col_num"])
            block_type = template_block["block_type"]
            table_name = template_block["table_name"]
            column_name = template_block["column_name"]
            default_text = template_block["default_text"]
            curr_num = block_row_num
            block_data = table_map.get(block_row_num,{}).get(block_col_num,None)
            if block_data is None:
                _save_no_block_error(section_class_id,section_name,block_list,template_block,result_map)
            # 실제 저장 및 출력되는 데이터 맵
            text_info = _get_text_info(block_data[1])
            
            block_text = block_data[1].get("ocr",{}).get("text","")
            if block_type == "key":
                if block_text != default_text:
                    _save_block_check_error(section_class_id,section_name,block_data,template_block,block_text,result_map)
                else:
                    print(";;;;;;;;;;데이터일치",block_text, default_text)
            elif block_type == "val":
                # 입력하려는 column_name이 이미 들어가 있으면 신규 row
                if tmp_map[table_name].get(column_name,None):
                    result_list[table_name].append(tmp_map[table_name])
                    tmp_map[table_name] = defaultdict(dict)
                tmp_map[table_name][column_name] = text_info
                print("값출력*******", column_name," : ",text_info)
        for table_name, data_map in tmp_map.items():
            if data_map:  # 빈 dict가 아니라면
                result_list[table_name].append(data_map)
        print(section_class_id,section_name,":",result_list)
        result_map["result"] = result_list
        print(",,,,,,,,,,,,,,,,,,,",result_list)
        return block_list
    else:
        raise Exception("올바른 섹션 타입이 아닙니다.")

def _get_text_info(block_map):
    text_info = {}
    text_info["page_num"] = block_map.get("page_num",-1)
    text_info["block_box"] = block_map.get("block_box",[-1,-1,-1,-1])
    text_info["section_class_id"] = block_map.get("section_class_id",-1)
    text_info["section_row"] = block_map.get("row",-1)
    text_info["section_col"] = block_map.get("col",-1)
    text_info["structed_text"] = block_map.get("ocr",{}).get("text","")
    return text_info

def _save_block_check_error(section_class_id,section_name,block_data,template_block,block_text,result_map):
    print("=]=[========데이터불일치",block_text, template_block["default_text"])
    block_row_num = int(template_block["block_row_num"])
    block_col_num = int(template_block["block_col_num"])
    default_text = template_block["default_text"]
    
    section_parent = dococr_query_util.select_row_map("selectSectionParent",(section_class_id,))
    error_folder = Path(CLASS_FOLDER) / str(section_parent["doc_class_id"]) / str(section_parent["layout_class_id"]) / "error" / f"{section_class_id}_{section_name}"
    error_img_path = str(error_folder / f"{block_row_num}_{block_col_num}_{result_map["process_id"]}.png")
    error_json_path = str(error_folder / f"{block_row_num}_{block_col_num}_{result_map["process_id"]}.json")
    error_json = block_data[1]
    error_json["add_correction"] = {"section_class_id":section_class_id,"section_name":section_name,
                                    "block_row_num":block_row_num,"block_col_num":block_col_num,"default_text":default_text,"error_text":block_text}
    block_img = type_convert_util.convert_type(block_data[0],"np_bgr","file_path",params="")
    file_util.file_copy(block_img,error_img_path)
    json_util.save(error_json_path,error_json)
    #raise Exception("양식과 다른 곳이 발견되어 작업을 중단합니다. 관리자에게 문의하시기 바랍니다.")

def _save_no_block_error(section_class_id,section_name,block_list,template_block,result_map):
    block_row_num = int(template_block["block_row_num"])
    block_col_num = int(template_block["block_col_num"])
    default_text = template_block["default_text"]
    
    error_folder = Path(CLASS_FOLDER) / "error" / f"{section_class_id}_{section_name}"
    error_json_path = error_folder / f"no_block_{block_row_num}_{block_col_num}_{result_map['process_id']}.json"
    error_json = defaultdict(dict)
    for block_data in block_list:
        row = block_data[1].get("row",-1)
        col = block_data[1].get("col",-1)
        error_json[row][col] = block_data[1]
    error_json["error_info"] = {"section_class_id":section_class_id,"section_name":section_name,
                                    "block_row_num":block_row_num,"block_col_num":block_col_num,"default_text":default_text}
    json_util.save(str(error_json_path),error_json)
    raise Exception(f"{error_json_path}| 필수 오브젝트가 존재하지 않습니다. 관리자에게 문의하시기 바랍니다.")
    
    
function_map = {
    "structuring_by_type": {"function": structuring_by_type, "input_type": "np_bgr", "output_type": "np_bgr", "param": "section_class_id,section_name,section_type"},
}