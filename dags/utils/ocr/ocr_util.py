from collections import Counter, deque
from pathlib import Path
from airflow.models import Variable, XCom
from typing import Any, List, Dict, Tuple
import uuid
import cv2
import numpy as np
import pytesseract
from scipy.ndimage import interpolation as inter
from utils.dev import draw_block_box_util
from utils.com import json_util, file_util
from utils.img import type_convert_util
from typing import Tuple, List
import numpy as np
import cv2
from pathlib import Path

RESULT_FOLDER = Variable.get("RESULT_FOLDER", default_var="/opt/airflow/data/result")
TEMP_FOLDER = Variable.get("TEMP_FOLDER", default_var="/opt/airflow/data/temp")
STEP_INFO_DEFAULT = {
    "name":"separate block default",
    "type":"separate_block_step_list",
    "step_list":[
        {"name":"save","param":{"save_key":"tmp_save"}}
    ]
}

def ocr(block_data:Tuple[Any,Dict], input_img_type:str="np_bgr", step_info:Dict=None, result_map:dict=None) -> Dict:
    """
    이미지 전처리 함수
    :param data: 이미지 파일 경로 또는 numpy 배열
    :param data_type: 입력 데이터의 타입 ("file_path", "np_bgr", "np_gray" 등)
    :param step_info: 전처리 단계 정보 (기본값은 STEP_INFO_DEFAULT)
    :param result_map: 결과를 저장할 맵 (기본값은 빈 딕셔너리)
    :return: 전처리된 이미지 또는 결과
    """
    if step_info is None:
        step_info = STEP_INFO_DEFAULT
    if result_map is None:
        result_map = {}
    step_list = step_info.get("step_list", STEP_INFO_DEFAULT["step_list"])
    return ocr_step_list(block_data=block_data, input_img_type=input_img_type, step_list=step_list, result_map=result_map)

def ocr_step_list(block_data:Tuple[Any,Dict], input_img_type:str="np_bgr", step_list:List[Dict]=None, result_map:dict=None) -> Dict:
    """
    이미지 전처리 함수
    :param data: 이미지 파일 경로 또는 numpy 배열
    :param data_type: 입력 데이터의 타입 ("file_path", "np_bgr", "np_gray" 등)
    :param step_list: 전처리 단계 정보 (기본값은 STEP_INFO_DEFAULT["step_list"])
    :param result_map: 결과를 저장할 맵 (기본값은 빈 딕셔너리)
    :return: 전처리된 이미지 또는 결과
    """
    if step_list is None:
        step_list = STEP_INFO_DEFAULT["step_list"]
    if result_map is None:
        result_map = {}
    process_id = f"_ocr_{str(uuid.uuid4())}"
    result_map["process_id"] = process_id
    result_map["folder_path"] = result_map.get("folder_path",f"{TEMP_FOLDER}/{process_id}")
    result_map["cache"] = {}
    result_map["save_path"] = {}
    
    output = block_data
    before_output_type = input_img_type

    for idx, stepinfo in enumerate(step_list):
        print("step :",stepinfo["name"])
        if stepinfo["name"] not in function_map:
            print(f"경고: '{stepinfo['name']}' 함수가 정의되지 않아 다음 단계를 진행합니다.")
            continue  # 정의되지 않은 함수는 건너뜀
        function_info = function_map[stepinfo["name"]]
        convert_param = stepinfo.get("convert_param", {})
        input = (type_convert_util.convert_type(output[0],before_output_type,function_info["input_type"],params=convert_param), output[1])
        output = function_info["function"](input,**stepinfo["param"],result_map=result_map)
        before_output_type = function_info["output_type"]
    
    return output[1]

def cache(block_data:Tuple[Any,Dict],cache_key:str,result_map:dict)->Tuple[Any,Dict]:
    result_map["cache"][f"filepath_{cache_key}"] = block_data
    return block_data

def load(_,cache_key:str,result_map:dict)->Tuple[Any,Dict]:
    return result_map["cache"][f"filepath_{cache_key}"]

def save(block_data:Tuple[Any,Dict],save_key:str="tmp",tmp_save:bool=False,result_map:dict=None)->Tuple[Any,Dict]:
    if not result_map:
        result_map = {}
    if tmp_save:
        if result_map.get("folder_path", "temp").startswith(TEMP_FOLDER) or result_map.get("folder_path", "temp").startswith(RESULT_FOLDER) :
            img_save_path = Path(result_map.get("folder_path","temp")) / f"{save_key}.png"
            json_save_path = Path(result_map.get("folder_path","temp")) / f"{save_key}.json"
        else : 
            img_save_path = Path(TEMP_FOLDER) / result_map.get("folder_path","temp") / f"{save_key}.png"
            json_save_path = Path(TEMP_FOLDER) / result_map.get("folder_path","temp") / f"{save_key}.json"
        file_util.file_copy(block_data[0],img_save_path)
        json_util.save(str(json_save_path),block_data[1])
    result_map["save_path"][save_key]=block_data
    return block_data

def tesseract(block_data:Tuple[Any,Dict], lang:str="kor", config:str="--psm 6", iter_save:bool=False, result_map:dict=None) -> List[Tuple[Any, Dict]]:
    img_np_bgr, block_map = block_data
    
    gray = cv2.cvtColor(img_np_bgr, cv2.COLOR_BGR2GRAY)
    
    # 스케일 -> 이진화
    # 확대
    x_scale = 2.0
    y_scale = 2.0
    scaled = cv2.resize(gray, None, fx=x_scale, fy=y_scale, interpolation=cv2.INTER_LINEAR)

    # #강화된 이진화
    # thresh = cv2.adaptiveThreshold(
    #     scaled, 255,
    #     cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
    #     cv2.THRESH_BINARY, 11, 2
    # )
    _, thresh = cv2.threshold(scaled, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)

    # 🔧 morphology로 결손 복원
    kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (3, 3))
    thresh = cv2.morphologyEx(thresh, cv2.MORPH_OPEN, kernel, iterations=1)
    morphed = cv2.morphologyEx(thresh, cv2.MORPH_CLOSE, kernel, iterations=1)
    
    thresh_invert = cv2.bitwise_not(morphed)

    
    data_from_scaled = pytesseract.image_to_data(
        thresh_invert, lang=lang, config=config, output_type=pytesseract.Output.DICT)
    if not any(word.strip() for word in data_from_scaled.get('text', [])):
        print("추가 ocr")
        data_from_scaled = pytesseract.image_to_data(
            thresh_invert, lang=lang, config="--oem 3 --psm 8", output_type=pytesseract.Output.DICT)
        
    if iter_save:
        # ocr 처리 이미지 기준 블록 그리기
        draw_block_box_util.draw_block_box_step_list((thresh_invert, data_from_scaled), input_img_type="np_gray", 
            step_list=[{"name": "tesseract_data_to_json", "param": {}},
                    {"name": "draw_block_box_xywh2", "param": {"box_color": 2, "iter_save": True}}],
            result_map={"folder_path":result_map["process_id"]})
    
    #증가배율에 맞춰 좌표 복구
    ocr_data = data_from_scaled.copy()
    for key in ['left', 'width']:
        ocr_data[key] = [value / x_scale for value in data_from_scaled[key]]
    for key in ['top', 'height']:
        ocr_data[key] = [value / y_scale for value in data_from_scaled[key]]
    
    # #블록별 추출 문자 박스 그리기
    # converted_draw_block_list = []
    # for i in range(len(converted['level'])):
    #     block_id = converted['text'][i]
    #     block_box = [converted['left'][i], converted['top'][i], converted['width'][i], converted['height'][i]]
    #     converted_draw_block_list.append({'block_id': block_id, 'block_box': block_box})
    #draw_block_box_util.draw_block_box_step_list((img_np_bgr, converted_draw_block_list), input_img_type="np_bgr", 
    #     step_list=[{"name": "draw_block_box_xywh", "param": {"box_color": 2, "iter_save": True}}],
    #     result_map={"folder_path":result_map["process_id"]})
    combined_text = ' '.join([word for word in ocr_data['text'] if word.strip() != ''])
    conf_values = [int(c) for c in ocr_data['conf'] if ((isinstance(c, str) and c.isdigit()) or isinstance(c, int)) and int(c) >= 0]
    mean_conf = sum(conf_values)/len(conf_values) if conf_values else 0
    block_map['ocr'] = {"tesseract":{"text":combined_text,"conf":mean_conf,"data":ocr_data}}
    print("-_-_-",block_map)
    if iter_save:
        save((type_convert_util.convert_type(thresh_invert, "np_gray", "file_path"), block_map), save_key=block_map["block_id"], tmp_save=True, result_map=result_map)
    return (img_np_bgr,block_map)

function_map = {
    #common
    "cache": {"function": cache, "input_type": "file_path", "output_type": "file_path","param":"cache_key"},
    "load": {"function": load, "input_type": "any", "output_type": "file_path","param":"cache_key"},
    "save": {"function": save, "input_type": "file_path", "output_type": "file_path","param":"save_key,tmp_save"},
    #ocr
    "tesseract": {"function": tesseract, "input_type": "np_bgr", "output_type": "np_bgr", "param": "lang,config,iter_save"},
    
}