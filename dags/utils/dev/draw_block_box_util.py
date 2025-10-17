from collections import Counter
from pathlib import Path
from airflow.models import Variable, XCom
from typing import Tuple, Any, List
import uuid
import cv2
from PIL import Image, ImageDraw, ImageFont
from scipy.ndimage import interpolation as inter
from utils.com import file_util
from utils.img import type_convert_util

RESULT_FOLDER = Variable.get("RESULT_FOLDER", default_var="/opt/airflow/data/result")
TEMP_FOLDER = Variable.get("TEMP_FOLDER", default_var="/opt/airflow/data/temp")
STEP_INFO_DEFAULT = {
    "name":"draw block box default",
    "type":"draw_block_box_step_list",
    "step_list":[
        {"name":"save","param":{"save_key":"tmp_save"}}
    ]
}

def draw_block_box(data:Tuple[Any,List], input_img_type:str="file_path", output_img_type:str="file_path", step_info:dict=None, result_map:dict=None) -> Any:
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
    return draw_block_box_step_list(data=data, input_img_type=input_img_type, output_img_type=output_img_type, step_list=step_list, result_map=result_map)

def draw_block_box_step_list(data:Tuple[Any,List], input_img_type:str="file_path", output_img_type:str="file_path", step_list:dict=None, result_map:dict=None) -> Any:
    """
    이미지 전처리 함수
    :param data: 이미지 파일 경로 또는 numpy 배열
    :param data_type: 입력 데이터의 타입 ("file_path", "np_bgr", "np_gray" 등)
    :param output_type: 출력 데이터의 타입 ("file_path", "np_bgr", "np_gray" 등)
    :param step_list: 전처리 단계 정보 (기본값은 STEP_INFO_DEFAULT["step_list"])
    :param result_map: 결과를 저장할 맵 (기본값은 빈 딕셔너리)
    :return: 전처리된 이미지 또는 결과
    """
    if step_list is None:
        step_list = STEP_INFO_DEFAULT["step_list"]
    if result_map is None:
        result_map = {}
    process_id = f"_box_{str(uuid.uuid4())}"
    result_map["process_id"] = process_id
    result_map["folder_path"] = result_map.get("folder_path",f"{TEMP_FOLDER}/{process_id}")
    result_map["cache"] = {}
    result_map["save_path"] = {}
    
    output = data
    before_output_type = input_img_type

    for stepinfo in step_list:
        print("step :",stepinfo["name"])
        if stepinfo["name"] not in function_map:
            print(f"경고: '{stepinfo['name']}' 함수가 정의되지 않아 다음 단계를 진행합니다.")
            continue  # 정의되지 않은 함수는 건너뜀
        function_info = function_map[stepinfo["name"]]
        
        convert_param = stepinfo.get("convert_param", {})
        input = (type_convert_util.convert_type(output[0],before_output_type,function_info["input_type"],params=convert_param), output[1])
        output = function_info["function"](input,**stepinfo["param"],result_map=result_map)
        before_output_type = function_info["output_type"] if function_info["output_type"]!="any" else before_output_type
    
    result = type_convert_util.convert_type(output[0],before_output_type,output_img_type)
    return result

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

def draw_block_box_xywh1(block_data:Tuple[Any,List], box_color:int=1, iter_save:bool=False, result_map:dict=None) -> tuple:
    """
    블록 박스를 그리는 유틸리티 함수(한글은 깨짐)
    :param block_data: (이미지 numpy 배열, 블록 정보 딕셔너리)
    :param area_type: 영역 타입 ("horizontal" 또는 "vertical")
    :param offset: 오프셋 값
    :param width: 너비
    :param height: 높이
    :param iter_save: 반복 저장 여부
    :return: (이미지 numpy 배열, 블록 정보 딕셔너리)
    """
    img_np_bgr, block_list = block_data
    num=1
    for block_map in block_list:
        if isinstance(block_map, list) and len(block_map) == 4:
            block_map = {"block_box": block_map}
        if "block_id" in block_map:
            block_id = block_map["block_id"]
        else:
            block_id = num
            
        x, y, width, height = block_map["block_box"]
        try:
            if box_color == -1:
                color = CV_COLOR_MAP[(num % len(CV_COLOR_MAP))]
            else:
                color = CV_COLOR_MAP[box_color]
        except KeyError:
            color = CV_COLOR_MAP[0]
        
        cv2.rectangle(img_np_bgr, (int(x), int(y)), (int(x + width), int(y + height)), color, 2)
        cv2.putText(
            img_np_bgr,           # 출력할 이미지 (numpy 배열)
            str(block_id),        # 출력할 텍스트 (여기서는 블록 ID)
            (int(x+3), int(y+15)),               # 텍스트의 좌측 아래 기준 좌표 (x, y)
            cv2.FONT_HERSHEY_SIMPLEX,  # 폰트 종류 (여기서는 기본 산세리프)
            0.5,                  # 폰트 크기 (scale)
            color,                # 텍스트 색상 (BGR 튜플)
            1,                    # 두께 (thickness)
            cv2.LINE_AA           # 선 타입 (Anti-Aliased, 부드러운 글씨)
        )
        num += 1
    
    if iter_save:
        img_path = type_convert_util.convert_type(img_np_bgr, "np_bgr", "file_path")
        save(img_path, result_map=result_map, save_key="dw"+str(width)+"-"+str(height), tmp_save=True)
    return img_np_bgr, block_map

def draw_block_box_xywh2(block_data:Tuple[Any,List], box_color:int=1, iter_save:bool=False, result_map:dict=None) -> tuple:
    """
    블록 박스를 그리는 유틸리티 함수
    :param block_data: (이미지 numpy 배열, 블록 정보 딕셔너리)
    :param box_color: 박스 및 글자색
    :param iter_save: 반복 저장 여부
    :return: (이미지 numpy 배열, 블록 정보 딕셔너리)
    """
    img_pil, block_list = block_data
    draw = ImageDraw.Draw(img_pil)
    font_path="/opt/airflow/data/common/font/NanumGothic.ttf"
    font_size=16
    font = ImageFont.truetype(font_path, font_size)
    
    num=1
    for block_map in block_list:
        print("bbbbbbb",block_map)
        if isinstance(block_map, list) and len(block_map) == 4:
            block_map = {"block_box": block_map}
        if "block_id" in block_map:
            block_id = block_map["block_id"]
        else:
            block_id = num
            
        try:
            if box_color == -1:
                color = CV_COLOR_MAP[(num % len(CV_COLOR_MAP))]
            else:
                color = CV_COLOR_MAP[box_color]
        except KeyError:
            color = CV_COLOR_MAP[0]
        rgb_color = (color[2], color[1], color[0])
        
        x, y, w, h = [int(v) for v in block_map['block_box']]
        block_id = str(block_map.get('block_id', ''))
        # 사각형(상자) 그리기
        draw.rectangle([x, y, x + w, y + h], outline=rgb_color, width=2)
        # 텍스트(한글 포함) 그리기
        draw.text((x + 4, y + 4), block_id, font=font, fill=rgb_color)

        num += 1
    
    if iter_save:
        img_path = type_convert_util.convert_type(img_pil, "pil", "file_path")
        save(img_path, result_map=result_map, save_key=f"dw{w}-{h}", tmp_save=True)
    return img_pil, block_map

def contours_to_json(block_data:Tuple[Any,List], result_map:dict=None) -> tuple:
    #contours, hierarchy = cv2.findContours의 contours를 변환하는 함수
    img, contours = block_data
    result = []
    for idx, contour in enumerate(contours):
        x, y, w, h = cv2.boundingRect(contour)
        block_info = {
            "block_id": idx + 1,
            "block_box": [x, y, w, h]  # x, y, w, h
        }
        result.append(block_info)
    return (img, result)

def tesseract_data_to_json(block_data:Tuple[Any,List], result_map:dict=None) -> tuple:
    #contours, hierarchy = cv2.findContours의 contours를 변환하는 함수
    img, data = block_data
    result = []
    for i in range(len(data['level'])):
        block_id = data['text'][i]
        block_box = [data['left'][i], data['top'][i], data['width'][i], data['height'][i]]
        result.append({'block_id': block_id, 'block_box': block_box})
    return (img, result)

function_map = {
    #common
    "cache": {"function": cache, "input_type": "file_path", "output_type": "file_path","param":"cache_key"},
    "load": {"function": load, "input_type": "any", "output_type": "file_path","param":"cache_key"},
    "save": {"function": save, "input_type": "file_path", "output_type": "file_path","param":"save_key"},
    "draw_block_box_xywh": {"function": draw_block_box_xywh2, "input_type": "pil", "output_type": "pil", "param": "box_color,iter_save"},
    "draw_block_box_xywh1": {"function": draw_block_box_xywh1, "input_type": "np_bgr", "output_type": "np_bgr", "param": "box_color,iter_save"},
    "draw_block_box_xywh2": {"function": draw_block_box_xywh2, "input_type": "pil", "output_type": "pil", "param": "box_color,iter_save"},
    #json 변환
    "contours_to_json": {"function": contours_to_json, "input_type": "any", "output_type": "any", "param": ""},
    "tesseract_data_to_json": {"function": tesseract_data_to_json, "input_type": "any", "output_type": "any", "param": ""},
    
}

CV_COLOR_MAP = {
    0: (0, 255, 0),  # 녹색
    1: (255, 0, 0),  # 파란색
    2: (0, 0, 255),  # 빨간색
    3: (255, 255, 0),  # 청록색
    4: (255, 0, 255),  # 자홍색
    5: (0, 255, 255),  # 노란색
    6: (255, 255, 255),  # 흰색
    7: (0, 0, 0),  # 검은색
}