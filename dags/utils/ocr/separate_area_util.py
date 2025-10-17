from collections import Counter
from pathlib import Path
from airflow.models import Variable, XCom
from typing import Any, List
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
    "name":"separate area default",
    "type":"separate_area_step_list",
    "step_list":[
        {"name":"save","param":{"save_key":"tmp_save"}}
    ]
}

def separate_area(data:Any, data_type:str="file_path", output_type:str="file_path", step_info:dict=None, result_map:dict=None) -> Any:
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
    return separate_area_step_list(data=data, data_type=data_type, output_type=output_type, step_list=step_list, result_map=result_map)

def separate_area_step_list(data:Any, data_type:str="file_path", output_type:str="file_path", step_list:dict=None, result_map:dict=None) -> Any:
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
    process_id = f"_spa_{str(uuid.uuid4())}"
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


def separate_areas_set1(
    img_np_bgr: np.ndarray,
    area_type: str = "top_left",
    area_ratio: List[float] = None,
    area_box: List[int] = None,
    result_key: str = "_area",
    iter_save: bool = False,
    result_map: dict = None,
    **kwargs
) -> np.ndarray:
    """
    이미지를 설정에 따라 단일 영역으로 분리(crop)합니다.
    file_util.py의 설정 방식에 맞춰 단일 영역 처리를 위해 수정되었습니다.

    :param img_np_bgr: BGR 채널을 가진 numpy 배열(OpenCV 이미지)
    :param area_type: 기준점 ('top_left', 'top_center' 등)
    :param area_box: [x, y, w, h] 기준점 기준 영역 박스
    :param kwargs: 추가 파라미터 (사용되지 않음)
    :param iter_save: 비교를 위한 원본 이미지 저장 여부
    :return: 분리된 이미지(np.ndarray).
    """
    h_img, w_img = img_np_bgr.shape[:2]
    if area_box is None:
        if area_ratio is None:
            area_box = [0, 0, -1, -1]
        else:
            x_ratio,y_ratio,w_ratio,h_ratio = area_ratio
            if w_ratio == -1:
                w=-1
            if h_ratio == -1:
                h=-1
            x = int(w_img * x_ratio)
            y = int(h_img * y_ratio)
            w = (int(w_img * w_ratio)) if w_ratio != -1 else -1
            h = (int(h_img * h_ratio)) if h_ratio != -1 else -1
            area_box = [x, y, w, h]

    width = int(area_box[2]) if area_box and len(area_box) > 2 else -1
    height = int(area_box[3]) if area_box and len(area_box) > 3 else -1
    h_img, w_img = img_np_bgr.shape[:2]
    
    # iter_save가 True일 경우, 영역 분리 전 원본 이미지를 저장합니다.
    if iter_save:
        file_path = type_convert_util.convert_type(img_np_bgr, "np_bgr", "file_path")
        save(file_path, "separate_areas_set1_original",result_map=result_map)

    # 1. 기준점(anchor) 계산
    anchor_points = {
        "top_left": (0, 0), "top_center": (w_img // 2, 0), "top_right": (w_img, 0),
        "center_left": (0, h_img // 2), "center": (w_img // 2, h_img // 2), "center_right": (w_img, h_img // 2),
        "bottom_left": (0, h_img), "bottom_center": (w_img // 2, h_img), "bottom_right": (w_img, h_img)
    }
    anchor_x, anchor_y = anchor_points.get(area_type, (0, 0))
    if area_type not in anchor_points:
        print(f"경고: area_type '{area_type}'이 유효하지 않아 top_left로 처리합니다.")

    # 2. 시작 좌표 계산 (offset 적용)
    start_x = anchor_x + int(area_box[0])
    start_y = anchor_y + int(area_box[1])

    # 3. 너비와 높이 계산 (-1 처리)
    crop_w = width if width != -1 else w_img - start_x
    crop_h = height if height != -1 else h_img - start_y

    # 4. 최종 좌표 계산 (이미지 경계 내로 조정)
    x1 = max(0, start_x)
    y1 = max(0, start_y)
    x2 = min(w_img, start_x + crop_w)
    y2 = min(h_img, start_y + crop_h)

    if x1 >= x2 or y1 >= y2:
        print(f"경고: 영역이 이미지 범위를 벗어나 유효하지 않습니다. ({x1},{y1},{x2},{y2})")
        return img_np_bgr

    # 5. 이미지 자르기
    cropped_image = img_np_bgr[y1:y2, x1:x2]
    print(f"영역 분리 완료: pos=({x1}, {y1}), size=({x2-x1}, {y2-y1})")
    result_map[result_key] = (x1,y1)
    return cropped_image

function_map = {
    #common
    "cache": {"function": cache, "input_type": "file_path", "output_type": "file_path","param":"cache_key"},
    "load": {"function": load, "input_type": "any", "output_type": "file_path","param":"cache_key"},
    "save": {"function": save, "input_type": "file_path", "output_type": "file_path","param":"save_key,tmp_save"},
    "separate_areas_set1": {"function": separate_areas_set1, "input_type": "np_bgr", "output_type": "np_bgr", "param": "area_type,area_ratio,area_box,result_key,iter_save"},
}