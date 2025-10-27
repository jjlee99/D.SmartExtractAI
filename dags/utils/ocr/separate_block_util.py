from collections import deque
from pathlib import Path
from airflow.models import Variable, XCom
from typing import Any
import uuid
import cv2, os
import numpy as np
from scipy.ndimage import interpolation as inter
from utils.dev import draw_block_box_util
from utils.com import json_util, file_util
from utils.img import type_convert_util
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

def separate_block(block_data:tuple[Any, dict], input_img_type:str="np_bgr", output_img_type:str="file_path", step_info:dict=None, result_map:dict=None) -> list[tuple[Any, dict]]:
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
    return separate_block_step_list(block_data=block_data, input_img_type=input_img_type, output_img_type=output_img_type, step_list=step_list, result_map=result_map)

def separate_block_step_list(block_data:tuple[Any, dict], input_img_type:str="np_bgr", output_img_type:str="file_path", step_list:list[dict]=None, result_map:dict=None) -> list[tuple[Any, dict]]:
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
    process_id = f"_spb_{str(uuid.uuid4())}"
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
        # 함수가 list로 리턴한 경우 각각을 재귀호출하여 이후 단계를 처리
        if isinstance(output, list):
            tmp_output = []
            print("output is list, separate_block_step_list called recursively", len(output))
            for output_data in output:
                if output_data[1]["child"] > 0: # 최종결과만 다음 작업 진행
                    tmp_output.append(output_data)
                    continue
                each_output = separate_block_step_list(output_data, input_img_type=function_info["output_type"], output_img_type=output_img_type, step_list=step_list[idx+1:], result_map=result_map)
                tmp_output.extend(each_output)
            return tmp_output
        before_output_type = function_info["output_type"]
    
    result = (type_convert_util.convert_type(output[0], before_output_type, output_img_type), output[1])
    result_list = []
    result_list.append(result)
    return result_list

def cache(block_data:tuple[Any, dict],cache_key:str,result_map:dict)->tuple[Any, dict]:
    result_map["cache"][f"filepath_{cache_key}"] = block_data
    return block_data

def load(_,cache_key:str,result_map:dict)->tuple[Any, dict]:
    return result_map["cache"][f"filepath_{cache_key}"]

def save(block_data:tuple[Any,dict],save_key:str="tmp",tmp_save:bool=False,result_map:dict=None)->tuple[Any,dict]:
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

def one_block(block_data:tuple[Any, dict], result_map:dict=None) -> tuple[Any, dict]:
    if result_map is None:
        result_map = {}
    block_data[1]['child'] = 0
    return block_data

def separate_block_by_line(
    block_data: tuple[Any, dict],
    horizontal_first: bool = True,
    horizontal_line_ratio: float = 0.7,
    horizontal_min_gap: int = 15,
    vertical_line_ratio: float = 0.7,
    vertical_min_gap: int = 15,
    iter_save: bool = False,
    result_map: dict = None
) -> list[tuple[np.ndarray, dict]]:
    """
    이미지에서 수평선 또는 수직선을 기준으로 반복적으로 영역을 분리합니다.
    각 방향의 분할은 더 이상 선이 검출되지 않을 때까지 반복됩니다.

    :param block_data: (입력 이미지, 메타데이터) 튜플. 입력 이미지는 BGR np.ndarray, 메타데이터는 dict.
    :param horizontal_first: True면 수평선(가로) 먼저, False면 수직선(세로) 먼저 분할을 시도합니다.
    :param horizontal_line_ratio: 수평선 검출 시 사용할 커널의 너비 비율 (0~1).
    :param horizontal_min_gap: 수평선 기반 최소 분할 영역 높이(픽셀).
    :param vertical_line_ratio: 수직선 검출 시 사용할 커널의 높이 비율 (0~1).
    :param vertical_min_gap: 수직선 기반 최소 분할 영역 너비(픽셀).
    :param iter_save: 디버깅 및 중간 결과 이미지 저장 여부.
    :param result_map: 결과 정보를 저장할 dict 객체. (기본값: None)
    :return: (분할된 이미지, 해당 영역 메타데이터) 튜플의 리스트.
    """

    if result_map is None:
        result_map = {}
    
    horizontal_queue = deque()
    vertical_queue = deque()
    # 큐 및 결과 리스트 초기화
    complete_block_list = []
    
    is_horizontal_turn = horizontal_first
    if is_horizontal_turn:
        horizontal_queue.append(block_data)
    else:
        vertical_queue.append(block_data)

    # 3. horizontal/vertical큐 기준 while문
    num=0
    while horizontal_queue or vertical_queue:
        num += 1
        if is_horizontal_turn:
            print(f"Processing horizontal turn {num}")
            if not horizontal_queue:
                is_horizontal_turn = False
                continue
            pop_data = horizontal_queue.popleft()
            result_list = _separate_areas_by_lines_with_rotation(pop_data,result_map=result_map,
                                                                line_ratio=horizontal_line_ratio,
                                                                min_gap=horizontal_min_gap,
                                                                rotation=False, iter_save=iter_save)
            if num==1 and len(result_list)==0:
                print("첫 수평 분할에서 유효한 분할선을 찾지 못했습니다. 수직 우선 분할로 전환합니다.")
                is_horizontal_turn = False
                vertical_queue.append(pop_data)
                continue
            # 수평 자르기 결과를 수직 큐에 추가
            for item in result_list:
                vertical_queue.append(item)
            pop_data[1]['child'] = len(result_list)  # 자식 개수 업데이트
            complete_block_list.append(pop_data)
        else:
            print(f"Processing vertical turn {num}")
            if not vertical_queue:
                is_horizontal_turn = True
                continue
            pop_data = vertical_queue.popleft()
            result_list = _separate_areas_by_lines_with_rotation(pop_data, result_map=result_map,
                                                                line_ratio=vertical_line_ratio,
                                                                min_gap=vertical_min_gap,
                                                                rotation=True, iter_save=iter_save)
            if num==1 and len(result_list)==0:
                print("첫 수직 분할에서 유효한 분할선을 찾지 못했습니다. 수평 우선 분할로 전환합니다.")
                is_horizontal_turn = True
                horizontal_queue.append(pop_data)
                continue
            # 수직 자르기 결과를 수평 큐에 추가
            for item in result_list:  
                horizontal_queue.append(item)
            pop_data[1]['child'] = len(result_list)  # 자식 개수 업데이트
            complete_block_list.append(pop_data)
        if num>300:
            break
    # 4. 최종 결과 저장
    return complete_block_list

def split_image_contours_gaps(block_data: tuple[Any, dict], min_start_point:int=10, min_gap_width:int=20, angle:int=0, iter_save:bool=False, result_map:dict=None):
    """
    이미지 내 텍스트 사이의 수직 공백을 감지하여 이미지를 분할합니다.

    Args:
        image_path (str): 원본 이미지 파일 경로.
        output_dir (str): 잘라낸 이미지를 저장할 디렉토리 이름.
        min_gap_width (int): 분할 기준으로 삼을 최소 공백의 너비 (픽셀).

    Returns:
        list[str]: 저장된 개별 이미지 파일의 경로 리스트.
    """
    img_np_bgr,block_map = block_data  # block_data는 (이미지 경로, 메타데이터) 형태이므로, 이미지 경로를 추출합니다.
    img_np_bgr = _rotate(img_np_bgr, angle)
    rotated_h, rotated_w, _ = img_np_bgr.shape
    block_id = block_map.get("block_id", "block_name")
    section_name = block_map.get("section_name", "section_name")
    section_class_id = block_map.get("section_class_id", "section_class_id")
    page_num = block_map.get("page_num", -1)

    # 2. 이미지 전처리 (텍스트를 흰색, 배경을 검은색으로)
    gray = cv2.cvtColor(img_np_bgr, cv2.COLOR_BGR2GRAY)
    # Otsu의 이진화는 임계값을 자동으로 계산해줍니다.
    _, thresh = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)

    # 3. 모폴로지 연산으로 텍스트 블록들을 하나로 연결
    # h = img.shape[0]
    # kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (10,10))
    kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (5,5))
    # kernel = np.ones((10,10), np.uint8)
    closed = cv2.morphologyEx(thresh, cv2.MORPH_CLOSE, kernel, iterations=2)

    # 4. 연결된 텍스트 블록의 윤곽선(Contour) 찾기
    contours, _ = cv2.findContours(closed, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

    # 5. 각 윤곽선의 경계 상자(bounding box)를 구하고 x좌표 기준으로 정렬
    bounding_boxes = [cv2.boundingRect(c) for c in contours]
    # 너무 작은 노이즈성 윤곽선은 제거 (너비와 높이가 10픽셀 이상인 것만)
    bounding_boxes = [b for b in bounding_boxes if b[2] > 10 and b[3] > 10]
    bounding_boxes.sort(key=lambda x: x[0])

    first_gap = None
    # 6. 텍스트 블록 사이에서 min_gap_width보다 넓은 모든 수평 공백 찾기
    if len(bounding_boxes) >= 2:
        for i in range(len(bounding_boxes) - 1):
            prev_box = bounding_boxes[i]
            curr_box = bounding_boxes[i+1]
            
            gap_start = prev_box[0] + prev_box[2]
            gap_end = curr_box[0]
            gap_width = gap_end - gap_start
            
            if gap_start > min_start_point and gap_width >= min_gap_width:
                first_gap = {'start': gap_start, 'end': gap_end, 'width': gap_width}
                break
    
    # 7. 분할 기준을 충족하는 공백 중 가장 왼쪽에 있는 공백을 기준으로 이미지 분할
    if first_gap is None:
        bounding_boxes_list = [list(b) for b in bounding_boxes]
        draw_block_box_util.draw_block_box_step_list((closed, bounding_boxes_list), input_img_type="np_gray", step_list=[{"name": "draw_block_box_xywh", "param": {"box_color": 2, "iter_save": True}}],result_map={"folder_path":block_id})
        print("분할할 만큼 충분히 넓은 수평 공백을 찾지 못했습니다. 이미지를 통째로 처리합니다.")
        return block_data
    
    print(f"가장 왼쪽의 유효 공백(너비: {first_gap['width']}px, 시작점: {first_gap['start']})을 기준으로 분할합니다.")
    start_point = 0
    cut_point = first_gap['start'] + first_gap['width'] // 2
    end_point = img_np_bgr.shape[1]
    
    
    
    # 원본 기준 좌표 변환
    first_image = _rotate(img_np_bgr[:, start_point:cut_point],-angle)
    second_image = _rotate(img_np_bgr[:, cut_point:end_point],-angle)
    orig_box = block_map.get("block_box", [0, 0, 0, 0])
    orig_x, orig_y, orig_w, orig_h = orig_box
    
    first_box = _reorient_box(orig_box,[start_point, 0, cut_point-start_point, orig_h],angle)
    second_box = _reorient_box(orig_box,[cut_point, 0, end_point-cut_point, orig_h],angle)

    # 6. 이미지 자르기 및 저장
    cutted_block_data = []
    cutted_block_data.append((first_image,{"block_id": f"{block_id}_g1","block_box": first_box,"child":0,"section_class_id":section_class_id,"section_name":section_name,"page_num":page_num}))
    cutted_block_data.append((second_image,{"block_id": f"{block_id}_g2","block_box": second_box,"child":0,"section_class_id":section_class_id,"section_name":section_name,"page_num":page_num}))

    if iter_save:
        save((type_convert_util.convert_type(cutted_block_data[0][0], "np_bgr", "file_path"),cutted_block_data[0][1]), save_key=cutted_block_data[0][1]["block_id"], tmp_save=True, result_map=result_map)
        save((type_convert_util.convert_type(cutted_block_data[1][0], "np_bgr", "file_path"),cutted_block_data[1][1]), save_key=cutted_block_data[1][1]["block_id"], tmp_save=True, result_map=result_map)
        
    complete_block_list = []
    block_map["child"]=2
    complete_block_list.append((img_np_bgr,block_map))
    complete_block_list.extend(cutted_block_data)
    
    return complete_block_list


def split_image_by_vertical_gaps(
    block_data: tuple[Any, dict], 
    min_start_point:int=None, 
    min_gap_width:int=None, 
    min_start_ratio:float=None, 
    min_gap_ratio:float=None, 
    angle:int=0, 
    iter_save:bool=False, 
    result_map:dict=None
):
    """
    이미지 내 텍스트 사이의 수직 공백을 감지하여 이미지를 분할합니다.

    Args:
        image_path (str): 원본 이미지 파일 경로.
        output_dir (str): 잘라낸 이미지를 저장할 디렉토리 이름.
        min_gap_width (int): 분할 기준으로 삼을 최소 공백의 너비 (픽셀).

    Returns:
        list[str]: 저장된 개별 이미지 파일의 경로 리스트.
    """
    # 설정값 초기화
    img_np_bgr,block_map = block_data  # block_data는 (이미지 경로, 메타데이터) 형태이므로, 이미지 경로를 추출합니다.
    img_np_bgr = _rotate(img_np_bgr, angle)
    rotated_h, rotated_w, _ = img_np_bgr.shape
    # 글자 이전 여백으로 잘못 추출되는 것을 방지하기 위한 설정값
    if min_start_point is None:
        if min_start_ratio is None:
            min_start_point = 10
        else:
            min_start_point = int(min_start_ratio * rotated_w)
    # 일정 이상의 공백만 추출하기 위한 설정값
    if min_gap_width is None:
        if min_gap_ratio is None:
            min_gap_width = 20
        else:
            min_gap_width = int(min_gap_ratio * rotated_w)
    block_id = block_map.get("block_id", "area_name")
    section_name = block_map.get("section_name", "section_name")
    page_num = block_map.get("page_num", -1)
    section_class_id = block_map.get("section_class_id", "section_class_id")
    
    # 2. 이미지 전처리 (텍스트를 흰색, 배경을 검은색으로)
    gray = cv2.cvtColor(img_np_bgr, cv2.COLOR_BGR2GRAY)
    # Otsu의 이진화는 임계값을 자동으로 계산해줍니다.
    _, thresh = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)

    # 3. 모폴로지 연산으로 텍스트 블록들을 하나로 연결
    # h = img.shape[0]
    # kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (10,10))
    kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (5,5))
    # kernel = np.ones((10,10), np.uint8)
    closed = cv2.morphologyEx(thresh, cv2.MORPH_CLOSE, kernel, iterations=2)

    first_gap = None
    consecutive_count = 0
    edge = 2

    for x in range(rotated_w):
        if x < min_start_point:
            continue
        kernel_region = closed[0+edge:rotated_h-edge, x:x+1]
        if np.all(kernel_region == 0):
            if consecutive_count == 0:
                gap_start = x
            consecutive_count += 1
            if consecutive_count >= min_gap_width: # 최소 갭 너비보다 커지면 추출 break
                if gap_start == min_start_point:
                    print("예상하지 못한 위치에서 수평 공백을 발견했습니다. 이미지를 통째로 처리합니다.")
                    return block_data
                gap_end = x + 1
                first_gap = {'start': gap_start, 'end': gap_end, 'width': gap_end - gap_start}
                break
        else:
            consecutive_count = 0
            gap_start = None

    # 7. 분할 기준을 충족하는 공백 중 가장 왼쪽에 있는 공백을 기준으로 이미지 분할
    if first_gap is None:
        print("분할할 만큼 충분히 넓은 수평 공백을 찾지 못했습니다. 이미지를 통째로 처리합니다.")
        return block_data
    
    print(f"가장 왼쪽의 유효 공백(너비: {first_gap['width']}px, 시작점: {first_gap['start']})을 기준으로 분할합니다.")
    start_point = 0
    cut_point = first_gap['start'] + first_gap['width'] // 2
    end_point = img_np_bgr.shape[1]
    
    # 원본 기준 좌표 변환
    first_image = _rotate(img_np_bgr[:, start_point:cut_point],-angle)
    second_image = _rotate(img_np_bgr[:, cut_point:end_point],-angle)
    orig_box = block_map.get("block_box", [0, 0, 0, 0])
    
    first_box = _reorient_box(orig_box,[start_point, 0, cut_point-start_point, rotated_h],angle)
    second_box = _reorient_box(orig_box,[cut_point, 0, end_point-cut_point, rotated_h],angle)

    # 6. 이미지 자르기 및 저장
    cutted_block_data = []
    cutted_block_data.append((first_image,{"block_id": f"{block_id}_g1","block_box": first_box,"child":0,"section_class_id":section_class_id,"section_name":section_name,"page_num":page_num}))
    cutted_block_data.append((second_image,{"block_id": f"{block_id}_g2","block_box": second_box,"child":0,"section_class_id":section_class_id,"section_name":section_name,"page_num":page_num}))

    if iter_save:
        save((type_convert_util.convert_type(cutted_block_data[0][0], "np_bgr", "file_path"),cutted_block_data[0][1]), save_key=cutted_block_data[0][1]["block_id"], tmp_save=True, result_map=result_map)
        save((type_convert_util.convert_type(cutted_block_data[1][0], "np_bgr", "file_path"),cutted_block_data[1][1]), save_key=cutted_block_data[1][1]["block_id"], tmp_save=True, result_map=result_map)
        
    complete_block_list = []
    block_map["child"]=2
    complete_block_list.append((img_np_bgr,block_map))  
    complete_block_list.extend(cutted_block_data)
    
    return complete_block_list

def split_image_by_left_ratio(
    block_data: tuple[Any, dict], 
    split_ratio:float=None, 
    angle:int=0, 
    iter_save:bool=False, 
    result_map:dict=None
):
    """
    이미지를 split_ratio 비율에 따라 분할합니다.

    Args:
        block_data (tuple): 이미지 np array와 메타데이터 dict.
        split_ratio (float, optional): 0~1 사이 값으로 좌우 분할 비율 지정. None일 경우 기존 공백 기준 분할.
        angle (int, optional): 이미지 회전 각도.
        iter_save (bool, optional): 분할 이미지 저장 여부.
        result_map (dict, optional): 저장 결과 매핑.

    Returns:
        list[tuple[np.ndarray, dict]]: 분할된 이미지와 메타데이터 리스트 (최대 2개).
    """
    img_np_bgr, block_map = block_data
    img_np_bgr = _rotate(img_np_bgr, angle)
    rotated_h, rotated_w, _ = img_np_bgr.shape

    split_ratio = max(0, min(split_ratio, 1))  # 0~100 범위 제한
    cut_point = int(rotated_w * split_ratio)

    first_image = _rotate(img_np_bgr[:, :cut_point], -angle)
    second_image = _rotate(img_np_bgr[:, cut_point:], -angle)

    orig_box = block_map.get("block_box", [0, 0, 0, 0])

    first_box = _reorient_box(orig_box, [0, 0, cut_point, rotated_h], angle)
    second_box = _reorient_box(orig_box, [cut_point, 0, rotated_w - cut_point, rotated_h], angle)

    block_id = block_map.get("block_id", "area_name")
    section_name = block_map.get("section_name", "section_name")
    page_num = block_map.get("page_num", -1)
    section_class_id = block_map.get("section_class_id", "section_class_id")

    cutted_block_data = []
    cutted_block_data.append((first_image, {
        "block_id": f"{block_id}_r1",
        "block_box": first_box,
        "child": 0,
        "section_class_id": section_class_id,
        "section_name": section_name,
        "page_num": page_num
    }))
    cutted_block_data.append((second_image, {
        "block_id": f"{block_id}_r2",
        "block_box": second_box,
        "child": 0,
        "section_class_id": section_class_id,
        "section_name": section_name,
        "page_num": page_num
    }))

    if iter_save:
        save((type_convert_util.convert_type(cutted_block_data[0][0], "np_bgr", "file_path"), cutted_block_data[0][1]),
             save_key=cutted_block_data[0][1]["block_id"], tmp_save=True, result_map=result_map)
        save((type_convert_util.convert_type(cutted_block_data[1][0], "np_bgr", "file_path"), cutted_block_data[1][1]),
             save_key=cutted_block_data[1][1]["block_id"], tmp_save=True, result_map=result_map)

    block_map["child"] = 2
    complete_block_list = [(img_np_bgr, block_map)]
    complete_block_list.extend(cutted_block_data)

    return complete_block_list


#내부 함수
def _rotate(img_np_bgr: np.ndarray, angle:float) -> np.ndarray:
    """이미지 회전 내부 함수"""
    # 90도 단위 회전은 손실없이 회전
    if angle % 90 == 0:
        k = int(angle // 90) % 4  # 90도 단위 회전 횟수
        if k < 0:
            k = 4 + k
        return np.rot90(img_np_bgr, k=k)
    
    h, w = img_np_bgr.shape[:2]
    center = (w//2, h//2)
    M = cv2.getRotationMatrix2D(center, angle, 1.0)

    # 회전 후 이미지 크기 계산
    cos = np.abs(M[0, 0])
    sin = np.abs(M[0, 1])
    new_w = int((h * sin) + (w * cos))
    new_h = int((h * cos) + (w * sin))

    # 회전 중심 조정 (중심 이동)
    M[0, 2] += (new_w - w) / 2
    M[1, 2] += (new_h - h) / 2

    # 이미지 테두리에서 가장 많은 색(최빈값) 계산 (예: 흰색/검정)
    def get_most_common_border_color(img):
        top = img[0, :]
        bottom = img[-1, :]
        left = img[1:-1, 0]
        right = img[1:-1, -1]
        border = np.concatenate([top, bottom, left, right], axis=0)
        colors, counts = np.unique(border, axis=0, return_counts=True)
        return colors[counts.argmax()]
    most_common = get_most_common_border_color(img_np_bgr)

    rotated = cv2.warpAffine(
        img_np_bgr, M, (new_w, new_h),
        flags=cv2.INTER_CUBIC,
        borderMode=cv2.BORDER_CONSTANT,
        borderValue=tuple(most_common.tolist())
    )
    return rotated

def _reorient_box(main_block_box,sub_block_box,rotated_angle):
    k = (360+rotated_angle) % 360
    orig_x, orig_y, orig_w, orig_h = main_block_box
    x,y,w,h = sub_block_box
    if k == 90:
        new_x = orig_w - y - h + orig_x
        new_y = x + orig_y
        new_w = h
        new_h = w
    elif k == 180:
        new_x = orig_x + x
        new_y = orig_h - h + orig_y - y
        new_w = w
        new_h = h
    elif k == 270:
        new_x = y + orig_x
        new_y = x + orig_y
        new_w = h
        new_h = w
    else:
        new_x = x + orig_x
        new_y = y + orig_y
        new_w = w
        new_h = h
    return [new_x, new_y, new_w, new_h]
    
def _separate_areas_by_lines_with_rotation(
    block_data: tuple[Any, dict],
    result_map: dict,
    line_ratio: float = 0.7,
    min_gap: int = 15,
    rotation: bool = False,  # 수평은 False, 수직은 True로 설정
    iter_save: bool = False,
    **kwargs
) -> tuple[list[tuple[np.ndarray, str]], bool]:
    """
    return 나뉜 블록의 이미지, 블록맵 목록을 리턴.
    유효한 블록들만 리턴해야함. 의미 없는 블록(너무 작음)은 제외.
    전부 의미없는 블록일 경우 빈 리스트 리턴.
    """
    img_np_bgr, block_map = block_data
    direction = "h"
    # -90도 회전(시계방향)
    if rotation:
        direction = "v"
        img_np_bgr = _rotate(img_np_bgr, -90)
    
    gray = cv2.cvtColor(img_np_bgr, cv2.COLOR_BGR2GRAY)
    _, binary = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)
    
    black_pixel_count = cv2.countNonZero(binary)
    total_pixel_count = binary.shape[0] * binary.shape[1]
    black_ratio = black_pixel_count / total_pixel_count

    # 검정색이 60% 이상이면 라인을 추출할 의미가 없다고 판단하여 빈 리스트 반환
    if black_ratio > 0.6:
        print("Too many black pixels, skipping line detection")
        return []

    closing_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (5, 1))  # 수평선
    closing = cv2.morphologyEx(binary, cv2.MORPH_CLOSE, closing_kernel, iterations=1) # 반전 시 끊김 연결
    dilated_binary = cv2.dilate(closing, cv2.getStructuringElement(cv2.MORPH_RECT, (3, 3)), iterations=1)
    img_h, img_w = dilated_binary.shape

    if iter_save:
        # 라인 디텍트
        block_id = block_map.get("block_id")
        save((type_convert_util.convert_type(dilated_binary, "np_gray", "file_path"),{}), f"{block_id}_delited_binay", tmp_save=True, result_map=result_map)
        meta_path = Path(TEMP_FOLDER) / result_map["folder_path"] / f"{block_id}.json"
        json_util.save(str(meta_path), block_map)

    horizontal_kernel_size = int(img_w * line_ratio)
    #너무 작은 커널 사이즈는 의미가 없으므로 빈 리스트 반환
    if horizontal_kernel_size < 5:
        return []

    # 반전이미지를 긴 커널로 OPEN하여 긴 선 외의 검정선 제거
    detected_lines = cv2.morphologyEx(
        dilated_binary,
        cv2.MORPH_OPEN,
        cv2.getStructuringElement(cv2.MORPH_RECT, (horizontal_kernel_size, 1)),
        iterations=2
    )
    if iter_save:
        # 라인 디텍트
        block_id = block_map.get("block_id")
        save((type_convert_util.convert_type(detected_lines, "np_gray", "file_path"),{}), f"{block_id}_line_detect", tmp_save=True, result_map=result_map)
        meta_path = Path(TEMP_FOLDER) / result_map["folder_path"] / f"{block_id}.json"
        json_util.save(str(meta_path), block_map)

    contours, _ = cv2.findContours(detected_lines, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    y_coords = []
    for c in contours:
        x, y, w, h = cv2.boundingRect(c)
        y_coords.append(y)       # 상단
        y_coords.append(y + h)   # 하단
    if not y_coords:
        # 유효한 분할선이 발견되지 않았을 경우 빈 리스트 반환
        return []
    

    # 분할된 영역의 메타데이터를 저장하기 위한 경로
    block_id = block_map.get("block_id", "area_name")
    section_name = block_map.get("section_name", "section_name")
    section_class_id = block_map.get("section_class_id", "section_class_id")
    page_num = block_map.get("page_num", -1)
    orig_x, orig_y, orig_w, orig_h = block_map.get("block_box", [0, 0, 0, 0])
    
    results = []
    split_num = 1
    split_points = sorted(set([0] + y_coords + [img_h]))
    for i in range(len(split_points) - 1):
        print(f"Processing split {i+1}/{len(split_points)-1} for {block_id} in {direction} direction")
        y1, y2 = split_points[i], split_points[i + 1]
        if y2 - y1 < min_gap:
            print(f"Skipping split {i+1} for {block_id} in {direction} direction due to insufficient height ({y2 - y1}px < {min_gap}px)")
            continue
        sub_img = img_np_bgr[y1:y2, :]
        sub_block_id = f"{block_id}_{direction}{split_num}"
        split_num += 1
        
        sub_block_map = {
            "block_id": sub_block_id,
            "block_box": [0, y1, img_w, y2 - y1],
            "section_class_id":section_class_id,
            "section_name": section_name,
            "page_num":page_num
        }

        results.append((sub_img, sub_block_map))
    
    print("Detected split areas:", len(results))
    # -90도 회전했었다면 각 이미지들 원상복구, 좌표 수정
    if rotation:
        restored_results = []
        for sub_img, sub_block_map in results:
            # 결과 이미지들을 다시 90도(반시계방향) 회전
            restored_img = _rotate(sub_img, 90)
            origin_box = block_map.get("block_box", [0, 0, 0, 0])
            sub_block_box = sub_block_map.get("block_box", [0, 0, 0, 0])
            
            sub_block_map["block_box"] = _reorient_box(origin_box,sub_block_box,-90)
            restored_results.append((restored_img, sub_block_map))
            
            if iter_save:
                # 디버깅용 이미지 저장
                sub_block_id = sub_block_map.get("block_id")
                save((type_convert_util.convert_type(restored_img, "np_bgr", "file_path"),{}), sub_block_id, tmp_save=True, result_map=result_map)
                meta_path = Path(TEMP_FOLDER) / result_map["folder_path"] / f"{sub_block_id}.json"
                json_util.save(str(meta_path), sub_block_map)
        results = restored_results
    else:
        location_correction_results = []
        for sub_img, sub_block_map in results:
            orig_x, orig_y, _, _ = block_map.get("block_box", [0, 0, 0, 0])
            x, y, w, h = sub_block_map.get("block_box", [0, 0, 0, 0])
            # 회전 전 좌표 계산
            new_x = orig_x + x
            new_y = orig_y + y
            new_w = w
            new_h = h
            sub_block_map["block_box"] = [new_x, new_y, new_w, new_h]
            
            location_correction_results.append((sub_img, sub_block_map))
            if iter_save:
                # 디버깅용 이미지 저장
                sub_block_id = sub_block_map.get("block_id", "unknown")
                save((type_convert_util.convert_type(sub_img, "np_bgr", "file_path"),{}), sub_block_id, tmp_save=True, result_map=result_map)
                meta_path = Path(TEMP_FOLDER) / result_map["folder_path"] / f"{sub_block_id}.json"
                json_util.save(str(meta_path), sub_block_map)
        results = location_correction_results
    return results

function_map = {
    #common
    "cache": {"function": cache, "input_type": "file_path", "output_type": "file_path","param":"cache_key"},
    "load": {"function": load, "input_type": "any", "output_type": "file_path","param":"cache_key"},
    "save": {"function": save, "input_type": "file_path", "output_type": "file_path","param":"save_key"},
    #set
    "one_block": {"function": one_block, "input_type": "np_bgr", "output_type": "np_bgr", "param": "step_info,result_map"}, #gbn=
    "separate_block_by_line": {"function": separate_block_by_line, "input_type": "np_bgr", "output_type": "np_bgr", "param": "horizontal_first,horizontal_line_ratio,horizontal_min_gap,vertical_line_ratio,vertical_min_gap,iter_save"}, #gbn=v/h
    "split_image_by_vertical_gaps": {"function": split_image_by_vertical_gaps, "input_type": "np_bgr", "output_type": "np_bgr", "param": "min_start_point,min_gap_width,min_start_ratio,min_gap_ratio,angle,iter_save"}, #gbn=g
    "split_image_by_left_ratio": {"function": split_image_by_left_ratio, "input_type": "np_bgr", "output_type": "np_bgr", "param": "split_ratio,angle,iter_save"}, #gbn=r
}


    