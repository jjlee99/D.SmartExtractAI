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

def img_preprocess(data:Any, data_type:str="file_path", output_type:str="file_path", step_info:Dict=None, result_map:dict=None) -> Any:
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
    return img_preprocess_step_list(data=data, data_type=data_type, output_type=output_type, step_list=step_list, result_map=result_map)

def img_preprocess_step_list(data:Any, data_type:str="file_path", output_type:str="file_path", step_list:List=None, result_map:dict=None) -> Any:
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

def save(file_path:str,save_key:str="tmp",tmp_save:bool=True,result_map:dict=None)->str:
    if tmp_save:
        if result_map.get("folder_path", "temp").startswith(TEMP_FOLDER) or result_map.get("folder_path", "temp").startswith(RESULT_FOLDER) :
            save_path = Path(result_map.get("folder_path","temp")) / f"{save_key}.png"
        else : 
            save_path = Path(TEMP_FOLDER) / result_map.get("folder_path","temp") / f"{save_key}.png"
        file_util.file_copy(file_path,save_path)
    result_map["save_path"][save_key]=file_path
    return file_path

def scale1(img_np_bgr:np.ndarray,width:int,height:int, **kwargs) -> np.ndarray:
    """
    이미지를 지정한 크기(width, height)에 맞게 비율을 유지하며 리사이즈하고,
    남는 공간은 흰색(255)으로 채웁니다.

    :param img_np_bgr: BGR 채널을 가진 numpy 배열(OpenCV 이미지)
    :param width: 결과 이미지의 가로 크기
    :param height: 결과 이미지의 세로 크기
    :return: 크기 조정 및 중앙 정렬된 BGR numpy 배열
    """
    original_height, original_width = img_np_bgr.shape[:2]

    # 비율을 유지하면서 목표 크기를 넘지 않는 최대 크기 계산
    scale_ratio = min(width/original_width, height/original_height)

    # 새로운 크기 계산
    new_width = int(original_width * scale_ratio)
    new_height = int(original_height * scale_ratio)
    
    if scale_ratio < 1.0:
        resized_image = cv2.resize(img_np_bgr, (new_width, new_height), interpolation=cv2.INTER_AREA)
    else:
        resized_image = cv2.resize(img_np_bgr, (new_width, new_height), interpolation=cv2.INTER_CUBIC)
    background = np.full((height, width, 3), 255, dtype=np.uint8)

    # 리사이징된 이미지를 하얀색 배경 중앙에 붙여넣기
    paste_x = (width - new_width) // 2
    paste_y = (height - new_height) // 2
    background[paste_y:paste_y+new_height, paste_x:paste_x+new_width] = resized_image

    return background

def scale2(img_np_bgr:np.ndarray, calc_type:str="width", length:int=1024, **kwargs) -> np.ndarray:
    """
    type이 'width' 또는 'height'일 때, 해당 길이를 length로 맞추고
    비율에 맞게 다른 축도 조정하는 함수

    :param img_np_bgr: BGR 채널을 가진 numpy 배열(OpenCV 이미지)
    :param calc_type: 'long', 'short', 'width', 'height'
    :param length: 조정할 길이
    :return: 비율 유지하며 크기 조정된 BGR numpy 배열
    """
    original_height, original_width = img_np_bgr.shape[:2]
    if calc_type == "long":
        if original_height > original_width:
            calc_type = "height"
        else:
            calc_type = "width"
    elif calc_type == "short":
        if original_height < original_width:
            calc_type = "height"
        else:
            calc_type = "width"
    
    if calc_type == "width":
        scale_ratio = length / original_width
        new_width = length
        new_height = int(original_height * scale_ratio)
    elif calc_type == "height":
        scale_ratio = length / original_height
        new_height = length
        new_width = int(original_width * scale_ratio)
    else:
        raise ValueError("type은 'width' 또는 'height'만 가능합니다.")

    # 리사이즈
    if scale_ratio < 1.0:
        resized_image = cv2.resize(img_np_bgr, (new_width, new_height), interpolation=cv2.INTER_AREA)
    else:
        resized_image = cv2.resize(img_np_bgr, (new_width, new_height), interpolation=cv2.INTER_CUBIC)

    return resized_image



def gray(img_np_bgr: np.ndarray, **kwargs) -> np.ndarray:
    """
    이미지를 그레이스케일로 변환합니다.

    :param img_np_bgr: BGR 채널을 가진 numpy 배열(OpenCV 이미지)
    :return: 그레이스케일로 변환된 numpy 배열
    """
    return cv2.cvtColor(img_np_bgr, cv2.COLOR_BGR2GRAY)

def denoising1(img_np_bgr: np.ndarray, **kwargs) -> np.ndarray:
    """
    컬러 이미지에 대해 Non-Local Means 알고리즘으로 노이즈를 제거합니다.

    :param img_np_bgr: BGR 채널을 가진 numpy 배열(OpenCV 이미지)
    :return: 노이즈가 제거된 BGR numpy 배열
    """
    denoised_np_bgr = cv2.fastNlMeansDenoisingColored(
        src=img_np_bgr,
        h=3,                  # 밝기 성분 강도
        hColor=3,             # 색상 성분 강도
        templateWindowSize=7, # 검사 패치 크기
        searchWindowSize=21   # 검색 윈도우 크기
    )
    return denoised_np_bgr

def denoising2(img_np_bgr: np.ndarray, **kwargs) -> np.ndarray:
    """
    컬러 이미지에 대해 미디언 블러(median blur)로 노이즈를 제거합니다.

    :param img_np_bgr: BGR 채널을 가진 numpy 배열(OpenCV 이미지)
    :return: 노이즈가 제거된 BGR numpy 배열
    """
    denoised_np_bgr = cv2.medianBlur(img_np_bgr, 3)
    return denoised_np_bgr

def threshold(img_np_gray:np.ndarray,thresh:int=127,type:int=cv2.THRESH_BINARY, **kwargs) -> np.ndarray:
    """
    이미지를 임계값 기준으로 이진화합니다.
    thresh : 임계값
    type : 다음 코드값에 따라 지정된 작업을 실행합니다.
      cv2.THRESH_BINARY : 임계값보다 크면 255(흰색) 작으면 0(검정)
      cv2.THRESH_BINARY_INV : 임계값보다 크면 0(검정) 작으면 255(흰색)
      cv2.THRESH_TOZERO : 임계값 이하만 0(검정) 그 외 현상유지
      cv2.THRESH_TOZERO_INV : 임계값 이상만 0(검정) 그 외 현상유지
      cv2.THRESH_OTSU : 이 옵션을 추가하면 임계값을 자동 지정함
    """
    ret, binary_np_gray = cv2.threshold(img_np_gray, thresh=thresh, maxval=255, type=type)
    return binary_np_gray

def adaptive_threshold(img_np_gray:np.ndarray,type:int=cv2.THRESH_BINARY,block:int=11, **kwargs) -> np.ndarray:
    """
    국소적 자동 임계값을 기준으로 이진화합니다.
    type : 다음 코드값에 따라 지정된 작업을 실행합니다.
      cv2.THRESH_BINARY : 임계값보다 크면 255(흰색) 작으면 0(검정)
      cv2.THRESH_BINARY_INV : 임계값보다 크면 0(검정) 작으면 255(흰색)
      cv2.THRESH_TOZERO : 임계값 이하만 0(검정) 그 외 현상유지
      cv2.THRESH_TOZERO_INV : 임계값 이상만 0(검정) 그 외 현상유지
    block : 작을수록 잡음 제거력이 떨어지며, 클수록 이미지 뭉개짐이 높아집니다
    """
    binary_np_gray = cv2.adaptiveThreshold(
        img_np_gray, maxValue=255,
        adaptiveMethod=cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
        thresholdType=type,
        blockSize=block, C=2
    )
    return binary_np_gray

def morphology1(img_np_bgr: np.ndarray, kernel:tuple=(3,3), op:int=cv2.MORPH_OPEN, **kwargs) -> np.ndarray:
    """
    형태학적 연산(열기 → 닫기)을 적용해 노이즈를 제거하고 객체 경계를 보정합니다.
    열기 후 닫기 - 검정을 늘리는 쪽으로 노이즈 제거
    닫기 후 열기 - 흰색 객체를 늘리는 쪽으로 노이즈 제거
    [열기 : 침식 후 팽창] → [닫기 : 팽창 후 침식]
    :param img_np_bgr: 이진화된 numpy 배열(OpenCV 이미지)
    :return: 형태학적 연산이 적용된 numpy 배열
    """
    target_kernel = np.ones(kernel, np.uint8) #커널은 중앙 홀수로 작업
    morphology_img_np_bgr = cv2.morphologyEx(img_np_bgr, op, target_kernel)
    return morphology_img_np_bgr

def morphology2(img_np_bgr: np.ndarray, **kwargs) -> np.ndarray:
    """
    형태학적 연산(열기 → 닫기)을 적용해 노이즈를 제거하고 객체 경계를 보정합니다.
    열기 후 닫기 - 검정을 늘리는 쪽으로 노이즈 제거
    닫기 후 열기 - 흰색 객체를 늘리는 쪽으로 노이즈 제거
    [열기 : 침식 후 팽창] → [닫기 : 팽창 후 침식]
    :param img_np_bgr: 이진화된 numpy 배열(OpenCV 이미지)
    :return: 형태학적 연산이 적용된 numpy 배열
    """
    kernel = np.ones((3,3), np.uint8, **kwargs) #커널은 중앙 홀수로 작업
    open_img_np_bgr = cv2.morphologyEx(img_np_bgr, cv2.MORPH_OPEN, kernel)
    morphology_img_bin = cv2.morphologyEx(open_img_np_bgr, cv2.MORPH_CLOSE, kernel)
    return morphology_img_bin


def canny(img_np_bgr: np.ndarray, **kwargs) -> np.ndarray:
    # 엣지 검출
    edges = cv2.Canny(img_np_bgr, 30, 100, apertureSize=3)
    return edges

def thinner(img_np_bgr: np.ndarray, **kwargs) -> np.ndarray:
    # 전처리 예시: 선 굵기 줄이기
    kernel = np.ones((3,3), np.uint8)
    edges = cv2.erode(img_np_bgr, kernel, iterations=1)
    return edges

def separate_areas_set1(
    img_np_bgr: np.ndarray,
    area_type: str = "top_left",
    offset: List[int] = None,
    width: int = None,
    height: int = None,
    iter_save: bool = False,
    result_map: dict = None,
    **kwargs
) -> np.ndarray:
    """
    이미지를 설정에 따라 단일 영역으로 분리(crop)합니다.
    file_util.py의 설정 방식에 맞춰 단일 영역 처리를 위해 수정되었습니다.

    :param img_np_bgr: BGR 채널을 가진 numpy 배열(OpenCV 이미지)
    :param area_type: 기준점 ('top_left', 'top_center' 등)
    :param offset: [x, y] 오프셋
    :param width: 영역 너비 (-1은 끝까지)
    :param height: 영역 높이 (-1은 끝까지)
    :param kwargs: 추가 파라미터 (사용되지 않음)
    :param iter_save: 비교를 위한 원본 이미지 저장 여부
    :return: 분리된 이미지(np.ndarray).
    """
    if offset is None:
        offset = [0, 0]
    h_img, w_img = img_np_bgr.shape[:2]
    
    # iter_save가 True일 경우, 영역  분리 전 원본 이미지를 저장합니다.
    if iter_save:
        file_path = type_convert_util.convert_type(img_np_bgr, "np_bgr", "file_path")
        save(file_path, "separate_areas_set1_original",result_map=result_map)

    if width is None or height is None:
        print(f"경고: 영역의 너비(width) 또는 높이(height)가 지정되지 않아 건너뜁니다.")
        return img_np_bgr


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
    start_x = anchor_x + offset[0]
    start_y = anchor_y + offset[1]

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

    return cropped_image

def del_blank_set1(img_np_bgr: np.ndarray, padding: int = 5, **kwargs) -> np.ndarray:
    """
    이미지에서 불필요한 상하좌우 공백을 제거하고 약간의 여백(padding)을 줍니다.
    이미지 내에 긴 선이 없거나 너무 얇은 선만 있는 경우 추천
    :param img_np_bgr: BGR 채널을 가진 numpy 배열(OpenCV 이미지)
    :param padding: 잘라낸 이미지 주위에 추가할 픽셀 수
    :return: 공백이 제거된 BGR numpy 배열
    """
    # 1. 그레이스케일 변환 및 이진화 (콘텐츠를 흰색으로)
    gray = cv2.cvtColor(img_np_bgr, cv2.COLOR_BGR2GRAY)
    # 배경이 주로 흰색(255)이므로, 콘텐츠(검정)를 찾기 위해 반전(THRESH_BINARY_INV)
    _, thresh = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)

    # 2. 콘텐츠 영역 찾기
    contours, _ = cv2.findContours(thresh, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

    if not contours:
        # 콘텐츠가 없으면 원본 반환
        print("콘텐츠가 없습니다.")
        return img_np_bgr

    # 3. 모든 콘텐츠를 포함하는 하나의 큰 바운딩 박스 계산
    all_points = np.concatenate(contours, axis=0)
    x, y, w, h = cv2.boundingRect(all_points)

    # 4. 패딩 적용
    h_img, w_img = img_np_bgr.shape[:2]
    x_start = max(0, x - padding)
    y_start = max(0, y - padding)
    x_end = min(w_img, x + w + padding)
    y_end = min(h_img, y + h + padding)

    # 5. 원본 이미지에서 해당 영역 잘라내기
    cropped_img = img_np_bgr[y_start:y_end, x_start:x_end]
    print('공백 제거 완료')
    return cropped_img

def del_blank_set2(
    img_np_bgr: np.ndarray,
    line_ratios: List[float] = [0.05, 0.05], # [length_ratio, height_ratio] 순서
    padding_ratios: List[float] = [0.01, 0.01, 0.01, 0.01], # [top, bottom, left, right] 순서
    iter_save: bool = False,
    result_map: dict = None,
    **kwargs
) -> np.ndarray:
    """
    이미지 내의 긴 수평/수직선을 기준으로 상하좌우 공백을 제거합니다.
    이미지 크기 비율 패딩을 통해 표 외부도 일부 포함 가능합니다.

    :param img_np_bgr: BGR 채널을 가진 numpy 배열(OpenCV 이미지)
    :param line_ratios: [이미지 너비 대비 '긴 선' 길이 비율, 이미지 높이 대비 '긴 선' 길이 비율] 순서의 리스트.
    :param padding_ratios: [상단, 하단, 좌측, 우측] 순서의 패딩 비율 리스트 (이미지 콘텐츠 영역 크기 대비).
    :param iter_save: 중간 결과물(긴 선 이미지) 저장 여부
    :return: 상하좌우 공백이 제거된 BGR numpy 배열
    """
    if iter_save:
        file_path = type_convert_util.convert_type(img_np_bgr, "np_bgr", "file_path")
        save(file_path, "del_blank_set2_origin",result_map=result_map)
        
    # 1. 전처리
    gray = cv2.cvtColor(img_np_bgr, cv2.COLOR_BGR2GRAY)
    binary = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)[1]
    dilated = cv2.dilate(binary, np.ones((3, 3), np.uint8), iterations=1)
    h, w = dilated.shape

    # line_ratios 언팩
    if len(line_ratios) != 2:
        raise ValueError("line_ratios는 [length_ratio, height_ratio] 순서의 2개 float 값을 포함해야 합니다.")
    line_length_ratio, line_height_ratio = line_ratios

    # --- 수평선 추출 ---
    horizontal_size = int(w * line_length_ratio)
    if horizontal_size >= 2:
        horizontal_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (horizontal_size, 1))
        horizontal_lines = cv2.morphologyEx(dilated, cv2.MORPH_OPEN, horizontal_kernel, iterations=2)
    else:
        horizontal_lines = np.zeros_like(dilated)

    # --- 수직선 추출 ---
    vertical_size = int(h * line_height_ratio)
    if vertical_size >= 2:
        vertical_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (1, vertical_size))
        vertical_lines = cv2.morphologyEx(dilated, cv2.MORPH_OPEN, vertical_kernel, iterations=2)
    else:
        vertical_lines = np.zeros_like(dilated)

    # --- 레이아웃 기준 좌/우측 경계 찾기 ---
    combined_lines = cv2.bitwise_or(horizontal_lines, vertical_lines)
    if iter_save:
        file_path = type_convert_util.convert_type(combined_lines, "np_gray", "file_path")
        save(file_path, "del_blank_set2_combined_lines",result_map=result_map)
    if np.any(combined_lines > 0):
        rows, cols = np.where(combined_lines > 0)
        y_start, y_end = np.min(rows), np.max(rows)
        x_start, x_end = np.min(cols), np.max(cols)
    else:
        # 기본값 할당
        y_start, y_end = 0, h
        x_start, x_end = 0, w
    
    # padding_ratios 언팩
    if len(padding_ratios) != 4:
        raise ValueError("padding_ratios는 [top, bottom, left, right] 순서의 4개 float 값을 포함해야 합니다.")
    padding_top_ratio, padding_bottom_ratio, padding_left_ratio, padding_right_ratio = padding_ratios
    # 4. 백분율 패딩을 픽셀 값으로 변환
    # 긴 수평선이 감지된 영역의 높이
    content_h = (y_end + 1) - y_start
    # 긴 수직선이 감지된 영역의 너비
    content_w = (x_end + 1) - x_start
    print("kkkkkkkkkkkkkkkkkkkkcontent_length : ",content_h, content_w)
    if iter_save:
        cropped_img = img_np_bgr[y_start:(y_end + 1), x_start:(x_end+1)]
        file_path = type_convert_util.convert_type(cropped_img, "np_bgr", "file_path")
        save(file_path, "del_blank_set2_content_img",result_map=result_map)
    
    pad_top_px = int(content_h * padding_top_ratio)
    pad_bottom_px = int(content_h * padding_bottom_ratio)
    pad_left_px = int(content_w * padding_left_ratio)
    pad_right_px = int(content_w * padding_right_ratio)
    print("kkkkkkkkkkkkkkkkkkkkpadding_length : ",pad_top_px, pad_bottom_px, pad_left_px, pad_right_px)
    # pad_top_px = int(h * padding_top_ratio)
    # pad_bottom_px = int(h * padding_bottom_ratio)
    # pad_left_px = int(w * padding_left_ratio)
    # pad_right_px = int(w * padding_right_ratio)
    
    # 5. 패딩 적용 및 최종 좌표 계산
    target_y_start = y_start - pad_top_px
    target_y_end = (y_end + 1) + pad_bottom_px
    target_x_start = x_start - pad_left_px
    target_x_end = (x_end + 1) + pad_right_px
    print("kkkkkkkkkkkkkkkkkkkktarget_length : ",target_y_start, target_y_end, target_x_start, target_x_end)
    if iter_save:
        cropped_img = img_np_bgr[y_start:(y_end + 1), x_start:(x_end + 1)]
        file_path = type_convert_util.convert_type(cropped_img, "np_bgr", "file_path")
        save(file_path, "del_blank_set2_normalize_img",result_map=result_map)

    # 6. 새 캔버스(배경) 생성
    new_h = target_y_end - target_y_start
    new_w = target_x_end - target_x_start

    if new_h <= 0 or new_w <= 0:
        print("결과 이미지 크기가 0 또는 음수이므로 원본을 반환합니다.")
        return img_np_bgr

    # 이미지 테두리에서 가장 많이 나타나는 색상(배경색)을 추정
    top_border = img_np_bgr[0, :]
    bottom_border = img_np_bgr[-1, :]
    left_border = img_np_bgr[1:-1, 0]
    right_border = img_np_bgr[1:-1, -1]
    border_pixels = np.concatenate([top_border, bottom_border, left_border, right_border], axis=0)
    
    colors, counts = np.unique(border_pixels.reshape(-1, 3), axis=0, return_counts=True)
    background_color = tuple(colors[counts.argmax()].tolist())

    background = np.full((new_h, new_w, 3), background_color, dtype=np.uint8)

    # 7. 원본 이미지에서 복사할 영역과 새 캔버스에 붙여넣을 영역 계산
    src_y_start = max(0, target_y_start)
    src_y_end = min(h, target_y_end)
    src_x_start = max(0, target_x_start)
    src_x_end = min(w, target_x_end)

    dest_y_start = max(0, -target_y_start)
    dest_x_start = max(0, -target_x_start)

    copy_h = src_y_end - src_y_start
    copy_w = src_x_end - src_x_start

    if copy_h > 0 and copy_w > 0:
        background[dest_y_start : dest_y_start + copy_h, dest_x_start : dest_x_start + copy_w] = \
            img_np_bgr[src_y_start:src_y_end, src_x_start:src_x_end]
    print('공백 제거 및 패딩 적용 완료')
    return background

def calc_padding_set2(img_np_bgr: np.ndarray, result_map:dict, line_ratios: List[float] = [0.05, 0.05], result_key:str="_pad", iter_save:bool=False) -> np.ndarray:
    # 1. 전처리
    gray = cv2.cvtColor(img_np_bgr, cv2.COLOR_BGR2GRAY)
    binary = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)[1]
    dilated = cv2.dilate(binary, np.ones((3, 3), np.uint8), iterations=1)
    h, w = dilated.shape

    # line_ratios 언팩
    if len(line_ratios) != 2:
        raise ValueError("line_ratios는 [length_ratio, height_ratio] 순서의 2개 float 값을 포함해야 합니다.")
    line_length_ratio, line_height_ratio = line_ratios

    # --- 수평선 추출 ---
    horizontal_size = int(w * line_length_ratio)
    if horizontal_size >= 2:
        horizontal_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (horizontal_size, 1))
        horizontal_lines = cv2.morphologyEx(dilated, cv2.MORPH_OPEN, horizontal_kernel, iterations=2)
    else:
        horizontal_lines = np.zeros_like(dilated)

    # --- 수직선 추출 ---
    vertical_size = int(h * line_height_ratio)
    if vertical_size >= 2:
        vertical_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (1, vertical_size))
        vertical_lines = cv2.morphologyEx(dilated, cv2.MORPH_OPEN, vertical_kernel, iterations=2)
    else:
        vertical_lines = np.zeros_like(dilated)

    # --- 레이아웃 기준 좌/우측 경계 찾기 ---
    combined_lines = cv2.bitwise_or(horizontal_lines, vertical_lines)
    if iter_save:
        if result_map["combined_line_path"]:
            params = {"file_path":result_map["combined_line_path"]}
        else:
            params = None
        type_convert_util.convert_type(combined_lines, "np_gray", "file_path",params=params)
    if np.any(combined_lines > 0):
        rows, cols = np.where(combined_lines > 0)
        y_start, y_end = np.min(rows), np.max(rows)
        x_start, x_end = np.min(cols), np.max(cols)
    else:
        # 기본값 할당
        y_start, y_end = 0, h
        x_start, x_end = 0, w
    
    # 4. 백분율 패딩을 픽셀 값으로 변환
    # 긴 수평선이 감지된 영역의 높이
    content_h = (y_end + 1)- y_start
    # 긴 수직선이 감지된 영역의 너비
    content_w = (x_end + 1) - x_start

    # 패딩픽셀을 비율로 환산
    top_ratio_calc = round(y_start / content_h, 4)
    bottom_ratio_calc = round((h - (y_end + 1)) / content_h, 4)
    left_ratio_calc = round(x_start / content_w, 4)
    right_ratio_calc = round((w - (x_end + 1)) / content_w, 4)

    result_map[result_key] = [top_ratio_calc,bottom_ratio_calc,left_ratio_calc,right_ratio_calc]
    return img_np_bgr



def calc_angle_set1(img_np_bgr: np.ndarray,angle_key:str, result_map:dict, iterations:int=3, iter_save:bool=False) -> np.ndarray:
    """
    다각형 근사화를 활용한 표 인식 및 미세회전
    문서 방향 조정을 위해 text_orientation_set과 함께 사용 추천
    """
    target_img=img_np_bgr
    total_angle=0
    idx=1
    while idx<=iterations:
        # 1. 회전을 위한 전처리
        # 1-1. 그레이스케일
        gray = cv2.cvtColor(target_img, cv2.COLOR_BGR2GRAY)
        # 1-2. 이진화 (표 경계를 명확하게)
        _, thresh = cv2.threshold(gray, 128, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)
        # # 임시 저장
        # if iter_save:
        #     file = type_convert_util.convert_type(thresh,"np_bgr","file_path")
        #     save(file,f"rotate1_{idx}")
        
        # 2. 보정각도 추출
        # 2-1. 윤곽선 검출
        contours, _ = cv2.findContours(thresh, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        # 2-2. 가장 큰 윤곽선 선택
        largest_contour = max(contours, key=cv2.contourArea)
        # 2-3. 다각형 근사화 (4꼭지점 추출)
        epsilon = 0.02 * cv2.arcLength(largest_contour, True)
        approx = cv2.approxPolyDP(largest_contour, epsilon, True)
        if not len(approx) == 4:
            break
        else:
            # 2-4. 꼭지점 기준 각도 계산(상단만 체크)
            corners = approx.reshape(4, 2)
            # 꼭지점 정렬 (x+y 기준으로 좌상단, 우상단, 좌하단, 우하단)
            sorted_corners = sorted(corners, key=lambda pt: (pt[0] + pt[1]))
            top_left, top_right, bottom_left, bottom_right = sorted_corners
            # 상단 벡터 (우상단 - 좌상단)
            dx_top = top_right[0] - top_left[0]
            dy_top = top_right[1] - top_left[1]
            angle_top = np.degrees(np.arctan2(dy_top, dx_top))
            # 하단 벡터 (우상단 - 좌상단)
            dx_top = bottom_right[0] - bottom_left[0]
            dy_top = bottom_right[1] - bottom_left[1]
            angle_bottom = np.degrees(np.arctan2(dy_top, dx_top))
            # 좌측 벡터 (좌하단 - 좌상단)
            dx_left = bottom_left[0] - top_left[0]
            dy_left = bottom_left[1] - top_left[1]
            angle_left = np.degrees(np.arctan2(dy_left, dx_left)) + 90
            # 우측 벡터 (좌하단 - 좌상단)
            dx_left = bottom_right[0] - top_right[0]
            dy_left = bottom_right[1] - top_right[1]
            angle_right = np.degrees(np.arctan2(dy_left, dx_left)) + 90
            # 평균
            avg_angle = (angle_top+angle_bottom+angle_left+angle_right)/4
            print(f"angle{idx} : ",total_angle,avg_angle, angle_left, angle_right, angle_top, angle_bottom)
            if avg_angle < 0.1:
                break
            
            # 3. 타겟이미지를 보정 각도만큼 회전
            rotated = _rotate(target_img,avg_angle)
            # 4. 반복 처리를 위한 작업
            total_angle+=avg_angle
            target_img = rotated
            idx+=1
    
    result_map["cache"][f"angle_{angle_key}"] = total_angle
    return target_img


def before_angle1(img_np_bgr: np.ndarray, **kwargs) -> np.ndarray:
    # 1. 그레이스케일
    gray = cv2.cvtColor(img_np_bgr, cv2.COLOR_BGR2GRAY)
    # 2. 이진화 (표 경계를 명확하게)
    _, thresh = cv2.threshold(gray, 128, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)
    return thresh

def calc_angle1(img_np_gray: np.ndarray, angle_key: str, result_map:dict) -> float:
    # 윤곽선 검출
    contours, _ = cv2.findContours(img_np_gray, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    # 가장 큰 윤곽선 선택
    largest_contour = max(contours, key=cv2.contourArea)
    # 다각형 근사화 (4꼭지점 추출)
    epsilon = 0.02 * cv2.arcLength(largest_contour, True)
    approx = cv2.approxPolyDP(largest_contour, epsilon, True)
    
    if not len(approx) == 4:
        print("표의 4꼭지점을 찾을 수 없습니다.")
        result_map["cache"][f"angle_{angle_key}"] = 0
        return img_np_gray
    else:
        corners = approx.reshape(4, 2)
        # 꼭지점 정렬 (x+y 기준으로 좌상단, 우상단, 좌하단, 우하단)
        sorted_corners = sorted(corners, key=lambda pt: (pt[0] + pt[1]))
        top_left, top_right, bottom_left, bottom_right = sorted_corners

        # 상단 벡터 (우상단 - 좌상단)
        dx_top = top_right[0] - top_left[0]
        dy_top = top_right[1] - top_left[1]
        angle_top = np.degrees(np.arctan2(dy_top, dx_top))
        # 가로선을 0도로 맞추는 보정 각도
        correction_angle_top = -angle_top

        # 좌측 벡터 (좌하단 - 좌상단)
        dx_left = bottom_left[0] - top_left[0]
        dy_left = bottom_left[1] - top_left[1]
        angle_left = np.degrees(np.arctan2(dy_left, dx_left))
        # 세로선을 90도로 맞추는 보정 각도
        correction_angle_left = 90 - angle_left

        # 보정 각도 출력 및 반환 (가로선 기준 또는 평균, 필요에 따라 선택)
        print(f"가로선 보정 각도: {correction_angle_top:.2f}도")
        print(f"세로선 보정 각도: {correction_angle_left:.2f}도")
        result_map["cache"][f"angle_{angle_key}"] = (correction_angle_top*correction_angle_left)/2 * -1
        return img_np_gray

def calc_angle_set2(img_np_bgr:np.ndarray,angle_key:str, result_map:dict,delta:float=0.25,limit:int=5,iterations:int=3,iter_save:bool=False) -> np.ndarray:
    """
    각도별 커널 탐색을 활용한 수평선/수직선 인식 및 미세회전
    문서 방향 조정을 위해 text_orientation_set과 함께 사용 추천
    """
    target_img=img_np_bgr
    total_angle=0
    idx=1
    
    while idx<=iterations:
        # delta 간격으로 limit값의 ± 범위를 돌려보며 적절한 각도 탐색 
        angles = np.arange(-limit, limit + delta, delta)
        scores = []
        
        (img_h, img_w) = target_img.shape[:2]
        min_length = int(min(img_w,img_h) * 0.1)
        min_length2 = int(min(img_w,img_h) * 0.4)
        
        def long_kernal_score(arr, angle):
            i=0
            #짧은선
            horizon_kernel = np.ones((min_length, 1), np.uint8)
            vertical_kernel = np.ones((1, min_length), np.uint8)
            #긴선
            horizon_kernel2 = np.ones((min_length2, 1), np.uint8)
            vertical_kernel2 = np.ones((1, min_length2), np.uint8)
            
            # # 1-3. 작은 객체 제거
            # kernel_horiz = np.ones((1, line_kernel), np.uint8)  # 가로 방향 커널 (길이 조정 필요)
            # horizontal = cv2.morphologyEx(binary, cv2.MORPH_OPEN, kernel_horiz)

            # # 수직선 강조 (글자 제거, 수직선만 남기기)
            # kernel_vert = np.ones((line_kernel, 1), np.uint8)   # 세로 방향 커널 (길이 조정 필요)
            # vertical = cv2.morphologyEx(binary, cv2.MORPH_OPEN, kernel_vert)
            
            # # 수평+수직 합치기
            # line_filtered = cv2.add(horizontal, vertical)
            
            # # 1.3. 반전
            # lines_inv = cv2.bitwise_not(line_filtered)
            
            # # 1-4. 에지 검출 (Canny)
            # edges = cv2.Canny(lines_inv, 50, 150, apertureSize=3)
        
            #3,3 커널을 사용하여 침식(검정 늘어남)
            eroded = cv2.erode(arr, np.ones((3, 3), np.uint8), iterations=1)

            # data = inter.rotate(arr, angle, reshape=False, order=0)
            data = _rotate(eroded,angle)
            
            # 1-1. 그레이스케일
            gray = cv2.cvtColor(data, cv2.COLOR_BGR2GRAY)
            # 1-2. 이진화 (표 경계를 명확하게)
            thresh = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)[1]
                        
            # 임시 저장
            if iter_save:
                file = type_convert_util.convert_type(thresh,"np_bgr","file_path")
                save(file,f"rotate21_{idx}_{angle}",result_map=result_map)

            #짧은 선과 긴 선을 기준으로 침식(검정 늘어남)=>
            #1-2이진화에서 반전한 이미지를 침식했으므로 실제로는 검정 수직수평선을 찾아 길이를 1 줄임
            #긴 선일수록 픽셀이 많이 남음
            horizon_eroded = cv2.erode(thresh, horizon_kernel)
            vertical_eroded = cv2.erode(thresh, vertical_kernel)
            horizon_eroded2 = cv2.erode(thresh, horizon_kernel2)
            vertical_eroded2 = cv2.erode(thresh, vertical_kernel2)
            
            #각 작업의 픽셀개수를 세어 점수 계산
            score = cv2.countNonZero(horizon_eroded) + cv2.countNonZero(vertical_eroded) + cv2.countNonZero(horizon_eroded2) + cv2.countNonZero(vertical_eroded2)
            # 임시 저장 
            if iter_save :
                tmp=cv2.add(horizon_eroded,vertical_eroded)
                tmp2=cv2.add(horizon_eroded2,vertical_eroded2)
                tmp3=cv2.add(tmp,tmp2)
                i+=1
                file = type_convert_util.convert_type(tmp3,"np_bgr","file_path")
                save(file,f"rotate22_{idx}_{angle}_{score}",result_map=result_map)
            
            return score
        
        for angle in angles:
            score = long_kernal_score(target_img, angle)
            scores.append(score)
        
        # best_angle이 일정 이하인 경우 무시
        threshold_val = img_w * 0.05 * img_h * 0.05
        best_angle = angles[scores.index(max(scores))]
        if max(scores) <= threshold_val:
            best_angle = 0
        total_angle+=best_angle
        print(f"angle_score {idx}",total_angle,best_angle, max(scores), scores)
        
        # 0도일 경우 반복 중지
        if best_angle==0:
            break
        # 3. 타겟이미지를 보정 각도만큼 회전
        rotated = _rotate(target_img,best_angle)
    
        # 4. 반복 처리를 위한 작업
        target_img = rotated
        idx+=1
    result_map["cache"][f"angle_{angle_key}"] = total_angle
    return target_img 

def calc_angle_set3(img_np_bgr:np.ndarray,angle_key:str, result_map:dict,iterations:int=3,iter_save:bool=False) -> np.ndarray:
    """
    허프변환을 활용한 직선 인식 및 미세회전
    문서에 따른 수치 조정이 많이 필요함(미완)
    문서 방향 조정을 위해 text_orientation_set과 함께 사용 추천
    """
    target_img=img_np_bgr
    total_angle=0
    idx=1
    tolerance = 2
    
    height, width = img_np_bgr.shape[:2]
    line_kernel = int(min(width,height) * 0.2)  # 전체 크기 10% 이상 길이의 선을 기준으로 문자 제거
    while idx<=iterations:
        # 1-1. 그레이스케일
        gray = cv2.cvtColor(target_img, cv2.COLOR_BGR2GRAY)
        # 1-2. 이진화 (표 경계를 명확하게)
        _, binary = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)
        # 1-3. 작은 객체 제거
        kernel_horiz = np.ones((1, line_kernel), np.uint8)  # 가로 방향 커널 (길이 조정 필요)
        horizontal = cv2.morphologyEx(binary, cv2.MORPH_OPEN, kernel_horiz)

        # 수직선 강조 (글자 제거, 수직선만 남기기)
        kernel_vert = np.ones((line_kernel, 1), np.uint8)   # 세로 방향 커널 (길이 조정 필요)
        vertical = cv2.morphologyEx(binary, cv2.MORPH_OPEN, kernel_vert)
        
        # 수평+수직 합치기
        line_filtered = cv2.add(horizontal, vertical)
        
        # 1.3. 반전
        lines_inv = cv2.bitwise_not(line_filtered)
        
        # 1-4. 에지 검출 (Canny)
        edges = cv2.Canny(lines_inv, 50, 150, apertureSize=3)
        
        # 임시 저장
        if iter_save:
            file = type_convert_util.convert_type(edges,"np_bgr","file_path")
            save(file,f"rotate3_{idx}",result_map=result_map)
        
        # 2. Hough Line Transform으로 선 검출
        lines = cv2.HoughLines(edges, 1, np.pi/180, threshold=150)
        angles = []
        if lines is not None:
            for line in lines:
                rho, theta = line[0]
                # 각도를 도 단위로 변환
                angle = np.degrees(theta)
                # 0~179도 범위로 정규화
                angle = angle % 180
                if angle < 0:
                    angle += 180
                angles.append(angle)
        
        if not angles:
            print("선을 찾을 수 없습니다.")
            break
        grouped_angles = []
        for angle in angles:
            grouped_angle = round(angle / tolerance) * tolerance
            grouped_angles.append(grouped_angle)
        tolerance = tolerance/2
        # 가장 많이 나타나는 각도 찾기
        angle_counter = Counter(grouped_angles)
        dominant_angle = angle_counter.most_common(1)[0][0]
        
        # 후보군 중 가장 적게 회전하는 각도 탐색
        candidates = [0, 90, -90]
        differences = [abs(dominant_angle - candidate) for candidate in candidates]
        target_angle = candidates[differences.index(min(differences))]
        rotation_angle = target_angle - dominant_angle

        total_angle+=rotation_angle
        print(f"osd {idx}",total_angle,rotation_angle, angles)
            
        # 3. 타겟이미지를 보정 각도만큼 회전
        rotated = _rotate(target_img,rotation_angle)
        
        # 4. 반복 처리를 위한 작업
        target_img = rotated
        idx+=1
    result_map["cache"][f"angle_{angle_key}"] = total_angle
    return target_img

def calc_angle_set4(img_np_bgr:np.ndarray,angle_key:str, result_map:dict,iterations:int=3,iter_save:bool=False) -> np.ndarray:
    """
    각도별 수평/수직 픽셀들의 변화 활용해 미세회전
    문서 방향 조정을 위해 text_orientation_set과 함께 사용 추천
    """
    target_img=img_np_bgr
    total_angle=0
    idx=1
    
    while idx<=iterations:
        # 1-1. 그레이스케일
        gray = cv2.cvtColor(target_img, cv2.COLOR_BGR2GRAY)
        # 1-2. 이진화 (표 경계를 명확하게)
        thresh = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)[1]

        delta = 0.25
        limit = 5
        angles = np.arange(-limit, limit + delta, delta)
        scores = []

        def histogram_score(arr, angle):
            data = inter.rotate(arr, angle, reshape=False, order=0)
            histogram = np.sum(data, axis=1, dtype=float)
            score = np.sum((histogram[1:] - histogram[:-1]) ** 2, dtype=float)
            return score
        
        for angle in angles:
            score = histogram_score(thresh, angle)
            scores.append(score)

        best_angle = angles[scores.index(max(scores))]
        total_angle+=best_angle
        print(f"osd {idx}",total_angle,best_angle,scores)
        
        # 3. 타겟이미지를 보정 각도만큼 회전
        rotated = _rotate(target_img,best_angle)
        
        # 4. 반복 처리를 위한 작업
        target_img = rotated
        idx+=1
    result_map["cache"][f"angle_{angle_key}"] = total_angle
    return target_img

def text_orientation_set(img_np_bgr:np.ndarray,angle_key:str, result_map:dict,iterations:int=2,iter_save:bool=False) -> np.ndarray:
    """
    테서랙트의 텍스트 방향과 문자 종류 감지를 활용한 90도 단위 회전
    미세조정을 위해 calc_angle_set1,3,5 등과 함께 사용 추천
    """
    target_img=img_np_bgr
    total_angle=0
    idx=1
    while idx<=iterations:
        # 1. 회전을 위한 전처리
        # 1-1. 노이즈 제거
        denoised = cv2.fastNlMeansDenoisingColored(
            src=target_img,
            h=3,                  # 밝기 성분 강도
            hColor=3,             # 색상 성분 강도
            templateWindowSize=7, # 검사 패치 크기
            searchWindowSize=21   # 검색 윈도우 크기
        )
        # 1-2. 그레이스케일
        gray = cv2.cvtColor(denoised, cv2.COLOR_BGR2GRAY)
        # 1-3. 이진화 (표 경계를 명확하게)
        _, binary = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
        # 1-4. 태서렉트 입력을 위해 rgb로 변경
        rgb = cv2.cvtColor(binary, cv2.COLOR_GRAY2RGB)
        # 임시 저장
        if iter_save:
            file = type_convert_util.convert_type(rgb,"np_bgr","file_path")
            save(file,f"rotate2_{idx}",result_map=result_map)
        
        # 2. ocr을 이용한 보정 각도 추출
        try:
            osd = pytesseract.image_to_osd(rgb)
        except pytesseract.TesseractError as e:
            print(f"Tesseract OSD Error: {e}")
            break
    
        rotation = 0
        for info in osd.split('\n'):
            if 'Rotate: ' in info:
                rotation = int(info.split(': ')[1])
            if 'Orientation confidence:' in info:
                orientation_confidence = float(info.split(': ')[1])
            if 'Script: ' in info:
                script_name = info.split(': ')[1]
            if 'Script confidence:' in info:
                script_confidence = float(info.split(': ')[1])
        if rotation == 0:
            print(f"osd {idx} break ",total_angle,rotation, orientation_confidence, script_name, script_confidence)
            break
        total_angle+=rotation
        print(f"osd {idx}",total_angle, rotation, orientation_confidence, script_name, script_confidence)
        
        # 3. 타겟이미지를 보정 각도만큼 회전
        rotated = _rotate(target_img,rotation)
        
        # 4. 반복 처리를 위한 작업
        target_img = rotated
        idx+=1
    result_map["cache"][f"angle_{angle_key}"] = total_angle
    return target_img

def before_orientation(img_np_bgr: np.ndarray, **kwargs) -> np.ndarray:
    # 1. 그레이스케일
    gray = cv2.cvtColor(img_np_bgr, cv2.COLOR_BGR2GRAY)
    # 2. 이진화 (표 경계를 명확하게)
    _, thresh = cv2.threshold(gray, 128, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)
    return thresh

def calc_orientation(img_np_bgr: np.ndarray,angle_key:str, result_map:dict) -> np.ndarray:
    """테서랙트 OSD를 이용한 방향 보정"""
    try:
        osd = pytesseract.image_to_osd(img_np_bgr)
        rotation = 0
        for info in osd.split('\n'):
            if 'Rotate: ' in info:
                rotation = int(info.split(': ')[1])
    except pytesseract.TesseractError as e:
        print(f"Tesseract OSD Error: {e}")
        rotation = 0            
    print(f"가로선 보정 각도: {rotation:.2f}도")
    result_map["cache"][f"angle_{angle_key}"] = rotation
    return img_np_bgr


def rotate(img_np_bgr: np.ndarray, result_map:dict, angle_key: str = None, angle_keys: List = []) -> np.ndarray:
    """이미지 회전 함수"""
    if len(angle_keys) > 0:
        angle = 0.0
        for key in angle_keys:
            angle += result_map["cache"].get(f"angle_{key}", 0)
    else:
        angle = result_map["cache"].get(f"angle_{angle_key}", 0)
    if angle == 0:
        return img_np_bgr
    rotated = _rotate(img_np_bgr, angle)
    return rotated

def line_tracking(img_np_gray:np.ndarray, iter_save:bool=False, result_map:dict=None, **kwargs) -> np.ndarray:
    _, binary = cv2.threshold(img_np_gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)
    # 임시 저장
    if iter_save:
        print("binary:", binary.shape)
        file = type_convert_util.convert_type(binary,"np_gray","file_path")
        save(file,f"line_binary",result_map=result_map)
    
    # 2. 수평/수직 라인 강조 (모폴로지 연산)
    horizontal = binary.copy()
    vertical = binary.copy()

    # 수평 라인 검출
    cols = horizontal.shape[1]  #가로픽셀수
    horizontal_size = cols // 30  # 표 구조에 따라 조정
    horizontal_structure = cv2.getStructuringElement(cv2.MORPH_RECT, (horizontal_size, 1))
    horizontal = cv2.erode(horizontal, horizontal_structure)
    horizontal = cv2.dilate(horizontal, horizontal_structure)

    # 수직 라인 검출
    rows = vertical.shape[0]  #세로픽셀수
    vertical_size = rows // 30  # 표 구조에 따라 조정
    vertical_structure = cv2.getStructuringElement(cv2.MORPH_RECT, (1, vertical_size))
    vertical = cv2.erode(vertical, vertical_structure)
    vertical = cv2.dilate(vertical, vertical_structure)

    # 3. 라인 합치기 (표 전체 구조)
    table_mask = cv2.add(horizontal, vertical)

    # 4. 라인 추적 (연결성 보완)
    # 라벨링으로 연결된 라인 추출
    num_labels, labels, stats, centroids = cv2.connectedComponentsWithStats(table_mask, connectivity=8)

    # 결과 시각화
    output = cv2.cvtColor(img_np_gray, cv2.COLOR_GRAY2BGR)
    for i in range(1, num_labels):
        x, y, w, h, area = stats[i]
        if area > 50:  # 작은 잡음 제거
            cv2.rectangle(output, (x, y), (x + w, y + h), (0, 255, 0), 2)

    # 임시 저장
    if iter_save:
        print("output:", output.shape)
        file = type_convert_util.convert_type(output,"np_bgr","file_path")
        save(file,f"detected_lines",result_map=result_map)
    return output
    
#내부 함수
def _rotate(img_np_bgr: np.ndarray, angle:float) -> np.ndarray:
    """이미지 회전 내부 함수"""
    # 90도 단위 회전은 손실없이 회전
    if angle % 90 == 0:
        k = int(angle // 90) % 4  # 90도 단위 회전 횟수
        if k < 0:
            k = 4 + k
        return np.rot90(img_np_bgr, k=k)

    img_h, img_w = img_np_bgr.shape[:2]
    center = (img_w//2, img_h//2)
    M = cv2.getRotationMatrix2D(center, angle, 1.0)

    # 회전 후 이미지 크기 계산
    cos = np.abs(M[0, 0])
    sin = np.abs(M[0, 1])
    new_w = int((img_h * sin) + (img_w * cos))
    new_h = int((img_h * cos) + (img_w * sin))

    # 회전 중심 조정 (중심 이동)
    M[0, 2] += (new_w - img_w) / 2
    M[1, 2] += (new_h - img_h) / 2

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

#이후
function_map = {
    # 공통
    "cache": {"function": cache,"input_type": "file_path","output_type": "file_path","param": "cache_key"},
    "load": {"function": load,"input_type": "any","output_type": "file_path","param": "cache_key"},
    "save": {"function": save,"input_type": "file_path","output_type": "file_path","param": "save_key,tmp_save"},

    # 전처리/스케일/그레이/노이즈
    "scale": {"function": scale1,"input_type": "np_bgr","output_type": "np_bgr","param": "width,height"},
    "scale1": {"function": scale1,"input_type": "np_bgr","output_type": "np_bgr","param": "width,height"},
    "scale2": {"function": scale2,"input_type": "np_bgr","output_type": "np_bgr","param": "calc_type,length"},
    "gray": {"function": gray,"input_type": "np_bgr","output_type": "np_gray","param": ""},
    "denoising": {"function": denoising1,"input_type": "np_bgr","output_type": "np_bgr","param": ""},
    "denoising1": {"function": denoising1,"input_type": "np_bgr","output_type": "np_bgr","param": ""},
    "denoising2": {"function": denoising2,"input_type": "np_bgr","output_type": "np_bgr","param": ""},

    # 이진화/형태학/엣지
    "threshold": {"function": threshold,"input_type": "np_gray","output_type": "np_gray","param": "thresh,type"},
    "adaptive_threshold": {"function": adaptive_threshold,"input_type": "np_gray","output_type": "np_gray","param": "type,block"},
    "morphology1": {"function": morphology1,"input_type": "np_bgr","output_type": "np_bgr","param": ""},
    "canny": {"function": canny,"input_type": "np_bgr","output_type": "np_bgr","param": ""},
    "thinner": {"function": thinner,"input_type": "np_bgr","output_type": "np_bgr","param": ""},

    # 공백/크롭/분리
    "del_blank_set1": {"function": del_blank_set1,"input_type": "np_bgr","output_type": "np_bgr","param": "padding"},
    "calc_padding_set2" : {"function": calc_padding_set2,"input_type": "np_bgr","output_type": "np_bgr","param": "area_type,offset,width,height,iter_save"},
    "del_blank_set2": {"function": del_blank_set2,"input_type": "np_bgr","output_type": "np_bgr","param": "line_ratios,padding_ratios,iter_save"},
    "separate_areas_set1": {"function": separate_areas_set1,"input_type": "np_bgr","output_type": "np_bgr","param": "area_type,offset,width,height,iter_save"},

    # 각도/방향 보정
    "calc_angle_set1": {"function": calc_angle_set1,"input_type": "np_bgr","output_type": "np_bgr","param": "angle_key,iterations,iter_save"},
    "calc_angle_set2": {"function": calc_angle_set2,"input_type": "np_bgr","output_type": "np_bgr","param": "angle_key,delta,limit,iterations,iter_save"},
    "calc_angle_set3": {"function": calc_angle_set3,"input_type": "np_bgr","output_type": "np_bgr","param": "angle_key,iterations,iter_save"},
    "calc_angle_set4": {"function": calc_angle_set4,"input_type": "np_bgr","output_type": "np_bgr","param": "angle_key,iterations,iter_save"},
    "text_orientation_set": {"function": text_orientation_set,"input_type": "np_bgr","output_type": "np_bgr","param": "angle_key,iterations,iter_save"},

    "before_angle1": {"function": before_angle1,"input_type": "np_bgr","output_type": "np_gray","param": ""},
    "calc_angle1": {"function": calc_angle1,"input_type": "np_gray","output_type": "np_gray","param": "angle_key"},
    "before_orientation": {"function": before_orientation,"input_type": "np_bgr","output_type": "np_gray","param": ""},
    "calc_orientation": {"function": calc_orientation,"input_type": "np_bgr","output_type": "np_bgr","param": "angle_key"},
    "rotate": {"function": rotate,"input_type": "np_bgr","output_type": "np_bgr","param": "angle_key,angle_keys"},

    # 표 구조 라인 추적
    "line_tracking": {"function": line_tracking,"input_type": "np_gray","output_type": "np_gray","param": "iter_save"
    }
}
