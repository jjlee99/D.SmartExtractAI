from airflow.decorators import task
import cv2
import numpy as np
from utils.com import file_util
import pytesseract
from typing import List, Dict, Any
from utils.img import type_convert_util
from pathlib import Path
from airflow.models import Variable
import json

@task(pool='ocr_pool') 
def ocr_dispatcher_task(file_info: Dict[str, Any], area_info: Dict[str, Any], **context) -> Dict[str, Any]:
    """
    OCR 타입에 따라 적절한 OCR 태스크를 선택하여 실행합니다.
    
    :param file_info: 처리할 파일 정보 딕셔너리
    :param area_info: OCR을 수행할 영역의 설정 정보 (ocr_type 포함)
    :param context: Airflow 컨텍스트 딕셔너리
    :return: OCR 결과가 추가된 file_info 딕셔너리
    """
    # 1. 원본 이미지를 받아 영역 별로 자른다.
    

    ocr_type = area_info.get("ocr_type", "text")  # 기본값은 "text"
    
    if ocr_type == "text":
        return _text_ocr_task(file_info, area_info, **context)
    elif ocr_type == "table":
        return _table_ocr_task(file_info, area_info, **context)
    else:
        raise ValueError(f"지원되지 않는 ocr_type입니다: {ocr_type}. 'text' 또는 'table'만 지원됩니다.")

def _text_ocr_task(file_info: Dict[str, Any], area_info: Dict[str, Any], **context) -> Dict[str, Any]:
    """
    이미지의 특정 영역에서 일반 텍스트를 OCR합니다.
    단어 단위로 위치 정보와 함께 텍스트를 추출합니다.

    :param file_info: 처리할 파일 정보 딕셔너리
    :param area_info: OCR을 수행할 영역의 설정 정보
    :param context: Airflow 컨텍스트 딕셔너리
    :return: OCR 결과가 추가된 file_info 딕셔너리
    """
    area_name = area_info.get("area_name", "unknown_area")

    # 처리할 이미지 경로를 찾습니다. (table_ocr_task와 동일한 로직)
    image_path = (
        file_info["file_path"].get(area_name)
        or file_info["file_path"].get("_result")
        or file_info["file_path"].get("_origin")
    )

    if not image_path:
        raise ValueError(
            f"'{area_name}' 영역의 이미지 경로를 찾을 수 없습니다. "
            f"'file_info[file_path]'에 '{area_name}', '_result' 또는 '_origin' 키 중 하나가 필요합니다. "
            f"현재 키: {list(file_info['file_path'].keys())}"
        )

    print(f"'{area_name}' 영역 Text OCR 시작. 사용할 이미지: {image_path}")
    
    ocr_results_with_pos: List[Dict[str, Any]] = []
    script_info = {} # 스크립트 정보 초기화
    try:
        img = cv2.imread(image_path)
        if img is None:
            raise FileNotFoundError(f"이미지 파일을 찾을 수 없거나 읽을 수 없습니다: {image_path}")

        # OCR 수행 전, 이미지 전체에 대해 스크립트 감지
        script_info = _detect_script(img)

        # Tesseract OCR을 'data' 형태로 수행하여 위치, 신뢰도 등 상세 정보 획득
        # psm 4: 텍스트의 단일 열로 가정. 일반적인 텍스트 영역에 더 적합합니다.
        # psm 6: 텍스트의 단일 블록으로 가정. 문장 단위 추출에 더 유리할 수 있습니다.
        data = pytesseract.image_to_data(
            img, lang='kor+eng', config='--psm 6', output_type=pytesseract.Output.DICT
        )

        # OCR 결과를 줄(line) 단위로 그룹화하기 위한 로직
        lines = {}
        for i in range(len(data['text'])):
            # 신뢰도가 0 이상이고 텍스트가 있는 경우만 처리
            if int(data['conf'][i]) > -1 and data['text'][i].strip():
                # (페이지, 블록, 문단, 줄) 번호를 키로 사용하여 단어들을 그룹화합니다.
                line_key = (data['page_num'][i], data['block_num'][i], data['par_num'][i], data['line_num'][i])
                
                if line_key not in lines:
                    lines[line_key] = []
                
                word_info = {
                    'text': data['text'][i],
                    'conf': float(data['conf'][i]),
                    'left': data['left'][i],
                    'top': data['top'][i],
                    'width': data['width'][i],
                    'height': data['height'][i]
                }
                lines[line_key].append(word_info)

        # 그룹화된 단어들을 문장으로 조합하고 전체 바운딩 박스 계산
        for line_key, words in sorted(lines.items()):
            if not words:
                continue

            # 줄의 텍스트 조합
            line_text = ' '.join(word['text'] for word in words)
            
            # 줄의 전체 바운딩 박스 계산 (모든 단어를 포함하는 최소 사각형)
            x_coords = [word['left'] for word in words]
            y_coords = [word['top'] for word in words]
            x_ends = [word['left'] + word['width'] for word in words]
            y_ends = [word['top'] + word['height'] for word in words]
            
            min_x, min_y = min(x_coords), min(y_coords)
            max_x, max_y = max(x_ends), max(y_ends)
            
            line_box = [min_x, min_y, max_x - min_x, max_y - min_y]

            # 줄의 평균 신뢰도 계산
            line_confidence = sum(word['conf'] for word in words) / len(words)

            ocr_results_with_pos.append({
                "text": line_text,
                "box": line_box,
                "confidence": round(line_confidence, 2)
            })
        
        print(f"'{area_name}' 영역 Text OCR 완료. 추출된 텍스트 라인 수: {len(ocr_results_with_pos)}")

    except Exception as e:
        print(f"'{area_name}' 영역 Text OCR 처리 중 오류 발생: {e}")
        # 오류 발생 시에도 후속 태스크가 비정상 종료되지 않도록 오류 정보를 결과에 포함
        ocr_results_with_pos.append({"error": f"ERROR: {str(e)}"})

    # 7. 결과를 file_info에 저장
    if "ocr_results" not in file_info:
        file_info["ocr_results"] = {}
    
    # 최종 결과 구조: 스크립트 정보와 라인 정보를 함께 저장
    final_result = {
        "script": script_info.get("script", "N/A"),
        "script_confidence": script_info.get("script_confidence", 0.0),
        "lines": ocr_results_with_pos
    }
    file_info["ocr_results"][area_name] = final_result

    # (선택) iter_save가 True일 경우, 디버깅용 이미지 저장
    if area_info.get("iter_save", False):
        debug_img = cv2.imread(image_path)
        for item in ocr_results_with_pos:
            if 'box' in item:
                x, y, w, h = item['box']
                cv2.rectangle(debug_img, (x, y), (x + w, y + h), (0, 255, 0), 2)
        
        FAILED_FOLDER = Variable.get("FAILED_FOLDER", default_var="/opt/airflow/data/failed")
        save_path = Path(FAILED_FOLDER) / context['dag_run'].run_id / f"{Path(file_info['file_path']['_origin']).stem}_{area_name}_text_detection.png"
        save_path.parent.mkdir(parents=True, exist_ok=True)
        cv2.imwrite(str(save_path), debug_img)
        print(f"디버그 이미지 저장: {save_path}")

    return file_info

def _table_ocr_task(file_info: Dict[str, Any], area_info: Dict[str, Any], **context) -> Dict[str, Any]:
    """
    이미지의 특정 영역 내 표 구조를 인식하고 OCR을 수행합니다.

    이 태스크는 다음 단계를 따릅니다:
    1. 이미지 영역에서 모든 수직, 수평선을 검출합니다.
    2. 선들의 교차로 만들어진 박스(셀)를 식별합니다.
    3. 각 박스(셀) 내부의 텍스트를 OCR로 추출합니다.
    4. 구조화된 결과(텍스트 및 좌표)를 file_info에 저장합니다.

    :param file_info: 처리된 이미지를 포함한 파일 정보 딕셔너리
    :param area_info: 특정 영역에 대한 설정 정보 (탐지 파라미터 포함)
    :param context: Airflow 컨텍스트 딕셔너리
    :return: OCR 결과가 추가된 file_info 딕셔너리
    """
    # 1. 이미지 및 파라미터 로드
    area_name = area_info.get("area_name", "unknown_area")

    # 처리할 이미지 경로를 찾습니다.
    # 우선순위:
    # 1. area_name 키: 특정 영역에 대해 분리된 이미지가 있는 경우 (예: file_info['file_path']['building_info'])
    # 2. '_result' 키: 이전 전처리 태스크의 결과물인 경우
    # 3. '_origin' 키: 원본 이미지 자체를 처리하는 경우 (현재 DAG와 같은 경우)
    image_path = (
        file_info["file_path"].get(area_name)
        or file_info["file_path"].get("_result")
        or file_info["file_path"].get("_origin")
    )

    if not image_path:
        raise ValueError(
            f"'{area_name}' 영역의 이미지 경로를 찾을 수 없습니다. "
            f"'file_info[file_path]'에 '{area_name}', '_result' 또는 '_origin' 키 중 하나가 필요합니다. "
            f"현재 키: {list(file_info['file_path'].keys())}"
        )
    print(f"'{area_name}' 영역 OCR 시작. 사용할 이미지: {image_path}")
    img = cv2.imread(image_path)
    if img is None:
        raise FileNotFoundError(f"이미지 파일을 찾을 수 없거나 읽을 수 없습니다: {image_path}")

    # 표 구조 분석 전, 이미지 전체에 대해 스크립트 감지
    script_info = _detect_script(img)

    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

    params = area_info.get("detection_params", {})
    
    # 파라미터가 없으면 기본값 사용
    h_kernel_divisor = params.get("h_kernel_divisor", 40)
    v_kernel_divisor = params.get("v_kernel_divisor", 40)
    min_cell_area = params.get("min_cell_area", 100)
    min_cell_width = params.get("min_cell_width", 10)    # 최소 셀 너비
    min_cell_height = params.get("min_cell_height", 10)   # 최소 셀 높이
    dilation_iterations = params.get("dilation_iterations", 2)
    adaptive_thresh_block_size = params.get("adaptive_thresh_block_size", 25)
    adaptive_thresh_c = params.get("adaptive_thresh_c", -3)

    print(f"사용 파라미터: {params}")

    # 2. 전처리
    # 적응형 스레시홀드로 깨끗한 이진 이미지 생성 (선을 흰색으로)
    binary_img = cv2.adaptiveThreshold(
        gray,
        255,
        cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
        cv2.THRESH_BINARY_INV,
        blockSize=adaptive_thresh_block_size,
        C=adaptive_thresh_c
    )

    # 3. 수평/수직선 검출
    height, width = binary_img.shape

    # 수평선 검출
    hor_kernel_size = width // h_kernel_divisor
    horizontal_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (hor_kernel_size, 1))
    detected_horizontal = cv2.morphologyEx(binary_img, cv2.MORPH_OPEN, horizontal_kernel, iterations=2)

    # 수직선 검출
    ver_kernel_size = height // v_kernel_divisor
    vertical_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (1, ver_kernel_size))
    detected_vertical = cv2.morphologyEx(binary_img, cv2.MORPH_OPEN, vertical_kernel, iterations=2)

    # 4. 선들을 합쳐 표 그리드 생성
    table_grid = cv2.add(detected_horizontal, detected_vertical)

    # 선들을 연결하여 셀 형태를 명확하게 만듦
    if dilation_iterations > 0:
        table_grid = cv2.dilate(table_grid, cv2.getStructuringElement(cv2.MORPH_RECT, (3, 3)), iterations=dilation_iterations)

    # 5. Contours를 이용해 셀(박스) 검출
    # cv2.RETR_EXTERNAL은 가장 바깥쪽 외곽선만 찾으므로, 모든 셀을 찾기 위해 cv2.RETR_LIST로 변경합니다.
    contours, _ = cv2.findContours(table_grid, cv2.RETR_LIST, cv2.CHAIN_APPROX_SIMPLE)

    # 면적으로 노이즈를 거르고 정렬
    cells = []
    # 디버깅을 위해 필터링 전 모든 contour를 저장
    all_detected_contours = []
    for c in contours:
        area = cv2.contourArea(c)
        x, y, w, h = cv2.boundingRect(c)
        # 필터링 전 contour 정보 저장
        all_detected_contours.append({'x': x, 'y': y, 'w': w, 'h': h, 'area': area})

        if area > min_cell_area and w > min_cell_width and h > min_cell_height:
            cells.append({'x': x, 'y': y, 'w': w, 'h': h, 'text': ''})

    if not cells:
        print(f"{area_name} 영역에서 셀을 찾지 못했습니다.")
        if "ocr_results" not in file_info:
            file_info["ocr_results"] = {}
        # 셀이 없더라도 스크립트 정보는 저장
        final_result = {
            "script": script_info.get("script", "N/A"),
            "script_confidence": script_info.get("script_confidence", 0.0),
            "cells": []
        }
        file_info["ocr_results"][area_name] = final_result
        return file_info

    # 셀을 위->아래, 왼쪽->오른쪽 순서로 정렬
    cells.sort(key=lambda c: (c['y'], c['x']))

    # 6. 각 셀에 대해 OCR 수행
    ocr_results = []
    for i, cell in enumerate(cells):
        x, y, w, h = cell['x'], cell['y'], cell['w'], cell['h']
        
        # 텍스트 잘림 방지를 위해 셀에 패딩 추가
        padding = 5
        cell_img_crop = gray[max(0, y - padding):min(height, y + h + padding), 
                             max(0, x - padding):min(width, x + w + padding)]

        if cell_img_crop.size == 0:
            continue

        # Tesseract로 텍스트 추출 (psm 6: 단일 텍스트 블록으로 가정)
        # (psm 7 : 줄단위)
        # (psm 8 : 단어 단위)
        ocr_config = r'--oem 3 --psm 6'
        data = pytesseract.image_to_data(
            cell_img_crop, lang='kor+eng', config=ocr_config, output_type=pytesseract.Output.DICT
        )

        # 추출된 단어와 신뢰도를 리스트에 저장
        words = []
        confidences = []
        for j in range(len(data['text'])):
            # 신뢰도가 0 이상이고 실제 텍스트가 있는 경우만 처리
            if int(data['conf'][j]) > -1 and data['text'][j].strip():
                words.append(data['text'][j].strip())
                confidences.append(float(data['conf'][j]))

        # 단어들을 합쳐 최종 텍스트를 만들고, 평균 신뢰도를 계산
        text = ' '.join(words)
        avg_confidence = sum(confidences) / len(confidences) if confidences else 0.0

        cell['text'] = text
        # cell['data'] = data
        cell['confidence'] = round(avg_confidence, 2)
        ocr_results.append(cell)
        print(f"셀 {i+1}/{len(cells)} OCR 완료: ({x},{y},{w},{h}) -> '{text[:30]}...' (신뢰도: {cell['confidence']:.2f}%)")

        
        # cell['text'] = text
        # ocr_results.append(cell)
        # print(f"셀 {i+1}/{len(cells)} OCR 완료: ({x},{y},{w},{h}) -> '{text[:30]}...'")

    # 7. 결과를 file_info에 저장
    if "ocr_results" not in file_info:
        file_info["ocr_results"] = {}
    
    # 최종 결과 구조: 스크립트 정보와 셀 정보를 함께 저장
    final_result = {
        "script": script_info.get("script", "N/A"),
        "script_confidence": script_info.get("script_confidence", 0.0),
        "cells": ocr_results
    }
    file_info["ocr_results"][area_name] = final_result

    # (선택) iter_save가 True일 경우, 디버깅용 이미지 저장
    if area_info.get("iter_save", False):
        debug_img = img.copy()

        # 디버깅 강화: 모든 검출된 contour와 그 면적을 표시 (필터링된 것은 빨간색)
        for contour_info in all_detected_contours:
            x, y, w, h, area = contour_info['x'], contour_info['y'], contour_info['w'], contour_info['h'], contour_info['area']

            # min_cell_area(영역), min_cell_width(너비),  min_cell_height(높이) 기준으로 필터링 통과 여부에 따라 색상 변경
            if area > min_cell_area and w > min_cell_width and h > min_cell_height:
                color = (0, 255, 0)  # 통과 (녹색)
            else:
                color = (0, 0, 255)  # 필터링됨 (빨간색)

            cv2.rectangle(debug_img, (x, y), (x + w, y + h), color, 2)
            # 면적(area)을 사각형 위에 텍스트로 표시
            cv2.putText(debug_img, f"A:{int(area)} W:{w} H:{h}", (x, y - 5), cv2.FONT_HERSHEY_SIMPLEX, 0.4, color, 1)
        
        # 디버그 파일을 임시 폴더가 아닌 전용 폴더에 저장하여 DAG 종료 후에도 확인할 수 있도록 합니다.
        # Airflow Variable에 DEBUG_FOLDER를 정의하거나, 없으면 기본 경로를 사용합니다.
        FAILED_FOLDER = Variable.get("FAILED_FOLDER", default_var="/opt/airflow/data/failed")
        save_path = Path(FAILED_FOLDER) / context['dag_run'].run_id / f"{Path(file_info['file_path']['_origin']).stem}_{area_name}_table_detection.png"
        save_path.parent.mkdir(parents=True, exist_ok=True)
        cv2.imwrite(str(save_path), debug_img)
        print(f"디버그 이미지 저장: {save_path}")
        print(f"필터링 임계값: min_area={min_cell_area}, min_width={min_cell_width}, min_height={min_cell_height}. 녹색: 통과, 빨간색: 필터링됨.")

    return file_info

def _detect_script(image_np: np.ndarray) -> Dict[str, Any]:
    """
    Tesseract OSD(Orientation and Script Detection)를 사용하여 이미지의 스크립트(언어)를 감지합니다.

    :param image_np: BGR 채널을 가진 numpy 배열(OpenCV 이미지)
    :return: 스크립트와 신뢰도 정보가 담긴 딕셔너리
    """
    try:
        # Tesseract OSD는 RGB 이미지를 기대하므로 변환합니다.
        rgb_img = cv2.cvtColor(image_np, cv2.COLOR_BGR2RGB)
        
        # image_to_osd는 감지된 정보를 딕셔너리 형태로 반환합니다.
        osd_data = pytesseract.image_to_osd(rgb_img, output_type=pytesseract.Output.DICT)
        
        script = osd_data.get('script', 'Unknown')
        confidence = osd_data.get('sconf', 0.0)
        
        print(f"Script detection: {script} (Confidence: {confidence:.2f})")
        return {"script": script, "script_confidence": confidence}

    except pytesseract.TesseractError as e:
        print(f"Script detection failed: {e}")
        return {"script": "Error", "script_confidence": 0.0}
