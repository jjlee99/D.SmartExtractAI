from pathlib import Path
from PIL import Image
import uuid
import json
import os
import cv2
import numpy as np
import pytesseract
import torch
from transformers import AutoProcessor, AutoModelForSequenceClassification
import torch.nn.functional as F
from utils.ai.machine_learning_dataset import extract_feature_for_table_doc_util
from utils.com import json_util
from utils.db import dococr_query_util
from utils.ocr import separate_area_util
from collections import defaultdict
import csv
import pickle
import joblib
import sklearn
import pandas as pd
# from pycaret.classification import *
    

def classify(ai_info: dict, file_info: dict, target_key: str, **context):
    ai_dir = ai_info["ai_dir"]
    model_name= ai_info["model_name"]
    output_csv_path = str(Path(ai_dir).parent.parent / "csv")
    save_input = ai_info["save_input"]
    class_key = ai_info["class_key"]
    
    # 프로세서
    with open(f'{ai_dir}/sklearn_pipeline_{model_name}.pkl','rb') as file:
        model = pickle.load(file)
    
    print("1. 모델 로드 완료")

    # 실행
    if target_key not in file_info["file_path"]:
        image_path = file_info["file_path"]["_origin"]
    image_path = file_info["file_path"][target_key]
    pred, confidence, input_kwargs = predict(image_path, model, output_csv_path=output_csv_path,class_key=class_key)
    if save_input:
        classify_result = {"pred": pred, "confidence": confidence, "input_kwargs":input_kwargs}
    else :
        classify_result = {"pred": pred, "confidence": confidence}
    return classify_result

def predict(image_path, model, output_csv_path='/opt/airflow/data/class/noclass/classify/csv',class_key:str=""):
    """예측 수행 (머신러닝 모델용으로 수정)"""
    # 딥러닝 관련 변수 제거 (processor, device)
    
    print("3. 이미지 전처리 및 특성 추출 시작")
    # image, words, boxes = preprocess_image(image_path)
    
    # print("4-1. 모델 입력 데이터프레임 생성")
    # # 모델의 입력 형식에 맞게 데이터프레임 생성
    # # 이전 학습 코드에서 사용된 'text_span_x_min', 'text_span_y_min', 'horiz_span_x_min' 등
    # # 특성들을 추출하여 DataFrame으로 만듬.
    
    # # 예시: 추출된 words와 boxes를 사용하여 데이터프레임을 만드는 로직
    # # OCR 결과 및 레이아웃 정보로 특성 데이터프레임 구성
    # # 이 부분은 상단의 50번째 줄 기반으로 재구성.
    # # 그외 모든 통계적 특성(`text_span_x_min`, `horiz_span_x_min`, `cleaned_processed_text`, `return_tensors`, `padding` 등)을 새로운 컬럼으로 추가
    # # 기존 일부 ocr 결과, 레이아웃 정보 + 통계적 특성으로 데이터프레임으로 만듬.
    # # 데이터프레임 확인을 위해 data/class/a_class/classify/csv 경로에 csv 파일을 생성해둠.
    
    # # 1. 텍스트와 박스 분리 (일반 텍스트와 선 문자)
    # # 이전 코드의 'process_and_save_data_line_span_coords' 함수에서 사용된 로직 재사용
    # FILTER_CHARS_FROM_TEXT = "─━│┃┄┅┆┇┈┉┊┋"
    # COUNT_HORIZ_CHARS = "─━┄┅"
    # COUNT_VERT_CHARS = "│┃┆┇┊┋"

    # valid_char_boxes = []
    # cleaned_chars = []
    # potential_horiz_line_boxes = []
    # potential_vert_line_boxes = []
    # horiz_char_count = 0
    # vert_char_count = 0

    # for i in range(min(len(words), len(boxes))):
    #     char = words[i]
    #     box = boxes[i]

    #     if len(box) != 4:
    #         continue

    #     if char in COUNT_HORIZ_CHARS:
    #         horiz_char_count += 1
    #         potential_horiz_line_boxes.append(box)
    #     if char in COUNT_VERT_CHARS:
    #         vert_char_count += 1
    #         potential_vert_line_boxes.append(box)

    #     if char.strip() == "" or char in FILTER_CHARS_FROM_TEXT:
    #         continue

    #     valid_char_boxes.append(box)
    #     cleaned_chars.append(char)

    # cleaned_processed_text = "".join(cleaned_chars)
    
    # # 2. 통계 피처 계산 
    # stats_data = {}
    
    # if valid_char_boxes:
    #     stats_data['text_span_x_min'] = float(valid_char_boxes[0][0])
    #     stats_data['text_span_y_min'] = float(valid_char_boxes[0][1])
    #     stats_data['text_span_x_max'] = float(valid_char_boxes[-1][2])
    #     stats_data['text_span_y_max'] = float(valid_char_boxes[-1][3])
    # else:
    #     stats_data.update({f'text_span_{coord}': None for coord in ['x_min', 'y_min', 'x_max', 'y_max']})

    # if potential_horiz_line_boxes:
    #     all_horiz_x_mins = [b[0] for b in potential_horiz_line_boxes]
    #     all_horiz_y_mins = [b[1] for b in potential_horiz_line_boxes]
    #     all_horiz_x_maxs = [b[2] for b in potential_horiz_line_boxes]
    #     all_horiz_y_maxs = [b[3] for b in potential_horiz_line_boxes]
    #     stats_data['horiz_span_x_min'] = float(min(all_horiz_x_mins))
    #     stats_data['horiz_span_y_min'] = float(min(all_horiz_y_mins))
    #     stats_data['horiz_span_x_max'] = float(max(all_horiz_x_maxs))
    #     stats_data['horiz_span_y_max'] = float(max(all_horiz_y_maxs))
    #     horiz_line_boxes_np = np.array(potential_horiz_line_boxes)
    #     stats_data['horiz_line_x_min_mean'] = float(np.mean(horiz_line_boxes_np[:, 0]))
    #     stats_data['horiz_line_y_min_mean'] = float(np.mean(horiz_line_boxes_np[:, 1]))
    #     stats_data['horiz_line_x_max_mean'] = float(np.mean(horiz_line_boxes_np[:, 2]))
    #     stats_data['horiz_line_y_max_mean'] = float(np.mean(horiz_line_boxes_np[:, 3]))
    #     stats_data['horiz_line_x_min_std'] = float(np.std(horiz_line_boxes_np[:, 0]))
    #     stats_data['horiz_line_y_min_std'] = float(np.std(horiz_line_boxes_np[:, 1]))
    #     stats_data['horiz_line_x_max_std'] = float(np.std(horiz_line_boxes_np[:, 2]))
    #     stats_data['horiz_line_y_max_std'] = float(np.std(horiz_line_boxes_np[:, 3]))
    # else:
    #     stats_data.update({f'horiz_span_{coord}': None for coord in ['x_min', 'y_min', 'x_max', 'y_max']})
    #     stats_data.update({f'horiz_line_{coord}_{stat}': None for coord in ['x_min', 'y_min', 'x_max', 'y_max'] for stat in ['mean', 'std']})

    # if potential_vert_line_boxes:
    #     all_vert_x_mins = [b[0] for b in potential_vert_line_boxes]
    #     all_vert_y_mins = [b[1] for b in potential_vert_line_boxes]
    #     all_vert_x_maxs = [b[2] for b in potential_vert_line_boxes]
    #     all_vert_y_maxs = [b[3] for b in potential_vert_line_boxes]
    #     stats_data['vert_span_x_min'] = float(min(all_vert_x_mins))
    #     stats_data['vert_span_y_min'] = float(min(all_vert_y_mins))
    #     stats_data['vert_span_x_max'] = float(max(all_vert_x_maxs))
    #     stats_data['vert_span_y_max'] = float(max(all_vert_y_maxs))
    #     vert_line_boxes_np = np.array(potential_vert_line_boxes)
    #     stats_data['vert_line_x_min_mean'] = float(np.mean(vert_line_boxes_np[:, 0]))
    #     stats_data['vert_line_y_min_mean'] = float(np.mean(vert_line_boxes_np[:, 1]))
    #     stats_data['vert_line_x_max_mean'] = float(np.mean(vert_line_boxes_np[:, 2]))
    #     stats_data['vert_line_y_max_mean'] = float(np.mean(vert_line_boxes_np[:, 3]))
    #     stats_data['vert_line_x_min_std'] = float(np.std(vert_line_boxes_np[:, 0]))
    #     stats_data['vert_line_y_min_std'] = float(np.std(vert_line_boxes_np[:, 1]))
    #     stats_data['vert_line_x_max_std'] = float(np.std(vert_line_boxes_np[:, 2]))
    #     stats_data['vert_line_y_max_std'] = float(np.std(vert_line_boxes_np[:, 3]))
    # else:
    #     stats_data.update({f'vert_span_{coord}': None for coord in ['x_min', 'y_min', 'x_max', 'y_max']})
    #     stats_data.update({f'vert_line_{coord}_{stat}': None for coord in ['x_min', 'y_min', 'x_max', 'y_max'] for stat in ['mean', 'std']})
    
    # stats_data['horiz_char_count'] = horiz_char_count
    # stats_data['vert_char_count'] = vert_char_count 
    
    # # 선 길이 계산 
    # LINE_ALIGNMENT_TOLERANCE = 5 
    # BOX_GAP_TOLERANCE = 10
    # horizontal_lengths = []
    # vertical_lengths = []
    
    # # 수평선 길이 계산
    # if potential_horiz_line_boxes:
    #     potential_horiz_line_boxes.sort(key=lambda b: (b[1], b[0]))
    #     current_line_segment = []
    #     for box in potential_horiz_line_boxes:
    #         if not current_line_segment:
    #             current_line_segment.append(box)
    #         else:
    #             prev_box = current_line_segment[-1]
    #             if abs(box[1] - prev_box[1]) <= LINE_ALIGNMENT_TOLERANCE and \
    #                box[0] - prev_box[2] <= BOX_GAP_TOLERANCE and \
    #                box[0] >= prev_box[0]: 
    #                 current_line_segment.append(box)
    #             else:
    #                 min_x = min(b[0] for b in current_line_segment)
    #                 max_x = max(b[2] for b in current_line_segment)
    #                 horizontal_lengths.append(max_x - min_x)
    #                 current_line_segment = [box] 
    #     if current_line_segment:
    #         min_x = min(b[0] for b in current_line_segment)
    #         max_x = max(b[2] for b in current_line_segment)
    #         horizontal_lengths.append(max_x - min_x)

    # # 수직선 길이 계산
    # if potential_vert_line_boxes:
    #     potential_vert_line_boxes.sort(key=lambda b: (b[0], b[1])) 
    #     current_line_segment = []
    #     for box in potential_vert_line_boxes:
    #         if not current_line_segment:
    #             current_line_segment.append(box)
    #         else:
    #             prev_box = current_line_segment[-1]
    #             if abs(box[0] - prev_box[0]) <= LINE_ALIGNMENT_TOLERANCE and \
    #                box[1] - prev_box[3] <= BOX_GAP_TOLERANCE and \
    #                box[1] >= prev_box[1]: 
    #                 current_line_segment.append(box)
    #             else:
    #                 min_y = min(b[1] for b in current_line_segment)
    #                 max_y = max(b[3] for b in current_line_segment)
    #                 vertical_lengths.append(max_y - min_y)
    #                 current_line_segment = [box]
    #     if current_line_segment:
    #         min_y = min(b[1] for b in current_line_segment)
    #         max_y = max(b[3] for b in current_line_segment)
    #         vertical_lengths.append(max_y - min_y)

    # stats_data['avg_horiz_line_length'] = float(np.mean(horizontal_lengths)) if horizontal_lengths else None
    # stats_data['std_horiz_line_length'] = float(np.std(horizontal_lengths)) if horizontal_lengths else None
    # stats_data['avg_vert_line_length'] = float(np.mean(vertical_lengths)) if vertical_lengths else None
    # stats_data['std_vert_line_length'] = float(np.std(vertical_lengths)) if vertical_lengths else None

    # # 나머지 특성들을 딕셔너리에 추가
    # row_data = {
    #     'cleaned_processed_text': cleaned_processed_text,
    #     'return_tensors': 'pt',
    #     'padding': 'max_length',
    #     'truncation': True, 
    #     'max_length': 128
    # }
    
    # row_data.update(stats_data)

    row_data = extract_feature_for_table_doc_util.preprocess_image_and_extract_features(image_path, class_key)
    
    # 단일 행의 데이터프레임으로 변환
    input_df = pd.DataFrame([row_data])
    
    # --- 추가된 CSV 저장 부분 --- (더이상 확인 필요 없을 시 삭제.)
    if output_csv_path:
        # 파일이 이미 존재하면 헤더 없이 추가
        if os.path.exists(output_csv_path):
            input_df.to_csv(f'{output_csv_path}/doc_data.csv', mode='a', header=True, index=False, encoding='utf-8-sig')
        # 파일이 없으면 새로 생성하며 헤더 포함
        else:
            input_df.to_csv(output_csv_path, mode='w', header=True, index=False, encoding='utf-8-sig')
        print(f"모델 입력 데이터가 '{output_csv_path}/doc_data.csv'에 저장되었습니다.")
    # -----------------------------
    if input_df.empty:
        print("경고: 모델 입력 데이터가 비어있습니다. 기본값 반환")
        return -1, 0.0, {} 
        
    print("4-2. 모델 예측 중...")
    
    # 이 메서드는 각 클래스에 대한 확률을 반환
    probabilities = model.predict_proba(input_df)
    print(f'각 클래스에 대한 예측 확률{probabilities}')
    pred = model.predict(input_df)[0]
    pred = int(pred)
    print(f'분류 결과 : {pred}')
    confidence = float(probabilities[0, pred])
    print(f'신뢰도 : {confidence}')
    print("4-3. 모델 예측 완료")
    
    input_kwargs = input_df.to_dict('records')

    return pred, confidence, input_kwargs

def preprocess_image(image_path):
    """이미지 로드, OCR, 바운딩 박스 정규화"""
    image = Image.open(image_path).convert("RGB")
    image_width, image_height = image.size

    process_id = f"_ic_{str(uuid.uuid4())}"
    # 1. 상단 헤더 영역만 분리하여 OCR 수행
    header_img, _ = separate_area_util.separate_area_step_list(
        image, data_type="pil", output_type="pil",
        step_list=[
            {"name":"save","param":{"save_key":"_origin","tmp_save":True}},
            {"name" : "separate_areas_set1", "param": {"area_name":"doc_subject","area_type":"top_center","area_ratio":[-0.083,0.068,0.188,0.068],"iter_save":False}},
            {"name":"save","param":{"save_key":"_cutted","tmp_save":True}}
        ],
        result_map={"folder_path":process_id}
    )
    header_width, header_height = header_img.size
    
    try:
        print("3-2. OCR 수행 시작")
        data = pytesseract.image_to_data(
            header_img, output_type=pytesseract.Output.DICT, lang='kor+eng', config='--psm 6 --oem 3')
        print("3-3. OCR 수행 완료")
    except Exception as e:
        print(f"OCR error: {e}")
        data = {"text": [], "left": [], "top": [], "width": [], "height": []}

    words = []
    boxes = []
    for word, x, y, w, h in zip(data['text'], data['left'], data['top'], data['width'], data['height']):
        if word.strip():
            words.append(word.strip())
            bbox = (x, y, x + w, y + h)
            norm_bbox = normalize_bbox(bbox, header_width, header_height)
            boxes.append(norm_bbox)

    # 2. 표(원본) 영역에서 수평/수직선만 검출 (OCR X)
    cv_image = np.array(image)
    if len(cv_image.shape) == 3:
        cv_image = cv2.cvtColor(cv_image, cv2.COLOR_RGB2GRAY)
    _, binary = cv2.threshold(cv_image, 180, 255, cv2.THRESH_BINARY_INV)
    # binary = cv2.MORPH_DILATE 작성중.
    
    # 수평선 검출 (비율 기반 커널)
    dilate_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (3,3))
    detect_horizontal = cv2.morphologyEx(binary, cv2.MORPH_DILATE, dilate_kernel, iterations=1)
    horizontal_kernel_ratio = 0.8
    vertical_kernel_ratio = 0.038
    
    horizontal_kernel_size = max(1, int(image_width * horizontal_kernel_ratio))
    horizontal_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (horizontal_kernel_size, 1))
    detect_horizontal = cv2.morphologyEx(detect_horizontal, cv2.MORPH_OPEN, horizontal_kernel, iterations=1)
    detect_horizontal = cv2.morphologyEx(detect_horizontal, cv2.MORPH_CLOSE, horizontal_kernel, iterations=2)
    contours_h, _ = cv2.findContours(detect_horizontal, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    for cnt in contours_h:
        x, y, w, h = cv2.boundingRect(cnt)
        bbox = (x, y, x + w, y + h)
        norm_bbox = normalize_bbox(bbox, image_width, image_height)
        words.append('─')
        boxes.append(norm_bbox)
    separate_area_util.separate_area_step_list(detect_horizontal, data_type='np_bgr', output_type='np_bgr',
        step_list=[{"name":"save","param":{"save_key":"_h_contour","tmp_save":True}}], result_map={"folder_path":process_id})

    # 수직선 검출 (비율 기반 커널)
    vertical_kernel_size = max(1, int(image_height * vertical_kernel_ratio))
    vertical_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (1, vertical_kernel_size))
    detect_vertical = cv2.morphologyEx(binary, cv2.MORPH_OPEN, vertical_kernel, iterations=2)
    contours_v, _ = cv2.findContours(detect_vertical, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    for cnt in contours_v:
        x, y, w, h = cv2.boundingRect(cnt)
        bbox = (x, y, x + w, y + h)
        norm_bbox = normalize_bbox(bbox, image_width, image_height)
        words.append('│')
        boxes.append(norm_bbox)
    separate_area_util.separate_area_step_list(detect_vertical, data_type='np_bgr', output_type='np_bgr',
        step_list=[{"name":"save","param":{"save_key":"_v_contour","tmp_save":True}}], result_map={"folder_path":process_id})
    # data에 words, boxes만 저장
    data['words'] = words
    data['boxes'] = boxes
    # data = {'words': words, 'boxes': boxes}
    ocr_save_dir = "/opt/airflow/data/class/a_class/classify/ocr"
    os.makedirs(ocr_save_dir, exist_ok=True)
    base_name = os.path.splitext(os.path.basename(image_path))[0]
    ocr_save_path = os.path.join(ocr_save_dir, f"{base_name}_ocr.json")
    json_util.save(ocr_save_path, data)


    if len(words) != len(boxes):
        min_len = min(len(words), len(boxes))
        words = words[:min_len]
        boxes = boxes[:min_len]
    if not words:
        words = ["[UNK]"]
        boxes = [[0, 0, 100, 100]]
    print(f"3-4. 전처리 완료 (단어 수: {len(words)}, 박스 수: {len(boxes)})")
    return image, words, boxes

def normalize_bbox(bbox, image_width, image_height, image1000=True):
    """바운딩 박스 정규화 (이미지 크기 기준 → 1000 기준)"""
    x1, y1, x2, y2 = bbox
    if image1000:
        x1 = int(1000 * (x1 / image_width))
        y1 = int(1000 * (y1 / image_height))
        x2 = int(1000 * (x2 / image_width))
        y2 = int(1000 * (y2 / image_height))
    return [x1, y1, x2, y2]


