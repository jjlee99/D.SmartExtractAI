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
from utils.com import json_util
from utils.db import dococr_query_util
from utils.ocr import separate_area_util

def classify(ai_info: dict, file_info: dict, target_key: str, **context):
    ai_dir = ai_info["ai_dir"]
    save_input = ai_info["save_input"]
    class_key = ai_info["class_key"]
    processor_name = ai_info["processor_name"]
    
    device = torch.device("cpu")  # 또는 "cuda"
    torch.set_default_dtype(torch.float32)

    # 프로세서와 모델 로드
    processor = AutoProcessor.from_pretrained(processor_name, use_fast=True)
    model = AutoModelForSequenceClassification.from_pretrained(ai_dir)
    
    model.to(device)
    model.eval()
    print("1. 모델 및 프로세서 로드 완료")

    # 실행
    if target_key not in file_info["file_path"]:
        image_path = file_info["file_path"]["_origin"]
    image_path = file_info["file_path"][target_key]
    pred, confidence, input_kwargs = predict(image_path, model, processor, device)
    if save_input:
        classify_result = {"pred": pred, "confidence": confidence, "input_kwargs":input_kwargs}
    else :
        classify_result = {"pred": pred, "confidence": confidence}
    return classify_result
    
    
def predict(image_path, model, processor, device, class_key):
    """예측 수행"""
    image, words, boxes = preprocess_image(image_path, class_key)
    print("4-1. 모델 입력 생성 시작")
    input_kwargs = {
        "text": words,
        "boxes": boxes,
        "return_tensors": "pt",
        "truncation": True,
        "padding": "max_length",
        "max_length": 128
    }
    if "pixel_values" in processor.model_input_names:
        print("pixel_values is in model_input_names")
        input_kwargs["images"] = image  # 이미지가 필요하면 추가
    
    encoding = processor(**input_kwargs)
    
    print("4-2. 모델 입력 생성 완료")
    with torch.no_grad():
        print("4-3. 모델 예측 중...")
        inputs = {k: v.to(device) for k, v in encoding.items()}
        outputs = model(**inputs)
        logits = outputs.logits
        probs = F.softmax(logits, dim=1)
        pred = torch.argmax(probs, dim=1).item()
        confidence = probs[0, pred].item()
    return pred, confidence, input_kwargs

def preprocess_image(image_path, class_key):
    """이미지 로드, OCR, 바운딩 박스 정규화"""
    image = Image.open(image_path).convert("RGB")
    image_width, image_height = image.size

    process_id = f"_ic_{str(uuid.uuid4())}"
    # 1. 상단 헤더 영역만 분리하여 OCR 수행
    section_list = dococr_query_util.select_list_map("selectSectionList", params=(class_key,))
    step_info = json.loads(section_list[0].get("separate_area")) if section_list else None
    if step_info is None:
        print("no separate_area step_info")
        data = {"text": [], "left": [], "top": [], "width": [], "height": []}
    else:
        header_img, _ = separate_area_util.separate_area(
            image, data_type="pil", output_type="pil",
            step_info=step_info,
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
