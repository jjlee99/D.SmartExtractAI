from airflow.decorators import task
import uuid
import torch
from torch.utils.data import Dataset, DataLoader, random_split
from transformers import AutoProcessor, AutoModelForSequenceClassification
from torch.optim import AdamW
from PIL import Image
import pytesseract
import os
import numpy as np
import cv2

from utils.com import json_util
from utils.ocr import separate_area_util

@task(pool='ocr_pool') 
def no_train_lilt(dataset: list, model_dir:str, horizontal_kernel_ratio: float = 0.8, vertical_kernel_ratio: float = 0.038):
    """LiLT 경량 모델 학습 및 검증 (메모리 최적화 버전)"""
    if not dataset:
        print("데이터셋이 비어있습니다.")
        return

    device = torch.device("cpu")  # CPU로 고정
    torch.set_default_dtype(torch.float32)

    # LiLT 모델 및 프로세서 로드
    processor = AutoProcessor.from_pretrained(
        "SCUT-DLVCLab/lilt-roberta-en-base",
        use_fast=True
    )
    # model = AutoModelForSequenceClassification.from_pretrained(
    #     "SCUT-DLVCLab/lilt-roberta-en-base",
    #     num_labels=2,
    #     torch_dtype=torch.float32
    # ).to(device)

    class DocDataset(Dataset):
        def __init__(self, data, processor):
            self.data = data
            self.processor = processor

        def __len__(self):
            return len(self.data)

        def __getitem__(self, idx):
            item = self.data[idx]
            image = Image.open(item["image_path"]).convert("RGB")
            image_width, image_height = image.size

            process_id = f"_cc_{str(uuid.uuid4())}"
            # 1. 상단 헤더 영역만 분리하여 OCR 수행
            header_img, _ = separate_area_util.separate_area_step_list(
                image, data_type="pil", output_type="pil",
                step_list=[{"name":"save","param":{"save_key":"_origin","tmp_save":True}},
                    {"name" : "separate_areas_set1", "param": {"area_name":"doc_subject","area_type":"top_center","area_ratio":[-0.083,0.068,0.188,0.068],"iter_save":False}},
                    {"name":"save","param":{"save_key":"_cutted","tmp_save":True}}
                ],
                result_map={"folder_path":process_id}
            )
            # header_img는 PIL.Image 객체
            header_width, header_height = header_img.size
            # 헤더 OCR
            try:
                data = pytesseract.image_to_data(header_img, output_type=pytesseract.Output.DICT, lang='kor+eng', config='--psm 6 --oem 3')
            except Exception as e:
                print(f"OCR error: {e}")
                data = {"text": [], "left": [], "top": [], "width": [], "height": []}
            
            # 1-1. OCR 결과를 lilt 학습을 위햐 변환
            def normalize_bbox(bbox, image_width, image_height):
                x1, y1, x2, y2 = bbox
                x1 = int(1000 * (x1 / image_width))
                y1 = int(1000 * (y1 / image_height))
                x2 = int(1000 * (x2 / image_width))
                y2 = int(1000 * (y2 / image_height))
                return [x1, y1, x2, y2]

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
            dilate_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (5,5))
            detect_horizontal = cv2.morphologyEx(binary, cv2.MORPH_DILATE, dilate_kernel, iterations=1)
            
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
            base_name = os.path.splitext(os.path.basename(item["image_path"]))[0]
            ocr_save_path = os.path.join(ocr_save_dir, f"{base_name}_ocr.json")
            json_util.save(ocr_save_path, data)

            # 워드와 박스 개수 검증 및 길이 맞추기
            if len(words) != len(boxes):
                print(f"Mismatch between words and boxes: words={len(words)}, boxes={len(boxes)}")
                min_len = min(len(words), len(boxes))
                words = words[:min_len]
                boxes = boxes[:min_len]
            if not words:
                words = ["[UNK]"]
                boxes = [[0, 0, 100, 100]]
                
            input_kwargs = {"text":words,
                "boxes":boxes,
                "return_tensors":"pt",
                "truncation":True,
                "padding":"max_length",
                "max_length":128}
            
            # JSON 파일을 저장할 기본 디렉토리 설정
            base_json_output_dir = '/opt/airflow/data/more'

            # 각 아이템별로 고유한 하위 폴더 생성 (process_id 사용)
            item_json_output_dir = os.path.join(base_json_output_dir, process_id)
            os.makedirs(item_json_output_dir, exist_ok=True)
            json_filename = f"{base_name}_input_data.json"
            item_json_save_path = os.path.join(item_json_output_dir, json_filename)
            json_util.save(item_json_save_path, input_kwargs)  # OCR 결과 저장
            
            return None

            encoding = self.processor(
                text=words,
                boxes=boxes,
                return_tensors="pt",
                truncation=True,
                padding="max_length",
                max_length=128
            )
            return {
                **{k: v.squeeze(0) for k, v in encoding.items()},
                "labels": torch.tensor(item["label"], dtype=torch.long)
            }
     
    #  
    DocDataset(dataset, processor)
    return None

    # 데이터셋 분할 (훈련 80%, 검증 20%)
    train_size = int(0.8 * len(dataset))
    val_size = len(dataset) - train_size
    train_dataset, val_dataset = random_split(dataset, [train_size, val_size])

    # 데이터로더 생성
    train_loader = DataLoader(
        DocDataset(train_dataset, processor),
        batch_size=2,
        shuffle=True
    )
    val_loader = DataLoader(
        DocDataset(val_dataset, processor),
        batch_size=2,
        shuffle=False
    )

    optimizer = AdamW(model.parameters(), lr=2e-5)

    for epoch in range(3):  # 3 에폭
        print(f"Epoch {epoch+1}")
        # 1. 학습
        model.train()
        train_loss = 0
        for batch in train_loader:
            try:
                inputs = {k: v.to(device) for k, v in batch.items() if k != "labels"}
                outputs = model(**inputs, labels=batch["labels"].to(device))
                loss = outputs.loss
                loss.backward()
                optimizer.step()
                optimizer.zero_grad()
                train_loss += loss.item()
            except Exception as e:
                print(f"Batch 실패: {str(e)}")
                continue
        print(f"Epoch {epoch+1} 훈련 손실: {train_loss/len(train_loader):.4f}")

        # 2. 검증
        model.eval()
        
        val_total = len(val_dataset)
        val_loss = 0
        correct = 0
        with torch.no_grad():
            for batch in val_loader:
                inputs = {k: v.to(device) for k, v in batch.items() if k != "labels"}
                outputs = model(**inputs, labels=batch["labels"].to(device))
                val_loss += outputs.loss.item()
                preds = torch.argmax(outputs.logits, dim=1)
                correct += (preds == batch["labels"].to(device)).sum().item()

        print(f"Epoch {epoch+1} 검증 손실: {val_loss/len(val_loader):.4f}, 정확도: {correct/val_total:.2%}")

    try:
        model.save_pretrained(model_dir)
    except (OSError, FileNotFoundError):  # 디렉토리가 없어서 난 오류는 폴더 생성 후 재실행
        os.makedirs(model_dir, exist_ok=True)
        model.save_pretrained(model_dir)

    print(f"모델 저장 완료: {model_dir}")

    return model_dir
