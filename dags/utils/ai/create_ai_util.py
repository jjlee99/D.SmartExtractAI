from collections import Counter
from datetime import datetime
from pathlib import Path
from airflow.models import Variable, XCom
from typing import Any, List, Dict
import uuid
import cv2
import numpy as np
from utils.ai.machine_learning_dataset import extract_feature_for_table_doc_util
import pytesseract
from scipy.ndimage import interpolation as inter
from utils.com import file_util
from utils.img import type_convert_util

RESULT_FOLDER = Variable.get("RESULT_FOLDER", default_var="/opt/airflow/data/result")
TEMP_FOLDER = Variable.get("TEMP_FOLDER", default_var="/opt/airflow/data/temp")
STEP_INFO_DEFAULT = {
    "name":"create_ai default",
    "type":"step_list",
    "step_list":[
        {"name":"save","param":{"save_key":"tmp_save"}}
    ]
}

def create_ai(dataset:list, step_info:Dict=None, result_map:dict=None) -> Any:
    """
    ai 생성 함수
    :param data: 이미지 파일 경로 또는 numpy 배열
    :param step_info: 생성 단계 정보 (기본값은 STEP_INFO_DEFAULT)
    :param result_map: 결과를 저장할 맵 (기본값은 빈 딕셔너리)
    :return: 결과
    """
    if step_info is None:
        step_info = STEP_INFO_DEFAULT
    if result_map is None:
        result_map = {}
    step_list = step_info.get("step_list", STEP_INFO_DEFAULT["step_list"])
    return create_ai_step_list(dataset=dataset, step_list=step_list, result_map=result_map)

def create_ai_step_list(dataset:list, step_list:List=None, result_map:dict=None) -> Any:
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
    
    output = dataset
    
    for stepinfo in step_list:
        print("step :",stepinfo["name"])
        if stepinfo["name"] not in function_map:
            print(f"경고: '{stepinfo['name']}' 함수가 정의되지 않아 다음 단계를 진행합니다.")
            continue  # 정의되지 않은 함수는 건너뜀
        function_info = function_map[stepinfo["name"]]
        input = output
        output = function_info["function"](input,**stepinfo["param"],result_map=result_map)
    return result_map

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

def train_lilt(dataset: list, model_dir:str, horizontal_kernel_ratio: float = 0.8, vertical_kernel_ratio: float = 0.038, class_key: str = "", result_map:dict=None):
    """LiLT 경량 모델 학습 및 검증 (메모리 최적화 버전)"""
    import torch
    from torch.utils.data import Dataset, DataLoader, random_split
    from transformers import AutoProcessor, AutoModelForSequenceClassification
    from torch.optim import AdamW
    from PIL import Image
    import pytesseract
    import os

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
    model = AutoModelForSequenceClassification.from_pretrained(
        "SCUT-DLVCLab/lilt-roberta-en-base",
        num_labels=2,
        torch_dtype=torch.float32
    ).to(device)

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
    result_map["_model_path"] = model_dir
    return dataset

def ml_line_classify(dataset: list, model_name:str=None, model_dir:str=None, result_map:dict=None,**context):
    """
    TargetEncoder를 포함한 sklearn 파이프라인을 사용하여 여러 모델을 학습하고 저장합니다.
    - dataset 리스트를 pandas DataFrame으로 변환
    - 전처리기(ColumnTransformer)와 다양한 모델을 포함하는 파이프라인 구축
    - 각 모델을 학습하고, pickle 파일로 저장
    """
    import os
    import pandas as pd
    import numpy as np
    import pickle
    from sklearn.model_selection import train_test_split
    from sklearn.compose import ColumnTransformer
    from sklearn.pipeline import Pipeline
    from sklearn.impute import SimpleImputer
    from sklearn.preprocessing import StandardScaler, OneHotEncoder
    from category_encoders import TargetEncoder
    
    # 여러 모델 import
    from sklearn.ensemble import ExtraTreesClassifier, RandomForestClassifier, GradientBoostingClassifier
    from sklearn.svm import SVC
    from sklearn.linear_model import LogisticRegression
    from xgboost import XGBClassifier
    from lightgbm import LGBMClassifier
    from sklearn.tree import DecisionTreeClassifier
    from sklearn.neighbors import KNeighborsClassifier
    from sklearn.naive_bayes import GaussianNB
    from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
    print("1. 데이터셋 전처리 및 특징 추출 시작...")
    processed_data = []
    for item in dataset:
        try:
            features = extract_feature_for_table_doc_util.preprocess_image_and_extract_features(item['image_path'])
            features['y'] = item['label']
            processed_data.append(features)
        except Exception as e:
            print(f"이미지 전처리 실패: {item['image_path']} - {e}")
            continue

    if not processed_data:
        print("전처리된 데이터가 없습니다. 학습을 중단합니다.")
        return
        
    df = pd.DataFrame(processed_data)
    print(df)
    # 결측치 처리
    df.fillna(value=np.nan, inplace=True) # TargetEncoder는 NaN 값을 처리할 수 있으므로, NaN으로 유지
    
    # 특성 그룹 정의(y를 뺀 컬럼)
    numerical_features = [col for col in features.keys() if col != 'y']
    onehot_encoding_features = []
    target_encoding_features = []
    
    # 전처리기 및 파이프라인 정의
    numerical_transformer = SimpleImputer(strategy='mean', add_indicator=True)
    onehot_transformer = Pipeline(steps=[
        ('imputer', SimpleImputer(strategy='most_frequent')),
        ('onehot', OneHotEncoder(handle_unknown='ignore'))
    ])
    target_transformer = TargetEncoder(
        cols=target_encoding_features,
        min_samples_leaf=20,
        smoothing=10
    )
    preprocessor = ColumnTransformer(
        transformers=[
            ('numerical', numerical_transformer, numerical_features),
            ('onehot', onehot_transformer, onehot_encoding_features),
            ('target', target_transformer, target_encoding_features)
        ],
        remainder='passthrough'
    )
    final_sklearn_pipeline = None

    
    X = df.drop('y', axis=1)
    y = df['y']

    # 데이터셋 분리
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=123)

    print("2. 여러 모델 학습 및 저장 시작...")
    if not model_dir:
        if result_map["model_dir"]:
            model_dir = result_map["model_dir"]
        else:
            CLASS_FOLDER = Variable.get("CLASS_FOLDER", default_var="/opt/airflow/data/class/")
            model_dir = f"{CLASS_FOLDER}/none/classify/model"
    os.makedirs(model_dir, exist_ok=True)
    
    # 학습할 여러 머신러닝 모델 정의
    models = {
        'ExtraTreesClassifier': ExtraTreesClassifier(n_jobs=-1, random_state=123),
        'RandomForestClassifier': RandomForestClassifier(n_jobs=-1, random_state=123),
        'GradientBoostingClassifier': GradientBoostingClassifier(random_state=123),
        'XGBClassifier': XGBClassifier(n_jobs=-1, random_state=123, use_label_encoder=False, eval_metric='logloss'),
        'LGBMClassifier': LGBMClassifier(n_jobs=-1, random_state=123),
        'SVC': SVC(random_state=123, probability=True),
        'LogisticRegression': LogisticRegression(n_jobs=-1, random_state=123),
        'DecisionTreeClassifier': DecisionTreeClassifier(random_state=123),
        'KNeighborsClassifier': KNeighborsClassifier(n_jobs=-1),
        'GaussianNB': GaussianNB(),
        'LinearDiscriminantAnalysis': LinearDiscriminantAnalysis()
    }
    model_path=""
    model = models[model_name]
    print(f"\n--- {model_name} 학습 시작 ---")
    try:
        # 새로운 파이프라인 생성 (매 반복마다 새로운 모델로 교체)
        final_sklearn_pipeline = Pipeline(steps=[
            ('preprocessor', preprocessor),
            ('final_imputer', SimpleImputer(strategy='mean')),
            ('classifier', model)
        ])
        
        # 파이프라인을 한 번에 학습 (y_train을 사용하여 전처리와 학습 동시 진행)
        final_sklearn_pipeline.fit(X_train, y_train)

        train_accuracy = final_sklearn_pipeline.score(X_train, y_train)
        test_accuracy = final_sklearn_pipeline.score(X_test, y_test)
        print(f"학습 데이터 정확도: {train_accuracy:.4f}")
        print(f"테스트 데이터 정확도: {test_accuracy:.4f}")

        now = datetime.now().strftime("%Y%m%d")
        gbn_dir = now
        file_name = f"{model_name}.pkl"
        model_path = os.path.join(model_dir, file_name)

        # 이미 존재할 경우 백업
        if os.path.exists(model_path):
            counter = 1
            bak_path = os.path.join(model_dir, gbn_dir, file_name)
            while os.path.exists(bak_path):
                gbn_dir = f"{now}_{counter}"
                bak_path = os.path.join(model_dir, gbn_dir, file_name)
                counter += 1
            os.makedirs(os.path.dirname(bak_path), exist_ok=True)
            file_util.file_move(model_path,bak_path)
            
        with open(model_path, 'wb') as f:
            pickle.dump(final_sklearn_pipeline, f)
        print(f"파이프라인이 '{model_path}' 파일로 저장되었습니다.")
    except Exception as e:
        print(f"모델 학습 실패: {model_name} - {str(e)}")
            
    print("\n모든 모델 학습 및 저장이 완료되었습니다.")
    result_map["_model_path"] = model_path
    return dataset

#이후
function_map = {
    # 공통
    "cache": {"function": cache, "param": "cache_key"},
    "load": {"function": load, "param": "cache_key"},
    "save": {"function": save, "param": "save_key,tmp_save"},
    "train_lilt": {"function": train_lilt, "param": "model_dir,horizontal_kernel_ratio,vertical_kernel_ratio,class_key,tmp_save"},
    "ml_line_classify": {"function": ml_line_classify, "param": "model_dir,model_name,tmp_save"},

}
