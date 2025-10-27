import json
from PIL import Image
from pathlib import Path
from airflow.decorators import task
import os
import cv2
import numpy as np  # np import 추가
from pandas.core.base import np
from torchvision.transforms import InterpolationMode
from utils.com import create_step_info_util
from utils.db import dococr_query_util
from utils.ai import create_ai_util
from utils.ai.machine_learning_dataset import extract_feature_for_table_doc_util
from utils.ocr import separate_area_util  # import 경로 수정
from utils.img import img_preprocess_util
from utils.com import json_util
from utils.com import file_util
from airflow.models import Variable
import torch.nn.functional as F  # 상단에 추가
import torch
import random
import shutil
import uuid

DATA_FOLDER = Variable.get("DATA_FOLDER", default_var="/opt/airflow/data")
RESULT_FOLDER = Variable.get("RESULT_FOLDER", default_var="/opt/airflow/data/result")
TEMP_FOLDER = Variable.get("TEMP_FOLDER", default_var="/opt/airflow/data/temp")
CLASS_FOLDER = Variable.get("CLASS_FOLDER", default_var="/opt/airflow/data/class/")
NONE_CLASS_FOLDER = Variable.get("NONE_CLASS_FOLDER", default_var="/opt/airflow/data/common/none_class") # 비서식 일반 문서 이미지
CREATE_DATASET_SIZE = Variable.get("DATASET_SIZE", default_var="200") # DATASET사이즈
CREATE_AUGMENT_POLICY = Variable.get("AUGMENT_POLICY", default_var="default") # default,always,skip


#데이터가 충분하지 않을 때 증강을 통해 데이터셋을 확장합니다.
@task(pool='ocr_pool') 
def image_data_augment(layout_info):
    doc_class_id = layout_info.get("doc_class_id")
    layout_class_id = layout_info.get("layout_class_id")
    layout_dir = f"{DATA_FOLDER}/class/{doc_class_id}/{layout_class_id}"

    origin_true_imgae_dir = f"{layout_dir}/classify/origin/true"   # 원본 문서 이미지
    origin_false_imgae_dir = f"{layout_dir}/classify/origin/false"   # 일반 문서 이미지
    ready_true_image_dir = f"{layout_dir}/classify/ready/true"   # 특정 서식 증강된 문서 이미지
    ready_false_image_dir = f"{layout_dir}/classify/ready/false" # 일반 증강된 문서 이미지
    threshold = int(CREATE_DATASET_SIZE)
    if CREATE_AUGMENT_POLICY == "default": # 목표치보다 데이터가 부족할 때만 증강
        ready_true_image_paths = file_util.get_image_paths(ready_true_image_dir)
        if len(ready_true_image_paths) < threshold:
            _iamge_data_augment(origin_true_imgae_dir,ready_true_image_dir,threshold,threshold) # 특정 서식 이미지 제한없이 증강
        ready_false_image_paths = file_util.get_image_paths(ready_false_image_dir)
        if len(ready_false_image_paths) < threshold:
            _iamge_data_augment(origin_false_imgae_dir,ready_false_image_dir,threshold,aug_limit=3) # 일반 문서 이미지가 있으므로 장당 증강 제한
            _balance_false_images(ready_true_image_dir,ready_false_image_dir)
    elif CREATE_AUGMENT_POLICY == "always":
        _iamge_data_augment(origin_true_imgae_dir,ready_true_image_dir,threshold,threshold) # 특정 서식 이미지 제한없이 증강
        _iamge_data_augment(origin_false_imgae_dir,ready_false_image_dir,threshold,aug_limit=3) # 일반 문서 이미지가 있으므로 장당 증강 제한
        _balance_false_images(ready_true_image_dir,ready_false_image_dir)
    elif CREATE_AUGMENT_POLICY == "skip":
        pass
    else:
        print(f"AUGMENT_POLICY는 'default', 'always', 'skip' 중 하나만 가능합니다. 현재 작업을 패스합니다.: {CREATE_AUGMENT_POLICY}")        
    return layout_info

def _iamge_data_augment(origin_dir:str, ready_dir:str, threshold:int=200, aug_limit:int=3):
    from torchvision import transforms
    from PIL import Image, ImageFilter
    from torchvision.transforms import functional as F
    
    image_paths = file_util.get_image_paths(origin_dir)
    os.makedirs(ready_dir, exist_ok=True)
    
    if len(image_paths) == 0:
        # 파일이 하나도 없는 경우 데이터 증강 불가
        print(f"증강을 위한 최소 데이터가 없습니다. 증강이 필요하다면 {origin_dir} 폴더에 파일을 넣어주세요.")
        return
    elif len(image_paths) < threshold:
        num_aug = min(int(threshold / len(image_paths)), aug_limit)
    else:
        num_aug = 1 # 원본만
    class GaussianBlur(object):
        def __init__(self, radius_min=0.05, radius_max=1.0):
            self.radius_min = radius_min
            self.radius_max = radius_max
        def __call__(self, img):
            radius = random.uniform(self.radius_min, self.radius_max)
            return img.filter(ImageFilter.GaussianBlur(radius))

    class Sharpen(object):
        def __init__(self, radius_range=(1, 3), percent_range=(100, 200), threshold_range=(0, 5)):
            self.radius_range = radius_range
            self.percent_range = percent_range
            self.threshold_range = threshold_range

        def __call__(self, img):
            radius = int(random.uniform(*self.radius_range))
            percent = int(random.uniform(*self.percent_range))
            threshold = int(random.uniform(*self.threshold_range))
            return img.filter(ImageFilter.UnsharpMask(radius=radius, percent=percent, threshold=threshold))
    class AddGaussianNoise(object):
        def __init__(self, mean=0.0, std=0.05):
            self.mean = mean
            self.std = std
            
        def __call__(self, img):
            tensor = transforms.ToTensor()(img)
            noise = torch.randn(tensor.size()) * self.std + self.mean
            noisy_tensor = tensor + noise
            noisy_tensor = torch.clamp(noisy_tensor, 0., 1.)
            return transforms.ToPILImage()(noisy_tensor)

    def get_augmentation(width, height):
        return transforms.Compose([
            transforms.Resize((height*2, width*2), interpolation=InterpolationMode.BICUBIC),
            transforms.RandomAffine(degrees=5, translate=(0.02, 0.02), fill=255, interpolation=InterpolationMode.BICUBIC),
            GaussianBlur(0.1, 1.0),                                # 가우시안 블러
            Sharpen(radius_range=(1, 3), percent_range=(120, 180), threshold_range=(1, 3)),
            AddGaussianNoise(0.0, 0.05),
            transforms.ColorJitter(brightness=0.2, contrast=0.2, saturation=0.2, hue=0.1),  # 채도·색조 변경 추가
            transforms.Resize((height, width), interpolation=InterpolationMode.BICUBIC),
        ])
    num = 0
    for img_path in image_paths:
        img = Image.open(img_path).convert('RGB')
        width, height = img.size
        augmentation = get_augmentation(width, height) if num_aug > 1 else None

        base_name = f"img{num}"
        orig_save_path = os.path.join(ready_dir, f"{base_name}_aug0.png") # 원본이미지
        img.save(orig_save_path)
        if augmentation:
            for i in range(1,num_aug):
                aug_img = augmentation(img) # 증강 함수
                save_path = os.path.join(ready_dir, f"{base_name}_aug{i}.png") # 증강이미지
                aug_img.save(save_path)
                print(f"증강 이미지 저장: {save_path}")

def _balance_false_images(true_path:str,false_path:str):
    def get_image_count(true_path,false_path):
        """각 디렉토리의 이미지 개수를 계산"""
        true_count = len(file_util.get_image_paths(true_path))
        false_count = len(file_util.get_image_paths(false_path))

        none_doc_count = len(file_util.get_image_paths_recursive(NONE_CLASS_FOLDER))
        
        print(f"{true_path} 파일 개수: {true_count}")
        print(f"{false_path} 파일 개수: {false_count}")
        print(f"NONE_DOC_IMAGE_DIR 파일 개수: {none_doc_count}")
        
        return {
            "true_count": true_count,
            "false_count": false_count,
            "none_doc_count": none_doc_count
        }
    counts_info = get_image_count(true_path,false_path)

    true_count = counts_info["true_count"]
    false_count = counts_info["false_count"]
    
    # false 이미지가 부족한 경우, NONE_DOC_IMAGE_DIR에서 파일을 복사하여 균형 맞추기
    needed_files = true_count - false_count
    if needed_files <= 0:
        print(f"FALSE_IMAGE_DIR에 충분한 파일이 있습니다. (필요: {needed_files})")
        return {"copied_count": 0, "total_false_count": false_count}

    none_doc_paths = file_util.get_image_paths_recursive(NONE_CLASS_FOLDER)
    none_doc_count = len(none_doc_paths)

    if needed_files > none_doc_count:
        print(f"경고: 사용 가능 파일 부족 (요청: {needed_files}, 실제: {none_doc_count})")
        needed_files = none_doc_count
        if false_count == 0 and none_doc_count == 0:
            raise ValueError("FALSE 학습을 위한 최소 데이터가 없습니다") # 필요 시 증강으로 수정

    # 수정 사항 3: needed_files가 0인 경우 처리
    selected_files = random.sample(none_doc_paths, needed_files) if needed_files > 0 else []
    
    os.makedirs(false_path, exist_ok=True)
    copied_count = 0
    for src_path in selected_files:
        try:
            new_filename = f"img{copied_count}_aug0.png"
            dst_path = os.path.join(false_path, new_filename)
            shutil.copy2(src_path, dst_path)
            copied_count += 1
            print(f"복사 완료: {src_path} -> {dst_path}")
        except Exception as e:
            print(f"복사 실패: {src_path} - {str(e)}")
    
    return {"copied_count": copied_count, "total_false_count": false_count + copied_count}

@task(pool='ocr_pool') 
def build_balanced_dataset(layout_info:dict):
    """균형이 맞춰진 데이터셋 구성"""
    layout_dir = f"{DATA_FOLDER}/class/{layout_info["doc_class_id"]}/{layout_info["layout_class_id"]}"
    true_folder = f"{layout_dir}/classify/preprc/true"   # 표준화된 특정 서식 문서 이미지
    false_folder = f"{layout_dir}/classify/preprc/false"   # 표준화된 일반 문서 이미지
    true_image_paths = file_util.get_image_paths(true_folder)
    false_image_paths = file_util.get_image_paths(false_folder)
    
    dataset = []
    
    # True 라벨 데이터 추가
    for path in true_image_paths:
        dataset.append({"image_path": path, "label": 1})
    
    # False 라벨 데이터 추가
    for path in false_image_paths:
        dataset.append({"image_path": path, "label": 0})
    
    print(f"데이터셋 구성 완료:")
    print(f"- True 라벨: {len(true_image_paths)}개")
    print(f"- False 라벨: {len(false_image_paths)}개")
    print(f"- 총 데이터셋 크기: {len(dataset)}개")
    
    return dataset

@task
def generating_masking_image(layout_info:dict):
    """
    이미지에서 수평선과 수직선을 감지하고 해당 바운딩 박스를 반환합니다.
    Args:
        true_folder (str): Airflow XCom에서 전달받은 파일 정보 사전.
    Returns:
        tuple: (수평선 바운딩 박스 리스트, 수직선 바운딩 박스 리스트)
    """
    # 파일 경로 딕셔너리에서 실제 경로(문자열)를 추출. 이중 딕셔너리 형태.
    true_folder = Path(CLASS_FOLDER)/str(layout_info.get('doc_class_id'))/str(layout_info.get('layout_class_id'))/"classify"/"preprc"/"true"
    true_image_paths = file_util.get_image_paths(true_folder)
    dataset = []
    iter_save = False
    all_combined_lines = None
    # True 라벨 데이터 추가
    save_folder = Path(CLASS_FOLDER)/str(layout_info.get('doc_class_id'))/str(layout_info.get('layout_class_id'))/"classify"/"asset"
    save_folder.mkdir(parents=True, exist_ok=True) # 폴더가 없으면 생성
            
    for image_path in true_image_paths:
        print(image_path)
        image = Image.open(image_path).convert("RGB")
        image_width, image_height = image.size

        cv_image = np.array(image)
        if len(cv_image.shape) == 3:
            cv_image = cv2.cvtColor(cv_image, cv2.COLOR_RGB2GRAY)
        _, binary = cv2.threshold(cv_image, 180, 255, cv2.THRESH_BINARY_INV)
            
        horizontal_kernel_ratio = 0.07
        vertical_kernel_ratio = 0.03
        horizontal_kernel_size = max(1, int(image_width * horizontal_kernel_ratio))
        horizontal_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (horizontal_kernel_size, 1))
        detect_horizontal = cv2.morphologyEx(binary, cv2.MORPH_OPEN, horizontal_kernel, iterations=1)
        
        vertical_kernel_size = max(1, int(image_height * vertical_kernel_ratio))
        vertical_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (1, vertical_kernel_size))
        detect_vertical = cv2.morphologyEx(binary, cv2.MORPH_OPEN, vertical_kernel, iterations=1)

        combined_lines = cv2.bitwise_or(detect_horizontal, detect_vertical)
        
        # 가운데를 기준으로 겹쳐서 마스크 생성
        if all_combined_lines is None:
            all_combined_lines = combined_lines
        else:
            # img1과 img2 크기
            h1, w1 = combined_lines.shape[:2]
            h2, w2 = all_combined_lines.shape[:2]
            
            # 결과 이미지 크기 (큰 쪽 사이즈)
            out_h = max(h1, h2)
            out_w = max(w1, w2)

            # 빈 배경 생성 (dtype, 채널은 img1으로 맞춘다고 가정)
            out1 = np.zeros((out_h, out_w), dtype=combined_lines.dtype)
            out2 = np.zeros((out_h, out_w), dtype=all_combined_lines.dtype)

            # img1 붙일 좌표
            y1 = (out_h - h1) // 2
            x1 = (out_w - w1) // 2
            out1[y1:y1+h1, x1:x1+w1] = combined_lines

            # img2 붙일 좌표
            y2 = (out_h - h2) // 2
            x2 = (out_w - w2) // 2
            out2[y2:y2+h2, x2:x2+w2] = all_combined_lines
            all_combined_lines = cv2.bitwise_or(out1, out2)
            
        
        if iter_save:
            inverted_combined = cv2.bitwise_not(combined_lines)
            save_path = save_folder / Path(image_path).name
            cv2.imwrite(str(save_path), inverted_combined)
    all_inverted_combined = cv2.bitwise_not(all_combined_lines)
    save_path = save_folder / "mask_image.png"
    cv2.imwrite(str(save_path), all_inverted_combined)

@task(pool='ocr_pool')
def train(dataset: dict, layout_info: dict):
    classify_ai_step_info = json.loads(layout_info.get("classify_ai_info"))
    result_map = {"model_dir": f"{CLASS_FOLDER}/{layout_info.get('doc_class_id')}/{layout_info.get('layout_class_id')}/classify/model"}
    result_map = create_ai_util.create_ai(dataset, classify_ai_step_info, result_map)
    return result_map

@task(pool='ocr_pool')
def complete_runtime(result_map:dict, layout_info:dict ,**context):
    dococr_query_util.update_map("updateCreateEnd",params=('C',str(layout_info.get("create_id")),))
    if layout_info.get("first_status") == 'W':
        # 정기 실행대상은 완료 후 다음 작업을 미준비로 자동 등록(루프 방지)
        dococr_query_util.insert_map("insertCreateUnready",params=(str(layout_info.get("doc_class_id")), str(layout_info.get("layout_class_id")),))
    
    run_id = context['dag_run'].run_id
    if run_id.startswith("manual__"):
        # 수동으로 실행 시 미준비 작업을 대기로 변경하고 종료
        dococr_query_util.update_map("updateCreateReady")
    elif run_id.startswith("scheduled__") or run_id.startswith("manual--") :
        # 스케줄로 실행 시 다음 작업 계속 진행
        next_layout = dococr_query_util.select_row_map("selectNextLayoutInfo")
        if next_layout is None:
            # 미준비 작업을 대기로 변경하고 종료
            dococr_query_util.update_map("updateCreateReady")
        else:
            # 미준비 작업을 대기로 변경하고 종료
            from airflow.models import DagRun, DagBag
            from airflow.utils.state import State
            from airflow.utils.session import provide_session
            from airflow import settings
            # 다음 학습 작업이 있으면 동일 DAG을 수동 트리거
            dag_id = context['dag'].dag_id
            session = settings.Session()
            import datetime
            try:
                new_run_id = f"manual--{datetime.datetime.now().isoformat()}"
                dr = DagRun(
                    dag_id=dag_id,
                    run_id=new_run_id,
                    conf=next_layout,
                    execution_date=context['ts'],  # 권장: context timestamp 활용
                    start_date=datetime.datetime.now(),
                    external_trigger=True,
                    state=State.RUNNING,
                )
                session.add(dr)
                session.commit()
            except Exception as e:
                session.rollback()
                raise e
            finally:
                session.close()