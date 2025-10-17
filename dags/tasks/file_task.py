from airflow.decorators import task
from pathlib import Path
import shutil, os
from airflow.models import Variable
import uuid
import json

from utils.com import json_util
from utils.db import dococr_query_util

TEMP_FOLDER = Variable.get("TEMP_FOLDER", default_var="/opt/airflow/data/temp")
RESULT_FOLDER = Variable.get("RESULT_FOLDER", default_var="/opt/airflow/data/result")
    
@task(pool='ocr_pool') 
def get_file_list_task(folder_path):
    files = [str(f) for f in Path(folder_path).iterdir() if f.is_file()]
    if not files:
        raise ValueError(f"No files found in: {folder_path}")
    return files

@task(pool='ocr_pool') 
def get_file_info_list_task(folder_path,**context):
    p = Path(folder_path)
    files = [str(f) for f in p.rglob("*") if f.is_file()]
    file_info_list = []
    for path in files:
        id = str(uuid.uuid4())
        content = {"file_id":id, "file_path":{"_origin":path}}
        file_info_list.append(content)
    return file_info_list

@task(pool='ocr_pool') 
def copy_results_folder_task(file_infoes: list, target_key:str=None, dest_folder:str=None, last_folder:str=None, **context):
    """
    file_infoes["file_path"]에 저장된 경로의 파일들을
    지정된 폴더로 복사. 파일명 앞에 result_map["process_id"]를 붙임.
    """
    if not dest_folder:
        dest_folder = str(Path(TEMP_FOLDER) / context['run_id'])
    if last_folder:
        dest_folder = f"{dest_folder}/{last_folder}"
    # 목표 폴더 생성
    dest_path_obj = Path(dest_folder)
    dest_path_obj.mkdir(parents=True, exist_ok=True)
    # 목표 폴더 내 파일 삭제
    for file_path in dest_path_obj.glob('*'):
        if file_path.is_file():
            file_path.unlink()
    
    total_copied = 0
    print(len(file_infoes))
    for file_info in file_infoes:
        file_path_map={}
        if target_key:
            if isinstance(target_key, list):
                for key in target_key:
                    file_path_map[key] = file_info["file_path"][key]
            else:
                file_path_map[target_key] = file_info["file_path"][target_key]
        else:
            file_path_map = file_info["file_path"]
        file_id = file_info["file_id"]
        
        for key, file_path in file_path_map.items():
            if isinstance(file_path,list):
                for i,path in enumerate(file_path):
                    src_filename = Path(path).name
                    dest_filename = f"{file_id}_{i}_{src_filename}"
                    dest_path = os.path.join(dest_folder, dest_filename)
                    try:
                        shutil.copy2(file_path, dest_path)
                        total_copied += 1
                        print(f"복사 완료: {file_path} → {dest_path}")
                    except Exception as e:
                        print(f"복사 실패: {file_path} → {dest_path}, 오류: {str(e)}")
            else:
                # 원본 파일명 추출
                src_filename = Path(file_path).name
                # 새 파일명 생성 (process_id + 원본 파일명)
                dest_filename = f"{file_id}_{src_filename}"
                dest_path = os.path.join(dest_folder, dest_filename)
                try:
                    shutil.copy2(file_path, dest_path)
                    total_copied += 1
                    print(f"복사 완료: {file_path} → {dest_path}")
                except Exception as e:
                    print(f"복사 실패: {file_path} → {dest_path}, 오류: {str(e)}")

    return dest_folder

@task(pool='ocr_pool') 
def clear_temp_folder_task(**context):
    run_id = context['run_id']
    folder = Path(TEMP_FOLDER) / run_id
    if folder.exists() and folder.is_dir():
        for item in folder.iterdir():
            if item.is_file():
                os.remove(item)
            elif item.is_dir():
                shutil.rmtree(item)
    return f"Cleared folder: {folder}"

@task(pool='ocr_pool') 
def save_file_info_task(file_info: dict, save_type: str="result", **context):
    if save_type == "result":
        run_id = context['run_id']
        original_filename_stem = Path(file_info["file_path"]["_origin"]).stem
        dest_folder = Path(RESULT_FOLDER) / run_id
        dest_folder.mkdir(parents=True, exist_ok=True)
        output_file = dest_folder / f"{original_filename_stem}.json"
        json_util.save(str(output_file),file_info)
    elif save_type == "temp":
        run_id = context['run_id']
        original_filename_stem = Path(file_info["file_path"]["_origin"]).stem
        dest_folder = Path(TEMP_FOLDER) / run_id
        dest_folder.mkdir(parents=True, exist_ok=True)
        output_file = dest_folder / f"{original_filename_stem}.json"
        json_util.save(str(output_file),file_info)
    else:
        raise ValueError(f"Invalid save_type: {save_type}")
    
    
# 새로히 추가
@task(pool='ocr_pool') 
def save_ocr_json_task(file_info: dict, dest_folder: str, area_name: str):
    """
    file_info에 저장된 구조화된 OCR 결과(텍스트 및 위치 정보)를
    지정된 폴더에 .json 파일로 저장합니다.

    :param file_info: 처리할 파일의 정보. ocr_results에 결과가 포함되어야 합니다.
    :param dest_folder: 결과 JSON 파일을 저장할 폴더 경로.
    :param area_name: 결과를 가져올 ocr_results 내의 영역 키.
    :return: 처리 결과가 반영된 file_info 딕셔너리.
    """
    # file_info에서 구조화된 OCR 데이터와 원본 파일명을 추출합니다.
    ocr_data = file_info.get("ocr_results", {}).get(area_name)
    # ocr_data가 비어있지 않은지(None이 아닌지)만 확인합니다. (리스트와 딕셔너리 형태 모두 허용)
    if not ocr_data:
        print(f"No OCR data found for area '{area_name}' in file {file_info['file_id']}. Skipping.")
        return file_info

    original_filename_stem = Path(file_info["file_path"]["_origin"]).stem

    # 결과 저장 폴더가 없으면 생성합니다.
    dest_path_obj = Path(dest_folder)
    dest_path_obj.mkdir(parents=True, exist_ok=True)

    # 저장할 파일 경로를 정의합니다. (예: 원본파일명.json)
    output_file = dest_path_obj / f"{original_filename_stem}.json"

    # OCR 결과를 JSON 파일에 씁니다.
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(ocr_data, f, ensure_ascii=False, indent=4)
    
    print(f"Saved OCR result to {output_file}")
    return file_info