from airflow.decorators import task
from pathlib import Path
import shutil, os
from airflow.models import Variable
import uuid, json
from utils.com import json_util
from pdf2image import convert_from_path
from datetime import datetime
from utils.db import dococr_query_util
from utils.com import file_util

TEMP_FOLDER = Variable.get("TEMP_FOLDER", default_var="/opt/airflow/data/temp")
RESULT_FOLDER = Variable.get("RESULT_FOLDER", default_var="/opt/airflow/data/result")
FAILED_FOLDER = Variable.get("FAILED_FOLDER", default_var="/opt/airflow/data/failed")
COMPLETE_FOLDER = Variable.get("COMPLETE_FOLDER", default_var="/opt/airflow/data/complete")
OCR_RESULT_FOLDER = Variable.get("OCR_RESULT_FOLDER", default_var="/opt/airflow/data/class/a_class/ocr")
UPLOAD_FOLDER = Variable.get("UPLOAD_FOLDER", default_var="/opt/airflow/data/upload")

#dag_id = 순환하지 않는 방향성있는 그래프로 표현한 워크플로우의 관리번호(속성: dag소스, 스케쥴 등)
#run_id = dag의 워크플로우에 따른 실행 인스턴스 관리번호(속성: 시작일시, 종료일시, 실행상태 등)
#task_id = 워크플로우의 개별 작업 단위(속성: task소스, 파라미터, 로그 등)
#target_id = 실행 인스턴스에서 동적으로 생성하여 작업할 대상 관리번호(속성: 파일경로, 클래스명, 전처리결과 등)

#폴더 안에 파일이 있는지 분기 있으면 setup_target_file_list, 없으면 end_runtime
@task.branch
def check_file_exists(folder_path:str=UPLOAD_FOLDER,**context):
    p = Path(folder_path)
    for f in p.rglob("*"):
        if f.is_file():
            return "setup_target_file_list"
    return "end_runtime"

@task(pool='ocr_pool') 
def setup_target_file_list(folder_path:str=UPLOAD_FOLDER,**context):
    run_id = context['dag_run'].run_id
    p = Path(folder_path)
    files = [f for f in p.rglob("*") if f.is_file()]
    file_info_list = []
    db_params_list = []
    for file_path in files:
        id = str(uuid.uuid4())
        if file_path.suffix.lower() == '.pdf':
            path_str = str(file_path)
            file_name = file_path.stem
            images = convert_from_path(path_str, dpi=300)
            output_folder = Path(TEMP_FOLDER) / run_id
            for i, image in enumerate(images):
                layout_id = str(uuid.uuid4())
                page_num = i+1
                img_file_path = os.path.join(output_folder, f'{file_name}_page_{page_num}.png')
                image.save(img_file_path, 'PNG')
                content = {
                    "file_id": id,
                    "layout_id":layout_id,
                    "page_num": page_num,
                    "file_path": {"_origin": img_file_path, "_origindoc": path_str},
                    # "layout_class_id": default_layout_class_id, # 분류 미처리 OCR 작업을 위한 기본값
                    # "doc_class_id": default_doc_class_id # 분류 미처리 OCR 작업을 위한 기본값
                }
                file_info_list.append(content)
                db_params_list.append((run_id, layout_id, json.dumps(content,ensure_ascii=False)))
        else: # 이미지 파일
            content = {
                "file_id":id, 
                "page_num":1, 
                "file_path":{"_origin":str(file_path)}, 
                # "layout_class_id":default_layout_class_id, # 분류 미처리 OCR 작업을 위한 기본값
                # "doc_class_id": default_doc_class_id # 분류 미처리 OCR 작업을 위한 기본값
            }
            file_info_list.append(content)
            db_params_list.append( (run_id,id,json.dumps(content,ensure_ascii=False)) )
    dococr_query_util.insert_map("insertTargetFile", params=db_params_list)
    return file_info_list

@task(pool='ocr_pool') 
def setup_layout_list_task(doc_class_id:str, **context):
    layout_list = dococr_query_util.select_list_map("selectLayoutList",(doc_class_id,))
    return layout_list

@task(pool='ocr_pool') 
def complete_runtime(doc_info:dict ,**context):
    dag_id = context['dag'].dag_id
    run_id = context['dag_run'].run_id
    dococr_query_util.update_map("updateRunEnd",params=("C",dag_id,run_id)) # C 정상완료
    
    now = datetime.now()
    # file_id = doc_info["doc_class_id"] + "_" + doc_info["file_id"]
    doc_class_id_value = doc_info.get("doc_class_id")

    # doc_class_id_value가 None인 경우 '0'으로 설정
    if doc_class_id_value is None:
        doc_class_id_str = '0'
    # doc_class_id_value가 정수(int)인 경우 문자열로 변환
    elif isinstance(doc_class_id_value, int):
        doc_class_id_str = str(doc_class_id_value)
    # 이미 문자열(str)인 경우 그대로 사용
    else:
        doc_class_id_str = doc_class_id_value

    file_id = f"{doc_class_id_str}_{doc_info['file_id']}"

    save_folder_path = Path(COMPLETE_FOLDER)/now.strftime('%Y')/now.strftime('%m')/now.strftime('%d')/file_id
    # 원본파일 이관 후 경로 업데이트
    origin_file_path = doc_info["doc_path"]["_originfile"]
    dest_file_path = file_util.file_move(origin_file_path, dest_folder=str(save_folder_path))
    doc_info["doc_path"]["_originfile"] = dest_file_path
    
    # 페이지별 이관
    page_infos = doc_info["page_infos"]
    for page_info in page_infos:
        page_num = page_info.get("page_num", 1)
        page_save_file_path = str(save_folder_path / f"_page{page_num}_normalized.png")
        # 정규화파일 이관 후 경로 업데이트
        normalized_page_file_path = page_info.get("file_path", {}).get("_normalize", "")
        dest_page_file_path = file_util.file_copy(normalized_page_file_path, dest_file=page_save_file_path)
        page_info["file_path"]["_normalize"] = dest_page_file_path
    
    json_file = Path(dest_file_path).with_suffix(".json")
    json_util.save(str(json_file),doc_info)
    complete_id = dococr_query_util.insert_map("insertComplete",params=(run_id,json.dumps(doc_info,ensure_ascii=False),),return_id=True)
    
    doc_info["complete_id"] = complete_id
    print(f"완료된 문서 정보: {doc_info}")
    return doc_info

@task(pool='ocr_pool') 
def failed_result_task(doc_info:dict=None,file_info:dict=None ,**context):
    if doc_info is None:
        if file_info is None:
            return
        else:
            layout_class_ids = [file_info.get("layout_class_id", None)]
            doc_class_id = dococr_query_util.select_doc_class_id(params=layout_class_ids)
            if "_origindoc" in file_info.get("file_path", {}):
                origin_path = {"_originfile": file_info.get("file_path", {}).get("_origindoc")} 
            elif "_origin" in file_info.get("file_path", {}):
                origin_path = {"_originfile": file_info.get("file_path", {}).get("_origin")} 
            doc_info = {
                "file_id": file_info["file_id"],
                "pages": [file_info], 
                "structed_doc": file_info["structed_layout"],
                "doc_class_id":doc_class_id,
                "doc_path":origin_path
            }
    origin_file_path = doc_info["doc_path"]["_originfile"]
    now = datetime.now()
    file_id = doc_info["file_id"]
    save_folder_path = Path(FAILED_FOLDER)/now.strftime('%Y')/now.strftime('%m')/now.strftime('%d')/file_id
    dest_file_path = file_util.file_move(origin_file_path, dest_folder=str(save_folder_path))
    doc_info["doc_path"]["_originfile"] = dest_file_path

    json_file = Path(dest_file_path).with_suffix(".json")
    json_util.save(str(json_file),doc_info)
    
    return doc_info

@task(pool='ocr_pool') 
def setup_table_list(doc_info:dict,**context):
    doc_class_id = doc_info.get("doc_class_id", None)
    table_list = dococr_query_util.select_list_map("selectRelatedTableList",(doc_class_id,))
    return table_list


     

