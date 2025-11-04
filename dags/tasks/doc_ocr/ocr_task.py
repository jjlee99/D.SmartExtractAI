from collections import defaultdict
import json
from airflow.decorators import task
import cv2
from utils.com import file_util, json_util
from utils.ocr import structuring_util
from utils.ocr import ocr_cleansing_util
import numpy as np
from utils.db import dococr_query_util
from utils.dev import draw_block_box_util
from utils.ocr import separate_area_util, separate_block_util, ocr_util, match_block_util
from typing import List, Dict, Any
from copy import deepcopy

@task(pool='ocr_pool') 
def ocr_task(file_info: Dict, target_key: Dict[str, Any], **context) -> Dict[str, Any]:
    """
    OCR 타입에 따라 적절한 OCR 태스크를 선택하여 실행합니다.
    
    :param file_info: 처리할 파일 정보 딕셔너리
    :param area_info: OCR을 수행할 영역의 설정 정보 (ocr_type 포함)
    :param context: Airflow 컨텍스트 딕셔너리
    :return: OCR 결과가 추가된 file_info 딕셔너리
    """
    # 1. 파일 로딩
    #    - file_info에 저장된 이미지를 원본으로 통칭
    #    - 2번작업으로 잘린 이미지들을 영역으로 통칭
    #    - 3번작업으로 잘린 이미지들을 블록으로 통칭
    #    - 블록맵은 {id:"",cell_box:[x,y,w,h],child:0} 형태로 저장
    #    - (원본이미지,블록맵)을 블록정보로 통칭
    #    - 원본의 블록맵 생성하여 A(가로),B(세로)큐 중 하나에 (원본이미지,블록맵) 입력
    #(area_info 기준 for문 시작)
    # 2. 영역 분할
    #    - 원본의 블록정보에서 area_info에 정의된 정보를 기준으로 영역 추출
    #    - 추출된 영역을 분석하여 블록맵 생성
    #    - 블록정보를 area_info에 설정된 값에 따라 A(가로),B(세로) 큐에 저장
    #(A/B큐 기준 while문 시작)
    # 3. while 루프를 통해 경계선 기준으로 파일 자르기(A큐, B큐)
    #    - 자르기 전 이미지는 부모, 자른 이미지는 자식으로 통칭
    #    - A, B큐의 모든 파일 처리할 때까지 반복
    #    - 플래그를 통해 이번 와일문에서 A큐를 작업할 지 B큐를 작업할지 결정(디폴트 A큐)
    #    (A큐 기준 while문)
    #    - A큐에서 이미지를 뽑아 수평으로 자르기
    #    - 자식id는 부모id + "_h" + n (n은 1부터 시작하는 인덱스)
    #    - 자식 좌표는 부모 좌표를 더하여 원본 기준 좌표로 계산
    #    - 자식 블록정보는 B큐에 입력
    #    - 자르는 작업이 완료된 부모는 child 개수를 추가한 블록정보를 C목록에 저장
    #    - A큐가 비어있으면 플래그를 B큐로 변경
    #    (B큐 기준 while문)
    #    - B큐에서 이미지를 뽑아 수직으로 자르기
    #    - 자식id는 부모id + "_v" + n (n은 1부터 시작하는 인덱스)
    #    - 자식 좌표는 부모 좌표를 더하여 원본 기준 좌표로 계산
    #    - 자식 블록정보는 A큐에 입력
    #    - 자르는 작업이 완료된 부모는 child 개수를 추가한 블록정보를 C목록에 저장
    #    - B큐가 비어있으면 플래그를 A큐로 변경
    #(A/B큐 기준 while문 종료)
    # 4. C목록에서 child 개수가 0인 블록정보를 기준으로 OCR 수행
    #    - 이미지 OCR 수행 후 결과를 블록맵에 ocr로 추가하여 D목록에 저장
    #    - D목록은 (img_np_bgr,{block_id:"tab_v2h3",block_box:[x,y,w,h], child:0, ocr:[{text:"", confidence:0.0}]}) 형태로 저장
    #    - D목록은 A큐, B큐에서 모두 처리된 블록정보를 포함
    #(area_info 기준 for문 종료)
    # 1. 파일 로딩
    original_image = file_info["file_path"][target_key]
    
    section_list = dococr_query_util.select_list_map("selectSectionList",params=(file_info["layout_class_id"],))
    structed_layout = defaultdict(list)
    
    # 2. 영역 분할
    for section_info in section_list:
        section_class_id = section_info["section_class_id"]
        section_name = section_info["section_name"]
        section_type = section_info["section_type"]
        separate_area_step_info = json.loads(section_info["separate_area"])
        img_np_bgr,result_map = separate_area_util.separate_area(original_image, data_type="file_path", output_type="np_bgr", step_info=separate_area_step_info)
        area_x, area_y = result_map["_area"]
        print(section_name,"separate_area completed:")
        block_map = {"block_id": section_name, "block_box": [area_x, area_y, img_np_bgr.shape[1], img_np_bgr.shape[0]],
                     "section_class_id":section_class_id,"section_name":section_name,"section_type":section_type, "page_num":file_info["page_num"]}
        block_data = (img_np_bgr,block_map)
        draw_block_box_util.draw_block_box_step_list((original_image, [block_map]), input_img_type="file_path", 
            step_list=[{"name": "draw_block_box_xywh", "param": {"box_color": 2, "iter_save": True}}])
        separate_block_step_info = json.loads(section_info["separate_block"])
        block_list = separate_block_util.separate_block(block_data, input_img_type="np_bgr", output_img_type="np_bgr", step_info=separate_block_step_info)
        print(section_name,"separate_block completed:", len(block_list))
        
        # 4. 블록 목록에서 child==0인 블록정보(추출대상)로 OCR 수행
        ocr_list = []
        # 리프노드만 추출하여 박스 그림
        block_box_list = [item[1]["block_box"] for item in block_list if item[1].get("child",-1) == 0]
        draw_block_box_util.draw_block_box_step_list((original_image, block_box_list), input_img_type="file_path", 
            step_list=[{"name": "draw_block_box_xywh", "param": {"box_color": 2, "iter_save": True}}])
        
        # 추출대상인 블록만 추출
        leaf_block_list = [block_data for block_data in block_list if block_data[1].get("child",-1) == 0]
        match_block_step_info = json.loads(section_info.get("match_block", '{"name": "match_block", "step_list": [{"name":"detect_row_col_set1","param":{"gap_threshold":25} },{"name":"save","param":{} }] }'))
        matched_block_list = match_block_util.match_block(leaf_block_list, step_info=match_block_step_info, result_map={"folder_path":section_name})
        
        table_map = []
        for block_data in matched_block_list:
            block_img_np_bgr, block_map = block_data
            print(section_name,"block_map:", block_map)
            ocr_step_info = json.loads(section_info["ocr"])
            block_map = ocr_util.ocr((block_img_np_bgr, block_map), input_img_type="np_bgr", step_info=ocr_step_info, result_map={"folder_path":section_name}) # ocr결과가 추가된 block_map 반환
            print("=========",block_map)
            #cleansing
            cleansing_step_info = json.loads(section_info["cleansing"])
            block_data = ocr_cleansing_util.ocr_cleansing((block_img_np_bgr, block_map), step_info=cleansing_step_info, result_map={"folder_path":section_name}) # 정제 데이터가 추가된 block_map 반환
            print("ddddddddddddddddddd",block_data[1])
            ocr_list.append(block_data[1])
            table_map.append(block_data)
        # #ocr 디버깅용
        #file_info.setdefault("ocr_results", {})[section_name] = ocr_list

        if section_info.get("structuring", "{}") == "{}":
            structuring_step_info = {"name":f"{section_name} structuring","type":"structuring_step_list",
                                    "step_list":[{"name":"structuring_by_type","param":{}}]}
        else:
            structuring_step_info = json.loads(section_info["structuring"])
            
        try:
            structed_result = structuring_util.structuring(table_map, step_info=structuring_step_info) # {table1:[{col1:val1,...},{col1:val1,...},...],table2:[...]}
            structed_section = structed_result.get("result",[])
            error_match_list = structed_result.get("error_match_list",[])
        except Exception as e:
            from pathlib import Path
            from airflow.models import Variable
            CLASS_FOLDER = Variable.get("CLASS_FOLDER", default_var="/opt/airflow/data/class")
            error_folder = Path(CLASS_FOLDER) / str(file_info["doc_class_id"]) / str(file_info["layout_class_id"]) / "error" / f"{section_class_id}_{section_name}"
            
            parts = str(e).split('|', 1)
            if len(parts) > 1:
                error_file_name = parts[0].strip()
                error_json_path = str(error_folder / f"{error_file_name}.json")
                error_img_path = str(error_folder / f"{error_file_name}.png")
                box_img = draw_block_box_util.draw_block_box_step_list((original_image, [block_map]), input_img_type="file_path", 
                    step_list=[{"name": "draw_block_box_xywh", "param": {"box_color": 2, "iter_save": False}}])
        
                file_util.file_copy(box_img,error_img_path)
                json_util.save(error_json_path,file_info)
            raise
        
        #draw_block_box_util.draw_block_box_step_list((original_image, ocr_list), input_img_type="file_path", step_list=[{"name": "draw_block_box_xywh", "param": {"box_color": 2, "iter_save": True}}])
        print(f"structed_section : {structed_section}")
        print(f"error_match_list : {error_match_list}")
        
        #현재 구역의 구조화된 데이터를 레이아웃 구조화 데이터로 취합
        for key, list_of_dicts in structed_section.items(): #building_info , [{"bild_id":{"structed_text":"123","box":[1,2,3,4],...},"b...},{"bild_id":{"st...}} }]
            # key가 빈 값이거나 "null"일 경우 넘어감
            if not key or key == "null" or key is None:
                continue
            # merged에 해당 key가 아직 없으면 그대로 리스트 복사
            if not structed_layout[key]:
                structed_layout[key] = [{} for _ in range(len(list_of_dicts))]
            for i, item_dict in enumerate(list_of_dicts): # 0, {"bild_id":{"structed_text":"123","box":[1,2,3,4],...},"b...}
                # 여러 dict가 있으면 각각 인덱스 딕셔너리에 병합
                structed_layout[key][i].update(item_dict)
        if "error_match_list" not in file_info:
            file_info["error_match_list"] = error_match_list
        else:
            file_info["error_match_list"].extend(error_match_list)
        
    run_id = context['dag_run'].run_id
    target_id = file_info.get("layout_id",file_info["file_id"])
    file_info["structed_layout"] = structed_layout
    dococr_query_util.update_map("updateTargetContent",(json.dumps(file_info, ensure_ascii=False),run_id,target_id))
    file_info["status"] = "success"
    return file_info
    
def _perform_ocr(img_np_bgr):
    
    return [
        {
            "text": "Sample OCR Text",
            "box": [10, 20, 100, 50],
            "confidence": 0.95
        }
    ]

@task(pool='ocr_pool') 
def only_ocr(file_info: Dict, target_key: Dict[str, Any], **context) -> Dict[str, Any]:
    """
    OCR 타입에 따라 적절한 OCR 태스크를 선택하여 실행합니다.
    
    :param file_info: 처리할 파일 정보 딕셔너리
    :param area_info: OCR을 수행할 영역의 설정 정보 (ocr_type 포함)
    :param context: Airflow 컨텍스트 딕셔너리
    :return: OCR 결과가 추가된 file_info 딕셔너리
    """
    original_image = file_info["file_path"][target_key]
    
    structed_layout = defaultdict(list)
    block_map = ocr_util.ocr((original_image, {}), input_img_type="file_path", step_info={"name": "doc_subject ocr", "type": "ocr_step_list", 
        "step_list": [{"name": "tesseract", "param": {"lang": "kor+eng", "config": "--oem 3 --psm 3", "iter_save": True}},
                      {"name": "save", "param": {"save_key":"block_ocr","tmp_save":True}}] }, 
        result_map={"folder_path":"block_ocr"}) # ocr결과가 추가된 block_map 반환

    file_info["status"] = "success"
    return file_info

@task(pool='ocr_pool') 
def aggregate_ocr_results_task(file_infos:list[dict],**context):
    """
    파일 정보 리스트에서 각 파일별로 분류 결과를 종합하고, 가장 신뢰도가 높은 클래스로 최종 분류 결과를 저장하는 함수.
    결과는 파일 정보에 추가되고, 분류 결과 및 파일 복사, DB 저장 등의 후처리를 수행한다.

    Args:
        file_infos (list): 각 파일의 정보(분류 결과 포함)가 담긴 딕셔너리 리스트
        class_keys (list): 분류 기준이 되는 클래스 키 리스트
        context (dict): Airflow 등에서 전달되는 context 정보(예: dag_run 등)
    Returns:
        list: 최종 분류 결과가 추가된 파일 정보 리스트
    """
    # 1) file_id별로 그룹핑
    file_groups = defaultdict(list)
    
    for file_info in file_infos:
        file_groups[file_info['file_id']].append(file_info)

    doc_info_list = []
    
    origin_path = {}
    for file_id, items in file_groups.items():
        # 2) page_num 순 정렬
        items_sorted = sorted(items, key=lambda x: x['page_num'])
        doc_class_id = None
        if len(items_sorted)>0:
            entry = items_sorted[0]
            doc_class_id = entry["doc_class_id"]
            if "_origindoc" in entry.get("file_path", {}):
                origin_path = {"_originfile": entry.get("file_path", {}).get("_origindoc")} 
            elif "_origin" in entry.get("file_path", {}):
                origin_path = {"_originfile": entry.get("file_path", {}).get("_origin")} 
        

        # 3) structed 병합 준비
        merged_structed = defaultdict(list)  # table명: list of dict(row)
        error_merged = {}
        error_cntr = 1            
        parecnt_table_info = {}
        keynum = defaultdict(int)  # table명: list of dict(row)
        table_list = dococr_query_util.select_list_map("selectRelatedTableList",params=(doc_class_id,))
        
        for table_info in table_list:
            keynum[table_info["table_name"]] = 1
            parecnt_table_info[table_info["table_name"]] = table_info["parent_table_name"]
        
        page_infos = []
        layout_class_ids = []
        for entry in items_sorted:
            layout_class_ids.append(str(entry["layout_class_id"]))
            error_match_list = entry.get('error_match_list', [])
            structed = entry.get('structed_layout', [])
            page_infos.append({
                "page_num": entry["page_num"],
                "file_path": {"_normalize":entry.get("file_path", {}).get("_normalize", "")},
                "layout_class_id": entry["layout_class_id"],
                "doc_class_id": doc_class_id
            })
            
            print("222222222",structed)
            for error_info in error_match_list:
                new_key = f"unmatched{error_cntr}"
                error_merged[new_key] = deepcopy(error_info)
                error_cntr+=1

            for table_name, rows in structed.items():
                if table_name not in merged_structed:
                    for row in rows:
                        row["_ID"] = keynum[table_name]
                        row["_PAR_ID"] = keynum[parecnt_table_info[table_name]]
                        keynum[table_name] += 1
                    merged_structed[table_name] = deepcopy(rows)
                else:
                    # 입력하려는 row들의 기존의 입력된 row들과 병합 여부를 확인
                    row_num = len(rows)
                    existing_rows = merged_structed[table_name]
                    # 기존의 입력된 row들의 최근 row들을 조회(입력하려는 row들과 동일한 row 추출)
                    last_existing_rows = existing_rows[-row_num:]
                    # 일괄 처리 작업이므로 전체 rows와 상관없이 첫 행만 체크하여 진행
                    # last_existing_rows 의 키 집합
                    last_existing_keys = set(last_existing_rows[0].keys())

                    # 우선 rows 중 첫번째 행을 기준으로 판단 (or 그냥 첫 행만 비교)
                    # 이 예시는 첫 row 기준 비교해서 overlapped 여부 판단
                    overlapped = False
                    # 전체 rows가 아닌, 첫 new_row 만 검사
                    if not rows:
                        continue
                    # 입력하려는 row들의 첫번째 키값들과 최근 row들의 첫번째 키값들을 비교하여 동일한 키가 있는지 확인
                    first_new_row = rows[0]
                    if last_existing_keys & set(first_new_row.keys()):
                        overlapped = True

                    # 결정된 overlapped 결과를 모든 new_row에 적용
                    if overlapped:
                        # 키 겹침으로 판단 → 모든 new_row append
                        for new_row in rows:
                            new_row["_ID"] = keynum[table_name]
                            new_row["_PAR_ID"] = keynum[parecnt_table_info[table_name]]
                            keynum[table_name] += 1
                            existing_rows.append(deepcopy(new_row))
                    else:
                        # 키 겹치지 않음 → 같은 인덱스끼리 병합 시도
                        if len(rows) == len(existing_rows):
                            for idx, new_row in enumerate(rows):
                                merged_row = dict(existing_rows[idx])  # 복사
                                merged_row.update(new_row)
                                existing_rows[idx] = merged_row
                        else:
                            for new_row in rows:
                                new_row["_ID"] = keynum[table_name]
                                new_row["_PAR_ID"] = keynum[parecnt_table_info[table_name]]
                                keynum[table_name] += 1
                                existing_rows.append(deepcopy(new_row))
        doc_class_id = dococr_query_util.select_doc_class_id(params=layout_class_ids) 
        # 결과 조립
        
        error_match = {"title_area": [error_merged]}
        
        doc_info_list.append({
            "file_id": file_id,
            "page_infos": page_infos,
            "structed_doc": dict(merged_structed),
            "error_match": dict(error_match),
            "doc_class_id":doc_class_id,
            "doc_path":origin_path,
            "pages": items_sorted  # 같은 file_id의 원본 page_num 순 리스트 (필요하다면 items_sorted 수정 가능)
        })

    return doc_info_list


    
    
    
    
    