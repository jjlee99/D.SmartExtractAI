from airflow.decorators import task
from pathlib import Path
import shutil, os, json
from airflow.models import Variable
import cv2

TEMP_FOLDER = Variable.get("TEMP_FOLDER", default_var="/opt/airflow/data/temp")
RESULT_FOLDER = Variable.get("RESULT_FOLDER", default_var="/opt/airflow/data/result")

def get_config(*keys):
    #레이아웃 클래스
    class_map = {
        "general_building_register":{ # 일반건축물대장
            "a_class":{ # 일반건축물대장 갑 1페이지
                "class_id":1111,
                "classify":{
                    "classify_id":1111,
                    "classify_ai":{
                        "ai_id":15,
                        "ai_dir":"/opt/airflow/data/class/a_class/classify/model",
                        "processor_name":"SCUT-DLVCLab/lilt-roberta-en-base",
                        "model_name":"SCUT-DLVCLab/lilt-roberta-en-base",
                        "class_key":"a_class",
                        "save_input":True,
                    },
                    "img_preprocess":{
                        "name":"a_class classify img_preprc",
                        "type":"img_preprocess_step_list",
                        "step_list":[
                            {"name":"cache","param":{"cache_key":"origin"}},
                            {"name":"calc_angle_set2","param":{"angle_key":"angle2_1","delta":8.0,"limit":40,"iterations":2,"iter_save":False}},
                            {"name":"calc_angle_set2","param":{"angle_key":"angle2_2","delta":1.0,"limit":8,"iterations":2,"iter_save":False}},
                            {"name":"calc_angle_set2","param":{"angle_key":"angle2_3","delta":0.125,"limit":1,"iterations":2,"iter_save":False}},
                            {"name":"text_orientation_set","param":{"angle_key":"orient","iterations":3,"iter_save":False}},
                            {"name":"load","param":{"cache_key":"origin"}},
                            {"name":"rotate","param":{"angle_keys":["angle2_1","angle2_2","angle2_3","orient"]}},
                            {"name":"del_blank_set2","param":{"line_ratios": [0.2, 0.06], "padding_ratios": [0.21, 0.29, 0.09, 0.12],"iter_save": False}},
                            {"name":"save","param":{"save_key":"_normalize","tmp_save":True}},
                        ], 
                    }
                },
                "ocr":{
                    "area_cnt":5,
                    "area_list":[
                        {
                            "area_name":"doc_subject",
                            "img_preprocess":{
                                "name":"doc_subject img_preprc",
                                "type":"img_preprocess_step_list",
                                "step_list":[
                                    {"name" : "separate_areas_set1", "param": {"area_name":"doc_subject","area_type":"top_center","offset":[-180,100],"width":410,"height":100,
                                                                            "process_type":"fix_text","key_list":["일반건축물대장(갑)"],"iter_save":False}},
                                    {"name":"save","param":{"save_key":""}},
                                ], 
                            },
                            "separate_area":{
                                "name":"doc_subject separate_area",
                                "type":"separate_area_step_list",
                                "step_list":[
                                    {"name" : "separate_areas_set1", "param": {"area_name":"doc_subject","area_type":"top_center","area_ratio":[-0.083,0.068,0.188,0.068],"iter_save":False}}, #"area_box":[-180,100,410,100],
                                ], 
                            },
                            "separate_block":{
                                "name":"doc_subject separate_block",
                                "type":"separate_block_step_list",
                                "step_list":[
                                    {"name" : "separate_block_by_line", "param": {"horizontal_first":True,"horizontal_line_ratio": 0.5, "horizontal_min_gap": 10, 
                                                                                "vertical_line_ratio": 0.85, "vertical_min_gap" :20,"iter_save":False}},
                                ], 
                            },
                            "ocr":{
                                "name":"doc_subject ocr",
                                "type":"ocr_step_list",
                                "step_list":[
                                    {"name" : "tesseract", "param": {"lang":"kor+eng","config":"--oem 3 --psm 6","iter_save":False}},
                                ],
                            },
                            "cleansing":{"name": "doc_subject cleansing", 
                                "type": "cleansing_step_list", 
                                "step_list": [
                                    {"name": "sanitize_text", "param": {"ocr_type": "tesseract"}}, 
                                    {"name": "apply_common_dictionary", "param": {"ocr_type": "tesseract"}}, 
                                    {"name": "apply_block_dictionary", "param": {"ocr_type": "tesseract"}},
                                    {"name": "pattern_check", "param": {"ocr_type": "tesseract"}},
                                    {"name": "save","param": {}}
                                ]
                            },
                            "structuring":{
                                "name":"doc_subject structuring",
                                "type":"structuring_step_list",
                                "step_list":[
                                    {"name":"structuring_one_row","param":{"doc_class":"general_building_register", "layout_class":"a_class", "section_name":"doc_subject"}},
                                ]
                            },
                            "ocr_type": "text",
                            "iter_save": True
                        },
                        {
                            "area_name":"building_info",
                            "img_preprocess":{
                                "name":"building_info img_preprc",
                                "type":"step_list",
                                "step_list":[
                                    {"name" : "separate_areas_set1", "param": {"area_name":"building_info","area_type":"top_left","offset":[50,205],"width":-1,"height":430,
                                                                            "process_type":"key_value_in_cell",
                                                                                "iter_save":False}},
                                    {"name":"save","param":{"save_key":""}},
                                    {"name" : "separate_areas_set2", "param": {"area_name":"building_info","first_axis": "horizontal", 
                                                                            "horizontal_line_ratio": 0.3, "horizontal_min_gap": 10, 
                                                                            "vertical_line_ratio": 0.85, "vertical_min_gap" :20,
                                                                                "iter_save": False}},
                                    {"name":"save","param":{"save_key":""}},
                                ]                                                                 
                            },
                            "separate_area":{
                                "name":"building_info separate_area",
                                "type":"separate_area_step_list",
                                "step_list":[
                                    {"name" : "separate_areas_set1", "param": {"area_name":"building_info","area_type":"top_left","area_ratio":[0.023,0.139,-1,0.098],"iter_save":False}}, #"area_box":[50,205,-1,145],
                                ], 
                            },
                            "separate_block":{
                                "name":"building_info separate_block",
                                "type":"separate_block_step_list",
                                "step_list":[
                                    {"name" : "separate_block_by_line", "param": {"horizontal_first":True,"horizontal_line_ratio": 0.4, "horizontal_min_gap": 10, 
                                                                                "vertical_line_ratio": 0.85, "vertical_min_gap" :20,"iter_save":False}},
                                    {"name":"split_image_by_vertical_gaps","param":{"min_start_point":10,"min_gap_width":30, "iter_save":True}},
                                ], 
                            },
                            "ocr":{
                                "name":"building_info ocr",
                                "type":"ocr_step_list",
                                "step_list":[
                                    {"name" : "tesseract", "param": {"lang":"kor+eng","config":"--oem 3 --psm 6","iter_save":False}},
                                ],
                            },
                            "cleansing":{
                                "name":"building_info cleansing",
                                "type": "cleansing_step_list", 
                                "step_list": [
                                    {"name": "sanitize_text", "param": {"ocr_type": "tesseract"}}, 
                                    {"name": "apply_common_dictionary", "param": {"ocr_type": "tesseract"}}, 
                                    {"name": "apply_block_dictionary", "param": {"ocr_type": "tesseract"}},
                                    {"name": "pattern_check", "param": {"ocr_type": "tesseract"}},
                                    {"name": "save","param": {}}
                                ]
                            },
                            "structuring":{
                                "name":"building_status structuring",
                                "type":"structuring_step_list",
                                "step_list":[
                                    {"name":"structuring_one_row","param":{"doc_class":"general_building_register", "layout_class":"a_class", "section_name":"building_info"}},
                                ]
                            },
                            "ocr_type": "table",
                            "iter_save": True,
                            "detection_params": {
                                "h_kernel_divisor": 40,
                                "v_kernel_divisor": 15,
                                "min_cell_area": 5000,
                                "min_cell_width": 20,
                                "min_cell_height": 50,
                                "dilation_iterations": 3,
                                "adaptive_thresh_block_size": 25,
                                "adaptive_thresh_c": -3
                            }
                        },
                        {
                            "area_name":"building_detail",
                            "img_preprocess":{
                                "name":"building_detail img_preprc",
                                "type":"step_list",
                                "step_list":[
                                    {"name" : "separate_areas_set1", "param": {"area_name":"building_info","area_type":"top_left","offset":[50,205],"width":-1,"height":430,
                                                                            "process_type":"key_value_in_cell",
                                                                                "iter_save":False}},
                                    {"name":"save","param":{"save_key":""}},
                                    {"name" : "separate_areas_set2", "param": {"area_name":"building_info","first_axis": "horizontal", 
                                                                            "horizontal_line_ratio": 0.3, "horizontal_min_gap": 10, 
                                                                            "vertical_line_ratio": 0.85, "vertical_min_gap" :20,
                                                                                "iter_save": False}},
                                    {"name":"save","param":{"save_key":""}},
                                ]                                                                 
                            },
                            "separate_area":{
                                "name":"building_detail separate_area",
                                "type":"separate_area_step_list",
                                "step_list":[
                                    {"name" : "separate_areas_set1", "param": {"area_name":"building_detail","area_type":"top_left","area_ratio":[0.023,0.237,-1,0.193],"iter_save":False}}, #"area_box":[50,350,-1,285],
                                ], 
                            },
                            "separate_block":{
                                "name":"building_detail separate_block",
                                "type":"separate_block_step_list",
                                "step_list":[
                                    {"name" : "separate_block_by_line", "param": {"horizontal_first":True,"horizontal_line_ratio": 0.4, "horizontal_min_gap": 10, 
                                                                                "vertical_line_ratio": 0.85, "vertical_min_gap" :20,"iter_save":False}},
                                    {"name":"split_image_by_vertical_gaps","param":{"min_start_point":25,"min_gap_width":4,"angle":90,"iter_save":True}},
                                ], 
                            },
                            "ocr":{
                                "name":"building_detail ocr",
                                "type":"ocr_step_list",
                                "step_list":[
                                    {"name" : "tesseract", "param": {"lang":"kor+eng","config":"--oem 3 --psm 6","iter_save":False}},
                                ],
                            },
                            "cleansing":{
                                "name":"building_detail cleansing",
                                "type": "cleansing_step_list", 
                                "step_list": [
                                    {"name": "sanitize_text", "param": {"ocr_type": "tesseract"}}, 
                                    {"name": "apply_common_dictionary", "param": {"ocr_type": "tesseract"}}, 
                                    {"name": "apply_block_dictionary", "param": {"ocr_type": "tesseract"}},
                                    {"name": "pattern_check", "param": {"ocr_type": "tesseract"}},
                                    {"name": "save","param": {}}
                                ]
                            },
                            "structuring":{
                                "name":"building_detail structuring",
                                "type":"structuring_step_list",
                                "step_list":[
                                    {"name":"structuring_one_row","param":{"doc_class":"general_building_register", "layout_class":"a_class", "section_name":"building_detail"}},
                                ]
                            },
                            "ocr_type": "table",
                            "iter_save": True,
                            "detection_params": {
                                "h_kernel_divisor": 40,
                                "v_kernel_divisor": 15,
                                "min_cell_area": 5000,
                                "min_cell_width": 20,
                                "min_cell_height": 50,
                                "dilation_iterations": 3,
                                "adaptive_thresh_block_size": 25,
                                "adaptive_thresh_c": -3
                            }
                        },
                        {
                            "area_name":"building_status",
                            "img_preprocess":{
                                "name":"building_status img_preprc",
                                "type":"step_list",
                                "step_list":[
                                    {"name" : "separate_areas_set1", "param": {"area_name":"building_status","area_type":"top_left","offset":[50,638],"width":-1,"height":310,
                                                                            "process_type":"key_value_in_cell",   
                                                                                "iter_save": False}},
                                    {"name":"save","param":{"save_key":""}},
                                    {"name" : "separate_areas_set2", "param": {"area_name":"building_status", "first_axis": "horizontal",
                                                                            "horizontal_line_ratio": 0.43, "horizontal_min_gap": 10, 
                                                                            "vertical_line_ratio":0.4, "vertical_min_gap" : 10,
                                                                            "iter_save": False}},
                                    {"name":"save","param":{"save_key":""}},
                                ]                
                            },
                            "separate_area":{
                                "name":"building_status separate_area",
                                "type":"separate_area_step_list",
                                "step_list":[
                                    {"name" : "separate_areas_set1", "param": {"area_name":"building_status","area_type":"top_left","area_ratio":[0.023,0.433,0.49,0.21],"iter_save": False}}, #"area_box":[50,638,-1,310],
                                ], 
                            },
                            "separate_block":{
                                "name":"building_status separate_block",
                                "type":"separate_block_step_list",
                                "step_list":[
                                    {"name" : "separate_block_by_line", "param": {"horizontal_first":True,"horizontal_line_ratio": 0.4, "horizontal_min_gap": 10, 
                                                                                "vertical_line_ratio": 0.85, "vertical_min_gap" :20,"iter_save":False}}
                                ], 
                            },
                            "ocr":{
                                "name":"building_status ocr",
                                "type":"ocr_step_list",
                                "step_list":[
                                    {"name" : "tesseract", "param": {"lang":"kor+eng","config":"--oem 3 --psm 6","iter_save":False}},
                                ],
                            },
                            "cleansing":{
                                "name":"building_status cleansing",
                                "type": "cleansing_step_list", 
                                "step_list": [
                                    {"name": "sanitize_text", "param": {"ocr_type": "tesseract"}}, 
                                    {"name": "apply_common_dictionary", "param": {"ocr_type": "tesseract"}}, 
                                    {"name": "apply_block_dictionary", "param": {"ocr_type": "tesseract"}},
                                    {"name": "pattern_check", "param": {"ocr_type": "tesseract"}},
                                    {"name": "save","param": {}}
                                ]
                            },
                            "structuring":{
                                "name":"building_status structuring",
                                "type":"structuring_step_list",
                                "step_list":[
                                    {"name":"structuring_multi_row","param":{"doc_class":"general_building_register", "layout_class":"a_class", "section_name":"building_status"}},
                                ]
                            },
                            "ocr_type": "table",
                            "iter_save": True,
                            "detection_params": {
                                "h_kernel_divisor": 40,
                                "v_kernel_divisor": 15,
                                "min_cell_area": 1000,
                                "min_cell_width": 40,
                                "min_cell_height": 20,
                                "dilation_iterations": 2
                            }
                        },
                        {
                            "area_name":"owner_status",
                            "img_preprocess":{
                                "name":"owner_status img_preprc",
                                "type":"step_list",
                                "step_list":[
                                    {"name" : "separate_areas_set1", "param": {"area_name":"owner_status","area_type":"top_left","offset":[50,638],"width":-1,"height":310,
                                                                            "process_type":"key_value_in_cell",
                                                                                "iter_save": False}},
                                    {"name":"save","param":{"save_key":""}},
                                    {"name" : "separate_areas_set2", "param": {"area_name":"owner_status", "first_axis": "horizontal",
                                                                            "horizontal_line_ratio": 0.43, "horizontal_min_gap": 10,
                                                                            "vertical_line_ratio":0.4, "vertical_min_gap" : 10,
                                                                            "iter_save": False}},
                                    {"name":"save","param":{"save_key":""}},
                                ]
                            },
                            "separate_area":{
                                "name":"owner_status separate_area",
                                "type":"separate_area_step_list",
                                "step_list":[
                                    {"name" : "separate_areas_set1", "param": {"area_name":"owner_status","area_type":"top_left","area_ratio":[0.513,0.433,0.486,0.21],"iter_save": False}}, #"area_box":[50,638,-1,310],
                                ], 
                            },
                            "separate_block":{
                                "name":"owner_status separate_block",
                                "type":"separate_block_step_list",
                                "step_list":[
                                    {"name" : "separate_block_by_line", "param": {"horizontal_first":True,"horizontal_line_ratio": 0.4, "horizontal_min_gap": 10, 
                                                                                "vertical_line_ratio": 0.85, "vertical_min_gap" :20,"iter_save":False}}
                                ], 
                            },
                            "ocr":{
                                "name":"owner_status ocr",
                                "type":"ocr_step_list",
                                "step_list":[
                                    {"name" : "tesseract", "param": {"lang":"kor+eng","config":"--oem 3 --psm 6","iter_save":False}},
                                ],
                            },
                            "cleansing":{
                                "name":"owner_status cleansing",
                                "type": "cleansing_step_list", 
                                "step_list": [
                                    {"name": "sanitize_text", "param": {"ocr_type": "tesseract"}}, 
                                    {"name": "apply_common_dictionary", "param": {"ocr_type": "tesseract"}}, 
                                    {"name": "apply_block_dictionary", "param": {"ocr_type": "tesseract"}},
                                    {"name": "pattern_check", "param": {"ocr_type": "tesseract"}},
                                    {"name": "save","param": {}}
                                ]
                            },
                            "structuring":{
                                "name":"owner_status structuring",
                                "type":"structuring_step_list",
                                "step_list":[
                                    {"name":"structuring_multi_row","param":{"doc_class":"general_building_register", "layout_class":"a_class", "section_name":"owner_status"}},
                                ]
                            },
                            "ocr_type": "table",
                            "iter_save": True,
                            "detection_params": {
                                "h_kernel_divisor": 40,
                                "v_kernel_divisor": 15,
                                "min_cell_area": 1000,
                                "min_cell_width": 40,
                                "min_cell_height": 20,
                                "dilation_iterations": 2
                            }
                        },
                    ]                
                },
                "save":{
                    "save_list":[
                        "classify_preprocess"
                    ],
                },
            },
            "b_class":{
                "class_id":1111,
                "classify":{
                    "classify_id":1111,
                    "classify_ai":{
                        "ai_id":15,
                        "ai_dir":"/opt/airflow/data/class/b_class/classify/model",
                        "processor_name":"SCUT-DLVCLab/lilt-roberta-en-base",
                        "model_name":"SCUT-DLVCLab/lilt-roberta-en-base",
                    },
                    "img_preprocess":{
                        "name":"b_class classify img_preprc",
                        "type":"step_list",
                        "step_list":[
                            {"name":"cache","param":{"cache_key":"origin"}},
                            {"name":"calc_angle_set2","param":{"angle_key":"angle2_1","delta":8.0,"limit":40,"iterations":2,"iter_save":False}},
                            {"name":"calc_angle_set2","param":{"angle_key":"angle2_2","delta":1,"limit":8,"iterations":2,"iter_save":False}},
                            {"name":"calc_angle_set2","param":{"angle_key":"angle2_3","delta":0.125,"limit":1,"iterations":2,"iter_save":False}},
                            {"name":"text_orientation_set","param":{"angle_key":"orint","iterations":2,"iter_save":False}},
                            {"name":"load","param":{"cache_key":"origin"}},
                            {"name":"rotate","param":{"angle_key":"angle2_1"}},
                            {"name":"rotate","param":{"angle_key":"angle2_2"}},
                            {"name":"rotate","param":{"angle_key":"angle2_3"}},
                            {"name":"rotate","param":{"angle_key":"orint"}},
                        ], 
                    }
                },
                "area_cut":{},
                "ocr":{},
                "save":{
                    "save_list":[
                        "classify_preprocess"
                    ],
                },
            }
        }
    }
    return _get_deep_info(class_map,*keys)

def get_image_paths(directory: str) -> list[str]:
    """해당 폴더의 이미지 경로 리스트 반환"""
    if not os.path.exists(directory):
        return [] # 임시 각주 해제
    return [os.path.join(directory, f) for f in os.listdir(directory) 
            if f.lower().endswith(('.png', '.jpg', '.jpeg'))]

def get_image_paths_recursive(directory: str) -> list[str]:
    """해당 폴더 하위까지 포함하여 이미지 경로 리스트 반환"""
    image_paths = []
    for root, dirs, files in os.walk(directory):
        for f in files:
            if f.lower().endswith(('.png', '.jpg', '.jpeg')):
                image_paths.append(os.path.join(root, f))
    return image_paths

def file_copy(src_file: str, dest_file:str=None, dest_folder:str=None) -> str:
    """
    파일을 복사하는 함수
    
    :param src_file: 복사할 파일 경로(문자열)
    :param dest_file: 붙여넣을 파일 경로(문자열)
    :param dest_folder: 붙여넣을 폴더 경로(문자열)
    :return: 실제로 복사된 파일 경로(문자열)
    """
    print("file_copy:",dest_file)
    src = Path(src_file)
    if dest_file is None:
        if dest_folder is not None:
            # d`est_folder가 폴더이므로, dest_file은 그 안에 src파일명을 붙임
            dest_file = Path(dest_folder) / src.name
        else:
            raise ValueError("dest_file이나 dest_folder가 존재하지 않습니다.")
    dest = Path(dest_file)

    # 붙여넣을 폴더가 없으면 생성
    dest.parent.mkdir(parents=True, exist_ok=True)

    # 파일명과 확장자 분리
    stem = dest.stem
    suffix = dest.suffix
    count = 1

    # 파일이 이미 존재하면 숫자를 붙여서 복사
    while dest.exists():
        dest = dest.parent / f"{stem}({count}){suffix}"
        count += 1

    # 파일 복사
    shutil.copy2(src, dest)
    
    return str(dest)

def file_move(src_file: str, dest_file:str=None, dest_folder:str=None) -> str:
    dest = file_copy(src_file,dest_file,dest_folder)
    # move_at이 True면 원본 파일 삭제
    try:
        Path(src_file).unlink()  # 원본 파일 삭제
    except Exception as e:
        print(f"원본 파일 삭제 실패: {e}")
    return dest


#내부함수
#dict 데이터 값 찾아가기
def _get_deep_info(data, *keys):
    for key in keys:
        if isinstance(data, dict) and key in data:
            data = data[key]
        else:
            return None
    return data
