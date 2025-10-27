from collections import Counter
import os
from pathlib import Path
from airflow.models import Variable, XCom
from typing import Tuple, Any, List
import uuid
import cv2
from PIL import Image, ImageDraw, ImageFont
from scipy.ndimage import interpolation as inter
from utils.com import file_util
from utils.img import type_convert_util, img_preprocess_util

RESULT_FOLDER = Variable.get("RESULT_FOLDER", default_var="/opt/airflow/data/result")
TEMP_FOLDER = Variable.get("TEMP_FOLDER", default_var="/opt/airflow/data/temp")
CLASS_FOLDER = Variable.get("CLASS_FOLDER", default_var="/opt/airflow/data/class")

def create_step(step_name:str, param:dict=None, result_map:dict=None) -> Any:
    """
    오퍼레이션별 스텝 생성 함수
    :param data: 이미지 파일 경로 또는 numpy 배열
    :param data_type: 입력 데이터의 타입 ("file_path", "np_bgr", "np_gray" 등)
    :param output_type: 출력 데이터의 타입 ("file_path", "np_bgr", "np_gray" 등)
    :param step_list: 단계 목록 (기본값은 STEP_INFO_DEFAULT["step_list"])
    :param result_map: 결과를 저장할 맵 (기본값은 빈 딕셔너리)
    :return: 전처리된 이미지 또는 결과
    """
    if result_map is None:
        result_map = {}
    if param is None:
        param = {}
    process_id = f"_step_{str(uuid.uuid4())}"
    result_map["process_id"] = process_id
    result_map["folder_path"] = result_map.get("folder_path",f"{TEMP_FOLDER}/{process_id}")
    
    print("step :",step_name)
    if step_name not in function_map:
        print(f"에러: '{step_name}' 함수가 정의되지 않았습니다.")
    function_info = function_map[step_name]
    
    output = function_info["function"](**param,result_map=result_map)
    return output


def sm_sample(img_file_path:str, result_map:dict=None) -> tuple:
    #필수 파리미터 검증

    #스텝 목록
    step01 = {"name":"cache","param": {"cache_key": "origin"}}
    
    #내부 파라미터 조정
    
    #step_info 생성
    step_list = [step01]
    step_info = {
        "name":"sp_sample",
        "type":"sp_sample step_list",
        "step_list":step_list
    }
    return step_info


def ip_img_normalize(img_file_path:str, line_ratio:list=[0.2,0.2], result_map:dict=None) -> tuple:
    #필수 파리미터 검증(없음)
    #스텝 목록
    step01 = {"name":"cache","param": {"cache_key": "origin"}}
    step02 = {"name":"calc_angle_set2","param": {"angle_key": "angle2_1", "delta": 8.0, "limit": 40, "iterations": 2, "iter_save": False}} # 8도 기준 수평 각도 찾기
    step03 = {"name":"calc_angle_set2","param": {"angle_key": "angle2_2", "delta": 1.0, "limit": 8, "iterations": 2, "iter_save": False}} # 1도 기준 수평 각도 찾기
    step04 = {"name":"calc_angle_set2","param": {"angle_key": "angle2_3", "delta": 0.125, "limit": 1, "iterations": 2, "iter_save": False}} # 0.125도 기준 수평 각도 찾기
    step05 = {"name":"text_orientation_set","param": {"angle_key": "orient", "iterations": 3, "iter_save": False}}
    step06 = {"name":"load","param": {"cache_key": "origin"}}
    step07 = {"name":"rotate","param": {"angle_keys": ["angle2_1", "angle2_2", "angle2_3", "orient"]}}
    step08 = {"name":"del_blank_set2","param": {"line_ratios": [-1, -1], "padding_ratios": [-1,-1,-1,-1],"iter_save": True}} # 파라미터 조정 대상
    step09 = {"name":"scale2","param": {"calc_type":"long", "length":3150}} # 너비와 높이 중 긴 쪽이 3150이 되도록 조정
    
    #내부 파라미터 조정
    #step08 파라미터 조정
    step08["param"]["line_ratios"] = line_ratio
    result_key = "_pad"
    img_np_bgr = type_convert_util.convert_type(img_file_path,"file_path","np_bgr")
    img_preprocess_util.calc_padding_set2(img_np_bgr,line_ratios=line_ratio,result_key=result_key,iter_save=True,result_map=result_map)
    step08["param"]["padding_ratios"] = result_map[result_key]
    #line_ratios의 너비/높이 기준 비율을 기준으로 선을 추출하고,
    #추출한 선 영역을 기준으로 padding_ratios만큼 이미지를 추출하기 때문에, 
    #calc_padding_set2를 통해 비율을 측정하여 입력
    
    #step_info 생성
    step_list = [step01, step02, step03, step04, step05, step06, step07, step08, step09]
    step_info = {
        "name":"ip_img_normalize",
        "type":"img_preprocess_step_list",
        "step_list":step_list
    }
    return step_info
def ip_mask_extract(img_file_path:str, result_map:dict=None) -> tuple:
    #필수 파리미터 검증(없음)
    #스텝 목록
    step01 = {"name":"cache","param": {"cache_key": "origin"}}
    
    #내부 파라미터 조정(없음)    
    #step_info 생성
    step_list = [step01]
    step_info = {
        "name":"ip_img_normalize",
        "type":"img_preprocess_step_list",
        "step_list":step_list
    }
    return step_info


def ai_ml_line_classify_extratree(save_input:bool=False, result_map:dict=None) -> tuple:
    return _ai_ml_line_classify("ExtraTreesClassifier",save_input,result_map)
def ai_ml_line_classify_decisiontree(save_input:bool=False, result_map:dict=None) -> tuple:
    return _ai_ml_line_classify("DecisionTreeClassifier",save_input,result_map)
def ai_ml_line_classify_gaussiannb(save_input:bool=False, result_map:dict=None) -> tuple:
    return _ai_ml_line_classify("GaussianNB",save_input,result_map)
def ai_ml_line_classify_gradientboosting(save_input:bool=False, result_map:dict=None) -> tuple:
    return _ai_ml_line_classify("GradientBoostingClassifier",save_input,result_map)
def ai_ml_line_classify_kneighborsclassifier(save_input:bool=False, result_map:dict=None) -> tuple:
    return _ai_ml_line_classify("KNeighborsClassifier",save_input,result_map)
def ai_ml_line_classify_lgbmclassifier(save_input:bool=False, result_map:dict=None) -> tuple:
    return _ai_ml_line_classify("LGBMClassifier",save_input,result_map)
def ai_ml_line_classify_lineardiscriminant(save_input:bool=False, result_map:dict=None) -> tuple:
    return _ai_ml_line_classify("LinearDiscriminantAnalysis",save_input,result_map)
def ai_ml_line_classify_logisticregression(save_input:bool=False, result_map:dict=None) -> tuple:
    return _ai_ml_line_classify("LogisticRegression",save_input,result_map)
def ai_ml_line_classify_randomforest(save_input:bool=False, result_map:dict=None) -> tuple:
    return _ai_ml_line_classify("RandomForestClassifier",save_input,result_map)
def ai_ml_line_classify_logisticregression(save_input:bool=False, result_map:dict=None) -> tuple:
    return _ai_ml_line_classify("LogisticRegression",save_input,result_map)
def ai_ml_line_classify_svc(save_input:bool=False, result_map:dict=None) -> tuple:
    return _ai_ml_line_classify("SVC",save_input,result_map)
def ai_ml_line_classify_xgbclassifier(save_input:bool=False, result_map:dict=None) -> tuple:
    return _ai_ml_line_classify("XGBClassifier",save_input,result_map)
def _ai_ml_line_classify(model_name:str, save_input:bool=False, result_map:dict=None) -> tuple:
    #필수 파리미터 검증(없음)
    #스텝 목록
    step01 = {"name":"ml_line_classify","param":{"model_name":"model_name","save_input":False }}
    
    #내부 파라미터 조정
    #step01 파라미터 조정
    step01["param"]["model_name"] = model_name
    step01["param"]["save_input"] = save_input
    
    #step_info 생성
    step_list = [step01]
    step_info = {
        "name":"ml_line_classify",
        "type":"classify_ai_step_list",
        "step_list":step_list
    }
    return step_info


def cs_contain_check(correct_text:str, target_key:str="_strcomp", result_map:dict=None) -> tuple:
    #필수 파리미터 검증
    if not correct_text:
        raise ValueError("correct_text는 필수 입력값입니다.")
    
    #스텝 목록
    step01 = {"name":"contain_check","param": {"correct_text": correct_text, "target_key": target_key}}
    
    #내부 파라미터 조정
    
    #step_info 생성
    step_list = [step01]
    step_info = {
        "name":"cs_contain_check",
        "type":"cs_contain_check step_list",
        "step_list":step_list
    }
    return step_info


def ss_bounding_box(area_type:str="top_left", area_ratio:List[float]=None, area_box:List[int]=None, result_map:dict=None) -> tuple:
    #필수 파리미터 검증
    if area_box is not None: #둘 다 입력 시 비율정보 무시
        area_ratio = None
    elif area_ratio is None:
        ValueError("area_ratio or area_box 중 하나는 반드시 입력되어야 합니다.")

    #스텝 목록
    step01 = {"name":"separate_areas_set1","param": {"area_type": area_type, "area_ratio": area_ratio, "area_box": area_box}}
    
    #내부 파라미터 조정
    
    #step_info 생성
    step_list = [step01]
    step_info = {
        "name":"ss_bounding_box",
        "type":"ss_bounding_box step_list",
        "step_list":step_list
    }
    return step_info

def or_tesseract(lang:str="kor+eng",config:str="--oem 3 --psm 6",result_map:dict=None) -> tuple:
    #필수 파리미터 검증(없음)
    
    #스텝 목록
    step01 = {"name":"tesseract","param": {"lang": lang, "config": config}}
    
    #내부 파라미터 조정
    
    #step_info 생성
    step_list = [step01]
    step_info = {
        "name":"or_tesseract",
        "type":"or_tesseract step_list",
        "step_list":step_list
    }
    return step_info

def cl_general_cleansing(ocr_type:str="tesseract",result_map:dict=None) -> tuple:
    #필수 파리미터 검증(없음)
    
    #스텝 목록
    step01 = {"name":"remove_whitespace","param": {"ocr_type": ocr_type}}
    step02 = {"name":"apply_common_dictionary","param": {"ocr_type": ocr_type}}
    step03 = {"name":"apply_block_dictionary","param": {"ocr_type": ocr_type}}
    step04 = {"name":"pattern_check","param": {"ocr_type": ocr_type}}
    #내부 파라미터 조정
    
    #step_info 생성
    step_list = [step01, step02, step03, step04]
    step_info = {
        "name":"cl_tesseract",
        "type":"cl_tesseract step_list",
        "step_list":step_list
    }
    return step_info



function_map = {
    ###dococr

    #ai, cl, cs, dw, ex, ip, mb, or, sb, sm, ss, st,
    #sample(sm)
    #img_preprocess(ip)
    "ip_img_normalize": {"function": ip_img_normalize, "param": "img_file_path,line_ratio"},
    "ip_mask_extract": {"function": ip_mask_extract, "param": "img_file_path,line_ratio"},
    #classify_ai(ai)
    "ai_ml_line_classify_extratree": {"function": ai_ml_line_classify_extratree, "param": "save_input"},
    "ai_ml_line_classify_decisiontree": {"function": ai_ml_line_classify_decisiontree, "param": "save_input"},
    "ai_ml_line_classify_gaussiannb": {"function": ai_ml_line_classify_gaussiannb, "param": "save_input"},
    "ai_ml_line_classify_gradientboosting": {"function": ai_ml_line_classify_gradientboosting, "param": "save_input"},
    "ai_ml_line_classify_kneighborsclassifier": {"function": ai_ml_line_classify_kneighborsclassifier, "param": "save_input"},
    "ai_ml_line_classify_lgbmclassifier": {"function": ai_ml_line_classify_lgbmclassifier, "param": "save_input"},
    "ai_ml_line_classify_lineardiscriminant": {"function": ai_ml_line_classify_lineardiscriminant, "param": "save_input"},
    "ai_ml_line_classify_logisticregression": {"function": ai_ml_line_classify_logisticregression, "param": "save_input"},
    "ai_ml_line_classify_randomforest": {"function": ai_ml_line_classify_randomforest, "param": "save_input"},
    "ai_ml_line_classify_logisticregression": {"function": ai_ml_line_classify_logisticregression, "param": "save_input"},
    "ai_ml_line_classify_svc": {"function": ai_ml_line_classify_svc, "param": "save_input"},
    "ai_ml_line_classify_xgbclassifier": {"function": ai_ml_line_classify_xgbclassifier, "param": "save_input"},
    #classify_string(cs)
    "cs_contain_check": {"function": cs_contain_check, "param": "target_key"},
    
    #match_block(mb)
    #seperate_area(ss)
    "ss_default": {"function": ss_bounding_box, "param": "area_type,area_ratio,area_box"},
    "ss_bounding_box": {"function": ss_bounding_box, "param": "area_type,area_ratio,area_box"},
    #seperate_block(sb)
    #OCR(or)
    "or_default": {"function": or_tesseract, "param": "lang,config"},
    "or_tesseract": {"function": or_tesseract, "param": "lang,config"},
    #cleansing(cl)
    "cl_default": {"function": cl_general_cleansing, "param": "ocr_type"},
    "cl_general_cleansing": {"function": cl_general_cleansing, "param": "ocr_type"},
    #structuring(st)
    #export(ex)

    ###etc
    #draw_block_box(dw)
    
}