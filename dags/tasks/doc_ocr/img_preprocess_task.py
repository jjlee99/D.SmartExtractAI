from collections import Counter
import json
from pathlib import Path
import uuid
from typing import Any, Union
import cv2
import numpy as np
import pytesseract
from scipy.ndimage import interpolation as inter
from airflow.decorators import task
from airflow.models import Variable, XCom
from utils.com import file_util
from utils.img import type_convert_util,img_preprocess_util

RESULT_FOLDER = Variable.get("RESULT_FOLDER", default_var="/opt/airflow/data/result")
TEMP_FOLDER = Variable.get("TEMP_FOLDER", default_var="/opt/airflow/data/temp")

@task(pool='ocr_pool') 
def img_preprocess_task(file_info:dict, layout_list:list, target_key:str="_origin",**context)->dict:
    print(f"img_preprocess_task 시작 - file_info: {file_info}")
    step_infos = [json.loads(item["img_preprocess_info"]) for item in layout_list]
    data = file_info["file_path"][target_key]
    if isinstance(data, list):
        if len(data) == len(step_infos):
            #step_infos면서 data도 동일한 길이의 list로 들어왔을 경우
            results = []
            for i, step_info in enumerate(step_infos):
                rslt_map = {}
                rslt_map["folder_path"] = file_info["file_id"]
                result,rslt_map = img_preprocess_util.img_preprocess(data[i],data_type="file_path",output_type="file_path",step_info=step_info, result_map=rslt_map)
                results.append(result)
                file_info["file_path"].update(rslt_map["save_path"])
            file_info["file_path"]["_result"] = results
            file_info["status"] = "success"
        else:
            #step_infos면서 data가 동일하지 않은 길이의 list로 들어왔을 경우(error)
            file_info["status"] = "error"
            file_info["status_msg"] = f"[img_preprocess_task]'{target_key}'의 길이({len(data)})가 step_infos의 길이({len(step_infos)})와 일치하지 않습니다."
    else:
        #step_infos면서 data가 list가 아닐 경우
        results = []
        for step_info in step_infos:
            rslt_map = {}
            rslt_map["folder_path"] = file_info["file_id"]
            result,rslt_map = img_preprocess_util.img_preprocess(data,data_type="file_path",output_type="file_path",step_info=step_info, result_map=rslt_map)
            results.append(result)
            file_info["file_path"].update(rslt_map["save_path"])
        file_info["file_path"]["_result"] = results
        file_info["status"] = "success"        
    return file_info
