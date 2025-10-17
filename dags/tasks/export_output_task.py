from collections import Counter
from pathlib import Path
import uuid
from typing import Any, List
import cv2
import numpy as np
import pytesseract
from scipy.ndimage import interpolation as inter
from airflow.decorators import task
from airflow.models import Variable, XCom
from utils.ocr import export_ocr_output_util
from utils.com import file_util
from utils.img import type_convert_util,img_preprocess_util,export_img_output_util

RESULT_FOLDER = Variable.get("RESULT_FOLDER", default_var="/opt/airflow/data/result")
TEMP_FOLDER = Variable.get("TEMP_FOLDER", default_var="/opt/airflow/data/temp")


@task(pool='ocr_pool') 
def export_output_task(doc_info:dict,target_key:str="_origin")->dict:
    rslt_map = {}
    rslt_map["folder_path"] = doc_info["file_id"]
    

    export_ocr_output_step_info = {
        "name":"export_ocr_output",
        "step_list":[
            {"name":"insert_ocr_result","param":{}},
            {"name":"db_to_excel","param":{}},
        ]
    }

    rslt_map = export_ocr_output_util.export_ocr_output(doc_info,step_info=export_ocr_output_step_info, result_map=rslt_map)
    doc_info["doc_path"].update(rslt_map["save_path"])
    
    doc_info["status"] = "success"

    return doc_info

