from PIL import Image
from pathlib import Path
import uuid
import numpy as np
import cv2
from airflow.models import Variable
from airflow.operators.python import get_current_context
from typing import Any

TEMP_FOLDER = Variable.get("TEMP_FOLDER", default_var="/opt/airflow/data/temp")
ocr_result_type = ["common", "tesseract", ]

#실제 변환 함수
def tesseract_to_common(ocr_data:dict):
    n_boxes = len(ocr_data['level'])
    results = []
    for i in range(n_boxes):
        level = ocr_data['level'][i]
        page_num = ocr_data['page_num'][i]
        block_num = ocr_data['block_num'][i]
        par_num = ocr_data['par_num'][i]
        line_num = ocr_data['line_num'][i]
        word_num = ocr_data['word_num'][i]
        left = _safe_float(ocr_data['left'][i])
        top = _safe_float(ocr_data['top'][i])
        width = _safe_float(ocr_data['width'][i])
        height = _safe_float(ocr_data['height'][i])
        conf = int(ocr_data['conf'][i]) if ocr_data['conf'][i].isdigit() else -1
        text = ocr_data['text'][i]

        result = {
            "level": level,
            "page_num": page_num,
            "block_num": block_num,
            "par_num": par_num,
            "line_num": line_num,
            "word_num": word_num,
            "box": [left, top, width, height],
            "conf": conf,
            "text": text.strip()
        }

        results.append(result)
    return results

#내부함수
def _safe_float(num_str, default=0.0):
    try:
        return float(num_str)
    except (ValueError, TypeError):
        return default