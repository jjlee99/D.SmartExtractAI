from airflow.decorators import task
from PIL import Image
import json
import cv2
import pytesseract
from utils.com import json_util
from utils.db import dococr_query_util

@task(pool='ocr_pool') 
def table_ocr_by_cell(file_info: dict, ocr_info: dict, **context):
    image_path = file_info["file_path"]["_origin"]
    file_id = file_info["file_id"]

    # 이미지 로드 (RGB)
    img = cv2.imread(image_path)
    img_rgb = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)

    td_model = ocr_info.get("td_model","default")
    tsr_model = ocr_info.get("tsr_model","default")
    print(td_model,tsr_model)
    # 표 및 구조 감지
    if td_model == tsr_model == "default":
        print("detect")
        result = td.detect(image_path)
        print(result)
        if not result:
            file_info["ocr_result"] = []
            return file_info
    
        cell_results = []
        idx = 0
        for table_info, image_name in result:
            idx+=1
            for table_bbox, cells in table_info.items():
                for cell in cells:
                    # 셀 영역 추출
                    x1, y1, w, h = cell
                    x2 = x1 + w
                    y2 = y1 + h
                    # 좌표 클리핑
                    x1 = max(0, min(x1, w))
                    y1 = max(0, min(y1, h))
                    x2 = max(0, min(x2, w))
                    y2 = max(0, min(y2, h))

                    if x2 <= x1 or y2 <= y1:
                        continue
                    cell_img = img_rgb[y1:y2, x1:x2]
                    # 셀 내 텍스트 추출
                    text = pytesseract.image_to_string(Image.fromarray(cell_img), lang='kor+eng').strip()
                    print(text)
                    cell_results.append({
                        "table":idx,
                        "bbox": [x1, y1, x2, y2],
                        "text": text
                    })
        file_info["ocr_result"] = cell_results

        run_id = context['dag_run'].run_id
        target_id = file_info.get("layout_id",file_info["file_id"])
        print(file_info)
        json_util.save("/opt/airflow/data/temp/"+run_id+"/"+target_id+".json",file_info)
        dococr_query_util.update_map("updateTargetContent",(json.dumps(file_info),run_id,target_id))

    return file_info
