import json
import os,cv2
from utils.dev import draw_block_box_util
from utils.db import dococr_query_util
import pytesseract
import uuid
import shutil
import numpy as np
from PIL import Image
from utils.ocr import separate_area_util

#이미지 전처리(헤더 ocr, 표 선 검출) 후, 피처별 통계정보 추출하여 dict 형태로 데이터 생성
def preprocess_image_and_extract_features(image_path: str):
    iter_save= True
    image = Image.open(image_path).convert("RGB")
    image_width, image_height = image.size
    
    process_id = str(uuid.uuid4())
    tmp_dir = os.path.join("/tmp", process_id)
    os.makedirs(tmp_dir, exist_ok=True)
    
    # section_list = dococr_query_util.select_list_map("selectSectionList", params=(layout_class_id,))
    # step_info = json.loads(section_list[0].get("separate_area")) if section_list else None # 첫번째 섹션정보는 항상 문서 분류를 위한 헤더로 사용. 제목이 없다면 헤더 정보 활용.
    # if step_info is None:
    #     print("no separate_area step_info")
    #     header_img = image
    #     data = {"text": [], "left": [], "top": [], "width": [], "height": []}
    # else:
    #     header_img, _ = separate_area_util.separate_area(
    #         image, data_type="pil", output_type="pil",
    #         step_info=step_info,
    #         result_map={"folder_path": tmp_dir}
    #     )
    #     header_width, header_height = header_img.size
    #     # 헤더 OCR
    #     try:
    #         data = pytesseract.image_to_data(
    #             header_img, output_type=pytesseract.Output.DICT, lang='kor+eng', config='--psm 6 --oem 3')
    #     except Exception as e:
    #         print(f"OCR error: {e}")
    #         data = {"text": [], "left": [], "top": [], "width": [], "height": []}
    
    # words = []
    # text_box = []
    # boxes = []
    # for word, x, y, w, h in zip(data['text'], data['left'], data['top'], data['width'], data['height']):
    #     if word.strip():
    #         words.append(word.strip())
    #         bbox = (x, y, x + w, y + h)
    #         norm_bbox1 = normalize_bbox(bbox, image_width, image_height)
    #         boxes.append(norm_bbox1)
    #         text_box.append(norm_bbox1)
    # draw_block_box_util.draw_block_box_step_list((header_img, data), input_img_type="pil", 
    #         step_list=[{"name": "tesseract_data_to_json", "param": {}},
    #                 {"name": "draw_block_box_xywh2", "param": {"box_color": 2, "iter_save": True}}],
    #         result_map={"folder_path":"__TEST_DATA"})
    # stats_data = {}
    # if text_box:
    #     # 전체 박스 중 최소/최대 좌표 계산
    #     x_min = min(box[0] for box in text_box)
    #     y_min = min(box[1] for box in text_box)
    #     x_max = max(box[2] for box in text_box)
    #     y_max = max(box[3] for box in text_box)

    #     stats_data['text_span_x_min'] = float(x_min)
    #     stats_data['text_span_y_min'] = float(y_min)
    #     stats_data['text_span_x_max'] = float(x_max)
    #     stats_data['text_span_y_max'] = float(y_max)
    #     print(stats_data)
    #     # stats_data['text_span_x_min'] = float(valid_char_boxes[0][0])
    #     # stats_data['text_span_y_min'] = float(valid_char_boxes[0][1])
    #     # stats_data['text_span_x_max'] = float(valid_char_boxes[-1][2])
    #     # stats_data['text_span_y_max'] = float(valid_char_boxes[-1][3])
    # else:
    #     stats_data.update({f'text_span_{coord}': None for coord in ['x_min', 'y_min', 'x_max', 'y_max']})
    
    cv_image = np.array(image)
    if len(cv_image.shape) == 3:
        cv_image = cv2.cvtColor(cv_image, cv2.COLOR_RGB2GRAY)
    _, binary = cv2.threshold(cv_image, 180, 255, cv2.THRESH_BINARY_INV)

    # horizontal_kernel_ratio = 0.8
    # vertical_kernel_ratio = 0.038
    # horizontal_kernel_size = max(1, int(image_width * horizontal_kernel_ratio))
    # horizontal_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (horizontal_kernel_size, 1))
    # detect_horizontal = cv2.morphologyEx(binary, cv2.MORPH_OPEN, horizontal_kernel, iterations=1)
    # detect_horizontal = cv2.morphologyEx(detect_horizontal, cv2.MORPH_CLOSE, horizontal_kernel, iterations=1)

    # vertical_kernel_size = max(1, int(image_height * vertical_kernel_ratio))
    # vertical_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (1, vertical_kernel_size))
    # detect_vertical = cv2.morphologyEx(binary, cv2.MORPH_OPEN, vertical_kernel, iterations=2)

    # # 두 선 검출 결과 합치기
    # combined_lines = cv2.bitwise_or(detect_horizontal, detect_vertical)
    # contours_combined, _ = cv2.findContours(combined_lines, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    # for cnt in contours_combined:
    #     x, y, w, h = cv2.boundingRect(cnt)
    #     bbox = (x, y, x + w, y + h)
    #     norm_bbox = normalize_bbox(bbox, image_width, image_height)
    #     words.append('─' if w> h else '│')
    #     boxes.append(norm_bbox)

    # if iter_save:
    #     inverted_combined = cv2.bitwise_not(combined_lines)
    #     separate_area_util.separate_area_step_list(inverted_combined, data_type='np_bgr', output_type='np_bgr',
    #         step_list=[{"name":"save","param":{"save_key":"_combined_contour","tmp_save":True}}], result_map={"folder_path":"line_export"})


    horizontal_kernel_ratio = 0.8
    vertical_kernel_ratio = 0.04
    horizontal_kernel_size = max(1, int(image_width * horizontal_kernel_ratio))
    horizontal_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (horizontal_kernel_size, 1))
    detect_horizontal = cv2.morphologyEx(binary, cv2.MORPH_OPEN, horizontal_kernel, iterations=1)
    detect_horizontal = cv2.morphologyEx(detect_horizontal, cv2.MORPH_CLOSE, horizontal_kernel, iterations=2)
    contours_h, _ = cv2.findContours(detect_horizontal, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    
    words = []
    boxes = []
    stats_data = {}
    
    for cnt in contours_h:
        x, y, w, h = cv2.boundingRect(cnt)
        bbox = (x, y, x + w, y + h)
        norm_bbox2 = normalize_bbox(bbox, image_width, image_height)
        words.append('─')
        boxes.append(norm_bbox2)
    if iter_save:
        inverted_horizontal = cv2.bitwise_not(detect_horizontal)
        separate_area_util.separate_area_step_list(inverted_horizontal, data_type='np_bgr', output_type='np_bgr',
                step_list=[{"name":"save","param":{"save_key":"_h_contour","tmp_save":True}}], result_map={"folder_path":"line_export"})

    vertical_kernel_size = max(1, int(image_height * vertical_kernel_ratio))
    vertical_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (1, vertical_kernel_size))
    detect_vertical = cv2.morphologyEx(binary, cv2.MORPH_OPEN, vertical_kernel, iterations=2)
    contours_v, _ = cv2.findContours(detect_vertical, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    for cnt in contours_v:
        x, y, w, h = cv2.boundingRect(cnt)
        bbox = (x, y, x + w, y + h)
        norm_bbox3 = normalize_bbox(bbox, image_width, image_height)
        words.append('│')
        boxes.append(norm_bbox3)
    if iter_save:
        inverted_vertical = cv2.bitwise_not(detect_vertical)
        separate_area_util.separate_area_step_list(inverted_vertical, data_type='np_bgr', output_type='np_bgr',
            step_list=[{"name":"save","param":{"save_key":"_v_contour","tmp_save":True}}], result_map={"folder_path":"line_export"})
            
    FILTER_CHARS_FROM_TEXT = "─━│┃┄┅┆┇┈┉┊┋"
    COUNT_HORIZ_CHARS = "─━┄┅"
    COUNT_VERT_CHARS = "│┃┆┇┊┋"

    potential_horiz_line_boxes = []
    potential_vert_line_boxes = []
    horiz_char_count = 0
    vert_char_count = 0
    for i in range(min(len(words), len(boxes))):
        char = words[i]
        box = boxes[i]
        if len(box) != 4:
            continue
        if char in COUNT_HORIZ_CHARS:
            horiz_char_count += 1
            potential_horiz_line_boxes.append(box)
        if char in COUNT_VERT_CHARS:
            vert_char_count += 1
            potential_vert_line_boxes.append(box)

    if potential_horiz_line_boxes:
        all_horiz_x_mins = [b[0] for b in potential_horiz_line_boxes]
        all_horiz_y_mins = [b[1] for b in potential_horiz_line_boxes]
        all_horiz_x_maxs = [b[2] for b in potential_horiz_line_boxes]
        all_horiz_y_maxs = [b[3] for b in potential_horiz_line_boxes]
        stats_data['horiz_span_x_min'] = float(min(all_horiz_x_mins))
        stats_data['horiz_span_y_min'] = float(min(all_horiz_y_mins))
        stats_data['horiz_span_x_max'] = float(max(all_horiz_x_maxs))
        stats_data['horiz_span_y_max'] = float(max(all_horiz_y_maxs))
        horiz_line_boxes_np = np.array(potential_horiz_line_boxes)
        stats_data['horiz_line_x_min_mean'] = float(np.mean(horiz_line_boxes_np[:, 0]))
        stats_data['horiz_line_y_min_mean'] = float(np.mean(horiz_line_boxes_np[:, 1]))
        stats_data['horiz_line_x_max_mean'] = float(np.mean(horiz_line_boxes_np[:, 2]))
        stats_data['horiz_line_y_max_mean'] = float(np.mean(horiz_line_boxes_np[:, 3]))
        stats_data['horiz_line_x_min_std'] = float(np.std(horiz_line_boxes_np[:, 0]))
        stats_data['horiz_line_y_min_std'] = float(np.std(horiz_line_boxes_np[:, 1]))
        stats_data['horiz_line_x_max_std'] = float(np.std(horiz_line_boxes_np[:, 2]))
        stats_data['horiz_line_y_max_std'] = float(np.std(horiz_line_boxes_np[:, 3]))
    else:
        stats_data.update({f'horiz_span_{coord}': None for coord in ['x_min', 'y_min', 'x_max', 'y_max']})
        stats_data.update({f'horiz_line_{coord}_{stat}': None for coord in ['x_min', 'y_min', 'x_max', 'y_max'] for stat in ['mean', 'std']})

    if potential_vert_line_boxes:
        all_vert_x_mins = [b[0] for b in potential_vert_line_boxes]
        all_vert_y_mins = [b[1] for b in potential_vert_line_boxes]
        all_vert_x_maxs = [b[2] for b in potential_vert_line_boxes]
        all_vert_y_maxs = [b[3] for b in potential_vert_line_boxes]
        stats_data['vert_span_x_min'] = float(min(all_vert_x_mins))
        stats_data['vert_span_y_min'] = float(min(all_vert_y_mins))
        stats_data['vert_span_x_max'] = float(max(all_vert_x_maxs))
        stats_data['vert_span_y_max'] = float(max(all_vert_y_maxs))
        vert_line_boxes_np = np.array(potential_vert_line_boxes)
        stats_data['vert_line_x_min_mean'] = float(np.mean(vert_line_boxes_np[:, 0]))
        stats_data['vert_line_y_min_mean'] = float(np.mean(vert_line_boxes_np[:, 1]))
        stats_data['vert_line_x_max_mean'] = float(np.mean(vert_line_boxes_np[:, 2]))
        stats_data['vert_line_y_max_mean'] = float(np.mean(vert_line_boxes_np[:, 3]))
        stats_data['vert_line_x_min_std'] = float(np.std(vert_line_boxes_np[:, 0]))
        stats_data['vert_line_y_min_std'] = float(np.std(vert_line_boxes_np[:, 1]))
        stats_data['vert_line_x_max_std'] = float(np.std(vert_line_boxes_np[:, 2]))
        stats_data['vert_line_y_max_std'] = float(np.std(vert_line_boxes_np[:, 3]))
    else:
        stats_data.update({f'vert_span_{coord}': None for coord in ['x_min', 'y_min', 'x_max', 'y_max']})
        stats_data.update({f'vert_line_{coord}_{stat}': None for coord in ['x_min', 'y_min', 'x_max', 'y_max'] for stat in ['mean', 'std']})
    
    stats_data['horiz_char_count'] = horiz_char_count
    stats_data['vert_char_count'] = vert_char_count 
    
    LINE_ALIGNMENT_TOLERANCE = 5 
    BOX_GAP_TOLERANCE = 10
    horizontal_lengths = []
    vertical_lengths = []
    
    if potential_horiz_line_boxes:
        potential_horiz_line_boxes.sort(key=lambda b: (b[1], b[0]))
        current_line_segment = []
        for box in potential_horiz_line_boxes:
            if not current_line_segment:
                current_line_segment.append(box)
            else:
                prev_box = current_line_segment[-1]
                if abs(box[1] - prev_box[1]) <= LINE_ALIGNMENT_TOLERANCE and \
                    box[0] - prev_box[2] <= BOX_GAP_TOLERANCE and \
                    box[0] >= prev_box[0]: 
                    current_line_segment.append(box)
                else:
                    min_x = min(b[0] for b in current_line_segment)
                    max_x = max(b[2] for b in current_line_segment)
                    horizontal_lengths.append(max_x - min_x)
                    current_line_segment = [box] 
        if current_line_segment:
            min_x = min(b[0] for b in current_line_segment)
            max_x = max(b[2] for b in current_line_segment)
            horizontal_lengths.append(max_x - min_x)

    if potential_vert_line_boxes:
        potential_vert_line_boxes.sort(key=lambda b: (b[0], b[1])) 
        current_line_segment = []
        for box in potential_vert_line_boxes:
            if not current_line_segment:
                current_line_segment.append(box)
            else:
                prev_box = current_line_segment[-1]
                if abs(box[0] - prev_box[0]) <= LINE_ALIGNMENT_TOLERANCE and \
                    box[1] - prev_box[3] <= BOX_GAP_TOLERANCE and \
                    box[1] >= prev_box[1]: 
                    current_line_segment.append(box)
                else:
                    min_y = min(b[1] for b in current_line_segment)
                    max_y = max(b[3] for b in current_line_segment)
                    vertical_lengths.append(max_y - min_y)
                    current_line_segment = [box]
        if current_line_segment:
            min_y = min(b[1] for b in current_line_segment)
            max_y = max(b[3] for b in current_line_segment)
            vertical_lengths.append(max_y - min_y)

    stats_data['avg_horiz_line_length'] = float(np.mean(horizontal_lengths)) if horizontal_lengths else None
    stats_data['std_horiz_line_length'] = float(np.std(horizontal_lengths)) if horizontal_lengths else None
    stats_data['avg_vert_line_length'] = float(np.mean(vertical_lengths)) if vertical_lengths else None
    stats_data['std_vert_line_length'] = float(np.std(vertical_lengths)) if vertical_lengths else None

    row_data = {
        **stats_data
    }
    
    shutil.rmtree(tmp_dir, ignore_errors=True)
    return row_data

def normalize_bbox(bbox, image_width, image_height, image1000=True):
    x1, y1, x2, y2 = bbox
    if image1000:
        x1 = int(1000 * (x1 / image_width))
        y1 = int(1000 * (y1 / image_height))
        x2 = int(1000 * (x2 / image_width))
        y2 = int(1000 * (y2 / image_height))
    return [x1, y1, x2, y2]
