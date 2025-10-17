import json, os
from pathlib import Path
import numpy as np
import chardet
from typing import Union, Any, List

import logging

from utils.com import file_util
logger = logging.getLogger(__name__)

def load(file_path:str)->str:
    try:
        # 파일을 바이너리 모드로 열어 인코딩 감지
        with open(file_path, 'rb') as file:
            raw_data = file.read()
            detected = chardet.detect(raw_data)
            encoding = detected['encoding']
            confidence = detected['confidence'] * 100
            language = detected.get('language') # language가 없을 수도 있음
        message = f"파일 '{file_path}'의 인코딩은 {confidence:.2f}%의 신뢰성으로 {encoding}입니다."
        if language:
            message += f"(언어:{language})"
        logger.info(message)
        
        # 감지된 인코딩으로 파일 읽기
        with open(file_path, 'r', encoding=encoding) as file:
            content = file.read()
        logger.info(f"'{os.path.abspath(file_path)}' 파일에서 JSON 데이터를 로드하였습니다.")
        return json.loads(content)
    except FileNotFoundError:
        logger.error(f"오류: '{os.path.abspath(file_path)}' 파일을 찾을 수 없습니다.")
    except UnicodeDecodeError:
        logger.error(f"오류: '{os.path.abspath(file_path)}' 파일을 디코딩할 수 없습니다.")
    except Exception as e:
        logger.critical(f"예상치 못한 오류가 발생했습니다: {str(e)}")
    return None

#json데이터 저장
def save(file_path:str, data:Any, duplicate_policy:str="exists_bakup")->None:
    """
    JSON 데이터를 지정된 파일 경로에 저장하는 함수

    :param file_path: 저장할 파일의 경로 (문자열)
    :param json_data: 저장할 JSON 데이터 (딕셔너리 또는 리스트)
    :param duplicate_policy: 중복 발생 시 처리(exists_bakup|overwrite|skip)
    """
    try:
        if os.path.exists(file_path):
            if duplicate_policy == "exists_bakup": # 기존 파일 백업 후 생성
                backup_path = file_path + ".bak"
                file_util.file_copy(file_path, backup_path) 
            elif duplicate_policy == "exists_bakup": # 기존 파일 백업 후 생성
                dest = Path(file_path)
                stem = dest.stem
                suffix = dest.suffix
                count = 1
                while dest.exists():
                    dest = dest.parent / f"{stem}({count}){suffix}"
                    count += 1
            elif duplicate_policy == "skip": # 기존 파일 리턴
                return file_path
            elif duplicate_policy == "overwrite": # 기존 파일 무시하고 덮어씀
                pass
        json_data = to_json_data(data)
        class NpEncoder(json.JSONEncoder):
            def default(self, obj):
                if isinstance(obj, np.integer):  # np.int32, np.int64 → Python int
                    return int(obj)
                if isinstance(obj, np.floating):  # np.float32, np.float64 → Python float
                    return float(obj)
                if isinstance(obj, np.ndarray):  # Numpy 배열 → Python list
                    return obj.tolist()
                return super().default(obj)
        
        with open(file_path, 'w', encoding='utf-8') as file:
            json.dump(json_data, file, ensure_ascii=False, indent=4, cls=NpEncoder)
        logger.info(f"JSON 데이터가 성공적으로 '{os.path.abspath(file_path)}'에 저장되었습니다.")
    except FileNotFoundError:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        file_path = save(file_path, data)
    except IOError as e:
        logger.error(f"파일 저장 중 오류가 발생했습니다: {e}")
        file_path = None
    except json.JSONDecodeError as e:
        logger.error(f"JSON 인코딩 중 오류가 발생했습니다: {e}")
        file_path = None
    except Exception as e:
        logger.critical(f"예상치 못한 오류가 발생했습니다: {e}")
        file_path = None
    return file_path

def to_json_text(data:Any) -> str:
    class NpEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, np.integer):  # np.int32, np.int64 → Python int
                return int(obj)
            if isinstance(obj, np.floating):  # np.float32, np.float64 → Python float
                return float(obj)
            if isinstance(obj, np.ndarray):  # Numpy 배열 → Python list
                return obj.tolist()
            return super().default(obj)
    if isinstance(data, (dict, list)):
        return json.dumps(data, ensure_ascii=False, indent=4, cls=NpEncoder)
    elif hasattr(data, 'to_dict'):  # DataFrame 또는 Series 객체 처리
        df_dict = data.to_dict(orient='records')
        return json.dumps(data, ensure_ascii=False, indent=4, cls=NpEncoder)
    elif isinstance(data, str):
        try:
            json.loads(data)
        except json.JSONDecodeError:
            logger.error("json으로 변환할 수 없는 문자열", json)
            return ""
        return data
    else:
        logger.error("json으로 변환할 수 없는 타입", type(data))
        return ""

def to_json_data(data:Any)->Union[List,dict]:
    if isinstance(data, (dict, list)):
        return data
    elif hasattr(data, 'to_dict'):  # DataFrame 또는 Series 객체 처리
        return data.to_dict(orient='records')
    elif isinstance(data, str):
        try:
            json_data = json.loads(data)
        except json.JSONDecodeError:
            logger.error("json으로 변환할 수 없는 문자열", json)
            return None
        return json_data
    else:
        logger.error("json으로 변환할 수 없는 타입", type(data))
        return None  # 또는 빈 DataFrame 반환

