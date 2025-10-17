from deep_translator import GoogleTranslator, PapagoTranslator
from langdetect import detect as _detect, DetectorFactory

# 결과 일관성을 위해 seed 고정
DetectorFactory.seed = 0

def is_korean(text: str) -> bool:
    """
    입력된 텍스트가 한국어인지 감지합니다.
    Args:
        text (str): 판별할 텍스트
    Returns:
        bool: 한국어이면 True, 아니면 False
    """
    try:
        lang = _detect(text)
        return lang == "ko"
    except Exception:
        return False

# 결과 일관성을 위해 seed 고정
DetectorFactory.seed = 0

def detect_lang(text: str) -> str:
    """
    입력된 텍스트의 언어코드를 감지하여 반환합니다.
    
    Args:
        text (str): 감지할 텍스트
        
    Returns:
        str: 감지된 언어코드 (예: 'ko', 'en', 'fr'), 실패 시 'unknown'
    """
    try:
        lang = _detect(text)
        return convert_lang(lang)
    except Exception:
        return ""

def translate(origin_text: str, from_lang: str = "auto", to_lang: str = "ko") -> str:
    """
    GoogleTranslator를 이용해 텍스트를 번역합니다.

    Args:
        origin_text (str): 번역할 원본 텍스트
        from_lang (str): 원본 언어 코드('auto' 사용 시 자동 감지)
        to_lang (str): 번역할 목표 언어 코드

    Returns:
        str: 번역된 텍스트. 실패 시 에러 메시지 반환.
    """
    try:
        result = GoogleTranslator(source=from_lang, target=to_lang).translate(origin_text)
        #result = PapagoTranslator(source=from_lang, target=to_lang).translate(origin_text)
        return result
    except Exception as e:
        logger.error(f"번역 실패: {e}")
        return "Error: 번역에 실패했습니다. 잠시 후 다시 시도해 주세요."

def convert_lang(lang: str) -> str:
    """
    EasyOCR 언어 코드를 구글 번역 언어 코드로 변환합니다.

    Args:
        lang (str): EasyOCR에서 사용하는 언어 코드(예: 'ko', 'en', 'zh_sim' 등).

    Returns:
        str: 구글 번역에서 사용할 언어 코드. 미지원 언어는 빈 문자열 반환.
    """
    LANGUAGE_CODE_MAPPING = {
        # 2자리 영문
        "en": "en",        # 영어
        "ko": "ko",        # 한국어
        "zh_sim": "zh",    # 중국어 간체
        "zh_tra": "zh-TW", # 중국어 번체
        "ja": "ja",        # 일본어
        "es": "es",        # 스페인어
        "fr": "fr",        # 프랑스어
        "de": "de",        # 독일어
        "ru": "ru",        # 러시아어
        "ar": "ar",        # 아랍어
        "pt": "pt",        # 포르투갈어
        "bo": "",          # 티베트어 (미지원)
        "mn": "",          # 몽골어 (미지원)
        "vi": "vi",        # 베트남어
        "ug": "",          # 위구르어 (미지원)
        "th": "th",        # 태국어
        "tl": "fil",       # 타갈로그어
        "hi": "hi",        # 힌디어
        "bn": "bn",        # 벵골어
        "ur": "ur",         # 우르두어
        # 3자리 영문
        "eng": "en", 
        "kor": "ko", 
        "zho": "zh", 
        "chi": "zh", 
        "jpn": "ja", 
        "spa": "es",
        "fra": "fr", 
        "fre": "fr", 
        "deu": "de", 
        "ger": "de", 
        "rus": "ru", 
        "ara": "ar",
        "por": "pt", 
        "bod": "", 
        "tib": "", 
        "mon": "", 
        "vie": "vi", 
        "uig": "", 
        "tha": "th",
        "tgl": "fil", 
        "fil": "fil", 
        "hin": "hi", 
        "ben": "bn", 
        "urd": "ur",
        # 한글
        "영어": "en",        # 영어
        "한국어": "ko",        # 한국어
        "중국어": "zh",    # 중국어 간체
        "중국어 번체": "zh-TW", # 중국어 번체
        "일본어": "ja",        # 일본어
        "스페인어": "es",        # 스페인어
        "프랑스어": "fr",        # 프랑스어
        "독일어": "de",        # 독일어
        "러시아어": "ru",        # 러시아어
        "아랍어": "ar",        # 아랍어
        "포르투갈어": "pt",        # 포르투갈어
        "티베트어": "",          # 티베트어 (미지원)
        "몽골어": "",          # 몽골어 (미지원)
        "베트남어": "vi",        # 베트남어
        "위구르어": "",          # 위구르어 (미지원)
        "태국어": "th",        # 태국어
        "타갈로그어": "fil",       # 타갈로그어
        "필리핀어": "fil",       # 타갈로그어
        "힌디어": "hi",        # 힌디어
        "벵골어": "bn",        # 벵골어
        "우르두어": "ur"         # 우르두어
    }
    return LANGUAGE_CODE_MAPPING.get(lang, "")

###############Util 공통함수###############
import logging

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    print(translate("Welcome to Natural Language Processing It is one of the most exciting research areas as of today We will see how Python can be used to work with text files. "))