# Airflow 공식 Docker 이미지를 기반으로 합니다.
# Python 3.11을 명시하려면 -python3.11 태그를 사용할 수 있습니다.
#FROM apache/airflow:2.10.5-python3.11
FROM apache/airflow:2.10.5

# 시스템 패키지 설치 추가
USER root
RUN apt-get update && \
    apt-get install -y libgl1-mesa-glx libglib2.0-0 \
    tesseract-ocr \
    tesseract-ocr-kor \
    libtesseract-dev \
    libleptonica-dev \
    pkg-config \
    git gcc g++ \
    poppler-utils \
    && apt-get clean
    

# requirements.txt 파일을 컨테이너 내부로 복사
COPY requirements.txt /requirements.txt

# entrypoint 설정
COPY entrypoint.sh /entrypoint.sh
COPY variables.json /variables.json
COPY pools.json /pools.json
COPY connections.list /connections.list
RUN chmod +x /entrypoint.sh

USER airflow

# pip cache를 비활성화하고 업그레이드하여 종속성 설치
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt

# 작업 디렉토리 설정
WORKDIR /opt/airflow

# COPY translate/custom_airflow/. /home/airflow/.local/lib/python3.12/site-packages/airflow/
# COPY translate/custom_flask_appbuilder/. /home/airflow/.local/lib/python3.12/site-packages/flask_appbuilder/

ENTRYPOINT ["/entrypoint.sh"]
