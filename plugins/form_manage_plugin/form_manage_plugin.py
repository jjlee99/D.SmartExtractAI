import os
from datetime import datetime
from airflow.plugins_manager import AirflowPlugin
from airflow.models import Variable
from flask import send_from_directory, abort, Blueprint, request, jsonify
from plugins.form_manage_plugin.views.block_dictionary_class_view import BlockDictionaryManageView
from plugins.form_manage_plugin.views.dictionary_class_view import DictionaryClassManageView
from plugins.form_manage_plugin.views.doc_class_view import DocClassManageView
from plugins.form_manage_plugin.views.layout_class_view import LayoutClassManageView
from plugins.form_manage_plugin.views.section_class_view import SectionClassManageView
from airflow.www.app import cached_app

DATA_FOLDER = Variable.get("DATA_FOLDER", default_var="/opt/airflow/data")

external_static_path = DATA_FOLDER  # 외부 이미지 폴더 절대경로 _ 
external_static_bp = Blueprint('exstatic', __name__)
download_bp = Blueprint('download_bp', __name__)
upload_bp = Blueprint('upload_bp', __name__)

@external_static_bp.route('/exstatic/<path:filename>')
def serve_external_static(filename):
    try:
        return send_from_directory(external_static_path, filename)
    except FileNotFoundError:
        abort(404)

@download_bp.route('/download/<string:filename>')
def download_file(filename):
    try:
        return send_from_directory(external_static_path, filename, as_attachment=True, mimetype=None)
    except FileNotFoundError:
        abort(404)

# 업로드 처리 (POST)
@upload_bp.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return "파일이 없습니다.", 400
    file = request.files['file']
    if file.filename == '':
        return "파일이 선택되지 않았습니다.", 400
    filename = file.filename
    
    upload_type = request.form.get('upload_type', 'class')
    if upload_type == 'temp':
        save_folder = os.path.join(DATA_FOLDER, 'temp', datetime.now().strftime('%Y%m%d%H%M%S'))
    if upload_type == 'layout_template':
        layout_class_id = request.form.get('layout_class_id')
        doc_class_id = request.form.get('doc_class_id')
        save_folder = os.path.join(DATA_FOLDER, 'class', doc_class_id, layout_class_id,"samlpe")
    else:
        raise ValueError("알 수 없는 업로드 타입입니다.")
   
    os.makedirs(save_folder, exist_ok=True)
    save_path = os.path.join(save_folder, filename)

    file.save(save_path)
    
    return jsonify({
        "message": f"파일이 성공적으로 업로드 되었습니다: {filename}",
        "file_path": save_path
    })

v_flask_blueprints = [
    external_static_bp,
    download_bp
]
v_appbuilder_views = [
    {"name": "문서서식 관리","category": "서식 관리","view": DocClassManageView()},
    {"name": "레이아웃 관리","category": "서식 관리","view": LayoutClassManageView()},
    {"name": "작업구역 관리","category": "서식 관리","view": SectionClassManageView()},
    {"name": "교정사전 관리","category": "서식 관리","view": DictionaryClassManageView()},
    {"name": "조건별 교정사전 관리","category": "서식 관리","view": BlockDictionaryManageView()},
    ## {"name": "데이터 조회","category": "문서서식 관리","view": DocClassManageView()}
]

# 플러그인 정의
class FormManagePlugin(AirflowPlugin):
    name = "form_manage_plugin"
    appbuilder_views = v_appbuilder_views
    flask_blueprints = v_flask_blueprints