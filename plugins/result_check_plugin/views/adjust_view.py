import datetime
import json
import traceback
import unicodedata
from markupsafe import Markup
from pathlib import Path
from dags.utils.com import file_util, json_util
from dags.utils.db import dococr_query_util
from flask_appbuilder import expose, has_access
from flask import current_app, g, redirect, url_for, request, Response, abort, flash, send_from_directory
from flask_babel import lazy_gettext as _, force_locale
from plugins.form_manage_plugin.util.db import manage_query_util
from plugins.result_check_plugin.views.general.form_manage_base_view import FormManageModelView
from dags.utils.img import type_convert_util
from wtforms import validators
class AdjustCheckView(FormManageModelView):
    route_base = "/adjust"  # 기본 경로를 지정
    endpoint = "adjust"  #  엔드포인트 이름을 지정

    list_title = _("교정 정보 목록")
    edit_title = _("교정 정보 수정")

    list_template = "form/adjust/adjust_list.html"
    edit_template = "form/adjust/adjust_edit.html"
    
    # 이 데코레이터는 이 메서드가 웹 경로에 노출되어야 함을 Airflow에게 알려줍니다.
    # '/my_custom_page' 경로로 접근할 수 있게 됩니다.
    label_columns = {
        "doc_name": _("문서명"),
        "filename": _("파일명"),
        "page_num": _("페이지 수"),
        "generated_dt": _("생성시간"),
        "rgdt": _("등록시간"),
        "updt": _("수정시간"),
    }
    type_columns = {
        "doc_name": "string",
        "filename": "string",
        "page_num": "integer",
        "generated_dt": "datetime",
        "rgdt" : "datetime",
        "updt" : "datetime",
    }

    validators_columns = {
        "doc_name": [validators.DataRequired()],
        "filename": [validators.DataRequired()],
        "page_num": [validators.DataRequired()],
        "generated_dt": [validators.DataRequired()],
    }
    default_columns = {
        "doc_name": "",
        "filename": "",
        "page_num": "",
        "generated_dt": "",
    }
    def text_formatter(value):
        if value is not None:
            return Markup('<p>{value}</p>').format(value=value)
        else:
            return Markup('<span class="label label-danger">Invalid</span>')
    formatters_columns = {"doc_name": text_formatter,
                         "filename": text_formatter,
                         }

    #목록정보
    list_columns = ["doc_name", "filename", "page_num", "generated_dt", "rgdt", "updt"]
    search_columns = ["doc_name", "filename", "page_num", "generated_dt", "rgdt", "updt"]


    def __init__(self):
        super().__init__()

    @expose("/list/")
    @has_access
    def list(self):
        # 1. doc_class_view.py의 list 함수에서 사용된 DB 조회 로직을 그대로 가져옴.
        # 페이지/페이지 사이즈/ offset 계산
        modelview_name = self.__class__.__name__ 
        page_arg_name = "page_" + modelview_name  
        page = request.args.get(page_arg_name, 0, type=int) 
        print("페이지 번호 : ",page)

        page_size_arg_name = "psize_" + modelview_name # 'psize_ResultCheckView'
        page_size = request.args.get(page_size_arg_name, self.default_page_size, type=int) # URL 쿼리에서 페이지 크기를 가져오거나, 없으면 기본값 사용

        offset = page * page_size # 건너뛸 로우 수
        # query_params_data = (page_size, offset) 
       # 검색 필드 추가

        # ----------------------------------------------------
        # 🚀 2. 검색 매개변수 추출 및 DB 쿼리 수정
        
        # request.args에서 검색 매개변수만 추출 (예: 'doc_name_like', 'rgdt_equals')
        search_args = {}
        for k, v in request.args.items():
            # 검색 조건은 '_like', '_equals' 등으로 끝난다고 가정
            if any(k.endswith(suffix) for suffix in ['_like', '_equals', '_gt', '_lt', '_in']) and v:
                search_args[k] = v
        
        print("검색 매개변수 : ", search_args)
        
        # 검색 조건 매개변수 값을 추출합니다. (값이 없으면 None 또는 빈 문자열을 사용)
        search_values = []
        for col in self.search_columns:
            field_name_in_url = f"{col}_like"
            value = request.args.get(field_name_in_url, None)

            if value:
                if col in ["doc_name", "filename"]:
                    search_values.append(f"%{value}%")
                elif col == "page_num":
                    search_values.append(value) 
                else:
                    search_values.append(f"%{value}%")
            else:
                search_values.append(None)
        
        query_params_data = tuple(search_values) + (page_size, offset)
        value_columns = manage_query_util.select_result_map("selectCompleteList", params=query_params_data)
        count_params_data = tuple(search_values) 
        count_result = manage_query_util.select_result_map("selectCompleteCount", params=count_params_data)

        actions = {}


        count = 0
        if count_result and isinstance(count_result, list) and count_result[0].get('count') is not None:
            count = count_result[0]['count']

        if value_columns:
            pk_col = list(value_columns[0].keys())[0]  # 첫 번째 dict의 첫 번째 키
            pks = [row[pk_col] for row in value_columns]
        else:
            pk_col = None
            pks = []
        
        self.update_redirect()
        return self.render_template(
            self.list_template,
            appbuilder=self.appbuilder,
            title=self.list_title,
            label_columns=self.label_columns,
            include_columns=self.list_columns,
            formatters_columns=self.formatters_columns,
            value_columns=value_columns,
            search_columns=self.search_columns,
            search_args=search_args,
            page=page,
            page_size=page_size,
            count=count,
            pks=pks,
            actions=actions,
            modelview_name=modelview_name,
        )
    @expose("/edit/<pk>", methods=["GET", "POST"])
    @has_access
    def edit(self, pk):
        if request.method == "POST":
            print("POST method")
            try:
                posted_data = request.json
                if posted_data is None:
                    # 400 오류가 발생하지 않게 하기 위한 방어 코드
                    current_app.logger.error("POST 요청 본문이 비어있거나 JSON 파싱에 실패했습니다.")
                    return Response("JSON request body is missing or invalid.", status=400)
                correct_data = posted_data.get('data', {})
                static_texts = correct_data.get('title_area') or correct_data.get('title area') or [{}]
                static_texts = static_texts[0] # 1row만 데이터가 있음
                updated_texts = correct_data.get('data_area') or correct_data.get('data area') or [{}]
                updated_texts = updated_texts[0] # 1row만 데이터가 있음
                # updated_texts 중복제거
                # manage_query_util.update_map("updateDataArea",(pk,))
                
                static_text_list = list(static_texts.values())
                updated_text_list = list(updated_texts.values())
                
                insert_list=[]
                
                for item in static_text_list:
                    trim_structed_text = item["structed_text"].replace(' ', '')
                    param = (trim_structed_text, trim_structed_text, item["section_class_id"], item["section_row"], item["section_col"])
                    result = manage_query_util.select_row_map("selectBlockDictionaryInsertInfo", param) # 미등록 error_text면 등록정보 리턴, 있으면 None
                    if result is not None:
                        tuple = (result["block_class_id"],result["error_text"],result["default_text"],)
                        insert_list.append(tuple)
                for item in updated_text_list:
                    trim_structed_text = item["structed_text"].replace(' ', '')
                    section_class_id = item["section_class_id"]
                    final_section_row = item["section_row"]
                    repeat_row_info = dococr_query_util.select_row_map("selectMultiRowInfo",(section_class_id,))
                    if repeat_row_info:
                        min_row_num = repeat_row_info.get("minnum",None)
                        max_row_num = repeat_row_info.get("maxnum",None)
                        # 멀티로우 레이아웃인 경우 반복 블록을 계산하여 다시 치환
                        if min_row_num is not None and max_row_num is not None:
                            repeat_row_cnt = max_row_num - min_row_num + 1  # 반복 블록의 행 개수 계산
                            index_num = (item["section_row"] - min_row_num)%repeat_row_cnt  # 현재 행이 반복 블록의 몇번째 행인지 계산
                            final_section_row = index_num + min_row_num  # 실제 검증할 행 번호 계산
                    param = (trim_structed_text, trim_structed_text, section_class_id, final_section_row, item["section_col"])
                    result = manage_query_util.select_row_map("selectBlockDictionaryInsertInfo", param) # 미등록 error_text면 등록정보 리턴, 있으면 None
                    if result is not None:
                        tuple = (result["block_class_id"],result["error_text"],item["default_text"],)
                        insert_list.append(tuple)
                manage_query_util.insert_map("insertBlockDictionary",insert_list)
                
            except Exception as e:
                raise e
            
            # ✅ 수정 후: flash 호출 및 리디렉션 URL 반환
            flash("정상적으로 처리되었습니다.", "success")

            # post_action_redirect()가 반환하는 Response 객체에서 Location 헤더를 추출하여 URL을 얻습니다.
            redirect_response = self.post_action_redirect()
            redirect_url = redirect_response.headers.get('Location', '/adjust/list/') # 기본값 설정

            result_json = {"status": "redirect", "url": redirect_url}
            json_str = json.dumps(result_json, ensure_ascii=False)
            return Response(json_str, mimetype="application/json", status=200)
        
        self.update_redirect()
        return self.render_template(
            self.edit_template,
            title=self.edit_title,
            adjust_id=pk
        )

    # 해당 메소드는 쿼리를 통해 pk를 기준으로 데이터를 json 형태로 반환하고, 이를 AJAX로 받아서 화면에 표시함.
    @expose("/adjust_load/<pk>", methods=["GET"])
    def adjust_load(self,pk):
        item = manage_query_util.select_row_map("selectCompleteErrorMatch", (pk,))
        img_list = manage_query_util.select_result_map("selectCompleteImgList", (pk,))
        if item and img_list:
            imgs = {}
            for img in img_list:
                url = type_convert_util.convert_type(img["file_path"],"file_path","url")
                imgs[img["page_num"]] = url
            data = json.loads(item["error_match"])
            # 🚀 중복 제거 로직: data_area 리스트 내에서 필드 이름 기준으로 중복 제거
            # 즉, 동일한 필드(예: TB_OCR_BILD_BASIC_INFO0BILD_NM)를 가진 항목은 
            # 배열 내에서 가장 먼저 나타나는 항목(가장 최근/최신으로 간주) 하나만 남기고 나머지는 삭제합니다.
            if 'data_area' in data and isinstance(data['data_area'], list):
                # 필드 이름(Key)을 저장할 Set
                seen_fields = set() 
                # 최종 결과를 저장할 리스트
                unique_data_area = []
                
                # 배열을 역순으로 순회하여 나중에 저장된 항목(최신으로 간주)을 우선적으로 처리할 수 있지만,
                # 여기서는 순서대로 순회하며 '첫 번째 발견된 항목'을 유지합니다.
                for d in data['data_area']:
                    # 딕셔너리 d의 모든 키(필드명)를 가져옵니다.
                    field_keys = list(d.keys())
                    
                    is_new = False
                    for key in field_keys:
                        if key not in seen_fields:
                            seen_fields.add(key)
                            is_new = True
                            # 같은 객체 내에 여러 필드가 있을 수 있으므로, 해당 객체의 모든 키를 등록합니다.
                            
                    # 만약 이 객체에 이전에 본 적 없는 필드가 하나라도 있다면, 이 객체를 추가합니다.
                    # 이로 인해 data_area 배열 내에 동일 필드명을 가진 객체는 오직 하나만 남게 됩니다.
                    if is_new:
                        unique_data_area.append(d)
                    else:
                        # 모든 필드가 이미 처리된 객체라면, 중복으로 간주하고 무시합니다.
                        current_app.logger.debug(f"Duplicate data_area element removed based on field key: {field_keys}")
                
                data['data_area'] = unique_data_area
            # 🚀 중복 제거 로직 종료
            result_json = {"status": "success", "imgs":imgs,"data": data}
            json_str = json.dumps(result_json, ensure_ascii=False)
            return Response(json_str, mimetype="application/json") # ajax 리턴
        else:
            result_json = {"status": "error", "message": str("조회 대상이 없습니다.")}
            json_str = json.dumps(result_json, ensure_ascii=False)
            return Response(json_str, mimetype="application/json") # ajax 리턴
        
    @expose("/section_mapping", methods=["GET"])
    @has_access
    def get_column_mapping(self):
        """
        TB_DS_COLUMN 테이블에서 COLUMN_NM과 COLUMN_DESC를 조회하여
        프론트엔드에서 사용할 맵핑 리스트를 JSON 형태로 반환합니다.
        """
        try:
            mapping_list = manage_query_util.select_list_map("selectSectionListAll")

            # 2. 결과가 딕셔너리 리스트 형태인지 확인
            if mapping_list is not None and isinstance(mapping_list, list):
                result_json = {"status": "success", "mapping": mapping_list}
                json_str = json.dumps(result_json, ensure_ascii=False)
                return Response(json_str, mimetype="application/json")
            else:
                current_app.logger.warning("Column mapping data is empty or invalid.")
                result_json = {"status": "error", "message": "컬럼 맵핑 데이터를 찾을 수 없습니다."}
                json_str = json.dumps(result_json, ensure_ascii=False)
                return Response(json_str, mimetype="application/json", status=404)

        except Exception as e:
            current_app.logger.error(f"Error fetching column mapping: {traceback.format_exc()}")
            result_json = {"status": "error", "message": f"서버 오류 발생: {str(e)}"}
            json_str = json.dumps(result_json, ensure_ascii=False)
            return Response(json_str, mimetype="application/json", status=500)
        
