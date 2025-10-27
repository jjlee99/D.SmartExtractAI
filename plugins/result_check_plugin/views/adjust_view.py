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
                static_texts = correct_data.get('고정_텍스트') or correct_data.get('고정 텍스트') or [{}]
                static_texts = static_texts[0] # 1row만 데이터가 있음
                updated_texts = correct_data.get('수정_텍스트') or correct_data.get('수정 텍스트') or [{}]
                updated_texts = updated_texts[0] # 1row만 데이터가 있음
                
                static_text_list = list(static_texts.values())
                updated_text_list = list(updated_texts.values())
                
                insert_list=[]
                for item in static_text_list:
                    param = (item["structed_text"], item["structed_text"], item["section_class_id"], item["section_row"], item["section_col"])
                    result = manage_query_util.select_row_map("selectBlockDictionaryInsertInfo", param) # 미등록 error_text면 등록정보 리턴, 있으면 None
                    if result is not None:
                        tuple = (result["block_class_id"],result["error_text"],result["default_text"],)
                        insert_list.append(tuple)
                for item in updated_text_list:
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
                    param = (item["structed_text"], item["structed_text"], section_class_id, final_section_row, item["section_col"])
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
            result_json = {"status": "success", "imgs":imgs,"data": data}
            json_str = json.dumps(result_json, ensure_ascii=False)
            return Response(json_str, mimetype="application/json") # ajax 리턴
        else:
            result_json = {"status": "error", "message": str("조회 대상이 없습니다.")}
            json_str = json.dumps(result_json, ensure_ascii=False)
            return Response(json_str, mimetype="application/json") # ajax 리턴
        
