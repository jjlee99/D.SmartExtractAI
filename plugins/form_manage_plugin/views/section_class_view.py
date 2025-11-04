import traceback
from typing import Any

from markupsafe import Markup
from flask import current_app, g, redirect, url_for, request, abort, flash
from flask_appbuilder import expose, has_access
from flask_appbuilder.widgets import FormWidget, SearchWidget, ShowWidget
from flask_appbuilder.actions import action
from flask_appbuilder.utils.base import get_safe_redirect, lazy_formatter_gettext
from flask_appbuilder.models.sqla.interface import SQLAInterface
from flask_appbuilder.urltools import get_order_args, get_page_args, get_page_size_args
from flask_babel import lazy_gettext as _, force_locale
from sqlalchemy import inspect, text
from werkzeug.wrappers import Response as WerkzeugResponse
from plugins.form_manage_plugin.views.general.form_manage_base_view import FormManageModelView
from plugins.form_manage_plugin.util.db import manage_query_util
from wtforms import validators

class SectionClassManageView(FormManageModelView):
    route_base = "/section"
    endpoint = "section"
    
    # 타이틀
    list_title = _("구역 목록")
    show_title = _("구역 상세")
    add_title = _("구역 추가")
    edit_title = _("구역 수정")

    #템플릿 경로
    list_template = "form/section/section_list.html"
    show_template = "form/section/section_show.html"
    add_template = "form/section/section_add.html"
    edit_template = "form/section/section_edit.html"

    #공통정보
    label_columns = {
        "section_name": _("구역명"),
        "doc_class_id": _("문서명ID"),
        "section_type": _("구역 타입"),
        "section_desc": _("설명"),
        "section_order": _("순서"),
        "separate_section_info": _("구역 설정 정보"),
        "separate_block_info": _("구역 블록화 정보"),
        "ocr_info": _("OCR 정보"),
        "cleansing_info": _("정제 정보"),
        "structuring_info": _("구조화 정보"),
        "rgdt": _("생성일"),
        "updt": _("수정일"),
        "doc_class_info": _("문서명"),
        "layout_class_info": _("레이아웃명"),
        "block_type": _("구역 블록타입"),
    }
    def text_formatter(value):
        if value is not None:
            return Markup('{value}').format(value=value)
        else:
            return Markup('<span class="label label-danger">Invalid</span>')
    formatters_columns = {"section_name": text_formatter}
    
    #입력(등록/수정) 폼 정보
    description_columns = {"section_name": _("구별을 위한 구역명을 입력하세요."),}
    type_columns = {
        "section_name": "string",
        "doc_class_id": "string",
        "section_type": "string",
        "section_desc": "string",
        "section_order": "integer",
        "separate_section_info": "text",
        "separate_block_info": "text",
        "ocr_info": "text",
        "cleansing_info": "text",
        "structuring_info": "text",
        "rgdt": "datetime",
        "updt": "datetime",
        "doc_class_info": "string",
        "layout_class_info": "string",
        "block_type": "string",
    }
    validators_columns = {
        "section_name": [validators.DataRequired()]
    }
    default_columns = {
        "section_name": "",
    }
    
    #목록정보
    list_columns = ["doc_class_info", "layout_class_info", "section_name", "section_type", "section_order", "rgdt", "updt"]
    search_columns = ["doc_class_info", "layout_class_info", "section_name", "section_type", "rgdt", "updt"]

    show_columns = ["section_class_id","layout_class_id","section_name","section_type","section_desc","section_order","separate_section_info","separate_block_info","ocr_info","cleansing_info","structuring_info","rgdt","updt"]
    add_columns = ["section_class_id","layout_class_id","section_name","section_type","section_desc","section_order","separate_section_info","separate_block_info","ocr_info","cleansing_info","structuring_info","rgdt","updt"]
    edit_columns = ["section_class_id","section_name","section_type","section_desc","section_order","separate_section_info","separate_block_info","ocr_info","cleansing_info","structuring_info"]
    
    add_exclude_cols = [] # 추가 시 입력불가 필드 목록
    edit_exclude_cols = [] # 수정 시 수정불가 필드 목록
    
    
    @expose("/list/")
    @has_access
    def list(self):
        print("section list")
         # 검색 파라미터 받기
        doc_class_info_like = request.args.get("doc_class_info_like", "")
        layout_class_info_like = request.args.get("layout_class_info_like", "")
        section_name_like = request.args.get("section_name_like", "")
        section_type_like = request.args.get("section_type_like", "")
        rgdt_like = request.args.get("rgdt_like", "")
        updt_like = request.args.get("updt_like", "")

        filters = {
            "doc_class_info": f"%{doc_class_info_like}%" if doc_class_info_like else None,
            "layout_class_info": f"%{layout_class_info_like}%" if layout_class_info_like else None,
            "section_name": f"%{section_name_like}%" if section_name_like else None,
            "section_type": f"%{section_type_like}%" if section_type_like else None,
            "rgdt": f"%{rgdt_like}%" if rgdt_like else None,
            "updt": f"%{updt_like}%" if updt_like else None,
        }

        value_columns = manage_query_util.select_list_map("selectSectionClassList", filters)
        actions = {}  # 체크박스 관련 기능 정의

        page = self.default_page_size

        if value_columns:
            pk_col = list(value_columns[0].keys())[0]  # 첫 번째 dict의 첫 번째 키
            pks = [row[pk_col] for row in value_columns]
        else:
            pk_col = None
            pks = []
        count = len(value_columns)

        self.update_redirect()
        return self.render_template(
            self.list_template,
            appbuilder=self.appbuilder,
            title=self.list_title,
            label_columns=self.label_columns,
            include_columns=self.list_columns,
            formatters_columns=self.formatters_columns,
            value_columns=value_columns,  # 실제값
            page=page,
            page_size=self.default_page_size,
            count=count,
            pks=pks,
            actions=actions,
            modelview_name=self.__class__.__name__,
            search_columns=self.search_columns
        )

    @expose("/add", methods=["GET", "POST"])
    @has_access
    def add(self):
        flash("구역의 추가는 레이아웃 설정에서 가능합니다.", "warning")
        return redirect("/layout/list")

    @expose("/show/<pk>", methods=["GET"])
    @has_access
    def show(self, pk):
        item = manage_query_util.select_row_map("selectSectionClass", (pk,))
        if not item:
            abort(404)
        self.prefill_show(item)
        self.update_redirect()
        return self.render_template(
            self.show_template,
            title=self.show_title,
            item=item,
            section_type_list=manage_query_util.select_list_map("selectCodeList",("sect_type",)),
        )

    @expose("/edit/<pk>", methods=["GET", "POST"])
    @has_access
    def edit(self, pk):
        if request.method == "POST":
            print("POST method")
            try:
                item={}
                for key, value in request.form.items():
                    item[key] = value
                self.pre_update(item)
            except Exception as e:
                print("에러 메시지:", e)
                traceback.print_exc()
            else:
                param = (
                    item["section_name"],
                    item["section_type"],
                    item.get("section_order", 0),
                    item["separate_section_info"],
                    item["separate_block_info"],
                    item["ocr_info"],
                    item["cleansing_info"],
                    item["structuring_info"],
                    item["section_class_id"],
                )
                manage_query_util.update_map("updateSectionClass", param)  # update 쿼리 실행
                flash("정상적으로 처리되었습니다.", "success")
                return self.post_action_redirect()
        item = manage_query_util.select_row_map("selectSectionClass", (pk,))
        if not item:
            abort(404)
        self.update_redirect()
        return self.render_template(
            self.edit_template,
            title=self.edit_title,
            item=item,
            section_type_list=manage_query_util.select_list_map("selectCodeList",("sect_type",)),
        )

    @expose("/delete/<pk>", methods=["POST"])
    @has_access
    def delete(self, pk):
        item = manage_query_util.select_row_map("selectSectionClass", (pk,))
        if not item:
            abort(404)
        try:
            # 구역은 삭제 전 체크 필요없음
            pass
        except Exception as e:
            print("에러 메시지:", e)
            traceback.print_exc()
        else:
            param = (item["section_class_id"],)
            manage_query_util.delete_map("deleteSectionClass", param)  # delete 쿼리 실행
            flash("정상적으로 처리되었습니다.", "success")
            self.update_redirect()
        return self.post_action_redirect()
        
