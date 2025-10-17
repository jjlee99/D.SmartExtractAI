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

class DocClassManageView(FormManageModelView):
    route_base = "/doc"
    endpoint = "doc"   

    # 타이틀
    list_title = _("문서서식 목록")
    show_title = _("문서서식 상세")
    add_title = _("문서서식 추가")
    edit_title = _("문서서식 수정")

    #템플릿 경로
    list_template = "form/doc/doc_list.html"
    show_template = "form/doc/doc_show.html"
    add_template = "form/doc/doc_add.html"
    edit_template = "form/doc/doc_edit.html"

    #공통정보
    label_columns = {
        "doc_name": _("문서서식명"),
        "use_yn": _("사용여부"),
        "rgdt": _("생성일"),
        "updt": _("수정일"),
    }
    description_columns = {
        "doc_name": _("구별을 위한 문서서식명을 입력하세요."),
        "use_yn": _("문서서식 사용여부"),}
    type_columns = {
        "doc_name": "string",
        "use_yn": "string",
        "rgdt": "datetime",
        "updt": "datetime",
    }
    validators_columns = {
        "doc_name": [validators.DataRequired()]
    }
    default_columns = {
        "doc_name": "",
        "use_yn": "N",
    }
    def text_formatter(value):
        if value is not None:
            return Markup('<p>{value}</p>').format(value=value)
        else:
            return Markup('<span class="label label-danger">Invalid</span>')
    formatters_columns = {"doc_name": text_formatter}
    
    
    #목록정보
    list_columns = ["doc_name", "rgdt", "updt"]
    search_columns = ["doc_name","rgdt","updt"]

    #상세 폼 정보
    show_columns = ["doc_name", "use_yn","rgdt", "updt"]
    #show_fieldsets = [(_("문서서식 정보"), {"fields": ["doc_name"]},)] 주석추가테스트
    
    
    #등록 폼 정보
    add_columns = ["doc_name"]
    #add_fieldsets = [(_("문서서식 정보"), {"fields": ["doc_name"]},)]
    add_exclude_cols = [] # 추가 시 입력불가 필드 목록
    
    #수정 폼 정보
    edit_columns = ["doc_name", "use_yn"]
    #edit_fieldsets = [(_("문서서식 정보"), {"fields": ["doc_name"]},)]
    edit_exclude_cols = [] # 수정 시 수정불가 필드 목록

     # 검색 가능한 컬럼
    search_columns = ['doc_name', 'rgdt', 'updt']
    
    
    def __init__(self):
        super().__init__()
        if not self.add_form:
            self.add_form = self.create_form(
                self.label_columns,
                self.add_columns,
                self.description_columns,
                self.validators_columns,
                self.type_columns,
                self.default_columns,
            )
        if not self.edit_form:
            self.edit_form = self.create_form(
                self.label_columns,
                self.edit_columns,
                self.description_columns,
                self.validators_columns,
                self.type_columns,
                self.default_columns,
            )

    @expose("/list/")
    @has_access
    def list(self):
        doc_name_like = request.args.get("doc_name_like", "")
        rgdt_like = request.args.get("rgdt_like")
        updt_like = request.args.get("updt_like")

        # None이면 SQL의 IS NULL 조건에 맞게 처리되도록
        doc_name_val = f"%{doc_name_like}%" if doc_name_like else None
        rgdt_val = f"%{rgdt_like}%" if rgdt_like else None
        updt_val = f"%{updt_like}%" if updt_like else None

        # selectDocClassList는 각 필드당 (%s IS NULL OR ... LIKE %s) 형태이므로 총 6개 파라미터 필요
        filters = (
            doc_name_val, doc_name_val,   # DOC_NM용 (%s, %s)
            rgdt_val, rgdt_val,           # RGDT용 (%s, %s)
            updt_val, updt_val            # UPDT용 (%s, %s)
        )
        
        value_columns = manage_query_util.select_list_map("selectDocClassList", filters)
        actions = {} # 체크박스 관련 기능 정의
        
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
            value_columns=value_columns, #실제값
            page=page,
            page_size=self.default_page_size,
            count=count,
            pks=pks,
            actions=actions,
            modelview_name=self.__class__.__name__,
            search_columns=self.search_columns  # 템플릿에서 사용
        )
    
    @expose("/add", methods=["GET", "POST"])
    @has_access
    def add(self):
        is_valid_form = True
        form = self.add_form.refresh()
        
        if request.method == "POST":
            if form.validate():
                self.process_form(form, True)
                item = {}

                try:
                    for key, value in form.data.items():
                        item[key] = value
                    self.pre_add(item)
                except Exception as e:
                    flash(str(e), "danger")
                else:
                    param = (item["doc_name"],)
                    manage_query_util.insert_map("insertDocClass",param) # update 쿼리 실행
                    self.post_add(item)
                    flash("정상적으로 처리되었습니다.", "success")
                finally:
                    return self.post_action_redirect() # 수정 후 이전 화면으로 이동
            else:
                is_valid_form = False
        
        widgets = {}
        widgets["add"] = FormWidget(
            route_base=self.route_base,
            form=form,
            include_cols=self.add_columns,
            exclude_cols=self.add_exclude_cols,
            #fieldsets=self.add_fieldsets,
        )
        if is_valid_form:
            self.update_redirect()
        if not widgets:
            return self.post_action_redirect()
        else:
            return self.render_template(
                self.add_template, 
                title=self.add_title, 
                widgets=widgets
            )

    @expose("/show/<pk>", methods=["GET"])
    @has_access
    def show(self, pk):
        item = manage_query_util.select_row_map("selectDocClass",(pk,))
        if not item:
            abort(404)
        self.prefill_show(item)
        
        widgets = {}
        actions = {}
        value_columns = [item[col] for col in self.show_columns]
        widgets["show"] = ShowWidget(
            pk=pk,
            label_columns=self.label_columns,
            include_columns=self.show_columns,
            value_columns=value_columns,
            formatters_columns=self.formatters_columns,
            actions=actions,
            #fieldsets=self.show_fieldsets,
            modelview_name=self.__class__.__name__,
        )
        self.update_redirect() # post_action_redirect를 위한 기록
        return self.render_template(
            self.show_template,
            pk=pk,
            title=self.show_title,
            widgets=widgets,
        )
    
    @expose("/edit/<pk>", methods=["GET", "POST"])
    @has_access
    def edit(self, pk):
        """
        Edit view.
        Override it to use a custom ``has_access_with_pk`` decorator to take into consideration resource for
        fined-grained access.
        """
        is_valid_form = True
        
        item = manage_query_util.select_row_map("selectDocClass",(pk,))
        if request.method == "POST":
            print("POST method")
            form = self.edit_form.refresh(obj=request.form)
            # 수정 예외 컬럼은 db값 그대로 update하기 위한 처리
            for filter_key in self.edit_exclude_cols:
                form[filter_key].data = item.get(filter_key)
            # trick to pass unique validation
            form._id = pk
            if form.validate():
                self.process_form(form, False) # form 정보를 item에 넣기 전에 추가 처리(보통은 안 함)
                try:
                    for key, value in form.data.items():
                        item[key] = value
                    self.pre_update(item) # item 정보를 DB에 넣기 전에 추가 처리
                except Exception as e:
                    flash(str(e), "danger")
                else:
                    param = (item["doc_name"],item["doc_class_id"],)
                    manage_query_util.update_map("updateDocClass",param) # update 쿼리 실행
                    self.post_update(item) # item 정보를 DB에 넣은 후에 추가 처리
                    flash("정상적으로 처리되었습니다.", "success")
                finally:
                    return self.post_action_redirect() # 수정 후 이전 화면으로 이동
            else:
                is_valid_form = False
        else:
            print("GET method")
            print(f"edit item : {item}")
            # Only force form refresh for select cascade events
            form = self.edit_form.refresh(data=item)
            # Perform additional actions to pre-fill the edit form.
            self.prefill_form(form, pk) # item 정보를 form에 넣기 전에 추가 처리

        widgets = {}
        widgets["edit"] = FormWidget(
            route_base=self.route_base,
            form=form,
            include_cols=self.edit_columns,
            exclude_cols=self.edit_exclude_cols,
            #fieldsets=self.edit_fieldsets,
        )
        if is_valid_form:
            self.update_redirect()
        if not widgets:
            return self.post_action_redirect()
        else:
            return self.render_template(
                self.edit_template,
                title=self.edit_title,
                widgets=widgets,
            )
    
    
    @expose("/delete/<pk>", methods=["POST"])
    @has_access
    def delete(self, pk):
        item = manage_query_util.select_row_map("selectDocClass",(pk,))
        if not item:
            abort(404)
        try:
            self.pre_delete(item)
        except Exception as e:
            flash(str(e), "danger")
        else:
            param = (item["doc_class_id"],)
            manage_query_util.delete_map("deleteDocClass",param) # update 쿼리 실행
            self.post_delete(item)
            flash("정상적으로 처리되었습니다.", "success")
            self.update_redirect()
        return self.post_action_redirect()
    
    def pre_delete(self, item):
        doc_class_id = item.get("doc_class_id")
        result = manage_query_util.check_map("checkDelDocClass",(doc_class_id,))
        if result > 0:
            raise Exception("레이아웃이 존재하는 문서템플릿은 삭제할 수 없습니다.")
        

    @expose("/download/<string:filename>")
    @has_access
    def download(self, filename):
        pass
        # return send_file(
        #     op.join(self.appbuilder.app.config["UPLOAD_FOLDER"], filename),
        #     download_name=uuid_originalname(filename),
        #     as_attachment=True,
        # )