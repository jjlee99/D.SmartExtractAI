from typing import Any

from markupsafe import Markup
from flask import request, flash, abort
from flask_appbuilder import expose, has_access
from flask_appbuilder.widgets import FormWidget
from flask_babel import lazy_gettext as _
from plugins.form_manage_plugin.util.db import manage_query_util
from plugins.form_manage_plugin.util.db.manage_query_util import delete_map, update_map
from dags.utils.db.dococr_query_util import select_list_map, select_row_map
from plugins.form_manage_plugin.views.general.form_manage_base_view import FormManageModelView
from wtforms import validators

class DictionaryClassManageView(FormManageModelView):
    route_base = "/dictionary"

    #타이틀
    list_title = _("교정사전 목록")
    edit_title = _("교정단어 수정")
    add_title = _("교정단어 추가")

    #템플릿 경로
    template_base = "form/dictionary"
    list_template = template_base + "/dictionary_list.html"
    edit_template = template_base + "/dictionary_edit.html"
    add_template = template_base + "/dictionary_add.html"

    label_columns = {
        "error_text": _("교정 대상 텍스트"),
        "crrct_text": _("교정 텍스트"),
    }
    add_columns = ["error_text", "crrct_text"]
    edit_columns = ["error_text", "crrct_text"]
    description_columns = {
        "error_text": _("오탈자를 입력해주세요."),
        "crrct_text": _("교정 단어를 입력해주세요."),
    }
    validators_columns = {
        "error_text": [validators.DataRequired()],
        "error_text": [validators.DataRequired()]
    }
    type_columns = {
        "error_text": "string",
        "crrct_text": "string",
    }
    default_columns = {
        "error_text": "",
        "crrct_text": "",
    }
    add_exclude_cols = []
    edit_exclude_cols = [] # 수정 시 수정불가 필드 목록
    #목록정보
    list_columns = ["error_text", "crrct_text"]

    def text_formatter(value):
        if value is not None:
            return Markup('<p>{value}</p>').format(value=value)
        else:
            return Markup('<span class="label label-danger">Invalid</span>')
    formatters_columns = {"error_text": text_formatter}
    

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
        # 전체 목록
        search_text = request.args.get("search_text", "") # 검색어
        datas = manage_query_util.select_list_map("selectDictionaryList", (f"%{search_text}%", f"%{search_text}%"))

        page = self.default_page_size

        if datas:
            pk_col = list(datas[0].keys())[0]  # 첫 번째 dict의 첫 번째 키
            pks = [row[pk_col] for row in datas]
        else:
            pk_col = None
            pks = []
        count = len(datas)
        
        self.update_redirect() # /list/가 'cud'액션 처리 후 이동하는 기준점이 되게함
        return self.render_template(
            self.list_template,
            appbuilder=self.appbuilder,
            title=self.list_title,
            value_columns=datas,
            label_columns=self.label_columns,
            include_columns=self.list_columns,
            formatters_columns=self.formatters_columns,
            page=page,
            page_size=self.default_page_size,
            count=count,
            pks=pks,
            actions={},
            modelview_name=self.__class__.__name__,
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
                    param = (item["error_text"],item["crrct_text"],)
                    manage_query_util.insert_map("insertDictionary",param) # update 쿼리 실행
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


    @expose("/edit/<pk>", methods=["GET", "POST"])
    @has_access
    def edit(self, pk):
        is_valid_form = True

        item = manage_query_util.select_row_map("selectDictionaryRow",(pk,))
        if request.method == "POST":
            form = self.edit_form.refresh(obj=request.form)
            # 수정 예외 컬럼은 db값 그대로 update하기 위한 처리
            for filter_key in self.edit_exclude_cols:
                form[filter_key].data = item.get(filter_key)
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
                    param = (item["error_text"],item["crrct_text"],item["common_crctn_id"])
                    manage_query_util.update_map("updateDictionary",param) # update 쿼리 실행
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
                row=item,
            )

    @expose("/delete/<pk>", methods=["POST"])
    @has_access
    def delete(self, pk):
        item = manage_query_util.select_row_map("selectDictionaryRow",(pk,))
        if not item:
            abort(404)
        try:
            self.pre_delete(item)
        except Exception as e:
            flash(str(e), "danger")
        else:
            param = (item["common_crctn_id"],)
            manage_query_util.delete_map("deleteDictionary",param) # update 쿼리 실행
            self.post_delete(item)
            flash("정상적으로 처리되었습니다.", "success")
            self.update_redirect()
        return self.post_action_redirect()
    
    def pre_delete(self, item):
        pass