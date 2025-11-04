from flask import request, jsonify, flash
from flask_appbuilder import expose, has_access
from flask_babel import lazy_gettext as _
from markupsafe import Markup
from flask_appbuilder.widgets import FormWidget
from flask import redirect, url_for
from plugins.form_manage_plugin.util.db import manage_query_util
from plugins.form_manage_plugin.views.general.form_manage_base_view import FormManageModelView

class BlockDictionaryManageView(FormManageModelView):
    route_base = "/block_dictionary"

    list_title = _("조건별 교정사전 목록")
    template_base = "form/block_dictionary"
    list_template = template_base + "/block_dictionary_list.html"

    label_columns = {
        "doc_nm": _("문서명"),
        "layout_nm": _("레이아웃명"),
        "section_nm": _("구역명"),
        "block_pos": _("블록 행/열"),
        "error_text": _("교정 대상 텍스트"),
        "crrct_text": _("교정 텍스트"),
    }
    list_columns = ["doc_nm", "layout_nm", "section_nm", "block_pos", "error_text", "crrct_text"]

    def text_formatter(value):
        if value:
            return Markup(f"<p>{value}</p>")
        return Markup('<span class="label label-danger">-</span>')

    formatters_columns = {
        "error_text": text_formatter,
        "crrct_text": text_formatter,
    }

    @expose("/list/")
    @has_access
    def list(self):
        # 검색 파라미터
        doc_id = request.args.get("doc_id")
        layout_id = request.args.get("layout_id")
        section_id = request.args.get("section_id")
        error_text = request.args.get("error_text", "")
        crrct_text = request.args.get("crrct_text", "")

        # 쿼리 실행
        params = {
            "doc_id": doc_id,
            "layout_id": layout_id,
            "section_id": section_id,
            "error_text": f"%{error_text}%",
            "crrct_text": f"%{crrct_text}%"
        }
        datas = manage_query_util.select_list_map("selectBlockDictionaryList", params)

        # 서식, 레이아웃, 섹션 목록 로드
        doc_list = manage_query_util.select_list_map("selectDocList")
        layout_list = manage_query_util.select_list_map("selectLayoutList", (doc_id,)) if doc_id else []
        section_list = manage_query_util.select_list_map("selectSectionList", (layout_id,)) if layout_id else []

       # 페이지네이션 관련 값 세팅
        page = self.default_page_size

        if datas:
            pk_col = list(datas[0].keys())[0]  # 첫 번째 dict의 첫 번째 키
            pks = [row[pk_col] for row in datas]
        else:
            pk_col = None
            pks = []
        count = len(datas)

        return self.render_template(
            self.list_template,
            title=self.list_title,
            can_edit=True,
            can_delete=True,
            appbuilder=self.appbuilder,
            value_columns=datas,
            label_columns=self.label_columns,
            include_columns=self.list_columns,
            formatters_columns=self.formatters_columns,
            doc_list=doc_list,
            layout_list=layout_list,
            section_list=section_list,
            request=request,
            page=page,
            page_size=self.default_page_size,
            count=count,
            pks=pks,
            actions={},
            modelview_name=self.__class__.__name__,
        )

    # AJAX: 특정 서식 → 레이아웃 목록 조회
    @expose("/layouts/<doc_id>")
    @has_access
    def layouts(self, doc_id):
        layout_list = manage_query_util.select_list_map("selectLayoutList", (doc_id,))
        return jsonify(layout_list)

    # AJAX: 특정 레이아웃 → 섹션 목록 조회
    @expose("/sections/<layout_id>")
    @has_access
    def sections(self, layout_id):
        section_list = manage_query_util.select_list_map("selectSectionList", (layout_id,))
        return jsonify(section_list)
    
    # AJAX: 특정 섹션의 → 블록 행/열 목록 조회
    @expose("/blocks/<section_id>")
    @has_access
    def blocks(self, section_id):
        block_list = manage_query_util.select_list_map("selectBlockList", (section_id,))
        return jsonify(block_list)

    @expose("/add", methods=["GET", "POST"])
    @has_access
    def add(self):
        if request.method == "POST":
            try:
                block_id = request.form.get("block_id")
                error_text = request.form.get("error_text")
                crrct_text = request.form.get("crrct_text")

                if not block_id or not error_text or not crrct_text:
                    flash("필수 항목이 누락되었습니다.", "danger")
                    return self.render_add_form()

                # DB insert
                param = (block_id, error_text, crrct_text)
                manage_query_util.insert_map("insertBlockDictionary", param)

                flash("정상적으로 등록되었습니다.", "success")
                return redirect(url_for("BlockDictionaryManageView.list"))
            except Exception as e:
                flash(str(e), "danger")
                return self.render_add_form()

        # GET 요청 시 form 렌더링
        return self.render_add_form()


    def render_add_form(self):
        doc_list = manage_query_util.select_list_map("selectDocList")
        return self.render_template(
            self.template_base + "/block_dictionary_add.html",
            title=_("조건별 교정사전 추가"),
            doc_list=doc_list,
            modelview_name=self.__class__.__name__,
        )
    
    @expose("/edit/<pk>", methods=["GET", "POST"])
    @has_access
    def edit(self, pk):
        self.update_redirect()
        crctn_id = int(pk)

        # GET 요청: 수정 폼 렌더링
        if request.method == "GET":
            data = manage_query_util.select_one_map_block("selectBlockDictionaryById", (crctn_id,))
            if not data:
                flash("데이터를 찾을 수 없습니다.", "danger")
                return redirect(url_for("BlockDictionaryManageView.list"))

            return self.render_template(
                self.template_base + "/block_dictionary_edit.html",
                title=_("교정사전 수정"),
                data=data,
                modelview_name=self.__class__.__name__,
            )

        # POST 요청: 수정 처리
        try:
            error_text = request.form.get("error_text")
            crrct_text = request.form.get("crrct_text")

            if not error_text or not crrct_text:
                flash("필수 항목이 누락되었습니다.", "danger")
                return redirect(url_for("BlockDictionaryManageView.edit", pk=crctn_id))

            param = (error_text, crrct_text, crctn_id)
            manage_query_util.update_map("updateBlockDictionary", param)

            flash("정상적으로 수정되었습니다.", "success")
            return redirect(url_for("BlockDictionaryManageView.list"))
        except Exception as e:
            flash(str(e), "danger")
            return redirect(url_for("BlockDictionaryManageView.edit", pk=crctn_id))
        
    @expose("/delete/<pk>", methods=["POST"])
    @has_access
    def delete(self, pk):
        crctn_id = int(pk)

        try:
            # 데이터 존재 여부 확인
            data = manage_query_util.select_one_map_block("selectBlockDictionaryById", (crctn_id,))
            if not data:
                flash("삭제할 데이터를 찾을 수 없습니다.", "danger")
                return redirect(url_for("BlockDictionaryManageView.list"))

            # 실제 삭제 쿼리 실행
            manage_query_util.delete_map("deleteBlockDictionary", (crctn_id,))

            flash("정상적으로 삭제되었습니다.", "success")
            return redirect(url_for("BlockDictionaryManageView.list"))

        except Exception as e:
            flash(str(e), "danger")
            return redirect(url_for("BlockDictionaryManageView.list"))