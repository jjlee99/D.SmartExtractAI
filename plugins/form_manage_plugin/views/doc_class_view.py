import json
from typing import Any
from pathlib import Path
from markupsafe import Markup
from dags.utils.com import file_util
from flask import current_app, g, redirect, url_for, request, abort, flash, jsonify
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
from wtforms.fields import SelectField
from flask_appbuilder.fieldwidgets import Select2Widget # 선택 사항: 드롭다운 UI 개선
from airflow.models import Variable

CLASS_FOLDER = Variable.get("CLASS_FOLDER", default_var="/opt/airflow/data/class")
DAGS_FOLDER = Variable.get("DAGS_FOLDER", default_var="/opt/airflow/dags")

class DocClassManageView(FormManageModelView):
    route_base = "/doc"
    endpoint = "doc"   

    # 타이틀
    list_title = _("문서 목록")
    show_title = _("문서 상세")
    add_title = _("문서 추가")
    edit_title = _("문서 수정")

    #템플릿 경로
    list_template = "form/doc/doc_list.html"
    show_template = "form/doc/doc_show.html"
    add_template = "form/doc/doc_add.html"
    edit_template = "form/doc/doc_edit.html"

    #공통정보
    label_columns = {
        "doc_name": _("문서명"),
        "use_yn": _("사용여부"),
        "rgdt": _("생성일"),
        "updt": _("수정일"),
    }
    description_columns = {
        "doc_name": _("구별을 위한 문서명을 입력하세요."),
        "use_yn": _("문서 사용여부"),}
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
    #show_fieldsets = [(_("문서 정보"), {"fields": ["doc_name"]},)] 주석추가테스트
    
    
    #등록 폼 정보
    add_columns = ["doc_name"]
    #add_fieldsets = [(_("문서 정보"), {"fields": ["doc_name"]},)]
    add_exclude_cols = [] # 추가 시 입력불가 필드 목록
    
    #수정 폼 정보
    edit_columns = ["doc_name", "use_yn"]
    #edit_fieldsets = [(_("문서 정보"), {"fields": ["doc_name"]},)]
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
        
        # 💡 핵심 수정: edit_form이 생성된 후 use_yn 필드를 SelectField로 오버라이드
        # 이 방법은 상위 create_form의 시그니처를 건드리지 않습니다.
        if hasattr(self.edit_form, 'use_yn'):
            # 기존 필드를 삭제하고
            delattr(self.edit_form, 'use_yn')
            
            # SelectField 인스턴스를 생성하여 추가합니다.
            setattr(self.edit_form, 'use_yn', SelectField(
                label=self.label_columns.get("use_yn", _("사용여부")),
                description=self.description_columns.get("use_yn", _("문서 사용여부")),
                choices=[
                    ("Y", "Y (사용)"),
                    ("N", "N (미사용)"),
                ],
                widget=Select2Widget() # Select2Widget을 임포트했다면 주석 해제하여 사용 가능
            ))
            
        # add_form에도 use_yn 필드가 있고 SelectField로 만들고 싶다면:
        if hasattr(self.add_form, 'use_yn'):
            # 'use_yn'이 add_columns에 포함되어 있지 않다면 이 블록은 무시됩니다.
            # 현재 코드에서 add_columns에는 use_yn이 없으므로 필요하지 않을 가능성이 높습니다.
            pass

        

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
    

   # --- 기존 _validate_doc_config 메서드는 내부 유틸리티로 그대로 유지 ---
    def _validate_doc_config(self, doc_class_id: int):
        """
        문서서식 ID를 받아 하위 엔티티 존재, 필수 Config, 그리고 JSON 추출 값 유효성을 검증합니다.
        (기존 로직 그대로 유지)
        """
        validation_messages = []
        
        # 1. Layout 목록 조회 (USE_YN='Y'인 것만)
        try:
            layouts = manage_query_util.select_list_map("selectLayoutSectionValidationList", (doc_class_id,)) 
        except Exception as e:
            validation_messages.append(_("DB 조회 중 오류가 발생했습니다: %(error)s", error=str(e)))
            return {"isValid": False, "message": "\n".join(validation_messages)}

        # 활성화된 Layout이 전혀 없는 경우
        if not layouts:
            validation_messages.append(_("사용여부가 활성화(USE_YN='Y')된 레이아웃이 존재하지 않습니다. 문서를 활성화할 수 없습니다."))
        
        for layout in layouts:
            layout_nm = layout.get('layout_nm', 'N/A')
            layout_class_id = layout.get('layout_class_id', -1)            

            # 1-1. Layout 필수 INFO 컬럼 누락 체크
            if not layout.get('img_preprocess_info') or not str(layout['img_preprocess_info']).strip():
                validation_messages.append(_("레이아웃 '%(nm)s'의 필수 정보(IMG_PREPROCESS_INFO)가 누락되었습니다.", nm=layout_nm))
            if not layout.get('classify_ai_info') or not str(layout['classify_ai_info']).strip():
                validation_messages.append(_("레이아웃 '%(nm)s'의 필수 정보(CLASSIFY_AI_INFO)가 누락되었습니다.", nm=layout_nm))

            # 1-2. CLASSIFY_AI_INFO에서 추출된 AI Model Dir 경로 체크
            ai_model_dir = layout.get('ai_model_dir')
            if not ai_model_dir or not str(ai_model_dir).strip():
                ai_model_dir = str(Path(CLASS_FOLDER)/str(doc_class_id)/str(layout_class_id)/"classify"/"model")
            try:
                path = Path(ai_model_dir)
                if not path.exists():
                    validation_messages.append(_("레이아웃 '%(nm)s'의 AI 모델이 존재하지 않습니다: %(path)s", nm=layout_nm, path=template_path))
                else:
                    count = sum(1 for _ in path.iterdir())
                    if count == 0:
                        validation_messages.append(_("레이아웃 '%(nm)s'의 AI 모델이 존재하지 않습니다: %(path)s", nm=layout_nm, path=template_path))
            except Exception as e:
                validation_messages.append(_("레이아웃 '%(nm)s'의 AI 모델 경로 확인 중 오류 발생: %(error)s", nm=layout_nm, error=str(e)))

            # 1-3. Section 존재 여부 체크
            if not layout.get('section_cnt'):
                validation_messages.append(_("레이아웃 '%(nm)s'에 연결된 구역이 존재하지 않습니다. 구역을 등록해야 합니다.", nm=layout_nm))
                continue
            
            # 1-4. TEMPLATE_FILE_PATH 체크
            template_path = layout.get('template_file_path')
            if template_path and str(template_path).strip():
                try:
                    if not Path(template_path).is_file():
                        validation_messages.append(_("레이아웃 '%(nm)s'의 템플릿 파일이 존재하지 않거나 경로가 잘못되었습니다: %(path)s", nm=layout_nm, path=template_path))
                except Exception as e:
                    validation_messages.append(_("레이아웃 '%(nm)s' 템플릿 파일 경로 확인 중 오류 발생: %(error)s", nm=layout_nm, error=str(e)))

            # 1-5. 마스킹 파일 체크
            mask_file_path = Path(CLASS_FOLDER)/str(doc_class_id)/str(layout_class_id)/"classify"/"asset"/"mask_image.png"
            if not mask_file_path.exists():
                validation_messages.append(_("레이아웃 '%(nm)s'의 마스킹 이미지 파일이 존재하지 않습니다. 경로: %(path)s", nm=layout_nm, path=str(mask_file_path)))

        # 2. 최종 결과 반환
        if validation_messages:
            stringified_messages = [str(msg) for msg in validation_messages]
            return {
                "isValid": False, 
                "message": str(_("문서 활성화 전 유효성 검증 오류가 발견되었습니다. 그래도 진행하시겠습니까?")),
                "details": "\n".join(stringified_messages) 
            }
        else:
            return {"isValid": True}

    # --- 1. 유효성 검증 전용 엔드포인트 추가 ---
    @expose("/validate/<pk>", methods=["POST"])
    @has_access
    def validate_doc_config(self, pk):
        """
        클라이언트에서 AJAX로 호출하여 유효성 검증만 수행하고 컨펌 요청을 반환하는 엔드포인트
        """
        data_source = request.json
        if not data_source:
             return jsonify({"isValid": False, "message": str(_("잘못된 요청입니다: 데이터가 누락되었습니다."))}), 400

        # use_yn이 'Y'인지 확인
        if data_source.get('use_yn') == 'Y':
            # 내부 검증 함수 호출
            validation_data = self._validate_doc_config(pk)

            if not validation_data.get('isValid'):
                # 유효성 검증 실패 -> 컨펌 요청 JSON 반환
                return jsonify({
                    "isValid": False,
                    "needsConfirmation": True,
                    "confirmationMessage": str(validation_data.get('message', _("설정 유효성 검증에 실패했습니다."))),
                    "details": str(validation_data.get('details', _("상세 오류 없음"))),
                    "pk": pk 
                })
            else:
                # 유효성 검증 성공 -> 성공 JSON 반환
                return jsonify({"isValid": True, "message": str(_("유효성 검증에 성공했습니다."))})
        
        # 'use_yn'이 'Y'가 아니면, 검증을 건너뛰고 성공 처리
        return jsonify({"isValid": True, "message": str(_("사용여부가 'Y'가 아니므로 검증을 건너뜁니다."))})
    
    
    
    @expose("/edit/<pk>", methods=["GET", "POST"])
    @has_access
    def edit(self, pk):
        """
        Edit view. 유효성 검증은 /validate/<pk>에서 처리되며, 이 함수는 순수하게 데이터 저장만 처리합니다.
        """
        is_valid_form = True
        item = manage_query_util.select_row_map("selectDocClass",(pk,))
        if not item:
            abort(404)
            
        if request.method == "POST":
            # 폼 데이터 처리 
            data_source = request.json if request.is_json else request.form
            
            # 💡 강제 진행 플래그는 여기서 사용하지 않습니다. (이미 JS에서 컨펌하고 저장 요청 시 경고 메시지는 무시됨)
            
            form = self.edit_form.refresh(data=data_source)
            
            for filter_key in self.edit_exclude_cols:
                form[filter_key].data = item.get(filter_key)
            form._id = pk
            
            if form.validate():
                self.process_form(form, False)
                try:
                    for key, value in form.data.items():
                        item[key] = value
                    self.pre_update(item)
                    doc_class_id = item["doc_class_id"]
                    doc_name = item["doc_name"]
                    use_yn = item["use_yn"]
                    # DB 저장 실행
                    param = (doc_name,use_yn,doc_class_id)
                    manage_query_util.update_map("updateDocClass",param) 

                    dag_id = f"dococr_{doc_class_id}"
                    dag_file_path = f"{DAGS_FOLDER}/{dag_id}.py"
                    if use_yn == 'Y':
                        template_path = r"data/common/template/img_classify_dag.py"
                        dag_name = f"{doc_name}<{doc_class_id}> 데이터 추출 프로세스"
                        with open(template_path, "r", encoding="utf-8") as f:
                            template_code = f.read()
                        dag_code = template_code.format(dag_id=dag_id, dag_name=dag_name, doc_class_id=doc_class_id, tag="")
                        with open(dag_file_path, "w", encoding="utf-8") as f:
                            f.write(dag_code)
                    else:
                        file_util.del_file(dag_file_path)

                    self.post_update(item)
                    flash("정상적으로 처리되었습니다.", "success")
                
                    # 저장 후 JSON 응답을 통해 리다이렉트 URL 반환
                    redirect_url = self.get_redirect()
                    return jsonify({"isValid": True, "redirect": redirect_url})
                
                except Exception as e:
                    flash(str(e), "danger")
                    return jsonify({"isValid": False, "message": str(e)}), 500
                    
            else:
                is_valid_form = False
                # 폼 유효성 검증 실패 시 JSON 응답 반환
                return jsonify({"isValid": False, "message": _("폼 입력 유효성 검증에 실패했습니다.")}), 400

        else:
            # GET method (렌더링)
            form = self.edit_form.refresh(data=item)
            self.prefill_form(form, pk)

        # GET 요청 렌더링 로직 (템플릿에 pk를 전달)
        widgets = {}
        widgets["edit"] = FormWidget(
            route_base=self.route_base,
            form=form,
            include_cols=self.edit_columns,
            exclude_cols=self.edit_exclude_cols,
            form_id='edit'
        )
        if is_valid_form:
            self.update_redirect()
        if not widgets:
            return self.post_action_redirect()
        else:
            return self.render_template(
                self.edit_template,
                pk=pk, 
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
        return redirect(url_for("DocClassManageView.list"))
        
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