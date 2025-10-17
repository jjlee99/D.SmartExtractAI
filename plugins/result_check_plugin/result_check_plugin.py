# -*- coding: utf-8 -*-
from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint
from flask_appbuilder import BaseView, expose
from plugins.result_check_plugin.views.check_view import ResultCheckView


# 2. Airflow 메뉴에 추가될 뷰를 정의하는 리스트입니다.
# 딕셔너리 형태로 name(메뉴명), category(카테고리명), view(뷰 클래스)를 지정합니다.
v_appbuilder_views = [
    {
        "name": "추출 결과", 
        # "category": "결과 조회", 
        "view": ResultCheckView()
    }
]

# 3. Airflow 플러그인을 정의하는 클래스입니다.
# Airflow는 이 클래스를 읽어 `appbuilder_views`에 정의된 뷰를 메뉴에 추가합니다.
class CustomMenuPlugin(AirflowPlugin):
    name = "custom_menu_plugin"
    appbuilder_views = v_appbuilder_views
