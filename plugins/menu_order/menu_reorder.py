import threading
import time
from airflow.plugins_manager import AirflowPlugin
from airflow.www.app import cached_app
from airflow.trs_loader import trs_label

class MenuReorderPlugin(AirflowPlugin):
    """
    메뉴 순서 재정립을 위한 airflow 플러그인
    """
    name = "menu_reorder_plugin"

    def __init__(self):
        super().__init__()
        self._schedule_menu_reorder()

    def _schedule_menu_reorder(self):
        """
        플러그인 로딩 후 메뉴 재정렬 로직을 지연 실행함.
        AppBuilder 메뉴가 완전 초기화될 수 있도록 보장.
        """
        def reorder():
            time.sleep(2)  # Wait for the appbuilder menu to be fully initialized
            try:
                app = cached_app()
                if app and hasattr(app, 'appbuilder'):
                    # 카테고리 재정렬
                    # 카테고리를 모두 pop해서 원하는대로 정렬
                    menu_dict = {}
                    desired_category_order = [
                        trs_label.get("www_extensions_init_appbuilder_links",{}).get("DAGs_label"),
                        '추출 결과',
                        '서식 관리',
                        trs_label.get("www_extensions_init_views",{}).get("admin_category"), # '시스템 관리'
                        trs_label.get("providers_fab_auth_manager_security_manager_override",{}).get("category"), # '권한 관리',
                        trs_label.get("www_extensions_init_views",{}).get("browse_category"), #'로그 관리',
                    ]
                    category_list = app.appbuilder.menu.menu
                    while len(category_list) > 0:
                        item = category_list.pop()
                        menu_dict[item.label] = item
                        
                    for name in desired_category_order:
                        if name in menu_dict:
                            category_list.append(menu_dict.pop(name))
                        else:
                            print(f"[Menu Reorder] Warning: Menu '{name}' not found.")
                    #남아있는 메뉴 추가
                    for item in menu_dict.values():
                        category_list.append(item)
                    
                    # 로그관리 메뉴 재정렬
                    # 로그관리를 모두 pop해서 원하는대로 정렬
                    menu_dict = {}
                    desired_log_order = [
                        trs_label.get("www_extensions_init_appbuilder_links",{}).get("Cluster Activity_label"), #'프로세스 모니터링',
                        trs_label.get("www_extensions_init_views",{}).get("audit_log_label"), #'사용자 액션 로그',
                        trs_label.get("www_extensions_init_views",{}).get("xcom_label"), #'작업결과 로그',
                        trs_label.get("www_extensions_init_views",{}).get("task_instance_label"), #'작업 로그',
                        trs_label.get("www_extensions_init_views",{}).get("dag_run_label"), #'프로세스 실행 로그',
                        trs_label.get("providers_fab_auth_manager_security_manager_override",{}).get("User_stat_label"), #'사용자 통계'
                    ]
                    log_menu = trs_label.get("www_extensions_init_views",{}).get("browse_category")
                    log_menu_list = app.appbuilder.menu.find(log_menu).childs
                    while len(log_menu_list) > 0:
                        item = log_menu_list.pop()
                        menu_dict[item.label] = item
                    for name in desired_log_order:
                        if name in menu_dict:
                            log_menu_list.append(menu_dict.pop(name))
                        else:
                            print(f"[Menu Reorder] Warning: Menu '{name}' not found.")
                    
                    #남아있는 메뉴 추가
                    for item in menu_dict.values():
                        log_menu_list.append(item)

                    print("==================Menu Reordering Complete=================")
                    print("New menu order:", [item.label for item in app.appbuilder.menu.menu])
                    print("===========================================================")
                    
            except Exception as e:
                print(f"[Menu Reorder] Error: {e}")

        # Start the reordering process in a separate thread.
        threading.Thread(target=reorder, daemon=True).start()