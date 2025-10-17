# from airflow import DAG
# from airflow.operators.python import PythonOperator

# def create_dag(dag_id, schedule, default_args, task_configs):
#     """
#     팩토리 패턴을 활용하여 DAG을 생성하는 함수
#     """
#     dag = DAG(dag_id, schedule_interval=schedule, default_args=default_args)
    
#     with dag:
#         for task_id, task_callable in task_configs.items():
#             PythonOperator(
#                 task_id=task_id,
#                 python_callable=task_callable,
#                 dag=dag
#             )
    
#     return dag

from airflow.models.dag import DAG
from airflow.decorators import task, task_group # Airflow 데코레이터 임포트

# task_definitions에서 태스크 정의를 가져옵니다.
# (task_definitions.py에서 get_task_definition 함수를 정의했다고 가정합니다.)
# from .task_definitions import get_task_definitions 

def create_dag(dag_id, schedule, default_args, task_definition_func, doc_class_id, doc_info_records=None):
    """
    동적 TaskFlow API 태스크 정의를 기반으로 DAG를 생성합니다.
    
    :param task_definition_func: 태스크 정의 로직을 포함하고 있는 함수
    :param doc_class_id: 현재 생성할 DAG에 전달할 문서 클래스 ID
    :return: 생성된 Airflow DAG 객체
    """
    
    with DAG(
        dag_id=dag_id, 
        schedule=schedule, 
        default_args=default_args, 
        catchup=False,
        tags=['dynamic', 'image_processing', f'doc_class_{doc_class_id}']
    ) as dag:
        
        # task_definition_func을 호출하여 TaskFlow API 기반의 태스크 인스턴스와
        # 의존성 관계를 DAG 컨텍스트 내에서 생성합니다.
        # task_definition_func은 이 호출을 통해 모든 PythonOperator 인스턴스(데코레이터 태스크)를 생성하고 
        # 의존성을 설정하는 역할을 수행합니다.
        task_definition_func(
            doc_class_id=doc_class_id,
            doc_info_records=doc_info_records)
        
    return dag

# 참고: task_definition_func이 task_definitions.py에 정의되어 있어야 합니다.