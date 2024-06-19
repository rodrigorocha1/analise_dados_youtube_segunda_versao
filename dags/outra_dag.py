from airflow.operators.empty import EmptyOperator
from airflow.models import DAG
from airflow.utils.task_group import TaskGroup
import pendulum

dag = DAG(
    dag_id='task_dinamica',
    schedule_interval=None,
    catchup=False,
    start_date=pendulum.datetime(2023, 9, 8, tz='America/Sao_Paulo')
)

task_inicio = EmptyOperator(
    task_id='task_inicio',
    dag=dag
)
result = 1


# def choose_branch(result):
#     if result > 0.5:
#         return ['task_a', 'task_b']
#     return ['task_c']


# branching = BranchPythonOperator(
#     task_id='branching',
#     python_callable=choose_branch,
#     op_args=[result],
#     dag=dag
# )

task_fim = EmptyOperator(
    task_id='task_fim',
    dag=dag
)

task_inicio  >> task_fim
