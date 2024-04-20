from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils import timezone

# with open("hello.text". "w") as f :    คือการเขียนเปิดไฟล์
#     fdfsf
#     fsffs

# print("Hello")

# เปิด DAG จะมี DAG_id , secdule , startdate
with DAG(
    "my_first_DAGs",
    start_date=timezone.datetime(2024 ,3, 23),
    schedule=None,
    tags =["DS525"],
):
    my_first_task = EmptyOperator(task_id="my_first_task")
    my_second_task = EmptyOperator(task_id="my_second_task")

    my_first_task >> my_second_task
