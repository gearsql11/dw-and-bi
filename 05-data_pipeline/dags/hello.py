import logging

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils import timezone
from airflow.operators.python import PythonOperator 


def _say_hello():
    logging.debug("This is DEBUG log")
    logging.info("hello")


# with open("hello.text". "w") as f :    คือการเขียนเปิดไฟล์
#     fdfsf
#     fsffs

# print("Hello")

# เปิด DAG จะมี DAG_id , secdule , startdate
with DAG(
    "hello",
    start_date=timezone.datetime(2024 ,3, 23),
    schedule=None,
    tags =["DS525"],
):
    start = EmptyOperator(task_id="start")
# การใช้ BashOperator
    echo_hello = BashOperator(
        task_id = "echo_hello",
        bash_command="echo 'hello'",
    )
# การใช้ pythonOperator
# ถ้ามี วงเล็บ เปิดขปิด แสดงว่า เรากำลังเรียกใช้ function
    say_hello = PythonOperator(
        task_id = "say_hello",
        python_callable =_say_hello,

    )

    end = EmptyOperator(task_id="end")

    start >> echo_hello >> end
    start >> say_hello >> end