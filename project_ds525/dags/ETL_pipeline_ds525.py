# import logging

# from airflow import DAG
# from airflow.operators.bash import BashOperator
# from airflow.operators.empty import EmptyOperator
# from airflow.utils import timezone
# from airflow.operators.python import PythonOperator 


# def _say_hello():
#     logging.debug("This is DEBUG log")
#     logging.info("hello")


# # with open("hello.text". "w") as f :    คือการเขียนเปิดไฟล์
# #     fdfsf
# #     fsffs

# # print("Hello")

# # เปิด DAG จะมี DAG_id , secdule , startdate
# with DAG(
#     "hello",
#     start_date=timezone.datetime(2024 ,3, 23),
#     schedule=None,
#     tags =["DS525"],
# ):
#     start = EmptyOperator(task_id="start")
# # การใช้ BashOperator
#     echo_hello = BashOperator(
#         task_id = "echo_hello",
#         bash_command="echo 'hello'",
#     )
# # การใช้ pythonOperator
# # ถ้ามี วงเล็บ เปิดขปิด แสดงว่า เรากำลังเรียกใช้ function
#     say_hello = PythonOperator(
#         task_id = "say_hello",
#         python_callable =_say_hello,

#     )

#     end = EmptyOperator(task_id="end")

#     start >> echo_hello >> end
#     start >> say_hello >> end






from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils import timezone


def _query():
    hook = PostgresHook(postgres_conn_id="my_neon_conn")
    conn = hook.get_conn()
    cur = conn.cursor()

    sql = "select * from chickens"
    cur.execute(sql)
    rows = cur.fetchall()
    for each in rows:
        print(each)


with DAG(
    "ETL_pipeline_ds525",
    start_date=timezone.datetime(2024, 4, 20),
    schedule=None,
):

    query = PythonOperator(
        task_id="query",
        python_callable=_query,
    )

    rename_columns = PostgresOperator(
        task_id="rename_columns",
        postgres_conn_id="my_neon_conn",
        sql="""
            create or replace view staging_chickens
            as
                select
                    year
                    , province
                    , value as number_of_chickens
                from chickens
          """,
    )

    summarize = PostgresOperator(
        task_id="summarize",
        postgres_conn_id="my_neon_conn",
        sql="""
            create or replace view chicken_summary
            as
                select
                    year
                    , avg(number_of_chickens) as avg_number_of_chickens
                from staging_chickens
                group by year
          """,
    )

    query >> rename_columns >> summarize