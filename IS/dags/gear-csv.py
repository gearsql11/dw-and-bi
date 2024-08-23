import json
import glob
import os
import requests
import logging
import pickle
import csv
import pandas as pd
import shutil

from airflow import DAG
from airflow.utils import timezone
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from datetime import datetime , timedelta
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from typing import List


data_path = '/opt/airflow/dags/data_csv'
data_folder = data_path + '/' # สร้าง folder สำหรับเก็บข้อมูลโดยการเชื่อม data_path
clean_folder = data_folder + 'cleaned'   #สร้าง folder cleaned ภายใต้โฟลเดอร์หลักสำหรับเก็บข้อมูล data_folder


def _get_files():
    if not os.path.exists(data_path):   #ตรวจสอบว่าโฟลเดอร์ที่ระบุใน data_path มีอยู่หรือไม่ ถ้าไม่มีจะสร้างโฟลเดอร์นั้นขึ้นมา
        os.makedirs(data_path)       
    exit_file_name = os.listdir(data_folder) #อ่านรายชื่อไฟล์ทั้งหมดที่มีอยู่ในโฟลเดอร์ data_folder และเก็บไว้ในตัวแปร exit_file_name
    url = 'https://opendata.onde.go.th/dataset/14-pm-25'
    links = []
    req = requests.get(url, verify=False)
    req.encoding = 'utf-8'
    soup = BeautifulSoup(req.text, 'html.parser') #ส่งคำขอไปยังเว็บไซต์ที่ระบุใน url และใช้ BeautifulSoup เพื่อแปลง HTML ที่ได้รับเป็นโครงสร้าง
    # print(soup.prettify())
    og = soup.find('meta', property='og:url')
    base = urlparse(url)
    for link in soup.find_all('a'):
        current_link = link.get('href')
        if str(current_link).endswith('json'):
            links.append(current_link) #ค้นหาลิงก์ทั้งหมดในหน้าเว็บ แล้วตรวจสอบว่าลิงก์ไหนที่ลงท้ายด้วย .json ถ้าพบจะเก็บลิงก์นั้นไว้ในลิสต์ links
    for link in links:
        names = link.split('/')[-1]
        names = names.strip()
        name = names.replace('pm_data_hourly-', '')
        if (name != 'data_dictionary.csv') & (name not in exit_file_name):
            req = requests.get(link, verify=False)
            url_content = req.content
            file_p = data_folder + name
            json_file = open(file_p, 'wb')
            json_file.write(url_content)
            json_file.close() #แยกชื่อไฟล์จากลิงก์ และตรวจสอบว่าชื่อไฟล์ไม่ใช่ data_dictionary.csv และไม่มีอยู่ในโฟลเดอร์แล้ว
                                # ถ้าเงื่อนไขผ่านจะส่งคำขอไปยังลิงก์เพื่อดาวน์โหลดไฟล์
                                # เขียนเนื้อหาของไฟล์ลงในไฟล์ใหม่ในโฟลเดอร์ data_folder
def _process_files():
    if not os.path.exists(data_path):
        os.makedirs(data_path)

    for filename in os.listdir(data_folder):
        if filename.endswith(".json"):
            json_file_path = os.path.join(data_folder, filename)
            csv_file_path = os.path.join(clean_folder, filename.replace('.json', '.csv'))
            
            # Read the JSON file and convert it to a CSV
            with open(json_file_path, 'r') as f:
                data = pd.read_json(f)
                
            data.to_csv(csv_file_path, index=False)

            # Read the CSV file back into a pandas DataFrame for further processing
            structured_data = pd.read_csv(csv_file_path)

            # Perform any further structuring on the DataFrame if needed
            # structured_data = ...

            # Overwrite the CSV file with the structured data
            structured_data.to_csv(csv_file_path, index=False)

            # Define the destination path in the data_test folder
            dest_file_path = os.path.join(data_path, filename.replace('.json', '.csv'))

            # Move the file to the data_test folder, replacing any existing file
            shutil.move(csv_file_path, dest_file_path)

            # Delete the original JSON file
            os.remove(json_file_path)

def _clean_df():
    if not os.path.exists(clean_folder): #ตรวจสอบว่าโฟลเดอร์ที่ระบุใน clean_folder มีอยู่หรือไม่ ถ้าไม่มีจะสร้างโฟลเดอร์นั้นขึ้นมา
        os.makedirs(clean_folder)
    column_names = ['station_id', 'name_th', 'name_en', 'area_th', 'area_en',
       'station_type', 'lat', 'long', 'date', 'time', 'pm25_color_id',
       'pm25_aqi', 'pm25_value', 'province', 'datetime']
    df_all = pd.DataFrame(columns=column_names) #กำหนดชื่อคอลัมน์ที่ต้องการให้กับ DataFrame df_all ที่จะใช้รวมข้อมูลจากทุกไฟล์
    for name in os.listdir(data_folder): #ตรวจสอบทุกไฟล์ในโฟลเดอร์ data_folder ถ้าเป็นไฟล์ .csv จะอ่านไฟล์นั้นเข้ามาใน DataFrame df
        if name.endswith('.csv'):
            path = data_folder + name
            df = pd.read_csv(path)
            area_en = []
            for area in df['area_en']: #สร้างลิสต์ area_en เพื่อเก็บข้อมูล province โดยใช้ค่าจากคอลัมน์ area_en โดยแยกชื่อจังหวัดออกมาจากข้อมูลเดิม
                area = area.strip().split(',')[-1].strip()
                area_en.append(area)
            df['province'] = area_en #เพิ่มคอลัมน์ province เข้าไปใน DataFrame df
            df['datetime'] = pd.to_datetime(df['date'] + ' ' + df['time']) #สร้างคอลัมน์ datetime โดยรวมค่าจากคอลัมน์ date และ time
            df_all = pd.concat([df_all, df], axis=0, ignore_index=True) #รวมข้อมูลจาก DataFrame df เข้ากับ DataFrame df_all
            df_all.drop_duplicates(inplace=True) #ลบข้อมูลที่ซ้ำกันออกจาก DataFrame df_all
            df_all.to_csv(clean_folder + '/all_data.csv') #บันทึก DataFrame df_all ที่ได้ลงในไฟล์ all_data.csv ในโฟลเดอร์ clean_folder

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    'gear-csv',
    default_args=default_args,
    start_date = timezone.datetime(2024, 5, 3),
    schedule = None,   #'@hourly'
    tags = ['IS','swu'],
) as dag:

    start = EmptyOperator(task_id="start", dag = dag,)

    get_files = PythonOperator(
        task_id = 'get_files',
        python_callable = _get_files,
    )

    process_files = PythonOperator(
        task_id = 'process_files',
        python_callable = _process_files,
    )

    gcs_path = 'pm25_raw/'
    bucket_name = 'is-project-gear' # bucket name on GCS
    csv_files = [file for file in os.listdir(data_folder) if file.endswith('.csv')]
    path = []
    for csv_file in csv_files:
        path.append(data_folder + csv_file)

    uploadFiles_to_gcs = LocalFilesystemToGCSOperator(
        task_id = 'upload_to_gcs',
        src = path,
        dst = gcs_path,  # Destination file in the bucket
        bucket = bucket_name,  # Bucket name
        gcp_conn_id = 'my_gcp_conn',  # Google Cloud connection id
        mime_type = 'text/csv',
    )

    clean_df = PythonOperator(
        task_id = 'clean_df',
        python_callable = _clean_df,
    ) 

    empty = EmptyOperator(task_id='empty')

    uploadFiles_clean_to_gcs = LocalFilesystemToGCSOperator(
        task_id = 'upload_clean_to_gcs',
        src = [clean_folder + '/all_data.csv'],
        dst = 'pm25_cleaned/',
        bucket = bucket_name,
        gcp_conn_id = 'my_gcp_conn',
        mime_type = 'text/csv',
    )
    
    create_dataset_on_bq = BigQueryCreateEmptyDatasetOperator(
        task_id = 'create_dataset',
        dataset_id = 'capstone_aqgs',
        gcp_conn_id = 'my_gcp_conn',
    )

    gcs_to_bq_pm25 = GCSToBigQueryOperator(
        task_id = 'gcs_to_bq_pm25',
        bucket = bucket_name,
        source_objects = ['pm25_cleaned/all_data.csv'],
        destination_project_dataset_table = 'capstone_aqgs.pm25_transaction',
        create_disposition = 'CREATE_IF_NEEDED',
        write_disposition = 'WRITE_TRUNCATE',
        gcp_conn_id = 'my_gcp_conn',
    )

    end = EmptyOperator(task_id = 'end')

    start >> get_files >> process_files >> [uploadFiles_to_gcs,clean_df] >> empty >> [uploadFiles_clean_to_gcs,create_dataset_on_bq] >> gcs_to_bq_pm25 >> end