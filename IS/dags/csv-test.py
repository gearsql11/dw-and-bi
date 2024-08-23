import os
import requests
import glob
import shutil

from bs4 import BeautifulSoup
from urllib.parse import urlparse
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from datetime import timedelta
from typing import List
from urllib.parse import urlparse


data_path = '/opt/airflow/dags/data_test'
data_folder = data_path + '/'
clean_folder = data_folder + 'cleaned'

def _get_files():
    if not os.path.exists(data_path):  #check ว่ามี folder หรือไม่ ถ้าไม่มีก็สร้าง folder ใหม่
        os.makedirs(data_path)
        
    if not os.path.exists(data_folder): #check ที่เก็บอยู่ในตัวแปร data_folder มีอยู่หรือไม่.
        os.makedirs(data_folder)
        
    # if not os.path.exists(clean_folder):
    #     os.makedirs(clean_folder)
    
    exit_file_name = os.listdir(data_folder) #ดึง files ทั้งหมดที่อยู่ใน folder data_folder
    url = 'https://opendata.onde.go.th/dataset/14-pm-25'
    links = [] #เอาไว้เก็บค่าต่าง ๆ ที่ถูกดึงมาจาก URL
    
    req = requests.get(url, verify=False)
    req.encoding = 'utf-8'
    soup = BeautifulSoup(req.text, 'html.parser') #การแปลง HTML เป็นโครงสร้างข้อมูล
    
    og = soup.find('meta', property='og:url')
    base = urlparse(url)
    
    for link in soup.find_all('a'): #loop ข้อมูลทั้งหมดที่มีอยู่ในหน้าเว็บที่แปลงเป็นโครงสร้างข้อมูล
        current_link = link.get('href') # ดึงค่าจาก a
        if str(current_link).endswith('json'): #check current_link ลงท้ายด้วย .json รึป่าว
            links.append(current_link)
    
    for link in links: # loop แต่ละตัวที่เก็บใน links และแยก url เพื่อเก็บชื่อตัวสุดท้าย
        names = link.split('/')[-1]
        names = names.strip() #ลบช่องว่าง (whitespace)
        name = names.replace('pm_data_hourly-', '')
        
        if (name != 'data_dictionary.csv') & (name not in exit_file_name): #ใช้ check ไฟล์ที่ไม่ต้องการให้ดาวน์โหลดซ้ำ
            req = requests.get(link, verify=False)
            url_content = req.content
            file_p = data_folder + name
            json_file = open(file_p, 'wb') #เขียนและบันทึกไฟล์ json
            json_file.write(url_content)
            json_file.close()

def _process_files():
    if not os.path.exists(data_path):
        os.makedirs(data_path)

    for filename in os.listdir(data_folder): #loop files ทั้งหมดที่อยู่ใน folder data_folder
        if filename.endswith(".json"):
            json_file_path = os.path.join(data_folder, filename) #นำไฟล์ json เข้า path data_folder
            csv_file_path = os.path.join(data_folder, filename.replace('.json', '.csv')) #ถ้าเป็นนามสกุล json ให้แทนที่เป็น csv
            
            # Read the JSON file and convert it to a CSV
            with open(json_file_path, 'r') as f:
                data = pd.read_json(f)
                
            # บันทึกข้อมูล DataFrame เป็น CSV
            data.to_csv(csv_file_path, index=False)

            # Read the CSV file back into a pandas DataFrame for further processing
            structured_data = pd.read_csv(csv_file_path)

            # Perform any further structuring on the DataFrame if needed
            # structured_data = ...

            # เขียน DataFrame ที่ประมวลผลแล้วกลับลงในไฟล์ CSV
            structured_data.to_csv(csv_file_path, index=False)

            # Define the destination path in the data_test folder
            dest_file_path = os.path.join(data_path, filename.replace('.json', '.csv'))

            # ย้ายไฟล์ไปที่โฟลเดอร์ data_test โดยแทนที่ไฟล์ที่มีอยู่แล้ว
            shutil.move(csv_file_path, dest_file_path)

            # Delete the original JSON file
            os.remove(json_file_path)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
}

with DAG(
    'csv-test',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule=None,
    tags=['swu','IS']
) as dag:


    start = EmptyOperator(task_id="start", dag = dag,)

    fetch_files = PythonOperator(
        task_id='fetch_files',
        python_callable=_get_files,
    )

    process_files = PythonOperator(
        task_id='process_files',
        python_callable=_process_files,
    )

    end = EmptyOperator(task_id = 'end')

    start >> fetch_files >> process_files >> end
