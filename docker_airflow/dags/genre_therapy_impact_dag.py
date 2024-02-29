'''
=================================================
Milestone 3

Nama  : Muhammad Hafidz Adityaswara
Batch : HCK-012

Program ini dibuat untuk melakukan automatisasi transform dan load data dari PostgreSQL ke ElasticSearch.
Adapun dataset yang dipakai adalah dataset mengenai Hasil Survei Musik & Kesehatan Mental.
=================================================
'''

# Import Libraries
import pandas as pd


from airflow.models import DAG #  untuk mendefinisikan DAG (Directed Acyclic Graph) dalam Airflow, yang mengatur alur kerja ETL.
from airflow.operators.python import PythonOperator #  untuk mendefinisikan tugas Python yang melakukan transformasi data.
from airflow.providers.postgres.operators.postgres import PostgresOperator #  untuk berinteraksi dengan database PostgreSQL.
from datetime import datetime, timedelta  # untuk menangani tanggal dan waktu.
from sqlalchemy import create_engine # untuk berinteraksi dengan database PostgreSQL melalui library SQLAlchemy
from elasticsearch import Elasticsearch # Duntuk berinteraksi dengan database Elasticsearch.
from elasticsearch.helpers import bulk # untuk melakukan operasi bulk data pada Elasticsearch.


def load_csv_to_postgres():
    '''
    Fungsi ini ditujukan untuk memasukkan data .csv ke dalam Postgres, yang nantinya dataset ini akan disimpan ke dalam database .

    Fungsi load_csv_to_postgres() melakukan proses:
    - Menghubungkan Python ke database PostgreSQL menggunakan SQLAlchemy.
    - Membaca data dari file CSV menggunakan Pandas.
    - Menulis data dari Pandas Dataframe ke tabel PostgreSQL.

    '''
    # Mendefinisikan Parameter Koneksi
    database = "milestone3"
    username = "milestone3"
    password = "milestone3"
    host = "postgres"

    # Membuat URL koneksi PostgreSQL
    postgres_url = f"postgresql+psycopg2://{username}:{password}@{host}/{database}"
    # Membuat Koneksi dengan SQLAlchemy
    engine = create_engine(postgres_url)
    conn = engine.connect()
    
    # Membaca Data CSV
    df = pd.read_csv('/opt/airflow/dags/P2M3_muhammad_hafidz.csv')
    # Menulis Data ke PostgreSQL
    df.to_sql('table_m3', conn, index=False, if_exists='replace')
    

def ambil_data():
    '''
    Fungsi ini mengambil semua data dari tabel table_m3 di database PostgreSQL dan menyimpannya ke file CSV, 
    sehingga memudahkan akses dan analisis data di luar database.

    '''
    # Mendefinisikan Parameter Koneksi
    database = "milestone3"
    username = "milestone3"
    password = "milestone3"
    host = "postgres"

    # Membuat URL koneksi PostgreSQL
    postgres_url = f"postgresql+psycopg2://{username}:{password}@{host}/{database}"
    # Membuat Koneksi dengan SQLAlchemy
    engine = create_engine(postgres_url)
    conn = engine.connect()

    # Membaca Data dari PostgreSQL
    df = pd.read_sql_query("select * from table_m3", conn) #nama table sesuaikan sama nama table di postgres 
    # Menyimpan Data ke File CSV
    df.to_csv('/opt/airflow/dags/P2M3_muhammad_hafidz_data_raw.csv', sep=',', index=False)
    

def preprocessing(): 
    '''
    Fungsi ini ditujukan untuk pembersihan data berupa menghandle data duplicated, missing value, 
    melakukan normalisasi pada nama kolom dan memperbaiki tipe data.

    '''   
    # load data
    data = pd.read_csv("/opt/airflow/dags/P2M3_muhammad_hafidz_data_raw.csv")
    # drop duplicate
    data.drop_duplicates(inplace=True)
    # mengubah nama tabel menjadi huruf kecil (lowercase) 
    data.rename(str.lower, axis='columns', inplace=True)
    # mengganti spasi dengan tanda garis bawah (_) dalam nama kolom
    data.rename(columns=lambda x: x.replace(' ', '_'), inplace=True)
    # menghapus tanda kurung siku dari nama kolom
    data.rename(columns=lambda x: x.replace('[', '').replace(']', ''), inplace=True)
    # handling missing value 
    data.dropna(inplace=True)
    #Berubah tipe data
    data['age'] = data['age'].astype(int)
    data['timestamp'] = pd.to_datetime(data['timestamp'], format='%m/%d/%Y %H:%M:%S')
    # save data clean
    data.to_csv('/opt/airflow/dags/P2M3_muhammad_hafidz_data_clean.csv', index=False)
    

def upload_to_elasticsearch():
    '''
    Fungsi ini ditujukan untuk mengupload data yang sudah dilakukan preprocessing kedalam elasticsearch.

    Fungsi upload_to_elasticsearch melakukan proses:
    - mengulangi setiap baris data praproses dalam file CSV.
    - setiap baris diubah menjadi kamus yang sesuai untuk pengindeksan Elasticsearch.
    - dokumen yang disiapkan kemudian diunggah ke indeks "table_m3" di Elasticsearch.

    ''' 
    # Impor dan Instansiasi
    es = Elasticsearch("http://elasticsearch:9200")
    # Memuat Data
    df = pd.read_csv('/opt/airflow/dags/P2M3_muhammad_hafidz_data_clean.csv')
    # Iterasi dan Unggah
    for i, r in df.iterrows():
        doc = r.to_dict()  # Convert the row to a dictionary
        res = es.index(index="table_m3", id=i+1, body=doc)
        print(f"Response from Elasticsearch: {res}")
        
        

default_args = {
    'owner': 'Muhammad Hafidz Adityaswara', 
    'start_date': datetime(2024, 2, 22, 12, 00) - timedelta(hours=7)
}

with DAG(
    "Phase_2_Milestone_3", # nama project
    description='Milestone_3', # deskripsi project
    schedule_interval='30 6 * * *', # atur schedule untuk menjalankan airflow pada 06:30. 
    default_args=default_args, 
    catchup=False
) as dag:
    # Task : 1
    '''  Fungsi ini ditujukan untuk mengupload data kedalam postgres.'''
    load_csv_task = PythonOperator(
        task_id='Load_csv_to_Postgres',
        python_callable=load_csv_to_postgres)
    
    #task: 2
    '''  Fungsi ini ditujukan untuk mengambil data dari postgres.'''
    ambil_data_pg = PythonOperator(
        task_id='Fetch_from_Postgres',
        python_callable=ambil_data) #
    

    # Task: 3
    '''  Fungsi ini ditujukan untuk menjalankan pembersihan data.'''
    edit_data = PythonOperator(
        task_id='Data_Cleaning',
        python_callable=preprocessing)

    # Task: 4
    '''  Fungsi ini ditujukan untuk mengupload data kedalam Elasticsearch.'''
    upload_data = PythonOperator(
        task_id='Post_to_Elasticsearch',
        python_callable=upload_to_elasticsearch)

    #proses untuk menjalankan di airflow
    load_csv_task >> ambil_data_pg >> edit_data >> upload_data



