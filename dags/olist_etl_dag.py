from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pathlib import Path
import sys

default_args = {
    'owner': 'anyoneai',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def extract_task():
    import sys
    from pathlib import Path
    project_root = Path('/home/aleare/dev/anyoneai_sprint-project-1')
    sys.path.insert(0, str(project_root))
    
    from src.extract import extract, get_public_holidays
    from src.config import DATASET_ROOT_PATH, PUBLIC_HOLIDAYS_URL, get_csv_to_table_mapping
    
    print("Extracting...")
    csv_table_mapping = get_csv_to_table_mapping()
    datasets = extract(DATASET_ROOT_PATH, csv_table_mapping, PUBLIC_HOLIDAYS_URL)
    holidays = get_public_holidays(PUBLIC_HOLIDAYS_URL, "2017")
    print(f"Extracted {len(datasets)} datasets, {len(holidays)} holidays")
    return {'datasets': datasets, 'holidays': holidays}

def load_task(**context):
    import sys
    from pathlib import Path
    from sqlalchemy import create_engine
    
    project_root = Path('/home/aleare/dev/anyoneai_sprint-project-1')
    sys.path.insert(0, str(project_root))
    
    from src.load import load
    
    print("Loading...")
    ti = context['ti']
    data = ti.xcom_pull(task_ids='extract')
    
    db_dir = project_root / 'database'
    db_dir.mkdir(exist_ok=True)
    db_path = db_dir / 'olist_airflow.db'
    
    engine = create_engine(f'sqlite:///{db_path}')
    
    all_data = data['datasets']
    all_data['public_holidays'] = data['holidays']
    
    load(all_data, engine)
    print(f"Loaded to {db_path}")

def transform_task():
    import sys
    from pathlib import Path
    from sqlalchemy import create_engine
    
    project_root = Path('/home/aleare/dev/anyoneai_sprint-project-1')
    sys.path.insert(0, str(project_root))
    
    from src.transform import run_queries
    
    print("Transforming...")
    db_path = project_root / 'database' / 'olist_airflow.db'
    
    engine = create_engine(f'sqlite:///{db_path}')
    results = run_queries(engine)
    print(f"{len(results)} transformations done")
    return results

with DAG(
    'olist_etl_pipeline',
    default_args=default_args,
    description='Olist ETL Pipeline',
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'olist'],
) as dag:
    
    extract = PythonOperator(task_id='extract', python_callable=extract_task)
    load = PythonOperator(task_id='load', python_callable=load_task, provide_context=True)
    transform = PythonOperator(task_id='transform', python_callable=transform_task)
    
    extract >> load >> transform
