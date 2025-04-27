from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
from airflow.decorators import task_group
import os
import shutil
import pandas as pd
from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder.appName("StudentPerformance").getOrCreate()

DATA_DIR = '/home/meqlad/venv/data'
PROCESSED_DIR = 'file:///home/meqlad/venv/processed_data/'

# Use Pendulum to get 1 day ago for start_date
start_date = pendulum.now().subtract(days=1)

default_args = {
    'owner': 'airflow',
    'start_date': start_date,
}

# ---------------------------
# Functions
# ---------------------------

def read_csv(file_name):
    path = os.path.join(DATA_DIR, file_name)
    df = spark.read.csv(path, header=True, inferSchema=True)
    df.show(5)
    return df

def save_parquet(df, file_name):
    output_path = os.path.join(PROCESSED_DIR, file_name)
    df.write.mode('overwrite').parquet(output_path)

def transform_student_info():
    df = read_csv('studentInfo.csv')
    df = df.dropna(subset=['final_result'])
    df = df.withColumnRenamed('highest_education', 'education_level')
    save_parquet(df, 'studentInfo.parquet')

def transform_courses():
    df = read_csv('courses.csv')
    df = df.dropDuplicates()
    save_parquet(df, 'courses.parquet')

def transform_assessments():
    df = read_csv('assessments.csv')
    df = df.filter(df['weight'] > 0)
    save_parquet(df, 'assessments.parquet')

def transform_student_assessment():
    df = read_csv('studentAssessment.csv')
    df = df.withColumnRenamed('score', 'student_score')
    save_parquet(df, 'studentAssessment.parquet')

def transform_student_registration():
    df = read_csv('studentRegistration.csv')
    df = df.fillna({'date_unregistration': -1})
    save_parquet(df, 'studentRegistration.parquet')

def transform_student_vle():
    df = read_csv('studentVle.csv')
    df = df.groupBy('id_student').sum('sum_click')
    save_parquet(df, 'studentVle.parquet')

def transform_vle():
    df = read_csv('vle.csv')
    df = df.dropDuplicates()
    save_parquet(df, 'vle.parquet')

def clean_processed_folder():
    if os.path.exists(PROCESSED_DIR.replace('file://', '')):
        shutil.rmtree(PROCESSED_DIR.replace('file://', ''))
    os.makedirs(PROCESSED_DIR.replace('file://', ''))

def final_summary():
    files = os.listdir(PROCESSED_DIR.replace('file://', ''))
    print(f"Total processed files: {len(files)}")
    for file in files:
        print(f" - {file}")

def run_streamlit():
    os.system('streamlit run /home/meqlad/venv/streamlit_script.py')

# ---------------------------
# DAG Definition
# ---------------------------

with DAG(
    'student_performance_pipeline',
    default_args=default_args,
    schedule=None,
    catchup=False,
    description='ETL Pipeline for Student Performance Dataset',
) as dag:

    start = PythonOperator(
        task_id='start',
        python_callable=lambda: print("Starting the pipeline..."),
    )

    @task_group(group_id='data_cleaning_tasks')
    def data_cleaning_group():
        clean_task = PythonOperator(
            task_id='clean_processed_folder',
            python_callable=clean_processed_folder,
        )
        return clean_task

    @task_group(group_id='data_transformation_tasks')
    def data_transformation_group():
        transform_student_info_task = PythonOperator(
            task_id='transform_student_info',
            python_callable=transform_student_info,
        )

        transform_courses_task = PythonOperator(
            task_id='transform_courses',
            python_callable=transform_courses,
        )

        transform_assessments_task = PythonOperator(
            task_id='transform_assessments',
            python_callable=transform_assessments,
        )

        transform_student_assessment_task = PythonOperator(
            task_id='transform_student_assessment',
            python_callable=transform_student_assessment,
        )

        transform_student_registration_task = PythonOperator(
            task_id='transform_student_registration',
            python_callable=transform_student_registration,
        )

        transform_student_vle_task = PythonOperator(
            task_id='transform_student_vle',
            python_callable=transform_student_vle,
        )

        transform_vle_task = PythonOperator(
            task_id='transform_vle',
            python_callable=transform_vle,
        )

        return [
            transform_student_info_task,
            transform_courses_task,
            transform_assessments_task,
            transform_student_assessment_task,
            transform_student_registration_task,
            transform_student_vle_task,
            transform_vle_task
        ]

    @task_group(group_id='finalization_tasks')
    def finalization_group():
        finalize = PythonOperator(
            task_id='final_summary',
            python_callable=final_summary,
        )
        return finalize

    run_streamlit_app = PythonOperator(
        task_id='run_streamlit_app',
        python_callable=run_streamlit,
    )

    end = PythonOperator(
        task_id='end',
        python_callable=lambda: print("Pipeline finished successfully!"),
    )

    # ---------------------------
    # Task Flow
    # ---------------------------

    start >> data_cleaning_group() >> data_transformation_group() >> finalization_group() >> run_streamlit_app >> end

