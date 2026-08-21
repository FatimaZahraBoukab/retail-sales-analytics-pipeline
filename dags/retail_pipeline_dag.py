from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

SPARK_CMD = "docker exec spark /opt/spark/bin/spark-submit"
KAFKA_PKG = "--conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
PG_PKG = "--conf spark.jars.ivy=/tmp/.ivy2 --packages org.postgresql:postgresql:42.7.3"

with DAG("retail_pipeline", start_date=datetime(2026, 1, 1), schedule=None, catchup=False) as dag:

    bronze = BashOperator(task_id="bronze", bash_command=f"{SPARK_CMD} {KAFKA_PKG} /opt/spark_jobs/bronze_job.py")
    silver = BashOperator(task_id="silver", bash_command=f"{SPARK_CMD} /opt/spark_jobs/silver_job.py")
    gold = BashOperator(task_id="gold", bash_command=f"{SPARK_CMD} {PG_PKG} /opt/spark_jobs/gold_job.py")

    bronze >> silver >> gold