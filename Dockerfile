FROM apache/airflow:2.10.2

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir -p /opt/airflow/raw_data /opt/airflow/silver_layer /opt/airflow/gold_layer /opt/airflow/dags/

COPY dags/ /opt/airflow/dags/
