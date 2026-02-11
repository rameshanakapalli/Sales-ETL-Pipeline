# Activate virtual environment
.\etl_env\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Initialize Airflow database (first time only)
airflow db init

# Start Airflow webserver
Start-Process "airflow" -ArgumentList "webserver --port 8080"

# Start Airflow scheduler
Start-Process "airflow" -ArgumentList "scheduler"

# Optional: Trigger DAG manually
# airflow dags trigger etl_sales_dag
