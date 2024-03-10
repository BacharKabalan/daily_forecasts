from prefect import flow, task
import psycopg2
import json
import requests
from datetime import timedelta
from prefect.schedules import IntervalSchedule



@flow(name='Fetch the last hour air quality data', log_prints = True)
def fetch_air_quality_record():
  """
  This is the main flow that ties all the tasks together.
  Here we get the questions, filter out the ones that are older than 
  24 hours and then start a subflow that follows the link, and scrapes
  the body of the question and all the answers within it.
  """
  json_data = fetch_airq_data()
  air_quality_record = extract_air_quality_indices(json_data)
  print(air_quality_record)
  ##### The string that we use to connect to the PostgreSQL #####
  pg_con = 'postgres://postgres:mysecretpassword@localhost:5432/postgres'
  create_airq_indices_table(pg_con)
  insert_air_quality_record(pg_con, air_quality_record)
# #####1#################################
# ###########   SubFlow   ##############
# ####################################
  
@task
def fetch_airq_data():
  api = '8c93f8fdb30434f2ae92e7c9078f93a49d160172'
  city = '@11768'#this corresponds to waltham-forest-dawlish-rd. see on this link: https://aqicn.org/station/united-kingdom/waltham-forest-dawlish-rd/
  url = f'https://api.waqi.info/feed/{city}/?token={api}'
  # Perform a GET request
  response = requests.get(url)
  data = response.json()
  return data


@task
def extract_air_quality_indices(json_data):
  record = []
  for item, value in json_data['data']['iaqi'].items():
    record.append(value['v'])
  return record


@task
def create_airq_indices_table(conn_params):
  """
  Task to create a table if it doesn't exist.
  """
  try:
    with psycopg2.connect(conn_params) as conn:
      with conn.cursor() as cur:
        create_table_query = """
        CREATE TABLE air_quality_data (
            id INT PRIMARY KEY AUTO_INCREMENT,
            co DECIMAL(5, 2),
            h INT,
            no2 DECIMAL(5, 2),
            o3 DECIMAL(5, 2),
            p DECIMAL(5, 1),
            pm10 INT,
            pm25 INT,
            so2 DECIMAL(5, 2),
            t DECIMAL(5, 2),
            w DECIMAL(5, 2),
            wg DECIMAL(5, 2)
        );
        """
        cur.execute(create_table_query)
        conn.commit()
  except Exception as e:
    print(f"Error creating table: {e}")


@task
def insert_air_quality_record(conn_params, air_quality_data):
  """
  Task to insert a new question into the table.
  """
  try:
    with psycopg2.connect(conn_params) as conn:
      with conn.cursor() as cur:
          insert_query = """
          INSERT INTO air_quality_data (co, h, no2, o3, p, pm10, pm25, so2, t, w, wg)
          VALUES (%f, %d, %f, %f, %f, %d, %d, %f, %f, %f, %f);
          """
          cur.execute(insert_query, air_quality_data)
          conn.commit()
  except Exception as e:
    print(f"Error inserting question: {e}")


if __name__ == "__main__":
  schedule = IntervalSchedule(interval=timedelta(minutes=60))

  fetch_air_quality_record.deploy(
      name="airq-flow-code-baked-into-an-image-deployment",
      work_pool_name="airq-pool-docker",
      image="bash1989/airq-docker-image:latest"
  )
