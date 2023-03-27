import requests
import pandas as pd
import pymysql
import os

host_name=os.environ.get('host')
user_name=os.environ.get('user')
passwords=os.environ.get('password')
database_name=os.environ.get('db_name')
ports=os.environ.get('port')
table='webdata'


def lambda_handler():
    url = "http://api.open-notify.org/iss-now.json"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        df = pd.DataFrame.from_dict(data["iss_position"], orient="index").T
        df["timestamp"] = data["timestamp"]
        return df


def uploader(df, database_name, table, user_name, passwords, host_name, ports):
    # engine=create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port})
    conn = pymysql.connect(
        host=host_name,
        user=user_name,
        password=passwords,
        database=database_name)
    cursor = conn.cursor()

    columns = ', '.join(list(df.columns))
    placeholders = ', '.join(['%s'] * len(df.columns))
    query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"

    values = [tuple(x) for x in df.values]

    cursor.executemany(query, values)

    conn.commit()

    cursor.close()

    print("completed")


df = lambda_handler()

uploader(df, database_name, table, user_name, passwords, host_name, ports)
