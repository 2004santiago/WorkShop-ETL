from dotenv import load_dotenv
import os
import pg8000
import pandas as pd
from sqlalchemy import create_engine

load_dotenv()

localhost = os.getenv('LOCALHOST')
port = os.getenv('PORT')
nameDB = os.getenv('DB_NAME')
userDB = os.getenv('DB_USER')
passDB = os.getenv('DB_PASS')


conn = pg8000.connect(
    database=nameDB ,
    user=userDB, 
    password=passDB, 
    host=localhost, 
    port= int(port)
)


candidates = './Data csv/candidates.csv'  
df = pd.read_csv(candidates)


try:
    df.to_sql('candidates', conn, if_exists='append', index=False)
    print('Data inserted successfully')
    
finally:
    conn.close()
    print('Connection closed')