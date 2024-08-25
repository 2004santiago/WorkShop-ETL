import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

localhost = os.getenv('LOCALHOST')
port = os.getenv('PORT')
nameDB = os.getenv('DB_NAME')
userDB = os.getenv('DB_USER')
passDB = os.getenv('DB_PASS')


location_file = './Data csv/candidates.csv'  
raw_table_database = 'candidates'  
names_file = ['first_name', 'last_name', 'email', 'applicant_date', 'country', 'experience_year', 
              'seniority', 'technology', 'code_challenge_score', 'technical_interview_score']

engine = create_engine(f'postgresql+psycopg2://{userDB}:{passDB}@{localhost}:{port}/{nameDB}')

try:
    df = pd.read_csv(location_file, sep=";",names=names_file)

    df.to_sql(raw_table_database, engine, if_exists='replace', index=False)

    print(f"Tabla '{raw_table_database}' creada y datos subidos exitosamente.")

except Exception as e:
    print(f"Error al subir los datos: {e}")

finally:
    engine.dispose()
