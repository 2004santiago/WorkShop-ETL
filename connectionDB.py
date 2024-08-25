import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

# Obtener las variables de entorno
localhost = os.getenv('LOCALHOST')
port = os.getenv('PORT')
nameDB = os.getenv('DB_NAME')
userDB = os.getenv('DB_USER')
passDB = os.getenv('DB_PASS')


location_file = './Data csv/candidates.csv'  
raw_table_database = 'candidates'  
names_file = ['first_name', 'last_name', 'email', 'applicant_date', 'country', 'experience_year', 
              'seniority', 'technology', 'code_challenge_score', 'technical_interview_score']

# Crear el motor de SQLAlchemy para la conexión a PostgreSQL
engine = create_engine(f'postgresql+psycopg2://{userDB}:{passDB}@{localhost}:{port}/{nameDB}')

try:
    # Leer el archivo CSV usando pandas y asignar nombres de columnas personalizados
    df = pd.read_csv(location_file, sep=";",names=names_file)

    # Subir los datos a la tabla en PostgreSQL
    df.to_sql(raw_table_database, engine, if_exists='replace', index=False)

    print(f"Tabla '{raw_table_database}' creada y datos subidos exitosamente.")

except Exception as e:
    print(f"Error al subir los datos: {e}")

finally:
    engine.dispose()
