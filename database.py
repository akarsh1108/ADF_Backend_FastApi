from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

DATABASE_URL_1 = 'mssql+pyodbc://@3B3LLQ2/ADF?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes'
DATABASE_URL_2 = 'mssql+pyodbc://@3B3LLQ2/db1?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes'
DATABASE_URL_3 = 'mssql+pyodbc://@3B3LLQ2/db2?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes'


engine_1 = create_engine(DATABASE_URL_1)
engine_2 = create_engine(DATABASE_URL_2)
engine_3 = create_engine(DATABASE_URL_3)

SessionLocal_1 = sessionmaker(autocommit=False, autoflush=False, bind=engine_1)
SessionLocal_2 = sessionmaker(autocommit=False, autoflush=False, bind=engine_2)
SessionLocal_3 = sessionmaker(autocommit=False, autoflush=False, bind=engine_3)

def get_db_1():
    db = SessionLocal_1()
    try:
        yield db
    finally:
        db.close()

def get_db_2():
    db = SessionLocal_2()
    try:
        yield db
    finally:
        db.close()

def get_db_3():
    db = SessionLocal_3()
    try:
        yield db
    finally:
        db.close()