import os
from sqlalchemy import create_engine

# Read DB connection from environment variables with sensible defaults
DB_HOST = os.getenv('DB_HOST', '127.0.0.1')
DB_PORT = int(os.getenv('DB_PORT', 3306))
DB_USER = os.getenv('DB_USER', 'root')
DB_PASS = os.getenv('DB_PASS', '')
DB_NAME = os.getenv('DB_NAME', 'maladies_db')

# SQLAlchemy engine factory
def get_engine(echo=False):
    uri = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"
    return create_engine(uri, echo=echo, pool_pre_ping=True)
