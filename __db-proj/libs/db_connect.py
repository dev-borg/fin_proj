"""
1. Context Management: 
  @contextmanager decorator allows get_session() to be used conveniently with the with statement, 
  ensuring proper resource management (session opening and closing) around database operations.

2. Generator Function:
  yiled in get_session() makes it a Generator Furnction

"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base
from contextlib import contextmanager

# Database connection parameters
config = {
    "dialect+driver": "mysql+mysqlconnector",
    'user': 'marco',
    'password': 'economics',
    'host': '127.0.0.1',
    'database': 'world_bank',
    'raise_on_warnings': True
}

# Construct the SQLAlchemy connection string
connection_str = f"{config['dialect+driver']}://{config['user']}:{config['password']}@{config['host']}/{config['database']}"

# Create the SQLAlchemy engine
engine = create_engine(connection_str)
print(f"Database connection established\nDatabase: {config['database']}\nUsing SQLAlchemy!\nDriver: {engine.dialect.driver}\n")

# Create configured "Session" class
session_factory = sessionmaker(autoflush=False, bind=engine)

# Create a scoped session (creates te scoped session threads)
SessionLocal = scoped_session(session_factory)

# Create declarative base class
Base = declarative_base()

# Generator Function (cf. yield, and how it maintains state and lazy execuition)
@contextmanager
def get_session():
    session = SessionLocal
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        raise
    finally:
        # Use session.close() to properly release resources associated with a session when 
        # you are done using it, ensuring efficient connection management and avoiding resource leaks.
        session.close()


# Dispose of SQLAlchemy db connection and removes SessionLocal thread creator object
def dispose_db_connections():
    print("Closing all database connection engines and all related Scoped Session thread creator objects")
    SessionLocal.remove()  # Remove the session from the current thread's scope
    engine.dispose()
