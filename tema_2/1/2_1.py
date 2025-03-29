import os

from dotenv import load_dotenv

from sqlalchemy import create_engine, Column, String, ForeignKey, Integer, Float, Text, Date
from sqlalchemy.orm import declarative_base, sessionmaker


load_dotenv()

DB_NAME = os.environ.get('DB_NAME')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')
DB_USER = os.environ.get('DB_USER')
DB_PASS = os.environ.get('DB_PASS')

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

Base = declarative_base()

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
Session = sessionmaker(bind=engine)


class Genre(Base):
    __tablename__ = 'genre'
    genre_id = Column(Integer, primary_key=True, index=True)
    name_genre = Column(String)


class Book(Base):
    __tablename__ = 'book'
    book_id = Column(Integer, primary_key=True, index=True)
    title = Column(String)
    author_id = Column(Integer, ForeignKey('author.author_id'))
    genre_id = Column(Integer, ForeignKey('genre.genre_id'))
    price = Column(Float)
    amount = Column(Integer)


class Author(Base):
    __tablename__ = 'author'
    author_id = Column(Integer, primary_key=True, index=True)
    name_author = Column(String)


class City(Base):
    __tablename__ = 'city'
    city_id = Column(Integer, primary_key=True, index=True)
    name_city = Column(String)
    days_delivery = Column(Integer)


class Client(Base):
    __tablename__ = 'client'
    client_id = Column(Integer, primary_key=True, index=True)
    name_client = Column(String)
    city_id = Column(Integer, ForeignKey('city.city_id'))
    email = Column(String(255))


class Buy(Base):
    __tablename__ = 'buy'
    buy_id = Column(Integer, primary_key=True, index=True)
    buy_description = Column(Text)
    cliend_id = Column(Integer, ForeignKey('client.client_id'))


class BuyBook(Base):
    __tablename__ = 'buy_book'
    buy_book_id = Column(Integer, primary_key=True, index=True)
    buy_id = Column(Integer, ForeignKey('buy.buy_id'))
    book_id = Column(Integer, ForeignKey('book.book_id'))
    amount = Column(Integer)


class Step(Base):
    __tablename__ = 'step'
    step_id = Column(Integer, primary_key=True, index=True)
    name_step = Column(String)


class BuyStep(Base):
    __tablename__ = 'buy_step'
    buy_step_id = Column(Integer, primary_key=True, index=True)
    buy_id = Column(Integer, ForeignKey('buy.buy_id'))
    step_id = Column(Integer, ForeignKey('step.step_id'))
    date_step_beg = Column(Date)
    date_step_end = Column(Date)


Base.metadata.create_all(bind=engine)