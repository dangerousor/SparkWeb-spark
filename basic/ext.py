#!/usr/bin/python
# -*- coding:utf-8 -*-
import redis
from sqlalchemy import CHAR, Column, DateTime, Float, JSON, String, Text, text, create_engine
from sqlalchemy.dialects.mysql import INTEGER
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import exists

Base = declarative_base()
metadata = Base.metadata


class DBTask(Base):
    __tablename__ = 'task'

    id = Column(INTEGER(11), primary_key=True, autoincrement=True)
    subtime = Column(DateTime, nullable=False)
    endtime = Column(DateTime)
    status = Column(Text, nullable=False)
    task = Column(JSON, nullable=False)
    user = Column(String(16), nullable=False)


class DBData(Base):
    __tablename__ = 'data'

    index = Column(INTEGER(11), primary_key=True, autoincrement=True)
    task_id = Column(INTEGER(11))
    step = Column(Text)
    data = Column(Text)


rd = redis.Redis(host='localhost', port=6379, db=0)


class DBWorker:
    db_uri = 'mysql+pymysql://root:liuxiaofeng@localhost:3306/SparkWebProject?charset=utf8'
    engine = create_engine(db_uri)
    DBsession = sessionmaker(bind=engine)

    def get_session(self):
        return self.DBsession()

    def insert(self, table):
        session = self.get_session()
        session.add(table)
        session.commit()
        session.close()

    def insert_all(self, tables):
        session = self.get_session()
        session.add_all(tables)
        session.commit()
        session.close()

    def search(self, params):
        session = self.get_session()
        res = session.query(exists().where(params)).scalar()
        session.close()
        return res

    def query(self, table, params):
        session = self.get_session()
        res = session.query(table).filter(params).all()
        session.close()
        return res

    def update_status(self, task_id, status, t=None):
        session = self.get_session()
        result = session.query(DBTask).filter(DBTask.id == task_id).first()
        result.status = status
        if t:
            result.endtime = t
        session.commit()
        return True
