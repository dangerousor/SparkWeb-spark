#!/usr/bin/python
# -*- coding:utf-8 -*-
import redis
from sqlalchemy import Column, DateTime, String, Text, create_engine, Boolean
from sqlalchemy.dialects.mysql import INTEGER
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import exists
from const import REDIS_DB, REDIS_HOST, REDIS_PORT, DB_URI

Base = declarative_base()
metadata = Base.metadata


class DBTask(Base):
    __tablename__ = 'task'

    id = Column(INTEGER(11), primary_key=True, autoincrement=True)
    subtime = Column(DateTime, nullable=False)
    endtime = Column(DateTime)
    status = Column(Text, nullable=False)
    task = Column(Text, nullable=False)
    user = Column(INTEGER(11), nullable=False)
    title = Column(Text)
    note = Column(Text)
    log = Column(Text)
    is_deleted = Column(Boolean, default=False)


class DBData(Base):
    __tablename__ = 'data'

    index = Column(INTEGER(11), primary_key=True, autoincrement=True)
    task_id = Column(INTEGER(11))
    step = Column(Text)
    data = Column(Text)


class DBUser(Base):
    __tablename__ = 'user'

    index = Column(INTEGER(11), primary_key=True, autoincrement=True)
    user_id = Column(String(16), nullable=False, unique=True)
    password = Column(Text)
    username = Column(Text)


rd = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)


class DBWorker:
    db_uri = DB_URI
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
        result.endtime = t
        session.commit()
        return True

    def update_log(self, task_id, log):
        session = self.get_session()
        result = session.query(DBTask).filter(DBTask.id == task_id).first()
        result.log = log
        session.commit()
        return True


class Cache:
    user = int()


cache = Cache()
