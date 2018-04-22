#!/usr/bin/python
'''
Created on 10/6/2015

Definition of Business entities

@author: alberto
'''

from sqlalchemy import Column, ForeignKey, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.schema import UniqueConstraint
from sqlalchemy import create_engine

# ORM - Use declarative model for database
# Define model entities / Database schema

Base = declarative_base()

class Channel(Base):
  __tablename__ = 'channel'
  id = Column(Integer, primary_key=True)
  name = Column(String(250), nullable=False, unique=True)

class Performer(Base):
  __tablename__ = 'performer'
  id = Column(Integer, primary_key=True)
  name = Column(String(250), nullable=False, unique=True)

class Song(Base):
  __tablename__ = 'song'
  id = Column(Integer, primary_key=True)
  name = Column(String(250), nullable=False)
  performer_id = Column(Integer, ForeignKey('performer.id'), nullable=False)
  performer = relationship(Performer)
  __table_args__ = (UniqueConstraint('name', 'performer_id'),)

class Play(Base):
  __tablename__ = 'play'
  id = Column(Integer, primary_key=True)
  channel_id = Column(Integer, ForeignKey('channel.id'), nullable=False)
  channel = relationship(Channel)
  song_id = Column(Integer, ForeignKey('song.id'), nullable=False)
  song = relationship(Song)
  startdate = Column(DateTime, nullable=False)
  enddate = Column(DateTime, nullable=False)
  __table_args__ = (UniqueConstraint('channel_id', 'startdate'),)


def getBase():
  return Base


def main():
  engine = create_engine('mysql://dev:!dev_@localhost/dev_songs')
  Base.metadata.drop_all(engine)
  Base.metadata.create_all(engine)
  
  session = sessionmaker(bind = engine)()
  ch = Channel(name="Channel 1")
  session.add(ch)
  session.commit()
  

if __name__ == '__main__':
    main()
