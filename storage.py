#!/usr/bin/python
'''
Created on 10/6/2015

@author: alberto
'''

import logging

import datetime

import sqlalchemy
import sqlalchemy.orm
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func, select, label
from sqlalchemy.sql.expression import or_, desc, text, column, ColumnClause, literal, literal_column
from sqlalchemy.types import INTEGER

from entities import getBase, Channel, Performer, Song, Play


Base = getBase()

class DatabaseService(object):
  """
  Song service.
  Handles connection to database
  """

  def __init__(self, dburl=None):
    """
    @param dburl: DB Backend to use
    """
    self.logger = logging.getLogger(__name__)

    if dburl == None:
      dburl = 'mysql://dev:!dev_@localhost/dev_songs?charset=utf8'
    self.dburl = dburl

    self.logger.debug("Starting song service. DB backend is: %s", (self.dburl))

    self.engine = create_engine(dburl, convert_unicode=False, echo=True)
    self.sessionMaker = None

    Base.metadata.create_all(self.engine)

  def recreateSchema(self):
    Base.metadata.drop_all(self.engine)
    Base.metadata.create_all(self.engine)

  def getSession(self):
    if self.sessionMaker == None:
      self.sessionMaker = sessionmaker(bind=self.engine, autocommit=False)

    return self.sessionMaker()

  def getSongService(self):
    """"
    Returns a new session in the database for transactions
    """
    return SongService(self.getSession())


class SongService(object):
  """
  Song service.
  Defines App Bussiness logic
  """

  def __init__(self, session):
    """
    @param session: Session to DB
    """
    self.session = session

  # def __del__(self):
  #  raise Exception,"Nobody calls me :-("

  def addChannel(self, name):
    ch = None
    try:
      ch = Channel(name=name)
      self.session.add(ch)
      self.session.commit()
    except sqlalchemy.exc.IntegrityError:
      self.session.rollback()
    except Exception, e:
      self.session.rollback()
      raise e
    return ch

  def addPerformer(self, name):
    performer = None
    try:
      performer = Performer(name=name)
      self.session.add(performer)
      self.session.commit()
    except sqlalchemy.exc.IntegrityError:
      self.session.rollback()
    except Exception, e:
      self.session.rollback()
      raise e
    return performer

  def addSong(self, name, performerName):
    song = None
    performer = None
    try:
      try:
        performer = self.session.query(Performer)\
        .filter(Performer.name == performerName).one()
      except sqlalchemy.orm.exc.NoResultFound:
        # Then add it
        self.session.begin_nested()
        performer = self.addPerformer(performerName)
      song = Song(name=name, performer=performer)
      self.session.add(song)
      self.session.commit()
    except sqlalchemy.exc.IntegrityError:
      self.session.rollback()
    except Exception, e:
      self.session.rollback()
      raise e
    return song

  def addPlay(self, title, performerName, start, end, channelName):
    play = None
    song = None
    channel = None
    try:
      try:
        channel = self.session.query(Channel)\
        .filter(Channel.name == channelName).one()
      except sqlalchemy.orm.exc.NoResultFound:
        # Then add it
        self.session.begin_nested()
        channel = self.addChannel(channelName)
      try:
        song = self.session.query(Song).join(Song.performer)\
        .filter(Song.name == title, Performer.name == performerName).one()
      except sqlalchemy.orm.exc.NoResultFound:
        # Then add it
        self.session.begin_nested()
        song = self.addSong(title, performerName)
      play = Play(channel = channel, song = song, startdate = start, enddate = end)
      self.session.add(play)
      self.session.commit() 
    except sqlalchemy.exc.IntegrityError:
      self.session.rollback()
    except Exception, e:
      self.session.rollback()
      raise e
    return play
  
  def getSongPlays(self, title, performerName, start, end):
    songs = self.session.query(Play)\
      .join(Play.song)\
      .join(Song.performer)\
      .join(Play.channel)\
      .filter(Song.name == title, Performer.name == performerName,
              Play.startdate < end, Play.enddate > start)\
      .order_by(Play.startdate).all()
    return songs
  
  def getChannelPlays(self, channel, start, end):
    songs = self.session.query(Play)\
      .join(Play.song)\
      .join(Song.performer)\
      .join(Play.channel)\
      .filter(Channel.name == channel,
              Play.startdate < end, Play.enddate > start)\
      .order_by(Play.startdate, Play.enddate).all()
    return songs
  
  def getTop(self, channels, start, limit = 40):
    """
    Get Top 40 songs for selected channels and given week
    
    Related query is:
SELECT *
FROM
(
  SELECT u1.song_id as song_id, u1.cc as cc, @rank1:=@rank1+1 AS rank1
  FROM
  (
    SELECT song_id, count(*) as cc
    from play
    where startdate>'2014-01-08'
    group by song_id
    order by cc desc
  ) u1,
  (SELECT @rank1:=0) r1
) a1
LEFT JOIN
(
  select u2.song_id as song_id, u2.cc as cc, @rank2:=@rank2+1 AS rank2
  FROM
  (
    select song_id, count(*) as cc
    from play
    where startdate>'2014-01-01'
    group by song_id
    order by cc desc
  ) u2,
  (SELECT @rank2:=0) r2
) a2
ON a1.song_id = a2.song_id
    """
    # Compute date ranges
    delta = datetime.timedelta(days=7)
    end = start + delta
    before = start - delta
    channelFilters = [Channel.name == ch for ch in channels]
    q1a = self.session.query(Play.song_id, 
            func.count('*').label('cc1'))\
            .join(Play.channel)\
            .filter(Play.startdate>start,
              Play.startdate<end, or_(*channelFilters))\
            .group_by(Play.song_id)\
            .order_by(desc('cc1')).limit(limit).subquery('u1')
    q2a= self.session.query(Play.song_id,
            func.count('*').label('cc2'))\
            .join(Play.channel)\
            .filter(Play.startdate>before,
              Play.startdate<start, or_(*channelFilters))\
            .group_by(Play.song_id)\
            .order_by(desc('cc2')).subquery('u2')
    q1b = self.session.query('@rank1:=0').subquery('r1')
    q2b = self.session.query('@rank2:=0').subquery('r2')
    
    sub1 = self.session.query("song_id", "cc1")\
      .add_column(literal_column("@rank1:=@rank1+1", type_=INTEGER).label("rank1"))\
      .select_from(q1a, q1b).subquery('a1')
    sub2 = self.session.query("song_id", "cc2")\
      .add_column(literal_column("@rank2:=@rank2+1", type_=INTEGER).label("rank2"))\
      .select_from(q2a, q2b).subquery('a2')
      
    final = self.session.query(Performer.name, Song.name,
                               literal_column("a1.song_id").label("song_id"),
                                "cc1", "cc2", "rank1", "rank2")\
      .select_from(sub1)\
      .outerjoin(sub2, sub1.c.song_id == sub2.c.song_id)\
      .join(Song, Song.id == sub1.c.song_id)\
      .join(Song.performer)
    
    return [{'performer' : row[0],
             'title' : row[1],
             'plays' : row[3],
             'previous_plays' : row[4],
             'rank' : int(row[5])-1,
             'previous_rank' : int(row[6])-1} for row in final.limit(limit).all()]

if __name__ == '__main__':

    ds = DatabaseService()
    #ds.recreateSchema()
    ss = ds.getSongService()
    ss.addChannel("Channel 1")

#     ss.addChannel("Channel 1")
#     ss.addChannel("Channel 2")
#     ss.addChannel("Channel 3")
#     ss.addPerformer("Artist 1")
#     ss.addPerformer("Artist 1")
#     ss.addPerformer("Artist 2")
#     ss.addPerformer("Artist 3")
#     ss.addPerformer("Artist 2")
#     ss.addPerformer("Artist 4")
#     ss.addSong("Song 1", "Artist 5")
#     ss.addSong("Song 2", "Artist 1")
#     ss.addSong("Song 3", "Artist 2")
#     ss.addSong("Song 4", "Artist 8")
#     ss.addSong("Song 1", "Artist 8")
#     ss.addPlay("Song X", "Artist X", datetime.datetime(2015, 6, 4, 10, 30, 32).isoformat(),
#                datetime.datetime(2015, 6, 4, 11, 30, 32).isoformat(), "Channel X")
#     ss.addPlay("Song X", "Artist X", datetime.datetime(2015, 6, 4, 12, 30, 32).isoformat(),
#                datetime.datetime(2015, 6, 4, 14, 30, 32).isoformat(), "Channel X")
# 
#     d1 = datetime.datetime(2015, 6, 4, 10, 30, 32)
#     d2 = datetime.datetime(2015, 6, 4, 14, 30, 32)
# 
#     out = ss.getSongPlays("Song X", "Artist X", d1, d2)
#     for a in out:
#       print a.channel.name, a.startdate, a.enddate
# 
#     out = ss.getChannelPlays("Channel X", d1, d2)
#     for a in out:
#       print a.song.performer.name, a.song.name, a.startdate, a.enddate
      
    out = ss.getTop(["Channel1", "Channel2"], 
                    datetime.datetime(2014, 1, 8), limit=3)
    print out
