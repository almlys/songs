#!/usr/bin/python
'''
Created on 10/6/2015

@author: alberto
'''
import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

import argparse
import traceback

import tornado.ioloop
import tornado.web

import json
import dateutil.parser

from storage import DatabaseService

class MainHandler(tornado.web.RequestHandler):
  """
  Define handler for webservice requests
  """

  def initialize(self, ds):
    self.ds = ds


  def write_error(self, status_code, **kwargs):
    """
    Catch errors and sent them as json
    """
    self.set_header('Content-Type', 'text/json')
    errors = [self._reason,]
    if self.settings.get("serve_traceback") and "exc_info" in kwargs:
      # in debug mode, try to send a traceback
      errors.append("".join(traceback.format_exception(*kwargs["exc_info"])))
    self.finish(json.dumps({
          'code': status_code,
          'errors': errors
     }, indent=True))

  def sendResponse(self, result):
    """
    Sends formated response to webservice client
    """
    self.write(json.dumps({'result' : result, 'code' : 0}, indent=True))

  def parseDate(self, date):
    """
    Parses incomming date
    """
    return dateutil.parser.parse(date)

  def handle_request(self, request):
    """
    Send GET/POST request to correct self.method
    """
    self.set_header('Content-Type', 'application/json')
    if hasattr(self, request):
      fnc = getattr(self, request)
      return fnc()
    raise tornado.web.HTTPError(500)

  def get(self, request):
    # self.write("Your request is: %s" %(request))
    if request in ("get_song_plays", "get_channel_plays", "get_top"):
      return self.handle_request(request)
    raise tornado.web.HTTPError(404)

  def post(self, request):
    # self.write("Your request is: %s" %(request))
    if request in ("add_channel", "add_performer", "add_song", "add_play"):
      return self.handle_request(request)
    raise tornado.web.HTTPError(404)

  def add_channel(self):
    channel = self.get_body_argument("name")
    logger.debug("Add channel: %s" % (channel))
    self.ds.getSongService().addChannel(channel)

  def add_performer(self):
    performer = self.get_body_argument("name")
    logger.debug("Add Performer: %s" % (performer))
    self.ds.getSongService().addPerformer(performer)

  def add_song(self):
    name = self.get_body_argument("title")
    performerName = self.get_body_argument("performer")
    logger.debug("Add Song: %s,%s" % (name, performerName))
    self.ds.getSongService().addSong(name, performerName)
    
  def add_play(self):
    title = self.get_body_argument("title")
    performerName = self.get_body_argument("performer")
    start = self.parseDate(self.get_body_argument("start"))
    end = self.parseDate(self.get_body_argument("end"))
    channel = self.get_body_argument("channel")
    logger.debug("Add Play: %s:%s, %s-%s on %s" 
                 % (title, performerName, start, end, channel))
    self.ds.getSongService().addPlay(title, performerName, \
                                     start, end, channel)
  
  def get_song_plays(self):
    title = self.get_query_argument("title")
    performerName = self.get_query_argument("performer")
    start = self.parseDate(self.get_query_argument("start"))
    end = self.parseDate(self.get_query_argument("end"))
    logger.debug("Get Song Plays: %s:%s, %s %s" 
                 % (title, performerName, start, end))
    plays = self.ds.getSongService().getSongPlays(title, performerName, \
                                     start, end)
    result = [{'channel' : p.channel.name,
                 'start' : p.startdate.isoformat(),
                   'end' : p.enddate.isoformat()} for p in plays]
    self.sendResponse(result)

  def get_channel_plays(self):
    channel = self.get_query_argument("channel")
    start = self.parseDate(self.get_query_argument("start"))
    end = self.parseDate(self.get_query_argument("end"))
    logger.debug("Get Channel Plays: %s, %s %s" 
                 % (channel, start, end))
    plays = self.ds.getSongService().getChannelPlays(channel, \
                                     start, end)
    result = [{'performer' : p.song.performer.name,
                   'title' : p.song.name,
                   'start' : p.startdate.isoformat(),
                     'end' : p.enddate.isoformat()} for p in plays]
    #for p in plays:
    #  print p.song.name
    self.sendResponse(result)
  
  def get_top(self):
    channels = json.loads(self.get_query_argument("channels"))
    start = self.parseDate(self.get_query_argument("start"))
    limit = int(self.get_query_argument("limit"))
    logger.debug("Get Top: %s, %s %i" 
                 % (channels, start, limit))
    top = self.ds.getSongService().getTop(channels, start, limit)
    print top
    self.sendResponse(top)
    


if __name__ == "__main__":
  parser = argparse.ArgumentParser(description='Songs Service Server')
  parser.add_argument('-P', action="store", dest="port",
                      default=9080, type=int)
  parser.add_argument('--drop-database', action="store_true", dest="dropDatabase",
                      help=("Drops database"), default=False)
  parser.add_argument('--backend', action="store", dest="dburl",
                        default="mysql://dev:!dev_@localhost/dev_songs?charset=utf8", type=str)

  args = parser.parse_args()
  port = args.port
  ds = DatabaseService(dburl=args.dburl)
  if args.dropDatabase:
    ds.recreateSchema()

  application = tornado.web.Application([
    (r"/(.*)", MainHandler, dict(ds=ds)),
    ], debug=True)
  logger.info("Starting service at port: %i" % (port))
  application.listen(port)
  tornado.ioloop.IOLoop.current().start()

