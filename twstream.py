#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
$ cat config.json
{
  "APP_KEY":"your app key",
  "APP_SECRET":"your app secret",
  "OAUTH_TOKEN":"your oauth token",
  "OAUTH_TOKEN_SECRET":"your oauth token secret"
}
'''
import json
import time
import codecs
import argparse
import requests
import logging
from twython import TwythonStreamer

def debug_enable():
  try:
    import http.client as http_client
  except ImportError:
    import httplib as http_client
  http_client.HTTPConnection.debuglevel = 1
  logging.getLogger().setLevel(logging.DEBUG)
  requests_log = logging.getLogger("requests.packages.urllib3")
  requests_log.setLevel(logging.DEBUG)
  requests_log.propagate = True

def genrecord(data):
  # Example Response
  # https://dev.twitter.com/rest/reference/get/statuses/show/id
  if "retweeted_status" in data:
    tweet_status = data["retweeted_status"]
  else:
    tweet_status = data
  record = {"screen_name": tweet_status["user"]["screen_name"],
            "link": "https://twitter.com/{}/status/{}".format(tweet_status["user"]["screen_name"],tweet_status["id"])
  }
  record.update({k:v for k,v in tweet_status.iteritems() if k in ["created_at", "text", "id"]})
  if "extended_tweet" in tweet_status and "full_text" in tweet_status["extended_tweet"]:
    record["text"] = tweet_status["extended_tweet"]["full_text"]
  #logging.debug(record["text"])
  return record


class MyStreamer(TwythonStreamer):
  def on_success(self, data):
    if 'text' in data:
      #codecs.open("tweet_detail.log", "a", "utf-8").write(json.dumps(data, ensure_ascii=False)+"\n")
      record = genrecord(data)
      codecs.open("tweet.log", "a", "utf-8").write(json.dumps(record, ensure_ascii=False, sort_keys=True)+"\n")
    else:
      logging.debug(str(data))

  def on_error(self, status_code, data):
    logging.error("status_code: %d %s" % (status_code, data))
    self.disconnect()


if __name__ == "__main__":
  logging.basicConfig(format='%(asctime)s %(name)s[%(levelname)s]: %(message)s')

  parser = argparse.ArgumentParser(description='This script aggregates twitter user timeline')
  parser.add_argument('-c', action='store', dest='config', default='config.json')
  parser.add_argument('--keyword', action='store', dest='keyword', default="")
  parser.add_argument('--location', action='store', dest='location', default="")
  parser.add_argument('--sample', action='store_true', dest='sample', default=False)
  parser.add_argument('--debug', action='store_true', dest='debug', default=False)
  args = parser.parse_args()

  if args.debug:
    debug_enable()

  filter_options={}
  if len(args.keyword) > 0:
    filter_options["track"] = args.keyword
  if len(args.location) > 0:
    filter_options["locations"] = args.location.strip()

  # https://twython.readthedocs.io/en/latest/api.html#streaming-interface
  key = json.load(open(args.config))
  stream = MyStreamer(key['APP_KEY'], key['APP_SECRET'], key['OAUTH_TOKEN'], key['OAUTH_TOKEN_SECRET'])

  flg = True
  while flg:
    flg = False
    start_time = time.time()
    try:
      if len(filter_options) > 0:
        logging.info("stream filter: %s" % filter_options)
        stream.statuses.filter(**filter_options)
      elif args.sample > 0:
        logging.info("stream sample")
        stream.statuses.sample()
      else:
        logging.info("stream user timeline")
        stream.user()
    except requests.exceptions.ChunkedEncodingError as e:
      logging.info(e)
      flg = True

    if flg:
      elapsed_time = time.time() - start_time
      if elapsed_time < 10:
        time.sleep(10 - elapsed_time)
