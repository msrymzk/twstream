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
from twython import TwythonStreamer

# logging
import logging
try:
  import http.client as http_client
except ImportError:
  import httplib as http_client
logging.basicConfig(format='%(asctime)s %(name)s[%(levelname)s]: %(message)s')
http_client.HTTPConnection.debuglevel = 1
logging.getLogger().setLevel(logging.DEBUG)
requests_log = logging.getLogger("requests.packages.urllib3")
requests_log.setLevel(logging.DEBUG)
requests_log.propagate = True


class MyStreamer(TwythonStreamer):
  def on_success(self, data):
    if 'text' in data:
      #codecs.open("tweet_detail.log", "a", "utf-8").write(json.dumps(data, ensure_ascii=False)+"\n")

      # Example Response
      # https://dev.twitter.com/rest/reference/get/statuses/show/id
      record = {"screen_name": data["user"]["screen_name"], "link": "https://twitter.com/{}/status/{}".format(data["user"]["screen_name"],data["id"])}
      record.update({k:v for k,v in data.iteritems() if k in ["created_at", "truncated", "text", "id"]})
      jstr = json.dumps(record, ensure_ascii=False, sort_keys=True)
      #print(jstr)
      codecs.open("tweet.log", "a", "utf-8").write(jstr+"\n")

  def on_error(self, status_code, data):
    logging.error("status_code: %d %s" % (status_code, data))
    self.disconnect()

if __name__ == "__main__":
  parser = argparse.ArgumentParser(description='This script aggregates twitter user timeline')
  parser.add_argument('-c', action='store', dest='config', default='config.json')
  parser.add_argument('--keyword', action='store', dest='keyword', default="")
  parser.add_argument('--location', action='store', dest='location', default="")
  parser.add_argument('--sample', action='store_true', dest='sample', default=False)
  args = parser.parse_args()

  filter_options={}
  if len(args.keyword) > 0:
    filter_options["track"] = args.keyword
  if len(args.location) > 0:
    filter_options["locations"] = args.location

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
      logging.error(e)
      flg = True

    if flg:
      elapsed_time = time.time() - start_time
      if elapsed_time < 10:
        time.sleep(10 - elapsed_time)
