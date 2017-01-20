#!/usr/bin/python
#
# script to connect to an elasticsearch instance and effectively stream
# incoming logs to files on the local filesystem

# Some adjustable parameters
MaxBytesPerLogfile=100000
BookMarkFile="a.bookmark"

import mmap
import atexit
import os.path

mm=0
index=0

def TidyUp():
  # small function to tidy up after ourselves
  mm.seek(0)
  mm.write(index)
  mm.flush()
  mm.close()

atexit.register(TidyUp)

# We use python's logging modules to do most of the heavy lifting on output
import logging
import logging.handlers
logger=logging.getLogger('test')
logger.setLevel(logging.DEBUG)
# This is awesome
ch=logging.handlers.RotatingFileHandler(filename="test.log",maxBytes=MaxBytesPerLogfile,backupCount=10)
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)

# modules from elasticsearch-py
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan

# The Elasticsearch client is in this object
es=Elasticsearch(['127.0.0.1'])

# This uses mmap to keep track of where we are. The value of 'index' is
# where we currently are and it's always in memory, but we use mmap to
# write it out to the file each time it gets changed

# But first we should create it if it doesn't exist

if not os.path.exists(BookMarkFile):
  with open(BookMarkFile, "wb") as f:
    f.write("0\n")
  with open(BookMarkFile, "r+b") as f:
    mm=mmap.mmap(f.fileno(),0)
    mm.resize(80)
    mm.close()

with open(BookMarkFile, "r+b") as f:
  mm=mmap.mmap(f.fileno(), 0)
  # map the current contents of the bookmark into memory as 'index'
  index=mm.readline().rstrip('\x00')
  # Just in case the file was edited manually and the nulls went away
  mm.resize(80)

  while 1:
    # keep track of our prior index number
    originalindex=index
    # Build the query string for this search, lucene style
    thisquery='{"query": {"range": {"idnumber": {"gt": %s }}}}' % (index,)
    # Execute the search against Elasticsearch
    scroll=scan(es,index="*",query=thisquery)
    # For each result
    for res in scroll:
      # Use python logging to send stuff out (just file at the moment)
      logger.debug(res)
      # If the index in this record is > than the current, update the current
      if res["_source"]["idnumber"]>index:
        index=res["_source"]["idnumber"]
    # There's a new index value
    # Write it to bookmark, flush it out and seek back to check
    # We may not need the flush() if it's expensive, because we're keeping
    # track in memory - we'd just have to make sure it gets flushed out if
    # we crap out
    if index>originalindex:
      mm.seek(0)
      mm.write(index)
      mm.flush()
      print "new bookmark: ",
      mm.seek(0)
      print mm.readline()

  mm.close()
