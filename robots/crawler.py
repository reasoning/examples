"""

    Stateful, Recoverable, Fault Tollerant Web Crawler
    --------------------------------------------------

    Can be adapted to either a multithreaded or event driven architecture.

    Explicit design goals are that it is crash-recoverable, and can scale to any number of domains and pattern matching 
    requirements using a very simple callback mechanism and a list of url's/referer's.

    Replace SQLite with Postgress, put this on AWS and it will scale to any size you want.

    It has general concepts of resources which are urls combined with data.  So a url is scheduled to crawl which
    becomes a link (for tracking referer and depth), and then we eventually end up with a resource and data.

    Sites are broken into a heirarchy of pages with depth, this might for instance map to an index page on a news
    site or a thread on reddit.  Callbacks are then added for each additional depth corresponding to a news article
    or comments/discussion thread.  

    Each previous step in the crawl defines the next.

    If the crawler crashes or is paused, it can resume where it left off since all of the state is stored in two
    databases, including the content of pages which are compressed using zlib.

    Downloads are separated from schedules, where a download is strictly IO and a schedule is acutal task processing
    such as pattern matching, parsing, or other operations on the data.  Note that typically in a crawler this is
    where a lot of the time is spent and why generally i would say that C++ has the advantage over Python
    regardles of what thread/event model is being used.

    Also on modern hardware with large numbers of cores, its no longer necessarily true that event driven designs
    have any inherent speed advantage over a large number of threads, and the event driven programming model has
    some drawbacks in terms of readability, maintainability, and debugability.  Threads are hard though and most
    engineers dont really know how, or even at what level of granularity, to do sychronisation.

    A note on the main purpose of this database driven design, since it may seem somewhat verbose, unusual, and
    perhaps overcomplicated.  It's not, i assure you, its just one of many "ways" of designing a crawler. For 
    example the popular open source web cloning tool HTTrack povides similar features (https://www.httrack.com/)

    All out speed is usually not a requirement of a crawler as opposed to stability, this is the goal here.

    Speed can be achieved by simply porting the code to another language that supports threads or kernel
    events in a realistic way.

    Most web sites will very rapidly restrict or limit crawlers that try to load too many pages too fast since
    that makes it very easy to detect the presence of a bot (especially on thats not obeying robots.txt)

    FYI, Python actually does not support real threads and using events would not net any performance unless
    we are talking about at the actual network/IO layer which is not the focus of this code.  So how is this
    Gevents stuff going to help ?

    Gevents and greenlets (seems) to work by monkey patching common Python libraries to add something like the
    C equivalent of setjmp/longjmp, essentially coroutines, to any operations that would block (for example file 
    or socket IO).  It claims to have less concurrency requirements than threads, and therfore be easier to 
    overlay onto existing single threaded code.  Which is particularly relevant to Python (see comment above).

    So this crawler was first built to be run multithreaded (minus any locking) and then support for Gevents
    was added last, should make for an interesting test.



	Author:	Emerson Clarke
    Email: emerson.clarke@gmail.com

	The MIT License (MIT)

	Copyright (c) 2021

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in
	all copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
	THE SOFTWARE.	
"""


import types
import time
import sys
import re
import os
import traceback
import threading
import sqlite3

#import numpy as np
#import pandas as pd
from optparse import OptionParser
from datetime import datetime


import colorama
colorama.init()
from termcolor import colored
from colorama import Fore, Back, Style

import urllib
import urllib.request
from urllib.parse import urlparse

import zlib
#import zipfile
#from zipfile import ZipFile

import traceback
import requests
from bs4 import BeautifulSoup

#import logging
#logging.basicConfig(filename='crawler.log', encoding='utf-8', level=logging.DEBUG)

from gevent import monkey, pool
#from gevent.pool import Pool
monkey.patch_all()

#--------------------------------------------------------------------------------------------------------------------

def white(str, bold=True):
	return colored(str,attrs=['bold'] if bold else [])

def yellow(str, bold=True):
	return colored(str,'yellow',attrs=['bold'] if bold else [])

def green(str, bold=True):
	return colored(str,'green',attrs=['bold'] if bold else [])

def blue(str, bold=True):
	return colored(str,'blue',attrs=['bold'] if bold else [])

def red(str, bold=True):
	return colored(str,'red',attrs=['bold'] if bold else [])

def magenta(str, bold=True):
	return colored(str,'magenta',attrs=['bold'] if bold else [])

def cyan(str, bold=True):
	return colored(str,'cyan',attrs=['bold'] if bold else [])


class Logger:

    def __init__(self):
        pass

    def debug(self,msg):
        #logging.debug()
        print(blue(msg))

    def info(self,msg):
        print(green(msg))

    def warning(self,msg):
        print(yellow(msg))

    def error(self,msg):
        print(red(msg))


#--------------------------------------------------------------------------------------------------------------------

# The programming model of the default Python DB API is not condusive to writing and clearly expressing
# SQL statements with logic and result sets, so wrap it a little.  Same is true for regex matching etc
# We want a boolean return value and a consistent recordset interface so we can write simple code.

class Record:

    def __init__(self):

        self.id = -1
    
        self.index = 0
        self.rows = []

    def reset(self):

        self.id = -1
    
        self.index = 0
        self.rows = []
    

    def __getitem__(self,key):
        if len(self.rows) > 0 and type(key) is int:
            return self.rows[self.index][key]
        return None

    def next():
        self.index = self.index+1


class Database:

    def __init__(self,name):
        self.conn = sqlite3.connect(name)
        self.cursor = self.conn.cursor()

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def execute(self,*args):
        # Unfortunately we have no choice but to always call fetchall() if we 
        # want a reliable interface that returns boolean.  Under the hood almost
        # all native database API's support a better model than this but the Python
        # API doesnt recognise that.
        
        #print(type(args[0]), args[0])
        #if len(args) > 1:
        #    print(type(args[1]), args[1])
        #print("Record",type(args[0]) is Record)
        
        if type(args[0]) is str:
            
            """
            print(type(args[0]), args[0])
            if len(args) > 1:
                print(type(args[1]), args[1])
            """

            self.cursor.execute(*args)
            # Even checking lastrowid isn't reliable, perhaps if you scan the statemet
            # for insert or replace, but im not doing that now
            #if self.cursor.lastrowid != -1:
            #    return True
            rowid = False
            sql = args[0].lower().strip()
            if sql.startswith("insert") and self.cursor.lastrowid > 0:
                rowid = True                
            elif sql.startswith("select"):
                rowid = False

            rows = []
            if not rowid:
                rows = self.cursor.fetchall()

            return len(rows) > 0 or rowid

        elif type(args[0]) is Record:     
            """
            print(type(args[0]), args[0])
            print(type(args[1]), args[1])
            if len(args) > 2:
                print(type(args[2]), args[2])
            """

            record = args[0]
            record.reset()

            self.cursor.execute(*args[1:])

            # Its not clear if 0 is a valid rowid because the driver seems to set lastrowid
            # to 0 even when a selecte statement returns no results
            rowid = False
            sql = args[1].lower().strip()
            if sql.startswith("insert") and self.cursor.lastrowid > 0:
                rowid = True
            elif sql.startswith("select"):
                rowid = False                
            
            if self.cursor.lastrowid != -1 and rowid:
                record.id = int(self.cursor.lastrowid)

            rows = []
            if not rowid:
                record.rows = self.cursor.fetchall()

            return len(record.rows) > 0 or record.id != -1


#--------------------------------------------------------------------------------------------------------------------


class Link:

    def __init__(self,url="",referer="",depth=0):
        self.url = url
        self.referer = referer
        self.depth = depth


class Session:

    def __init__(self):
        
        self.id = 0
        self.priority = 0
        self.state = ""
        
        self.task = 0
        self.name = ""
        
        self.resource = 0        
        self.link = None
        self.page = ""


class Crawler:

    
    STATE_INITIAL       = 0
    STATE_SLEEPING      = 1
    STATE_PROCESSING    = 2


    def __init__(self):

        self.events =0

        self.limit = 0
        self.processed = 0
        self.requested = 0
        self.sleeping = 0
        self.kill = False

        self.logger = Logger()
        self.callbacks = {}

        # In a multithreaded environment this would be a wait event which is released by the main thread
        # when it wants to exit
        self.sentinel = True

        self.content = Database("content.db")

        try:
            self.content.execute("create table pages(id integer primary key, html text, updated integer)")
            self.content.commit()
        except:
            pass

        self.manager = Database("manager.db")
        
        try:
            self.manager.execute("create table urls(id integer primary key, url text unique, referer text, weight integer, depth integer)")
            self.manager.execute("create table tasks(id integer primary key, name text unique, level integer)")
        
            self.manager.execute("create table resources(id integer primary key, url_id integer, page_id integer)")

            self.manager.execute("create table downloads(id integer primary key, resource_id integer, task_id integer, priority integer, state text, started integer, finished integer)")
            self.manager.execute("create table schedules(id integer primary key, resource_id integer, task_id integer, priority integer, state text, started integer, finished integer)")

            self.manager.commit()

        except:
            pass    

    def finalise(self):
        pass

    def initialise(self):
        pass   

    def active(self):
        # Adapted from thread model to use libuv style events
        return self.events > 0

    def callback(self, task, caller, level):
        # Add task to database with task name, callback caller and level

        
        if not self.manager.execute("select * from tasks where name = ?",(task,)):
            self.manager.execute("insert into tasks(name,level) values(?,?)",(task,level))
        self.manager.commit()

        self.callbacks[task] = caller


    def task(self, task):
        # Get task id from database, given task name

        taskId = 0
        record = Record()
        if self.manager.execute(record,"select id from tasks where name = ?",(task,)):
            taskId = int(record[0])
        else:
            self.logger.error("Crawler::task - Could not select task for name %s" % task)

        return taskId                    

    def resource(self, link):

        # Start a new transaction that we can roll back if we fail to add the resource by
        # flushing any pending transactions
        self.manager.commit()
        
        # Convert string url's into links
        if type(link) == str:
            link = Link(link)

        urlId = 0
        resourceId = 0

        record = Record()
        if self.manager.execute(record,"select id from urls where url = ?", (link.url,)):            
            urlId = int(record[0])
        elif self.manager.execute(record,"insert into urls(url,referer,weight,depth) values(?,?,0,?)",(link.url,link.referer,link.depth)):
            urlId = record.id
        else:
            self.logger.error("Crawler::resource - Could not insert or select url \"%s\"" % (link.url))
            self.manager.rollback()
            return 0
            
        if self.manager.execute(record,"select id from resources where url_id = ?",(urlId,)):
            resourceId = int(record[0])
        elif self.manager.execute(record,"insert into resources(url_id,page_id) values(?,0)",(urlId,)):
            resourceId = int(record.id)
        else:
            self.logger.error("Crawler::resource - Could not insert or select resource for url \"%s\"" % link.url)
            self.manager.rollback()
            return 0
        
        # Commit current transaction
        self.manager.commit()
        return resourceId



    def schedule(self, resource, task, priority=0, state=""):
        self.manager.execute("insert into schedules(resource_id,task_id,priority,state) values(?,?,?,?)",(resource,task,priority,state))
        self.manager.commit()

    def download(self, resource, task, priority=0, state=""):
        self.manager.execute("insert into downloads(resource_id,task_id,priority,state) values(?,?,?,?)",(resource,task,priority,state))
        self.manager.commit()


    def run(self):

        self.events = self.events+1

        state = Crawler.STATE_INITIAL

        while self.sentinel:

            record = Record()
            if self.manager.execute(record,"select * from downloads where started is null order by priority desc limit 1"):
                downloadId = int(record[0])     

                started = int(datetime.timestamp(datetime.now()))
                self.manager.execute("update downloads set started = ? where id = ?",(started,downloadId))
                self.manager.commit()

                session = Session()
                self.process_session(record,session)

                if session.link and len(session.page) == 0:
                    
                    self.logger.debug("Crawler::run - Processing session download \"%s\"" % session.name)
                    self.process_download(session)
                    
                    if False and len(session.page) == 0:
                        # If the download failed, allow retry (ideally download would return the http error from
                        # whatever url/request library we are using and we would base the decision on that)
                        self.manager.execute("update downloads set started = null where id = ?",(downloadId,))
                        self.manager.commit()
                    else:
                        finished = int(datetime.timestamp(datetime.now()))
                        self.manager.execute("update downloads set finished = ? where id = ?",(finished,downloadId))
                        self.manager.commit()
        
                if state == Crawler.STATE_SLEEPING:
                    self.sleeping = self.sleeping-1

                state = Crawler.STATE_PROCESSING

            elif self.manager.execute(record,"select * from schedules where started is null order by priority desc limit 1"):
                scheduleId = int(record[0])     

                started = int(datetime.timestamp(datetime.now()))
                self.manager.execute("update schedules set started = ? where id = ?",(started,scheduleId))
                self.manager.commit()

                session = Session()
                self.process_session(record,session)
                                
                if len(session.page) > 0:
                    self.logger.debug("Crawler::run - Processing session schedule \"%s\"" % session.name)
                    self.process_schedule(session)
                else:
                    self.logger.error("Crawler::run - Page size was zero, ignoring session schedule \"%s\"" % session.name)

                finished = int(datetime.timestamp(datetime.now()))
                self.manager.execute("update schedules set finished = ? where id = ?",(finished,downloadId))

                if state == Crawler.STATE_SLEEPING:
                    self.sleeping = self.sleeping-1

                state = Crawler.STATE_PROCESSING
            else:
                if state != Crawler.STATE_SLEEPING:
                    self.sleeping = self.sleeping+1

                if self.sleeping == self.events:
                    
                    self.logger.info("Crawler::run - finalising")
                    self.finalise()

                    time.sleep(5)

                    self.logger.info("Crawler::run - initialising")
                    self.initialise()
                else:

                    self.logger.info("Crawler::run - sleeping")

                    # Short  or long sleep depending on the existing state
                    if state != Crawler.STATE_SLEEPING:
                        time.sleep(0.1)
                    else:
                        time.sleep(1)
    			
                state = Crawler.STATE_SLEEPING
            
		    
            #time.sleep(0.1)
        
        self.events = self.events-1


    def process_session(self, record, session):
        # Called by the main crawler run loop with resource id, task id, priority and state from
        # the downloads or schedule table
    
        # resource, task, priority, state
        session.resource = int(record[1])
        session.task = int(record[2])
        session.priortiy = int(record[3])
        session.state = record[4]
        
        urlId = 0
        pageId = 0
		
        record = Record()
        if not self.manager.execute(record,"select name from tasks where id = ?",(session.task,)):
            self.logger.error("Crawler::process_session - No task matching id %d\n" % session.task)
            return

        session.name = record[0]

        if not self.manager.execute(record,"select url_id, page_id from resources where id = ?",(session.resource,)):
            self.logger.error("Crawler::process_session - No resource matching id %d\n" % session.resource)
            return

        urlId = int(record[0])
        pageId = int(record[1])

        record = Record()
        if not self.manager.execute(record,"select url, referer, depth from urls where id = ?",(urlId,)):
            self.logger.error("Crawler::process_session - No url matching id %d\n" % urlId)
            return

        # Create a link with url, referer, and depth
        session.link = Link(record[0],record[1],record[2])
        
        # Now extract the page content if it exists        
        if self.content.execute(record,"select html from pages where id = ?",(pageId,)):
            html = record[0]            
            html = zlib.decompress(html)

            if type(html) is not str:
                html = html.decode("utf-8")

            session.page = html

            if len(session.page) == 0:
                logger.error("Crawler::processSession - Page size was zero \"%s\"\n" % session.link.url);                  


    def process_schedule(self,session):
        
        if session.name in self.callbacks:
            caller = self.callbacks[session.name]
            caller(session,self)
        else:
            self.logger.error("Crawler::process_schedule - Callback for session name not found \"%s\"\n" % session.name)  
                      

    def process_download(self,session):
        
        # Get the html source of the page
        html = self.process_page(session.link)
        session.page = html
        
        if len(html) < 16:
            self.logger.error("Crawler::process_download - Failed to download page \"%s\"\n" % session.link.url)
            return

        pageId = 0

        try:
                
            # Compress the page content and store it as a binary blob
            if type(html) is not bytes:
                html = html.encode('utf-8')
            html = zlib.compress(html)
            updated = int(datetime.timestamp(datetime.now()))
            record = Record()
            self.content.execute(record,"insert into pages(html,updated) values(?,?)",(html,updated))
            self.content.commit()

            pageId = int(record.id)

            if not self.manager.execute("update resources set page_id = ? where id = ?",(pageId, session.resource)):
                self.logger.error("Crawler::process_download - No resource matching id %d\n" % session.resource)
            else:
                self.manager.commit()

        except Exception as e:

            self.logger.error("Crawler::process_download - Failed to insert page \"%s\"\n%s\n" % (session.link.url, sys.exc_info[0]))

        
        # Schedule the page content for crawling
        self.schedule(session.resource,session.task,session.priority,session.state)      


    def process_page(self, link):

        if link is None or type(link) is str:
            self.logger.error("Crawler::process_page - Invalid link %s\n" % link)
            return
        
        
        # Dont use default urllib headers as many sites will just outright block this or detect
        # a bot and redirect to an api/token login page

        headers={
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'
        }
       
        html = ""

        
        try:
                      
            #req = requests.get(link.url, headers)
            
            req = urllib.request.Request(link.url,data=None,headers=headers)            
            html = urllib.request.urlopen(req).read()	
        except Exception as e:                
            #traceback.print_exc()
            self.logger.error(traceback.format_exception(None, e, e.__traceback__))
            self.logger.error("Crawler::process_page - Could not process request\n%s\n" % sys.exc_info()[0])


        return html	
        




#--------------------------------------------------------------------------------------------------------------------



def YCombinatorComments(session, crawler):
    
    crawler.logger.debug("YCombinatorComments - Got comment !\n")

    soup = BeautifulSoup(session.page, 'html.parser')
    print(yellow(soup.prettify()))


def YCombinatorIndex(session,crawler):

    crawler.logger.debug("YCombinatorComments - Got index !\n")

    soup = BeautifulSoup(session.page, 'html.parser')
    print(magenta(soup.prettify()))
    
    # Get all <a> tags, and add the ones that link to comments to our crawl
    tags = soup.find_all('a')
    for a in tags:
        a = a.get('href').strip()
        if a.startswith("item?"):
            id = a[a.index("="):]
            page = "https://news.ycombinator.com/%s" % a
            crawler.download(crawler.resource(page),crawler.task("ycombinator_comments"),100,id);		



def YCombinatorCrawler(crawler):

    level = 0
    crawler.callback("ycombinator_index",YCombinatorIndex,level+1)
    crawler.callback("ycombinator_comments",YCombinatorComments,level+2)
                
    url = "https://news.ycombinator.com"
    crawler.download(crawler.resource(url),crawler.task("ycombinator_index"),100,"0")


#--------------------------------------------------------------------------------------------------------------------

links = {}
limit = 100

def LinkCrawlerPage(session,crawler):    

    if len(links) > limit:
        print("LinkCrawlerPage - Processing limit reached %s" % limit) 
        return

    link = session.link
    if not link.url in links:
        links[link.url] = set()	

    print("LinkCrawlerPage - Processing (%s/%s) %s" % (len(links),limit,link.url))         

    parent = urllib.parse.urlparse(link.url)

    soup = BeautifulSoup(session.page, 'html.parser')
    tags = soup.find_all('a',{"href" : True})
    for a in tags:
        a = a.get('href').strip()                
        try:
            u = a
            if "?" in u:
                u = u[:u.index("?")]
            if "/" not in u:
                a = urllib.parse.urljoin(link.url,a)
            elif u.startswith("//"):
                a = parent.scheme + ":" + a
            elif u.startswith("/"):
                a = parent.scheme + "://" + parent.netloc + a
        except:
            print("LinkCrawlerPage - Failed to join url %s + %s" % (link.url,a))

        child = None

        try:
            child = urllib.parse.urlparse(a)
        except:
            print("LinkCrawlerPage - Failed to parse url %s" % (a))

        # Same domain policy (dont allow sub-domains), only relative or absolute urls in the same
        # domain that we started with
        if child and (not child.scheme and not child.netloc or child.netloc == parent.netloc):                
            if not child.netloc or not child.scheme:
                try:
                    a = urllib.parse.urljoin(parent,child)
                except:
                    print("LinkCrawlerPage - Failed to parse url %s" % (a))
            
            links[link.url].add(a)
            page = Link(a,link.url,link.depth-1)            
            
            print("LinkCrawlerPage - Downloading %s" % (page.url))   

            crawler.download(crawler.resource(page),crawler.task("linkcrawler_page"),100,"");	



def LinkCrawler(crawler):    

    level = 0
    crawler.callback("linkcrawler_page",LinkCrawlerPage,level+1)

    url = "https://cnn.com"
    crawler.download(crawler.resource(url),crawler.task("linkcrawler_page"),100,"0")



#--------------------------------------------------------------------------------------------------------------------

def main():
	
    usage = """
    Crawler
    --------------------------------------------------------------
    Demo of a stateful, recoverable, fault tollerant web crawler.

    Author:	Emerson Clarke
    Email: emerson.clarke@gmail.com

    The MIT License (MIT)
    Copyright (c) 2021
    """


    parser = OptionParser(usage=usage)
    parser.add_option("-r","--reset",dest="reset",action="store_true",help="reset crawler state")		
    (options, args) = parser.parse_args()


    if options.reset:
    #if True:

        # Drop all tables, delete all data and allow the crawler to restart
        try:                
            manager = Database("manager.db")
            manager.execute("drop table urls")
            manager.execute("drop table tasks")
            manager.execute("drop table resources")
            manager.execute("drop table downloads")
            manager.execute("drop table schedules")
        except:
            pass

        try:
            content = Database("content.db")
            content.execute("drop table pages")
            content.commit()
        except:
            pass

    crawler = Crawler()

    # Crawl the Hacker News
    #ycombinator = YCombinatorCrawler(crawler)

    linkcrawler = LinkCrawler(crawler)

    # Insert Facebook/Reddit or whatever other large site you want to process here, they can 
    # all run in parallel, the crawler is completely task agnostic.

    
    # Flag events so that we remain active
    events = 5

    runner = pool.Pool(events)

    # Spawn the greenlets, conveniently since python manually passes the this pointer as the
    # first argument of every function, passing crawler to its own run function here requires
    # nothing in particular, carry on.
    for e in range(events):
        runner.spawn(Crawler.run,crawler)    

    runner.join()

    # Print the links from the link crawler
    for parent,children in links.items():
        for child in children:
            print("%s -> %s" % (parent,child))


    """
    # Flag 1 event so that we remain active
    #crawler.events = crawler.events+1
    crawler.events = events

    # Start a crawler thread
    #thread = threading.Thread(target=Crawler.run,args=[crawler])
    #thread.start()

    
    while crawler.active():
       
        # Single threaded (blocking)
        crawler.run()

        if os.path.exists("kill"):
            crawler.logger.error("Crawler kill file detected, exiting!")
            crawler.events = crawler.events-1
            crawler.sentinel = False

        time.sleep(60)        
    

    #thread.join()
    """




#--------------------------------------------------------------------------------------------------------------------


if __name__ == "__main__":


    main()
