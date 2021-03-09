"""

    And in memory URL scraper


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

from gevent import monkey, pool
monkey.patch_all()

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
import urllib.parse

import zlib
#import zipfile
#from zipfile import ZipFile

import traceback
import requests
from bs4 import BeautifulSoup

#import logging
#logging.basicConfig(filename='crawler.log', encoding='utf-8', level=logging.DEBUG)


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


class Link:

    def __init__(self,url="",referer="",depth=0):
        self.url = url
        self.referer = referer
        self.depth = depth
        self.retry = 0


class Graph:

    def __init__(self):
        
        self.id = 0
        self.nodes = {}
        self.edges = {}
        #self.edges = set()
        

    def has(self,url):
        (url, frag) = urllib.parse.urldefrag(url)
        return url in self.nodes

    def add(self,parent,child):
        pid = self.add_node(parent)
        cid = self.add_node(child)
        
        # Ignore self references
        if pid == cid:
            return

        # We could use a set, but may as well store the number of times parent links
        # to child, just in case its interesting. Create a tuple and map to count.
        tid = (pid,cid)

        try:
            self.edges[tid] += 1
        except:
            self.edges[tid] = 1            


    def add_node(self,url):
        # We will remove fragmets from urls, but keep queries          
        (url, frag) = urllib.parse.urldefrag(url)
        try:
            id = self.nodes[url]
            return id
        except:            
            self.id +=1
            self.nodes[url] = self.id
            return self.id


class Scraper:
    
    def __init__(self,url,depth=0,limit=0):

        # Zero depth means no depth or limit, depth is the url recursion depth
        # and limit is simply the maximum number of total pages to process.

        self.logger = Logger()
        
        self.limit = limit
        self.processed = 0
        self.running = 0

        self.origin = Link(url,"",depth)
        self.queue = []
        self.queue.append(self.origin)

        # Graph structure of urls
        self.graph = Graph()
        self.links = {}


    
    """
    def print(self):
        # Print the links from the link crawler
        nodes = 0
        edges = 0

        for parent,children in self.links.items():
            nodes += 1
            for child in children:
                edges += 1
                print("%s -> %s" % (parent,child))
        
        print(blue("%s nodes, %s edges" % (nodes,edges)))                

    """
    def print(self):
        # Print the url graph as we know it by creating a reverse map of the 
        # nodes and then walking the edges

        nodes = 0
        urls = {}
        for url,id in self.graph.nodes.items():
            urls[id] = url
            nodes += 1

        edges = 0
        for t in self.graph.edges:
            try:
                parent = urls[t[0]]
                child = urls[t[1]]
            except:
                pass

            edges += 1
            print(blue(parent),white("->"),cyan(child))        

        print(blue("%s nodes, %s edges" % (nodes,edges)))
    

    def run(self):
        
        while self.queue or self.running > 0:

            # Some basic effort to make sure that events which are waiting on other events dont exit
            if not self.queue:
                # Relinquish time slice
                time.sleep(0.1)
                continue

            self.running += 1

            self.logger.warning("Scraper::run - Processing (%s/%s)" % (self.processed,self.limit))  

            # Get the next link from the queue
            link = self.queue.pop(0)
            html = ""

            # Process page download for link
            try:
                html = self.process_page(link)
            except Exception as e:
                self.logger.error(traceback.format_exception(None, e, e.__traceback__))
                self.logger.error("Scraper::process - Could not process request\n%s" % sys.exc_info()[0])  

                # Retry each url 5 times, this may be because we are hitting the server too hard, or Gevent
                # is making urllib fail in interesting ways.
                if link.retry < 5:
                    link.retry += 1
                    self.queue.append(link)
            
            
            # Process links in page if we got some content
            if len(html) > 0:                

                try:
                    self.process_links(link,html)
                except Exception as e:
                    self.logger.error(traceback.format_exception(None, e, e.__traceback__))
                    self.logger.error("Scraper::run - Could not process links\n%s" % sys.exc_info()[0])  

                self.processed += 1

            self.running -= 1
            
            if self.processed >= self.limit:
                self.logger.warning("Scraper::run - Processing limit reached %s" % self.processed)  
                return



    def process_page(self,link):

        self.logger.info("Scraper::process_page - Processing page for %s" % link.url)

        headers={
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'
        }
       
        html = ""
        req = urllib.request.Request(link.url,data=None,headers=headers)            
        html = urllib.request.urlopen(req).read()	
        return html	

    
    def process_links(self,link,html):
        
        self.logger.info("Scraper::process_links - Processing links for %s" % link.url)

        # Ensure that the parent link is added to the graph, as we dont want to re-process
        # this and its likely to occur in the page, especially if its the root url
        self.graph.add_node(link.url)
        #(url,frag) = urllib.parse.urldefrag(link.url)
        #if not url in self.links:            
        #    self.links[url] = set()	


        if link.depth > 1 or link.depth <= 0:

            parent = urllib.parse.urlparse(link.url)

            soup = BeautifulSoup(html, 'html.parser')
            tags = soup.find_all('a',{"href" : True})

            for a in tags:
                a = a.get('href').strip()                
                # Well, that's dissapointing.  The urlparse cant handle "news" as a relative url
                # so we have to fix things up a bit first so it knows whats going on.
                # First try failed since "from?site=github.com/git" is a relative url with no scheme
                # or path, so try harder and look at everything before "?"
                # Actually, urllib cant parse most stuff, but fixing that is out of scope for now.
                try:
                    # Relative urls, try to detect no path or scheme the easy way
                    u = a
                    if "?" in u:
                        u = u[:u.index("?")]
                    # After trimming the query part 
                    if "/" not in u:
                        a = urllib.parse.urljoin(link.url,a)
                    elif u.startswith("//"):
                        # From slashdot.org, urls starting with //slashdot.org
                        a = parent.scheme + ":" + a
                    elif u.startswith("/"):
                        # More basic relative urls
                        a = parent.scheme + "://" + parent.netloc + a
                except:
                    self.logger.error("Scraper::process_links - Failed to join url %s + %s" % (link.url,a))

                child = None

                try:
                    child = urllib.parse.urlparse(a)
                except:
                    self.logger.error("Scraper::process_links - Failed to parse url %s" % (a))

                # Same domain policy (dont allow sub-domains), only relative or absolute urls in the same
                # domain that we started with
                if child and (not child.scheme and not child.netloc or child.netloc == parent.netloc):                
                    if not child.netloc or not child.scheme:
                        try:
                            a = urllib.parse.urljoin(parent,child)
                        except:
                            self.logger.error("Scraper::process_links - Failed to parse url %s" % (a))
                    
                    l = Link(a,link.url,link.depth-1)

                    # Skip the queue if the link is in the graph
                    if not self.graph.has(l.url):
                    #(lurl,frag) = urllib.parse.urldefrag(l.url)
                    #if not lurl in self.links:

                        self.logger.debug("Scraper::process_links - Queueing link %s" % l.url)
                        self.queue.append(l)
                        
                        #self.links[lurl] = set()
                    
                    # We can add the child link to the graph here, along with its edge to parent.  If parent
                    # and child are the same url, the graph will just ignore it.
                    self.graph.add(link.url,l.url)
                    #self.links[url].add(lurl)


    

#--------------------------------------------------------------------------------------------------------------------

def main():
	
    usage = """
    Scraper
    --------------------------------------------------------------
    An in memory URL scraper.

    Author:	Emerson Clarke
    Email: emerson.clarke@gmail.com

    The MIT License (MIT)
    Copyright (c) 2021
    """

    # Point the URL scraper at a url and go, second argument is url depth limit, last argument is page limit
    # but theres no limit on the number of urls found within that depth or page limit.

    #url = "https://news.ycombinator.com"
    #url = "https://osnews.com"
    #url = "https://slashdot.org"
    url = "https://cnn.com"
    scraper = Scraper(url,0,100)


    start = time.time()

    """
    # Single threaded
    scraper.run()
    scraper.print()
    """
    
    # Event driven
    #events = 50
    events = 20

    runner = pool.Pool(events)
    for e in range(events):
        runner.spawn(Scraper.run,scraper)
    runner.join()    
    scraper.print()

    stop = time.time()
    print(blue("%s seconds" % (stop-start)))





#--------------------------------------------------------------------------------------------------------------------


if __name__ == "__main__":


    main()
