## HTML/Web Crawling & Scraping Robots



### Stateful, Recoverable, Fault Tollerant Web Crawler

* Python based crawler.py is a stateful web crawler backed by SQLite database that can be adapted to either event or multithreaded design.  Currently uses Gevents to provide setjmp/longjmp coroutine based non blocking IO.

* Saves and recovers session state, extensible to any number of callback driven crawlers running at the same time

```

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
```


### Url Scraper

* Python and C++ (Reason) based implementations of a high performance in memory link crawler.

* Uses efficient graph storage of URL's where each occurs only once as a node and an id, then links are mapped by a separate set of edges which are essentially a tuple of two id's and a count of occurences.  Can be used to render site map/web graph style link
analysis of a page.

* Scraping can be limited by URL depth or total number of pages. Performance is around 20K links per minute (over 100 pages), depending on the origin site and number of embedded links.

```

class Graph
{
public:

	int Id;
	Map<String,int> Nodes;
	Set<long long> Edges;	

	ReadWriteLock Lock;

	Graph():Id(0)
	{}

	bool Has(Url url) 
	{
		ReadWriteLock::ReadLock read(Lock);
		url.Fragment.Clear();
		return Nodes.Contains(url);
	}

	void Add(const Sequence & parent, const Sequence & child)
	{
		ReadWriteLock::WriteLock write(Lock);
		int pid = AddNode(parent);
		int cid = AddNode(child);
		if (pid == cid) return;
		long long edge = (((long long)pid)<<0x20)|cid;
		Edges.Insert(edge);
	}

	int AddNode(Url url) 
	{
		ReadWriteLock::WriteLock write(Lock);
		url.Fragment.Clear();
		Iterand<Mapped<String,int>> it = Nodes.Insert(url,Id+1);
		if (it) ++Id;
		return it().Value();
	}
};



```