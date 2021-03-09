
using Reason::Structure::Map;
using Reason::Structure::Set;
using Reason::Structure::List;
using Reason::Network::Url;
using Reason::Network::Link;
using Reason::Language::Xpath::XpathNavigator;
using Reason::Language::Xml::XmlDocument;
using Reason::Language::Html::HtmlDocument;

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

class Scraper : public Threaded
{
public:
	

	int Limit;
	int Processed;
	int Running;
	Link Origin;
	Array<Link> Queue;
	class Graph Graph;

	ReadWriteLock Lock;

	Scraper(const Url & url, int depth=0, int limit=0):
		Limit(limit),Processed(0),Running(0),Origin(url,depth)
	{
        // Zero depth means no depth or limit, depth is the url recursion depth
        // and limit is simply the maximum number of total pages to process.	
		Queue.Push(Origin);
	}

	void Print()
	{
		// Print the url graph as we know it by creating a reverse map of the 
        // nodes and then walking the edges
        int nodes = 0;
        Hashmap<int,String> urls;
		Iterand<Mapped<String,int>> n = Graph.Nodes.Forward();
		while (n)
		{
            urls.Insert(n().Value(),n().Key());

            ++nodes;
			++n;
		}

        int edges = 0;
		Iterand<long long> e = Graph.Edges.Forward();
		while (e)
		{
			long long edge = e();
			int pid = (edge>>0x20)&0xFFFFFFFF;
			int cid = edge&0xFFFFFFFF;
			String parent = urls[pid];
			String child = urls[cid];

			printf("%s -> %s\n",parent.Print(),child.Print());   
			
			++edges;
			++e;			
		}

		printf("%d nodes, %d edges\n",nodes,edges);
	}


	void Run(void * thread)
	{
		int id = Thread::Identify();
		int retry=0;
		Link parent;

		while (Queue.Length() > 0 || Running > 0)
		{
			if (Queue.Length() == 0)
			{
				Thread::Sleep(20);
				if (++retry == 100 && Running == 0)
				{
					printf("Scraper::run - [%08d] Retry exceeded, exiting!\n",id);
					return;
				}

				continue;
			}			

			retry=0;
			Atomic::Inc((volatile int*)&Running);
			
			{
				ReadWriteLock::WriteLock write(Lock);	
				//Iterand<Link> queue = Queue.Pop(); // LIFO		
				Iterand<Link> queue = Queue.Pull(); // FIFO
				if (!queue)
					continue;			
				parent = queue();
			}

			parent.Fragment.Clear();
			Graph.AddNode(parent);

			if (parent.Depth > 1 || parent.Depth <= 0)
			{
				printf("Scraper::run - [%08d] Downloading: %s\n",id,parent.Data);

				HtmlDocument document;			
				Curl(parent).Download(document.Resource);

				if (document.Construct())
				{
					XpathNavigator nav(&document);
					if (nav.Select("//a/@href") && nav.IsAttributes())
					{
						for (nav.Forward();nav.Has();nav.Move())
						{
							String value = ((XmlAttribute*)nav())->Value;
							XmlEntity::Decode(value);
							Url url = value;

							if (url.IsRelative())
								url.Absolute(parent);

							// Same domain policy (dont allow sub-domains), only relative or absolute 
							// urls in the same domain that we started with
							if (url.Host == parent.Host)
							{
								Link child(url,parent.Depth-1);
								child.Referer = parent;
								child.Fragment.Clear();

								if (!Graph.Has(child))
								{
									ReadWriteLock::WriteLock write(Lock);	
									Queue.Push(child);
								}

								Graph.Add(parent,child);
							}
						}
					}
				}
				else
				{
					printf("Scraper::run - [%08d] Could not download link %s\n", id, parent.Print());
				}
			}
			
			Atomic::Inc((volatile int*)&Processed);
			Atomic::Dec((volatile int*)&Running);

			if (Processed >= Limit)
			{
				printf("Scraper::run - [%08d] Processing limit reached %d\n", id, Processed);
				return;
			}
		}
	}

};





int main(int argc, char* argv[])
{

	{
		// Point the URL scraper at a url and go, second argument is url depth limit, last argument is page limit
		// but theres no limit on the number of urls found within that depth or page limit.

		//Url url = "https://news.ycombinator.com";
		//Url url = "https://osnews.com";
		//Url url = "https://slashdot.org";
		Url url = "https://cnn.com";
		
		Scraper scraper(url,0,100);

		Timer timer;
		
		int threads = 100;
		//int threads = 10;
		//int threads = 1;
		
		for (int t=0;t<threads;++t)
			scraper.Start();

		for (int t=0;t<threads;++t)
			scraper.Threads[t].Join();

		scraper.Print();

		printf("Scraper completed in %g seconds\n",timer.ElapsedSeconds());
		
		
		exit(0);
	}

}