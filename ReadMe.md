LuceneCloudDirectory
====================

This project is a [Lucene.net](http://lucene.apache.org/core/) Directory provider that stores 
the content on a cloud provider and caches content in a local directory.  You use the CachedDirectory 
as you would Lucene's [FSDirectory](http://jsprunger.com/getting-started-with-lucene-net/).  This 
project is a fork of [AzureDirectory](https://azuredirectory.codeplex.com/) with support for other
cloud providers.

Supported Cloud Providers:

- Azure
- Amazon

There is also a CachedFolderProvider that allows you to flex the CachedDirectory without needing to
sign up for an account on one of these cloud providers.

Example Use
-----------

Open up any of the Test projects to see it in use.

1. Install Lucene.net NuGet package

2. Reference the built output from your chosen cloud provider project (e.g. /AmazonDirectory/bin/Debug/Lucene.Net.Store.Cloud.Amazon.dll)

3. Instantiate the CloudProvider (e.g. AmazonCloudProvider)

		string amazonKey = ConfigurationManager.AppSettings["AmazonKey"];
		string amazonSecret = ConfigurationManager.AppSettings["AmazonSecret"];
		string bucket = ConfigurationManager.AppSettings["AmazonBucket"];
		string region = ConfigurationManager.AppSettings["AmazonRegion"];
		
		ICloudProvider provider = new AmazonCloudProvider( amazonKey, amazonSecret, bucket, region );

4. Instantiate the CachedDirectory

		using (CachedDirectory cachedDirectory = new CachedDirectory(provider)) {

5. Use Lucene.net as you normally would

			using (StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_30)) {
				using (IndexWriter indexWriter = GetIndexWriter(cachedDirectory, analyzer)) {
					Document doc = new Document();
					doc.Add(new Field("id", DateTime.Now.ToFileTimeUtc().ToString(), Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.NO));
					doc.Add(new Field("Title", "the title", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.NO));
					doc.Add(new Field("Body", "this is a document body", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.NO));
					indexWriter.AddDocument(doc);
				}
			}

			using (IndexSearcher searcher = new IndexSearcher(cachedDirectory)) {
				SearchForPhrase(searcher, "title");
				SearchForPhrase(searcher, "body");
			}
		}

License
-------

[AzureDirectory](https://azuredirectory.codeplex.com/) is released as MS-PL
[Lucene.net](http://lucene.apache.org/core/) is released as Apache 2.0
This project is released as [Apache 2.0](http://www.apache.org/licenses/LICENSE-2.0)
