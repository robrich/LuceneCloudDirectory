namespace Lucene.Net.Store.Cloud.TestBase {
	using System;
	using System.Text;
	using Lucene.Net.Analysis.Standard;
	using Lucene.Net.Documents;
	using Lucene.Net.Index;
	using Lucene.Net.QueryParsers;
	using Lucene.Net.Search;
	using Version = Lucene.Net.Util.Version;

	public class ProgramBase {

		public void RunIndexOperations( ICloudProvider CloudProvider ) {
			try {

				// default CachedDirectory stores cache in local temp folder
				using ( CachedDirectory cachedDirectory = new CachedDirectory( CloudProvider ) ) {
					bool findexExists = IndexReader.IndexExists( cachedDirectory );

					using ( StandardAnalyzer analyzer = new StandardAnalyzer( Version.LUCENE_CURRENT ) ) {
						using ( IndexWriter indexWriter = GetIndexWriter( cachedDirectory, analyzer ) ) {

							indexWriter.SetRAMBufferSizeMB( 10.0 );
							//indexWriter.SetUseCompoundFile(false);
							//indexWriter.SetMaxMergeDocs(10000);
							//indexWriter.SetMergeFactor(100);

							Console.WriteLine( "Total existing docs is {0}", indexWriter.NumDocs() );
							Console.WriteLine( "Creating docs:" );
							int maxDocs = 10000;
							for ( int iDoc = 0; iDoc < maxDocs; iDoc++ ) {
								if ( iDoc % 1000 == 0 ) {
									Console.WriteLine( "- " + iDoc + "/" + maxDocs );
								}
								Document doc = new Document();
								doc.Add( new Field( "id", DateTime.Now.ToFileTimeUtc().ToString(), Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.NO ) );
								doc.Add( new Field( "Title", GeneratePhrase( 10 ), Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.NO ) );
								doc.Add( new Field( "Body", GeneratePhrase( 40 ), Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.NO ) );
								indexWriter.AddDocument( doc );
							}
							Console.WriteLine( "Total docs is {0}", indexWriter.NumDocs() );
						}
					}

					using ( new AutoStopWatch( "Creating searcher" ) ) {
						using ( IndexSearcher searcher = new IndexSearcher( cachedDirectory ) ) {
							SearchForPhrase( searcher, "dog" );
							SearchForPhrase( searcher, _random.Next( 32768 ).ToString() );
							SearchForPhrase( searcher, _random.Next( 32768 ).ToString() );
						}
					}
				}

			} catch ( Exception ex ) {
				Console.WriteLine( "Error: " + ex.Message );
				Console.WriteLine( ex.StackTrace );
			}

			Console.WriteLine( "Push return to exit" );
			Console.Read();
		}

		private IndexWriter GetIndexWriter( CachedDirectory cachedDirectory, StandardAnalyzer analyzer ) {
			IndexWriter indexWriter = null;
			while ( indexWriter == null ) {
				try {
					indexWriter = new IndexWriter( cachedDirectory, analyzer, !IndexReader.IndexExists( cachedDirectory ), new IndexWriter.MaxFieldLength( IndexWriter.DEFAULT_MAX_FIELD_LENGTH ) );
				} catch ( LockObtainFailedException ) {
					Console.WriteLine( "Lock is taken, Hit 'Y' to clear the lock, or anything else to try again" );
					if ( ( Console.ReadLine() ?? "" ).ToLower().Trim() == "y" ) {
						cachedDirectory.ClearLock( "write.lock" );
					}
				}
			}
			Console.WriteLine( "IndexWriter lock obtained, this process has exclusive write access to index" );
			return indexWriter;
		}

		private void SearchForPhrase( IndexSearcher searcher, string phrase ) {
			using ( new AutoStopWatch( string.Format( "Search for {0}", phrase ) ) ) {
				QueryParser parser = new QueryParser( Version.LUCENE_CURRENT, "Body", new StandardAnalyzer( Version.LUCENE_CURRENT ) );
				Query query = parser.Parse( phrase );

				TopDocs hits = searcher.Search( query, 100 );
				Console.WriteLine( "Found {0} results for {1}", hits.TotalHits, phrase );
				int max = hits.TotalHits;
				if ( max > 10 ) {
					max = 10;
					Console.WriteLine( "(showing top " + max + ")" );
				}
				for ( int i = 0; i < max; i++ ) {
					Console.WriteLine( "- " + hits.ScoreDocs[i].Doc );
				}
			}
		}

		private Random _random = new Random( (int)DateTime.Now.Ticks );

		private string[] sampleTerms = {
			"dog", "cat", "car", "horse", "door", "tree", "chair", "microsoft", "apple", "adobe", "google", "golf", "linux", "windows", "firefox", "mouse", "hornet", "monkey", "giraffe", "computer", "monitor",
			"steve", "fred", "lili", "albert", "tom", "shane", "gerald", "chris",
			"love", "hate", "scared", "fast", "slow", "new", "old"
		};

		private string GeneratePhrase( int MaxTerms ) {
			StringBuilder phrase = new StringBuilder();
			int nWords = 2 + _random.Next( MaxTerms );
			for ( int i = 0; i < nWords; i++ ) {
				phrase.AppendFormat( " {0} {1}", sampleTerms[_random.Next( sampleTerms.Length )], _random.Next( 32768 ).ToString() );
			}
			return phrase.ToString();
		}

	}
}
