namespace AmazonTestApp {
	using System.Configuration;
	using Lucene.Net.Store.Cloud;
	using Lucene.Net.Store.Cloud.Amazon;
	using Lucene.Net.Store.Cloud.TestBase;

	public class Program : ProgramBase {
		private static void Main( string[] args ) {

			// TODO: Fill in these settings in App.config
			string amazonKey = ConfigurationManager.AppSettings["AmazonKey"];
			string amazonSecret = ConfigurationManager.AppSettings["AmazonSecret"];
			string bucket = ConfigurationManager.AppSettings["AmazonBucket"]; // Must be unique among all buckets in the region
			ICloudProvider provider = new AmazonCloudProvider( amazonKey, amazonSecret, bucket );

			Program p = new Program();
			p.RunIndexOperations( provider );
		}
	}
}
