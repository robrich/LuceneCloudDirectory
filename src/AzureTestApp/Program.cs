namespace AzureTestApp {
	using Lucene.Net.Store.Cloud.Azure;
	using Lucene.Net.Store.Cloud.TestBase;
	using Microsoft.WindowsAzure;
	using Microsoft.WindowsAzure.Storage;

	public class Program : ProgramBase {
		private static void Main( string[] args ) {

			// default CachedDirectory stores cache in local temp folder
			CloudStorageAccount cloudStorageAccount = CloudStorageAccount.DevelopmentStorageAccount;
			CloudStorageAccount.TryParse( CloudConfigurationManager.GetSetting( "blobStorage" ), out cloudStorageAccount );
			AzureCloudProvider provider = new AzureCloudProvider( cloudStorageAccount );

			Program p = new Program();
			p.RunIndexOperations( provider );
		}
	}
}
