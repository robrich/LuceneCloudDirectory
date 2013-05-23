namespace CachedFolderTestApp {
	using System;
	using System.IO;
	using System.Reflection;
	using Lucene.Net.Store.Cloud;
	using Lucene.Net.Store.Cloud.Folder;
	using Lucene.Net.Store.Cloud.TestBase;

	public class Program : ProgramBase {
		private static void Main( string[] args ) {

			string path = new FileInfo( new Uri( Assembly.GetExecutingAssembly().CodeBase ).LocalPath ).Directory.FullName;
			path = Path.Combine( path, "lucenecloudfolder" ); // FRAGILE: it's in /bin/Debug

			ICloudProvider provider = new CachedFolderCloudProvider( path );

			Program p = new Program();
			p.RunIndexOperations( provider );
		}
	}
}
