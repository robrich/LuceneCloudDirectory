namespace Lucene.Net.Store.Cloud {
	using System.Collections.Generic;
	using System.IO;
	using Lucene.Net.Store.Cloud.Models;

	public interface ICloudProvider {
		void InitializeStorage();
		List<string> ListAll();
		FileMetadata FileMetadata( string name );
		void Delete( string name );
		Stream Download( string name );
		void Upload( string name, Stream content, FileMetadata FileMetadata );
		void Touch( string name );
		bool ObtainLock( string name );
		void Releaselock( string name );
		bool IsLocked( string name );
	}
}
