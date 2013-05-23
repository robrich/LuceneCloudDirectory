namespace Lucene.Net.Store.Cloud.Models {
	using System;

	public class FileMetadata {
		public string Name { get; set; }
		public bool Exists { get; set; }
		public long Length { get; set; }
		// .ToFileTimeUtc()
		public DateTime LastModified { get; set; }
	}
}