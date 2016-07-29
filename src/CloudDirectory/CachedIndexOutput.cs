namespace Lucene.Net.Store.Cloud {
	using System;
	using System.IO;
	using System.Diagnostics;
	using System.Threading;
	using Lucene.Net.Store.Cloud.Models;
	using Directory = Lucene.Net.Store.Directory;

	/// <summary>
	/// Implements IndexOutput semantics for a write/append only file
	/// </summary>
	public class CachedIndexOutput : IndexOutput {
		private readonly Directory cacheDirectory;
		private readonly ICloudProvider cloudProvider;
		private readonly string name;
		private readonly IndexOutput indexOutput;
		private readonly Mutex fileMutex;

		public CachedIndexOutput( ICloudProvider CloudProvider, Directory CacheDirectory, string Name ) {
			this.cloudProvider = CloudProvider;
			this.cacheDirectory = CacheDirectory;
			this.name = Name;

			this.fileMutex = BlobMutexManager.GrabMutex( this.name );
			this.fileMutex.WaitOne();
			try {
				// create the local cache one we will operate against...
				this.indexOutput = this.cacheDirectory.CreateOutput( this.name );
			} finally {
				this.fileMutex.ReleaseMutex();
			}
		}

		public override void Flush() {
			this.indexOutput.Flush();
		}

		protected override void Dispose( bool disposing ) {
			this.fileMutex.WaitOne();
			try {
				// make sure it's all written out
				this.indexOutput.Flush();

				long originalLength = this.indexOutput.Length;
				this.indexOutput.Dispose();

				using ( Stream blobStream = new StreamInput( this.cacheDirectory.OpenInput( this.name ) ) ) {
					this.cloudProvider.Upload( this.name, blobStream, new FileMetadata {
						Exists = true,
						LastModified = new DateTime( this.cacheDirectory.FileModified( this.name ) ),
						Length = originalLength
					} );
					Debug.WriteLine( "PUT {0} in cloud", this.name );
				}

#if FULLDEBUG
				Debug.WriteLine( "CLOSED WRITESTREAM " + this.name );
#endif
			} finally {
				this.fileMutex.ReleaseMutex();
			}
		}

		public override long Length {
			get { return this.indexOutput.Length; }
		}

		public override void WriteByte( byte b ) {
			this.indexOutput.WriteByte( b );
		}

		public override void WriteBytes( byte[] b, int length ) {
			this.indexOutput.WriteBytes( b, length );
		}

		public override void WriteBytes( byte[] b, int offset, int length ) {
			this.indexOutput.WriteBytes( b, offset, length );
		}

		public override long FilePointer {
			get { return this.indexOutput.FilePointer; }
		}

		public override void Seek( long pos ) {
			this.indexOutput.Seek( pos );
		}

	}
}
