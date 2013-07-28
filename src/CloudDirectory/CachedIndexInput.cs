namespace Lucene.Net.Store.Cloud {
	using System;
	using System.IO;
	using System.Diagnostics;
	using System.Threading;
	using Lucene.Net.Store.Cloud.Models;
	using Directory = Lucene.Net.Store.Directory;

	/// <summary>
	/// Implements IndexInput semantics for a read only blob
	/// </summary>
	public class CachedIndexInput : IndexInput {
		private readonly string name;
		private readonly IndexInput indexInput;
		private readonly Mutex fileMutex;

		public CachedIndexInput( ICloudProvider CloudProvider, Directory CacheDirectory, string Name ) {
			this.name = Name;

#if FULLDEBUG
			Debug.WriteLine( "Opening " + this.name );
#endif
			this.fileMutex = BlobMutexManager.GrabMutex( this.name );
			this.fileMutex.WaitOne();
			try {

				bool fFileNeeded = false;
				FileMetadata cloudMetadata = CloudProvider.FileMetadata( this.name );
				if ( !cloudMetadata.Exists ) {
					fFileNeeded = false;
					// TODO: Delete local if it doesn't exist on cloud?
					/*
					if (CacheDirectory.FileExists(this.name)) {
						CacheDirectory.DeleteFile(this.name);
					}
					*/
				} else if ( !CacheDirectory.FileExists( this.name ) ) {
					fFileNeeded = true;
				} else {
					long cachedLength = CacheDirectory.FileLength( this.name );

					long blobLength = cloudMetadata.Length;
					DateTime blobLastModifiedUTC = cloudMetadata.LastModified.ToUniversalTime();

					if ( !cloudMetadata.Exists || cachedLength != blobLength ) {
						fFileNeeded = true;
					} else {
						// there seems to be an error of 1 tick which happens every once in a while 
						// for now we will say that if they are within 1 tick of each other and same length 
						DateTime cachedLastModifiedUTC = new DateTime( CacheDirectory.FileModified( this.name ), DateTimeKind.Local ).ToUniversalTime();
						if ( cachedLastModifiedUTC < blobLastModifiedUTC ) {
							TimeSpan timeSpan = blobLastModifiedUTC.Subtract( cachedLastModifiedUTC );
							if ( timeSpan.TotalSeconds > 1 ) {
								fFileNeeded = true;
							} else {
#if FULLDEBUG
								Debug.WriteLine( "Using cache for " + this.name + ": " + timeSpan.TotalSeconds );
#endif
								// file not needed
							}
						}
					}
				}

				// if the file does not exist
				// or if it exists and it is older then the lastmodified time in the blobproperties (which always comes from the blob storage)
				if ( fFileNeeded ) {
					using ( StreamOutput fileStream = new StreamOutput( CacheDirectory.CreateOutput( this.name ) ) ) {

						Stream blobStream = CloudProvider.Download( this.name );
						blobStream.CopyTo( fileStream );

						fileStream.Flush();
						Debug.WriteLine( "GET {0} RETREIVED {1} bytes", this.name, fileStream.Length );

					}
				} else {
#if FULLDEBUG
					if ( !cloudMetadata.Exists ) {
						Debug.WriteLine( "Cloud doesn't have " + this.name );
					} else {
						Debug.WriteLine( "Using cached file for " + this.name );
					}
#endif
				}

				// open the file in read only mode
				this.indexInput = CacheDirectory.OpenInput( this.name );
			} finally {
				this.fileMutex.ReleaseMutex();
			}
		}

		// ctor for clone
		private CachedIndexInput( CachedIndexInput cloneInput ) {
			this.name = cloneInput.name;
			this.fileMutex = BlobMutexManager.GrabMutex( this.name );
			this.fileMutex.WaitOne();

			try {
#if FULLDEBUG
				Debug.WriteLine( "Creating clone for " + this.name );
#endif
				this.indexInput = cloneInput.indexInput.Clone() as IndexInput;
				// FRAGILE: ASSUME: File is already downloaded and current
			} catch ( Exception ) {
				// sometimes we get access denied on the 2nd stream...but not always. I haven't tracked it down yet but this covers our tail until I do
				Debug.WriteLine( "Dagnabbit, falling back to memory clone for " + this.name );
			} finally {
				this.fileMutex.ReleaseMutex();
			}
		}

		public override byte ReadByte() {
			return this.indexInput.ReadByte();
		}

		public override void ReadBytes( byte[] b, int offset, int len ) {
			this.indexInput.ReadBytes( b, offset, len );
		}

		public override long FilePointer {
			get { return this.indexInput.FilePointer; }
		}

		public override void Seek( long pos ) {
			this.indexInput.Seek( pos );
		}

		protected override void Dispose( bool disposing ) {
			this.fileMutex.WaitOne();
			try {
#if FULLDEBUG
				Debug.WriteLine( "CLOSED READSTREAM local " + this.name );
#endif
				this.indexInput.Dispose();
			} finally {
				this.fileMutex.ReleaseMutex();
			}
		}

		public override long Length() {
			return this.indexInput.Length();
		}

		public override Object Clone() {
			IndexInput clone = null;
			try {
				this.fileMutex.WaitOne();
				clone = new CachedIndexInput( this );
			} finally {
				this.fileMutex.ReleaseMutex();
			}
			Debug.Assert( clone != null );
			return clone;
		}

	}
}
