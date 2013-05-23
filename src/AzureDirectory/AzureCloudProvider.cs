namespace Lucene.Net.Store.Cloud.Azure {
	using System;
	using System.Collections.Generic;
	using System.Diagnostics;
	using System.IO;
	using System.IO.Compression;
	using System.Linq;
	using System.Threading;
	using Lucene.Net.Store.Cloud;
	using Lucene.Net.Store.Cloud.Models;
	using Microsoft.WindowsAzure.Storage;
	using Microsoft.WindowsAzure.Storage.Blob;

	public class AzureCloudProvider : ICloudProvider {
		private readonly CloudStorageAccount cloudStorageAccount;
		private readonly string catalog;
		private CloudBlobContainer blobContainer;

		public AzureCloudProvider( CloudStorageAccount CloudStorageAccount, string Catalog = null
#if COMPRESSBLOBS
			, bool CompressBlobs = true
#endif
			) {
			if ( cloudStorageAccount == null ) {
				throw new ArgumentNullException( "cloudStorageAccount" );
			}
			this.cloudStorageAccount = CloudStorageAccount;
			this.catalog = !string.IsNullOrEmpty( Catalog ) ? Catalog.ToLower() : "lucene";
#if COMPRESSBLOBS
			this.CompressBlobs = CompressBlobs;
#endif
		}

#if COMPRESSBLOBS
		public bool CompressBlobs { get; private set; }
#endif

		public void InitializeStorage() {
			this.blobContainer = this.cloudStorageAccount.CreateCloudBlobClient().GetContainerReference( this.catalog );
			this.blobContainer.CreateIfNotExists();
		}
		public List<string> ListAll() {
			return (
				from blob in this.blobContainer.ListBlobs()
				select blob.Uri.AbsolutePath.Substring( blob.Uri.AbsolutePath.LastIndexOf( '/' ) + 1 )
			).ToList();
		}
		// Always returns an object even if file doesn't exist
		public FileMetadata FileMetadata( string name ) {
			FileMetadata results = new FileMetadata {
				Name = name
			};
			try {
				CloudBlockBlob blob = this.blobContainer.GetBlockBlobReference( name );
				blob.FetchAttributes();
				results.Exists = true; // else it would've errored
				results.LastModified = blob.Properties.LastModified.Value.UtcDateTime;
				long blobLength;
				if ( long.TryParse( blob.Metadata["CachedLength"], out blobLength ) ) {
					results.Length = blobLength;
				} else {
					results.Length = blob.Properties.Length; // fall back to actual blob size
				}
			} catch ( Exception ) {
				results.Exists = false;
			}
			return results;
		}
		public void Delete( string name ) {
			CloudBlockBlob blob = this.blobContainer.GetBlockBlobReference( name );
			blob.DeleteIfExists();
		}
		public Stream Download( string name ) {
			Stream fileStream = new MemoryStream();
			CloudBlockBlob blob = this.blobContainer.GetBlockBlobReference( name );

#if COMPRESSBLOBS
			if ( this.ShouldCompressFile( name ) ) {
				// then we will get it fresh into local deflatedName 
				// StreamOutput deflatedStream = new StreamOutput(CacheDirectory.CreateOutput(deflatedName));
				MemoryStream deflatedStream = new MemoryStream();

				// get the deflated blob
				blob.DownloadToStream( deflatedStream );

				Debug.WriteLine( "GET {0} RETREIVED {1} bytes", name, deflatedStream.Length );

				// seek back to begininng
				deflatedStream.Seek( 0, SeekOrigin.Begin );

				// open output file for uncompressed contents

				// create decompressor
				DeflateStream decompressor = new DeflateStream( deflatedStream, CompressionMode.Decompress );

				byte[] bytes = new byte[65535];
				int nRead = 0;
				do {
					nRead = decompressor.Read( bytes, 0, 65535 );
					if ( nRead > 0 )
						fileStream.Write( bytes, 0, nRead );
				} while ( nRead == 65535 );
				fileStream.Flush();
				decompressor.Close(); // this should close the deflatedFileStream too

			} else
#endif
			{

				// get the blob
				blob.DownloadToStream( fileStream );

				fileStream.Flush();
				Debug.WriteLine( "GET {0} RETREIVED {1} bytes", name, fileStream.Length );

			}
			return fileStream;
		}
		public void Upload( string name, Stream content, FileMetadata FileMetadata ) {

			Stream blobStream;
			long originalLength = content.Length;

#if COMPRESSBLOBS
			// optionally put a compressor around the blob stream
			if ( this.ShouldCompressFile( name ) ) {
				// unfortunately, deflate stream doesn't allow seek, and we need a seekable stream
				// to pass to the blob storage stuff, so we compress into a memory stream
				MemoryStream compressedStream = new MemoryStream();

				try {
					using ( DeflateStream compressor = new DeflateStream( compressedStream, CompressionMode.Compress, true ) ) {
						// compress to compressedOutputStream
						content.CopyTo( compressor );
					}

					// seek back to beginning of comrpessed stream
					compressedStream.Seek( 0, SeekOrigin.Begin );

					Debug.WriteLine( "COMPRESSED {0} -> {1} {2}% to {3}",
					   originalLength,
					   compressedStream.Length,
					   ((float)compressedStream.Length / (float)originalLength) * 100,
					   name
					);
				} catch {
					// release the compressed stream resources if an error occurs
					compressedStream.Dispose();
					throw;
				}

				blobStream = compressedStream;
			} else
#endif
			{
				blobStream = content;
			}

			try {
				CloudBlockBlob _blob = this.blobContainer.GetBlockBlobReference( name );
				// push the blobStream up to the cloud
				_blob.UploadFromStream( blobStream );

				// set the metadata with the original index file properties
				_blob.Metadata["CachedLength"] = originalLength.ToString();
				_blob.Metadata["CachedLastModified"] = FileMetadata.LastModified.ToString();
				_blob.SetMetadata();

				Debug.WriteLine( "PUT {1} bytes to {0} in cloud", name, blobStream.Length );
			} finally {
				blobStream.Dispose();
			}
		}
		public void Touch( string name ) {
			//BlobProperties props = _blobContainer.GetBlobProperties(name);
			//_blobContainer.UpdateBlobMetadata(props);
			// I have no idea what the semantics of this should be...hmmmm...
			// we never seem to get called
			//SetCachedBlobProperties(props);
		}
		public bool ObtainLock( string _lockFile ) {
			string _leaseid = null;
			if ( this.leases.ContainsKey( _lockFile ) ) {
				_leaseid = this.leases[_lockFile];
			}
			CloudBlockBlob blob = this.blobContainer.GetBlockBlobReference( _lockFile );
			try {
				Debug.Print( "AzureLock:Obtain({0}) : {1}", _lockFile, _leaseid );
				if ( string.IsNullOrEmpty( _leaseid ) ) {
					_leaseid = blob.AcquireLease( TimeSpan.FromSeconds( 60 ), _leaseid );
					Debug.Print( "AzureLock:Obtain({0}): AcquireLease : {1}", _lockFile, _leaseid );

					// keep the lease alive by renewing every 30 seconds
					long interval = (long)TimeSpan.FromSeconds( 30 ).TotalMilliseconds;
					Timer _renewTimer = new Timer( obj => {
						try {
							AzureCloudProvider al = (AzureCloudProvider)obj;
							al.Renew(_lockFile, _leaseid);
						} catch ( Exception err ) {
							Debug.Print( err.ToString() );
						}
					}, this, interval, interval );
					this.timers.Add( _lockFile, _renewTimer );
				}
				return !string.IsNullOrEmpty( _leaseid );
			} catch ( StorageException webErr ) {
				if ( this._handleWebException( blob, webErr, _lockFile ) )
					return this.ObtainLock(_lockFile);
			}
			/*catch (StorageClientException err) {
				if (_handleStorageClientException(blob, err)) {
					return ObtainLock(_lockFile);
				}
			}*/
			return false;
		}
		public void Releaselock( string _lockFile ) {
			string _leaseid = null;
			if ( this.leases.ContainsKey( _lockFile ) ) {
				_leaseid = this.leases[_lockFile];
				this.leases.Remove( _lockFile );
			}
			Debug.Print( "AzureLock:Release({0}) {1}", _lockFile, _leaseid );
			if ( !string.IsNullOrEmpty( _leaseid ) ) {
				CloudBlockBlob blob = this.blobContainer.GetBlockBlobReference( _lockFile );
				blob.ReleaseLease( new AccessCondition { LeaseId = _leaseid } );
				Timer _renewTimer = null;
				if ( this.timers.ContainsKey( _lockFile ) ) {
					_renewTimer = this.timers[_lockFile];
				}
				if ( _renewTimer != null ) {
					_renewTimer.Dispose();
					this.timers.Remove( _lockFile );
				}
			}
		}
		public bool IsLocked( string _lockFile ) {
			ICloudBlob blob = this.blobContainer.GetBlobReferenceFromServer( _lockFile );
			string _leaseid = null;
			if ( this.leases.ContainsKey( _lockFile ) ) {
				_leaseid = this.leases[_lockFile];
			}
			try {
				Debug.Print( "IsLocked() : {0}", _leaseid );
				if ( string.IsNullOrEmpty( _leaseid ) ) {
					string tempLease = blob.AcquireLease( TimeSpan.FromSeconds( 60 ), _leaseid );
					if ( string.IsNullOrEmpty( tempLease ) ) {
						Debug.Print( "IsLocked() : TRUE" );
						return true;
					}
					blob.ReleaseLease( new AccessCondition() { LeaseId = tempLease } );
				}
				Debug.Print( "IsLocked() : {0}", _leaseid );
				return string.IsNullOrEmpty( _leaseid );
			} catch ( StorageException webErr ) {
				if ( this._handleWebException( blob, webErr, _lockFile ) ) {
					return this.IsLocked( _lockFile );
				}
			}
			/*catch (StorageClientException err) {
				if (_handleStorageClientException(blob, err)) {
					return IsLocked(_lockFile);
				}
			}*/
			_leaseid = null;
			return false;
		}

		public void Renew( string _lockFile, string _leaseid ) {
			if ( !string.IsNullOrEmpty( _leaseid ) ) {
				Debug.Print( "AzureLock:Renew({0} : {1}", _lockFile, _leaseid );
				CloudBlockBlob blob = this.blobContainer.GetBlockBlobReference( _lockFile );
				blob.RenewLease( new AccessCondition { LeaseId = _leaseid } );
			}
		}

		private readonly Dictionary<string, string> leases = new Dictionary<string, string>();
		private readonly Dictionary<string, Timer> timers = new Dictionary<string, Timer>();

		private bool _handleWebException( ICloudBlob blob, StorageException err, string _lockFile ) {
			if ( err.RequestInformation.HttpStatusCode == 404 ) {
				this.blobContainer.CreateIfNotExists();
				using ( MemoryStream stream = new MemoryStream() ) {
					using ( StreamWriter writer = new StreamWriter( stream ) ) {
						writer.Write( _lockFile );
						blob.UploadFromStream( stream );
					}
				}
				return true;
			}
			return false;
		}

		/*private bool _handleStorageClientException(ICloudBlob blob, StorageClientException err) {
			switch (err.ErrorCode) {
				case StorageErrorCode.ResourceNotFound:
					blob.UploadText(_lockFile);
					return true;

				case StorageErrorCode.ContainerNotFound:
					// container is missing, we should create it.
					_blobContainer.Delete();
					_azureDirectory.CreateContainer();
					return true;

				default:
					return false;
			}
		}*/

#if COMPRESSBLOBS
		public bool ShouldCompressFile( string path ) {
			if ( !this.CompressBlobs ) {
				return false;
			}

			string ext = Path.GetExtension( path );
			switch ( ext ) {
				case ".cfs":
				case ".fdt":
				case ".fdx":
				case ".frq":
				case ".tis":
				case ".tii":
				case ".nrm":
				case ".tvx":
				case ".tvd":
				case ".tvf":
				case ".prx":
					return true;
				default:
					return false;
			};
		}
#endif

	}
}
