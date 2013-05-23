namespace Lucene.Net.Store.Cloud {
	using System;
	using System.Collections.Generic;
	using System.IO;
	using System.Diagnostics;
	using System.Linq;
	using Directory = Lucene.Net.Store.Directory;

	/// <summary>
	/// Use CachedDirectory as you would FSDirectory or any other Lucene.net directory
	/// </summary>
	public class CachedDirectory : Directory {
		private readonly ICloudProvider cloudProvider;

		#region CTOR
		/// <summary>
		/// Create a CachedDirectory
		/// </summary>
		/// <param name="CloudProvider">the implimentation for interfacing with the cloud</param>
		/// <param name="Catalog">name of catalog (folder in blob storage)</param>
		/// <param name="CacheDirectory">local Directory object to use for local cache</param>
		public CachedDirectory( ICloudProvider CloudProvider, string Catalog = null, Directory CacheDirectory = null ) {
			if ( CloudProvider == null ) {
				throw new ArgumentNullException( "cloudProvider" );
			}
			this.cloudProvider = CloudProvider;

			string catalog = string.IsNullOrEmpty( Catalog ) ? "lucene" : Catalog.ToLower();

			if ( CacheDirectory != null ) {
				// save it off
				this.CacheDirectory = CacheDirectory;
			} else {
				string cachePath = Path.Combine( Environment.ExpandEnvironmentVariables( "%temp%" ), "LuceneCache" );
				DirectoryInfo cacheDir = new DirectoryInfo( cachePath );
				if ( !cacheDir.Exists ) {
					cacheDir.Create();
				}

				string catalogPath = Path.Combine( cachePath, catalog );
				DirectoryInfo catalogDir = new DirectoryInfo( catalogPath );
				if ( !catalogDir.Exists ) {
					catalogDir.Create();
				}

				this.CacheDirectory = FSDirectory.Open( catalogPath );
			}

			this.cloudProvider.InitializeStorage();
		}
		#endregion

		public void ClearCache() {
			foreach ( string file in this.CacheDirectory.ListAll() ) {
				this.CacheDirectory.DeleteFile( file );
			}
		}

		public Directory CacheDirectory { get; private set; }

		#region DIRECTORY METHODS
		/// <summary>Returns an array of strings, one for each file in the directory. </summary>
		public override string[] ListAll() {
			return (
				from f in this.cloudProvider.ListAll() ?? new List<string>()
				where !string.IsNullOrEmpty( f )
				select this.GetCloudName( f )
			).ToArray();
		}

		/// <summary>Returns true if a file with the given name exists. </summary>
		public override bool FileExists( string name ) {
			// this always comes from the server
			string cloudName = this.GetCloudName( name );
			return this.cloudProvider.FileMetadata( cloudName ).Exists;
		}

		/// <summary>Returns the time the named file was last modified. </summary>
		public override long FileModified( string name ) {
			// this always has to come from the server
			string cloudName = this.GetCloudName( name );
			return this.cloudProvider.FileMetadata( cloudName ).LastModified.ToFileTimeUtc();
		}

		/// <summary>Returns the length of a file in the directory. </summary>
		public override long FileLength( string name ) {
			// TODO: Can we use the cached length?
			string cloudName = this.GetCloudName( name );
			return this.cloudProvider.FileMetadata( cloudName ).Length;
		}

		/// <summary>Set the modified time of an existing file to now. </summary>
		public override void TouchFile( string name ) {
			string cloudName = this.GetCloudName( name );
			this.cloudProvider.Touch( cloudName );
			this.CacheDirectory.TouchFile( name );
		}

		/// <summary>Removes an existing file in the directory. </summary>
		public override void DeleteFile( string name ) {
			string cloudName = this.GetCloudName( name );
			Debug.WriteLine( "DELETE " + cloudName );

			this.cloudProvider.Delete( cloudName );

			if ( this.CacheDirectory.FileExists( name ) ) {
				this.CacheDirectory.DeleteFile( name );
			}
		}

		/// <summary>Creates a new, empty file in the directory with the given name.
		/// Returns a stream writing this file. 
		/// </summary>
		public override IndexOutput CreateOutput( string name ) {
			// FRAGILE: ASSUME: name doesn't have \ in it
			return new CachedIndexOutput( this.cloudProvider, this.CacheDirectory, name );
		}

		/// <summary>Returns a stream reading an existing file. </summary>
		public override IndexInput OpenInput( string name ) {
			// FRAGILE: ASSUME: name doesn't have \ in it
			return new CachedIndexInput( this.cloudProvider, this.CacheDirectory, name );
		}

		private readonly Dictionary<string, CachedLock> _locks = new Dictionary<string, CachedLock>();

		/// <summary>Construct a {@link Lock}.</summary>
		/// <param name="name">the name of the lock file</param>
		public override Lock MakeLock( string name ) {
			lock ( this._locks ) {
				if ( !this._locks.ContainsKey( name ) ) {
					string cloudName = this.GetCloudName( name );
					this._locks.Add( name, new CachedLock( this.cloudProvider, cloudName ) );
				}
				return this._locks[name];
			}
		}

		public override void ClearLock( string name ) {
			lock ( this._locks ) {
				if ( this._locks.ContainsKey( name ) ) {
					this._locks[name].Release();
				}
			}
			this.CacheDirectory.ClearLock( name );
		}

		/// <summary>Closes the store. </summary>
		protected override void Dispose( bool disposing ) {
		}
		#endregion

		private string GetCloudName( string name ) {
			if ( string.IsNullOrEmpty( name ) ) {
				throw new ArgumentNullException( "name" );
			}
			if ( name.Contains( "\\" ) ) {
				name = name.Substring( name.LastIndexOf( '\\' ) + 1 );
			}
			return name;
		}

	}
}
