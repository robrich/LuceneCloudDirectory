namespace Lucene.Net.Store.Cloud.Folder {
	using System;
	using System.Collections.Generic;
	using System.Diagnostics;
	using System.IO;
	using System.Linq;
	using Lucene.Net.Store.Cloud;
	using Lucene.Net.Store.Cloud.Models;

	/// <summary>
	/// This is a test implimentation that uses a file-system folder as if it was a cloud folder to flex CloudDirectory without the need for an account on a cloud service
	/// </summary>
	public class CachedFolderCloudProvider : ICloudProvider {
		private readonly string folderPath;

		public CachedFolderCloudProvider( string FolderPath ) {
			if ( FolderPath == null ) {
				throw new ArgumentNullException( "FolderPath" );
			}
			this.folderPath = FolderPath;
		}

		public void InitializeStorage() {
			if ( !Directory.Exists( this.folderPath ) ) {
				Directory.CreateDirectory( this.folderPath );
			}
		}
		public List<string> ListAll() {
			DirectoryInfo di = new DirectoryInfo( this.folderPath );
			List<string> allFiles = (
				from f in di.GetFiles()
				select f.Name
			).ToList();
			return allFiles;
		}
		public FileMetadata FileMetadata( string name ) {
			FileMetadata results = new FileMetadata();
			FileInfo fi = new FileInfo( this.GetFullPath( name ) );
			results.Exists = fi.Exists;
			if ( results.Exists ) {
				results.Length = fi.Length;
				results.LastModified = fi.LastWriteTimeUtc;
			}
			return results;
		}
		public void Delete( string name ) {
			string fullPath = this.GetFullPath( name );
			if ( File.Exists( fullPath ) ) {
				File.Delete( fullPath );
			} // else you wanted it gone and it is
		}
		public Stream Download( string name ) {
			Stream fs = new FileStream( this.GetFullPath( name ), FileMode.Open, FileAccess.Read );
			fs.Position = 0;
			return fs;
		}
		public void Upload( string name, Stream content, FileMetadata FileMetadata ) {
			// FileMode.Create is "create or overwrite" http://www.csharp-examples.net/filestream-open-file/
			using ( Stream fs = new FileStream( this.GetFullPath( name ), FileMode.Create, FileAccess.Write ) ) {
				content.CopyTo( fs );
				fs.Flush();
			}
		}
		public void Touch( string name ) {
			File.SetLastWriteTime( this.GetFullPath( name ), DateTime.Now );
		}
		public bool ObtainLock( string name ) {
			Debug.Assert( name.EndsWith( ".lock" ) );
			if ( this.IsLocked( name ) ) {
				return false;
			}
			using ( MemoryStream ms = new MemoryStream() ) {
				this.Upload( name, ms, new FileMetadata() );
			}
			return true;
		}
		public void Releaselock( string name ) {
			Debug.Assert( name.EndsWith( ".lock" ) );
			this.Delete( name );
		}
		public bool IsLocked( string name ) {
			Debug.Assert( name.EndsWith( ".lock" ) );
			return this.FileMetadata( name ).Exists;
		}

		private string GetFullPath( string name ) {
			if ( string.IsNullOrEmpty( name ) ) {
				throw new ArgumentNullException( "name" );
			}
			if ( name.Contains( "\\" ) ) {
				throw new ArgumentOutOfRangeException( "name", "name contains backslashes: " + name );
			}
			return Path.Combine( this.folderPath, name );
		}
	}
}
