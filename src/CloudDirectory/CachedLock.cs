namespace Lucene.Net.Store.Cloud {
	using System.Diagnostics;

	/// <summary>
	/// Implements lock semantics on CachedDirectory
	/// </summary>
	public class CachedLock : Lock {
		private readonly ICloudProvider cloudProvider;
		private readonly string lockFile;

		public CachedLock( ICloudProvider CloudProvider, string LockFile ) {
			this.cloudProvider = CloudProvider;
			this.lockFile = LockFile;
		}

		public override bool IsLocked() {
			return this.cloudProvider.IsLocked( this.lockFile );
		}

		public override bool Obtain() {
			return this.cloudProvider.ObtainLock( this.lockFile );
		}

		public override void Release() {
			Debug.Print( "CachedLock:Release({0})", this.lockFile );
			this.cloudProvider.Releaselock( this.lockFile );
		}

	}
}
