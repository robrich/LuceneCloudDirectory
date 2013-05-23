namespace Lucene.Net.Store.Cloud {
	using System;
	using System.Security.AccessControl;
	using System.Security.Principal;
	using System.Threading;

	public static class BlobMutexManager {

		public static Mutex GrabMutex( string name ) {
			string mutexName = "luceneSegmentMutex_" + name;
			try {
				return Mutex.OpenExisting( mutexName );
			} catch ( WaitHandleCannotBeOpenedException ) {
				SecurityIdentifier worldSid = new SecurityIdentifier( WellKnownSidType.WorldSid, null );
				MutexSecurity security = new MutexSecurity();
				MutexAccessRule rule = new MutexAccessRule( worldSid, MutexRights.FullControl, AccessControlType.Allow );
				security.AddAccessRule( rule );
				bool mutexIsNew = false;
				return new Mutex( false, mutexName, out mutexIsNew, security );
			} catch ( UnauthorizedAccessException ) {
				Mutex m = Mutex.OpenExisting( mutexName, MutexRights.ReadPermissions | MutexRights.ChangePermissions );
				MutexSecurity security = m.GetAccessControl();
				string user = Environment.UserDomainName + "\\" + Environment.UserName;
				MutexAccessRule rule = new MutexAccessRule( user, MutexRights.Synchronize | MutexRights.Modify, AccessControlType.Allow );
				security.AddAccessRule( rule );
				m.SetAccessControl( security );

				return Mutex.OpenExisting( mutexName );
			}
		}

	}
}
