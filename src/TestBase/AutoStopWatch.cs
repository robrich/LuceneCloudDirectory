namespace Lucene.Net.Store.Cloud.TestBase {
	using System;
	using System.Diagnostics;

	public class AutoStopWatch : IDisposable {
		private Stopwatch _stopwatch;
		private string _message;

		public AutoStopWatch( string message ) {
			this._message = message;
			Debug.WriteLine( String.Format( "{0} starting ", message ) );
			this._stopwatch = Stopwatch.StartNew();
		}

		public void Dispose() {

			this._stopwatch.Stop();
			long ms = this._stopwatch.ElapsedMilliseconds;

			Debug.WriteLine( String.Format( "{0} Finished {1} ms", this._message, ms ) );
		}

	}
}
