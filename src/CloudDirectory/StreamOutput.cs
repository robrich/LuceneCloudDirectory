namespace Lucene.Net.Store.Cloud {
	using System;
	using System.IO;

	/// <summary>
	/// Stream wrapper around an IndexOutput
	/// </summary>
	public class StreamOutput : Stream {
		public IndexOutput Output { get; private set; }

		public StreamOutput( IndexOutput Output ) {
			this.Output = Output;
		}

		public override bool CanRead {
			get { return false; }
		}

		public override bool CanSeek {
			get { return true; }
		}

		public override bool CanWrite {
			get { return true; }
		}

		public override void Flush() {
			this.Output.Flush();
		}

		public override long Length {
			get { return this.Output.Length; }
		}

		public override long Position {
			get { return this.Output.FilePointer; }
			set { this.Output.Seek( value ); }
		}

		public override int Read( byte[] buffer, int offset, int count ) {
			throw new NotImplementedException();
		}

		public override long Seek( long offset, SeekOrigin origin ) {
			switch ( origin ) {
				case SeekOrigin.Begin:
					this.Output.Seek( offset );
					break;
				case SeekOrigin.Current:
					this.Output.Seek( this.Output.FilePointer + offset );
					break;
				case SeekOrigin.End:
					throw new System.NotImplementedException();
					this.Output.Seek( this.Output.Length );
			}
			return this.Output.FilePointer;
		}

		public override void SetLength( long value ) {
			throw new NotImplementedException();
		}

		public override void Write( byte[] buffer, int offset, int count ) {
			this.Output.WriteBytes( buffer, offset, count );
		}

		public override void Close() {
			this.Cleanup();
			base.Close();
		}

		protected override void Dispose( bool disposing ) {
			if ( disposing ) {
				this.Cleanup();
			}
			base.Dispose( disposing );
		}

		private void Cleanup() {
			if ( this.Output != null ) {
				this.Output.Flush();
				this.Output.Close();
				this.Output = null;
			}
		}

	}
}
