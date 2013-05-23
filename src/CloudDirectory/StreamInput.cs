namespace Lucene.Net.Store.Cloud {
	using System.IO;

	/// <summary>
	/// Stream wrapper around IndexInput
	/// </summary>
	public class StreamInput : Stream {
		public IndexInput Input { get; private set; }

		public StreamInput( IndexInput Input ) {
			this.Input = Input;
		}

		public override bool CanRead {
			get { return true; }
		}

		public override bool CanSeek {
			get { return true; }
		}

		public override bool CanWrite {
			get { return false; }
		}

		public override void Flush() {
		}

		public override long Length {
			get { return this.Input.Length(); }
		}

		public override long Position {
			get { return this.Input.FilePointer; }
			set { this.Input.Seek( value ); }
		}

		public override int Read( byte[] buffer, int offset, int count ) {
			long pos = this.Input.FilePointer;
			long len = this.Input.Length();
			if ( count > ( len - pos ) ) {
				count = (int)( len - pos );
			}
			this.Input.ReadBytes( buffer, offset, count );
			return (int)( this.Input.FilePointer - pos );
		}

		public override long Seek( long offset, SeekOrigin origin ) {
			switch ( origin ) {
				case SeekOrigin.Begin:
					this.Input.Seek( offset );
					break;
				case SeekOrigin.Current:
					this.Input.Seek( this.Input.FilePointer + offset );
					break;
				case SeekOrigin.End:
					throw new System.NotImplementedException();
					this.Input.Seek( this.Input.Length() );
			}
			return this.Input.FilePointer;
		}

		public override void SetLength( long value ) {
			throw new System.NotImplementedException();
		}

		public override void Write( byte[] buffer, int offset, int count ) {
			throw new System.NotImplementedException();
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
			if ( this.Input != null ) {
				this.Input.Close();
				this.Input = null;
			}
		}

	}
}
