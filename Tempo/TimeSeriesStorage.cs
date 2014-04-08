using System;
using System.IO;
using Voron;
using Voron.Impl;
using Voron.Util.Conversion;

namespace Tempo
{
	public class TimeSeriesStorage : IDisposable
	{
		private readonly StorageEnvironment _storageEnvironment;
		public Guid Id { get; private set; }

		public TimeSeriesStorage(StorageEnvironmentOptions options)
		{
			_storageEnvironment = new StorageEnvironment(options);

			using (var tx = _storageEnvironment.NewTransaction(TransactionFlags.ReadWrite))
			{
				var metadata = _storageEnvironment.CreateTree(tx, "$metadata");
				var result = metadata.Read(tx, "id");
				if (result == null) // new db{
				{
					Id = Guid.NewGuid();
					metadata.Add(tx, "id", new MemoryStream(Id.ToByteArray()));
				}
				else
				{
					Id = new Guid(result.Reader.ReadBytes(16));
				}
			}
		}

		public Writer CreateWriter()
		{
			return new Writer(this);
		}

		public class Writer : IDisposable
		{
			private readonly TimeSeriesStorage _storage;
			private readonly Transaction _tx;

			public Writer(TimeSeriesStorage storage)
			{
				_storage = storage;
				_tx = _storage._storageEnvironment.NewTransaction(TransactionFlags.ReadWrite);
			}

			public void AppendHeartbeat(string watchId, DateTime time, double value)
			{
				var tree = _tx.State.GetTree(_tx, watchId);
				var key = EndianBitConverter.Big.GetBytes(time.Ticks);
				var valueAsBytes = EndianBitConverter.Big.GetBytes(value);

				tree.Add(_tx, new Slice(key), new MemoryStream(valueAsBytes));
			}

			public void Commit()
			{
				_tx.Commit();
			}

			public void Dispose()
			{
				if (_tx != null)
					_tx.Dispose();
			}
		}

		public void Dispose()
		{
			if (_storageEnvironment != null)
				_storageEnvironment.Dispose();
		}
	}
}