using System;
using System.Collections.Generic;
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

		public Reader CreateReader()
		{
			return new Reader(this);
		}

		public Writer CreateWriter()
		{
			return new Writer(this);
		}

		public class Heartbeat
		{
			public DateTime At;
			public double Value;
		}

		public class Reader : IDisposable
		{
			private readonly TimeSeriesStorage _storage;
			private readonly Transaction _tx;

			public Reader(TimeSeriesStorage storage)
			{
				_storage = storage;
				_tx = _storage._storageEnvironment.NewTransaction(TransactionFlags.Read);
			}

			public IEnumerable<Heartbeat> Query(string watchId, DateTime start, DateTime end)
			{
				var startBuffer = EndianBitConverter.Big.GetBytes(start.Ticks);
				var endBuffer = EndianBitConverter.Big.GetBytes(end.Ticks);

				var buffer = new byte[8];

				var tree = _tx.State.GetTree(_tx, watchId);
				using (var it = tree.Iterate(_tx))
				{
					it.MaxKey = new Slice(endBuffer);
					if (it.Seek(new Slice(startBuffer)) == false)
						yield break;

					do
					{
						var reader = it.CreateReaderForCurrent();
						reader.Read(buffer, 0, 8);
						yield return new Heartbeat
						{
							At = new DateTime(it.CurrentKey.ToInt64()),
							Value = EndianBitConverter.Big.ToDouble(buffer, 0)
						};
					} while (it.MoveNext());
				}
			}

			public void Dispose()
			{
				if (_tx != null)
					_tx.Dispose();
			}
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