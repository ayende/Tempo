using System;
using Tempo;
using Voron;

namespace Tryouts
{
	class Program
	{
		static void Main()
		{
			using (var tss = new TimeSeriesStorage(StorageEnvironmentOptions.CreateMemoryOnly()))
			{
				using (var w = tss.CreateWriter())
				{
					w.AppendHeartbeat("1234", DateTime.Now, 120);
					w.AppendHeartbeat("4321", DateTime.Now, 98);

					w.Commit();
				}
			}
		}
	}
}
