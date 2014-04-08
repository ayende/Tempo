using System;
using System.Diagnostics;
using Tempo;
using Voron;

namespace Tryouts
{
	class Program
	{
		static void Main()
		{
			using (var tss = new TimeSeriesStorage(StorageEnvironmentOptions.ForPath("test")))
			{
				var sp = Stopwatch.StartNew();

				int count = 0;
				int txCount = 0;
				DateTime now = DateTime.Now;
				while (sp.ElapsedMilliseconds < 5 * 1000)
				{
					using (var w = tss.CreateWriter())
					{
						for (int i = 0; i < 1; i++)
						{
							now = now.AddMinutes(1);
							//w.AppendHeartbeat("one", now, i);
							//w.AppendHeartbeat("two", now, i);
							//w.AppendHeartbeat("three", now, i);
							//w.AppendHeartbeat("four", now, i);
							w.AppendHeartbeat("five", now, i);
							count += 1;
						}
						txCount++;
						w.Commit();
					}
				}

				sp.Stop();

				Console.WriteLine(sp.Elapsed);
				Console.WriteLine("Num: {0:#,#}, Tx: {1:#,#}", count, txCount);

				Console.WriteLine(Math.Round((double)count / sp.ElapsedMilliseconds, 4));

			}
		}
	}
}
