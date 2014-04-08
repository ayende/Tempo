using System;
using System.Diagnostics;
using System.Linq;
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

				using (var reader = tss.CreateReader())
				{
					double sum = 0;
					var count = 0L;

					var sp = Stopwatch.StartNew();
					foreach (var heartbeat in reader.Query("five", DateTime.MinValue, DateTime.MaxValue))
					{
						sum += heartbeat.Value;
						count++;
					}

					Console.WriteLine("{0:#,#}", sp.ElapsedMilliseconds);
					Console.WriteLine("{0:#,#}", count);
					Console.WriteLine(sum);

				}


				

			}
		}
	}
}
