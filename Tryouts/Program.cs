using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
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

				var tasks = new List<Task>();

				int count = 0;
				for (int i = 0; i < 8; i++)
				{
					var key = i.ToString();
					tasks.Add(Task.Run(() =>
					{
						var now = DateTime.Now;
						while (sp.ElapsedMilliseconds <30* 1000)
						{
							now = now.AddMinutes(1);
							using (var w = tss.CreateWriter())
							{
								w.AppendHeartbeat(key, now, 5);
								w.Commit();
								Interlocked.Increment(ref count);
							}
						}
					}));
				}

				Task.WaitAll(tasks.ToArray());


				Console.WriteLine(sp.Elapsed);
				Console.WriteLine("{0:#,#}", count);



			}
		}
	}
}
