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
				var writer = Task.Run(() =>
				{
					int count = 0;
					while (sp.ElapsedMilliseconds < 30 * 1000)
					{
						var now = DateTime.Now;
						using (var w = tss.CreateWriter())
						{
							for (int j = 0; j < 8; j++)
							{
								var key = j.ToString();
								for (int i = 0; i < 1000; i++)
								{
									count++;
									w.AppendHeartbeat(key, now = now.AddMilliseconds(1), 120);

								}
							}
							w.Commit();
						}
					}
					return count;
				});

				var tasks = new List<Task<int>>();
				for (int i = 0; i < 8; i++)
				{
					var key = i.ToString();
					var task = Task.Run(() =>
					{
						using (var reader = tss.CreateReader())
						{
							return reader.Query(key, DateTime.MinValue, DateTime.MaxValue).Count();
						}
					});

					tasks.Add(task);
				}

				Task.WaitAll(tasks.ToArray());

				Console.WriteLine("{0:#,#}", writer.Result);

				Console.WriteLine(sp.ElapsedMilliseconds);

				Console.WriteLine("{0:#,#}", tasks.Sum(x => x.Result));


			}
		}
	}
}
