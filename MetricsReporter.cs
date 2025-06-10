using System.Diagnostics;
public class MetricsReporter
{
    private readonly ulong _duration;
    private readonly Metrics _metrics;
    private readonly ulong _reportDelay;

    public MetricsReporter(ulong duration, Metrics metrics, ulong reportDelay)
    {
        _duration = duration;
        _metrics = metrics;
        _reportDelay = reportDelay;
    }

    public void Start()
    {
        var startTime = Stopwatch.StartNew();
        int loops = 0;

        while (startTime.Elapsed.TotalSeconds < _duration)
        {
            Thread.Sleep(TimeSpan.FromSeconds(_reportDelay));
            loops++;
            var currentDuration = loops * (int)_reportDelay;

            Console.WriteLine($"Report after {currentDuration} seconds");
            Console.WriteLine($"\tNumber of keys written: {_metrics.WriteCount}");
            Console.WriteLine($"\tNumber of keys read: {_metrics.ReadCount}");
            Console.WriteLine($"\tTotal number of operations: {_metrics.TotalOperations}");
            Console.WriteLine($"\tOperations per second: {_metrics.OperationsPerSecond((ulong)currentDuration)}");
            Console.WriteLine($"\tAverage observed latency on write: {_metrics.AverageWriteLatency()} µs");
            Console.WriteLine($"\tAverage observed latency on read: {_metrics.AverageReadLatency()} µs");
        }
    }
}