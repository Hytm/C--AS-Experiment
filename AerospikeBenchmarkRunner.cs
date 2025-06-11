using Aerospike.Client;

public class AerospikeBenchmarkRunner
{
    private readonly string _hostIp;
    private readonly int _port;
    private readonly uint _writeRatio;
    private readonly int _concurrency;
    private readonly ulong _duration;
    private readonly ulong _maxKeys;
    private readonly ulong _reportDelay;
    private readonly ulong _size;
    private readonly bool _update;
    private readonly bool _truncate;
    private readonly bool _docker;

    private readonly AerospikeClient _client;

    public AerospikeBenchmarkRunner(string hostIp, int port, uint writeRatio, int concurrency, ulong duration, ulong maxKeys, ulong reportDelay, ulong size, bool update, bool truncate, bool docker)
    {
        _hostIp = hostIp;
        _port = port;
        _writeRatio = writeRatio;
        _concurrency = concurrency;
        _duration = duration;
        _maxKeys = maxKeys;
        _reportDelay = reportDelay;
        _size = size;
        _update = update;
        _truncate = truncate;
        _docker = docker;

        _client = AerospikeClientHelper.GetInstance(_hostIp, _port, _docker);

        if (!_client.Connected)
        {
            throw new AerospikeException("Failed to initialize Aerospike client. Please check your connection parameters.");
        }
    }

    public void Run()
    {
        var keys = new List<Key>();
        var keysLock = new object();
        var metrics = new Metrics();

        if (_truncate)
        {
            AerospikeClientHelper.CleanSet(_client);
        }

        var writeConcurrency = (int)(_concurrency * _writeRatio / 100);
        if (writeConcurrency < 1)
        {
            AerospikeClientHelper.ReadOnlyPreparation(_client, keys, keysLock, _maxKeys, _size);
        }

        var readConcurrency = _concurrency - writeConcurrency;
        var tasks = new List<Task>();

        var reporter = new MetricsReporter(_duration, metrics, _reportDelay);
        tasks.Add(Task.Run(() => reporter.Start()));

        var keysPerThread = _maxKeys / (ulong)_concurrency;
        if (keysPerThread < 1)
        {
            keysPerThread = 1;
        }
        for (int i = 0; i < writeConcurrency; i++)
        {
            var writer = new AerospikeWriter(_client, _duration, keys, keysLock, metrics, keysPerThread, _size, _update);
            tasks.Add(Task.Run(() => writer.Start()));
        }

        for (int i = 0; i < readConcurrency; i++)
        {
            var reader = new AerospikeReader(_client, _duration, keys, keysLock, metrics);
            tasks.Add(Task.Run(() => reader.Start()));
        }

        Task.WaitAll(tasks.ToArray());

        Console.WriteLine("Benchmark finished");
        Console.WriteLine($"\tNumber of keys written: {metrics.WriteCount}");
        Console.WriteLine($"\tTotal number of operations: {metrics.TotalOperations}");
        Console.WriteLine($"\tOperations per second: {metrics.OperationsPerSecond(_duration)}");
        Console.WriteLine($"\tAverage observed latency on write: {metrics.AverageWriteLatency()} µs");
        Console.WriteLine($"\tAverage observed latency on read: {metrics.AverageReadLatency()} µs");
    }
}
