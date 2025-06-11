using System.Diagnostics;
using Aerospike.Client;
public class AerospikeReader
{
    private readonly string _hostIp;
    private readonly int _port;
    private readonly bool _docker;
    private readonly ulong _duration;
    private readonly List<Key> _keys;
    private readonly object _keysLock;
    private readonly Metrics _metrics;

    public AerospikeReader(string hostIp, int port, bool docker, ulong duration, List<Key> keys, object keysLock, Metrics metrics)
    {
        _hostIp = hostIp;
        _port = port;
        _docker = docker;
        _duration = duration;
        _keys = keys;
        _keysLock = keysLock;
        _metrics = metrics;
    }

    public void Start()
    {
        var rp = new Policy();
        var random = new Random();

        var startTime = Stopwatch.StartNew();
        using (var client = AerospikeClientHelper.GetInstance(_hostIp, _port, _docker))
        {
            while (startTime.Elapsed.TotalSeconds < _duration)
            {
                Key key;
                lock (_keysLock)
                {
                    if (_keys.Count > 0)
                    {
                        key = _keys[random.Next(_keys.Count)];
                    }
                    else
                    {
                        continue;
                    }
                }
                var stopwatch = Stopwatch.StartNew();
                try
                {
                    client.Get(rp, key);
                    _metrics.IncrementReadCount();
                }
                catch (AerospikeException e)
                {
                    Console.WriteLine($"Error reading key {key.userKey}: {e.Message}\n{e.StackTrace}");
                }
                _metrics.AddReadLatency(stopwatch.ElapsedMicroseconds());
            }
        }
    }
}