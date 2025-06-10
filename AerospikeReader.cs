using System.Diagnostics;
using Aerospike.Client;
public class AerospikeReader
{
    private readonly AerospikeClient _client;
    private readonly ulong _duration;
    private readonly List<Key> _keys;
    private readonly object _keysLock;
    private readonly Metrics _metrics;

    public AerospikeReader(AerospikeClient client, ulong duration, List<Key> keys, object keysLock, Metrics metrics)
    {
        _client = client;
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
                _client.Get(rp, key);
                _metrics.IncrementReadCount();
            }
            catch (AerospikeException e)
            {
                Console.WriteLine($"Error reading key {key.userKey}: {e.Message}");
            }
            _metrics.AddReadLatency(stopwatch.ElapsedMicroseconds());
        }
    }
}