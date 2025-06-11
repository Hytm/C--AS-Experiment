using System.Diagnostics;
using Aerospike.Client;

public class AerospikeWriter
{
    private readonly string _hostIp;
    private readonly int _port;
    private readonly bool _docker;
    private readonly ulong _duration;
    private readonly List<Key> _keys;
    private readonly object _keysLock;
    private readonly Metrics _metrics;
    private readonly ulong _maxKeys;
    private readonly ulong _size;
    private readonly bool _update;

    public AerospikeWriter(string hostIp, int port, bool docker, ulong duration, List<Key> keys, object keysLock, Metrics metrics, ulong maxKeys, ulong size, bool update)
    {
        _hostIp = hostIp;
        _port = port;
        _docker = docker;
        _duration = duration;
        _keys = keys;
        _keysLock = keysLock;
        _metrics = metrics;
        _maxKeys = maxKeys;
        _size = size;
        _update = update;
    }

    public void Start()
    {
        var wp = new WritePolicy { commitLevel = CommitLevel.COMMIT_MASTER };
        ulong keysAdded = 0;
        var random = new Random();
        var fakeContent = AerospikeClientHelper.MakeFakeContent(_size);

        var startTime = Stopwatch.StartNew();
        using (var client = AerospikeClientHelper.GetInstance(_hostIp, _port, _docker))
        {
            while (startTime.Elapsed.TotalSeconds < _duration || _duration == 0)
            {
                var bin = new Bin("bin1", fakeContent);
                if (keysAdded < _maxKeys)
                {
                    var keyValue = Guid.NewGuid().ToString();
                    var key = new Key(AerospikeClientHelper.Namespace, AerospikeClientHelper.SetName, keyValue);
                    var stopwatch = Stopwatch.StartNew();
                    try
                    {
                        client.Put(wp, key, bin);
                        _metrics.IncrementWriteCount();

                        lock (_keysLock)
                        {
                            if (_keys.Count < 10000)
                            {
                                _keys.Add(key);
                            }
                        }
                        keysAdded++;
                    }
                    catch (AerospikeException e)
                    {
                        Console.WriteLine($"Error writing key {key.userKey}: {e.Message}\n{e.StackTrace}");
                    }
                    _metrics.AddWriteLatency(stopwatch.ElapsedMicroseconds());
                }
                else
                {
                    if (_update)
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
                            client.Put(wp, key, bin);
                            _metrics.IncrementWriteCount();
                        }
                        catch (AerospikeException e)
                        {
                            Console.WriteLine($"Error updating key {key.userKey}: {e.Message}\n{e.StackTrace}");
                        }
                        _metrics.AddWriteLatency(stopwatch.ElapsedMicroseconds());
                    }
                }
            }
        }
    }
}