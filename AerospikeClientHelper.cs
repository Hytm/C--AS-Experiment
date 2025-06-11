using Aerospike.Client;

public static class AerospikeClientHelper
{
    public const string Namespace = "test";
    public const string SetName = "benchmark";
    public const string SingleKey = "single_key";
    public const string BinName = "bin1";

    private static AerospikeClient? _instance;

    private static readonly string _hostIp = "127.0.0.1"; // Default host IP
    private static readonly int _port = 3000; // Default port
    private static readonly bool _docker = false; // Default to not using Docker
    
    private static readonly Lock _lock = new();
    private static AerospikeClient CreateClient(string hostIp, int port, bool docker)
    {
        var policy = new ClientPolicy();
        if (docker)
        {
            policy.useServicesAlternate = true;
        }
        policy.maxConnsPerNode = 200; // Set maximum connections per node

        var _client = new AerospikeClient(policy, new Host[] { new Host(hostIp, port) });
        if (!_client.Connected)
        {
            throw new AerospikeException("Failed to connect to Aerospike server");
        }

        return _client;
    }

    public static AerospikeClient GetInstance(string hostIp)
    {
        if (_instance == null)
        {
            lock (_lock)
            {
                if (_instance == null)
                {
                    _instance = CreateClient(hostIp, _port, _docker);
                }
            }
        }
        return _instance;
    }
    
    public static AerospikeClient GetInstance(string hostIp, int port)
    {
        if (_instance == null)
        {
            lock (_lock)
            {
                if (_instance == null)
                {
                    _instance = CreateClient(hostIp, port, _docker);
                }
            }
        }
        return _instance;
    }

    public static AerospikeClient GetInstance(string hostIp, int port, bool docker)
    {
        if (_instance == null)
        {
            lock (_lock)
            {
                if (_instance == null)
                {
                    _instance = CreateClient(hostIp, port, docker);
                }
            }
        }
        return _instance;
    }
    
    public static AerospikeClient GetInstance()
    {
        if (_hostIp == null)
        {
            throw new AerospikeException("Host IP is not set. Use GetInstance(string hostIp) to initialize.");
        }
        if (_port == 0)
        {
            throw new AerospikeException("Port is not set. Use GetInstance(string hostIp, int port) to initialize.");
        }

        if (_instance == null)
        {
            lock (_lock)
            {
                if (_instance == null)
                {
                    _instance = CreateClient(_hostIp, _port, _docker);
                }
            }
        }
        return _instance;
    }

    public static void CleanSet(AerospikeClient client)
    {
        var ip = new InfoPolicy();
        client.Truncate(ip, Namespace, SetName, null);
    }

    public static void ReadOnlyPreparation(AerospikeClient client, List<Key> keys, object keysLock, ulong maxKeys, ulong size)
    {
        var wp = new WritePolicy();
        ulong keysAdded = 0;
        var fakeContent = MakeFakeContent(size);

        Console.WriteLine($"Preparing {maxKeys} read-only keys");
        while (keysAdded < maxKeys)
        {
            var keyValue = Guid.NewGuid().ToString();
            var key = new Key(Namespace, SetName, keyValue);
            var bin = new Bin(BinName, fakeContent);

            try
            {
                client.Put(wp, key, bin);
                keysAdded++;
                lock (keysLock)
                {
                    if (keys.Count < 10000)
                    {
                        keys.Add(key);
                    }
                }
            }
            catch (AerospikeException e)
            {
                Console.WriteLine($"Error writing key {key.userKey}: {e.Message}");
            }
        }
        Console.WriteLine($"Preparation inserted {maxKeys} keys");
    }

    public static byte[] MakeFakeContent(ulong size)
    {
        return new byte[size];
    }
}