using Aerospike.Client;

public static class AerospikeClientHelper
{

    public const string Namespace = "test";
    public const string SetName = "benchmark";
    public const string SingleKey = "single_key";
    public const string BinName = "bin1";

    private static AerospikeClient _instance;

    private static string _hostIp;
    private static int _port = 3000; // Default port
    private static bool _docker = false; // Default to not using Docker
    private static bool _log = false; // Default to not logging
    private static readonly object _lock = new object();
    private static AerospikeClient createClient(string hostIp, int port, bool docker, bool log)
    {
        if (log)
        {
            Log.SetLevel(Log.Level.DEBUG);
            Log.SetCallback(LogCallback);
        }   
        
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
    
    private static void LogCallback(Log.Level level, String message)
    {
        if (level == Log.Level.DEBUG)
        {
            Console.WriteLine($"DEBUG: {message}");
        }
        else if (level == Log.Level.INFO)
        {
            Console.WriteLine($"INFO: {message}");
        }
        else if (level == Log.Level.WARN)
        {
            Console.WriteLine($"WARN: {message}");
        }
        else if (level == Log.Level.ERROR)
        {
            Console.WriteLine($"ERROR: {message}");
        }
    }

    public static AerospikeClient GetInstance(string hostIp, bool log)
    {
        if (_instance == null)
        {
            lock (_lock)
            {
                if (_instance == null)
                {
                    _instance = createClient(hostIp, _port, _docker, log);
                }
            }
        }
        return _instance;
    }
    
    public static AerospikeClient GetInstance(string hostIp, int port, bool log)
    {
        if (_instance == null)
        {
            lock (_lock)
            {
                if (_instance == null)
                {
                    _instance = createClient(hostIp, port, _docker, log);
                }
            }
        }
        return _instance;
    }

    public static AerospikeClient GetInstance(string hostIp, int port, bool docker, bool log)
    {
        if (_instance == null)
        {
            lock (_lock)
            {
                if (_instance == null)
                {
                    _instance = createClient(hostIp, port, docker, log);
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
                    _instance = createClient(_hostIp, _port, _docker, _log);
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