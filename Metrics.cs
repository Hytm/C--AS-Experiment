public class Metrics
{
    private int _writeCount;
    private int _readCount;
    private long _totalWriteLatency;
    private long _totalReadLatency;

    public int WriteCount => _writeCount;
    public int ReadCount => _readCount;
    public int TotalOperations => _writeCount + _readCount;

    public void IncrementWriteCount()
    {
        Interlocked.Increment(ref _writeCount);
    }

    public void IncrementReadCount()
    {
        Interlocked.Increment(ref _readCount);
    }

    public void AddWriteLatency(long latency)
    {
        Interlocked.Add(ref _totalWriteLatency, latency);
    }

    public void AddReadLatency(long latency)
    {
        Interlocked.Add(ref _totalReadLatency, latency);
    }

    public long AverageWriteLatency()
    {
        return _writeCount == 0 ? 0 : _totalWriteLatency / _writeCount;
    }

    public long AverageReadLatency()
    {
        return _readCount == 0 ? 0 : _totalReadLatency / _readCount;
    }

    public long OperationsPerSecond(ulong duration)
    {
        return duration == 0 ? 0 : TotalOperations / (int)duration;
    }
}