using System.Diagnostics;
using Aerospike.Client;
using System.CommandLine;
using System.CommandLine.Invocation;

public class Program
{    
    static async Task Main(string[] args)
    {
        var rootCommand = new RootCommand("Aerospike Benchmark Tool - Benchmarks Aerospike with read and write operations");

        var hostIpOption = new Option<string>(
            name: "--host-ip",
            description: "The host IP address",
            getDefaultValue: () => "127.0.0.1");
        hostIpOption.AddAlias("-H");

        var portOption = new Option<int>(
            name: "--port",
            description: "The port number",
            getDefaultValue: () => 3000);
        portOption.AddAlias("-p");

        var writeRatioOption = new Option<uint>(
            name: "--write-ratio",
            description: "The write ratio",
            getDefaultValue: () => 20);
        writeRatioOption.AddAlias("-w");

        var concurrencyOption = new Option<int>(
            name: "--concurrency",
            description: "The concurrency level",
            getDefaultValue: () => 2);
        concurrencyOption.AddAlias("-c");

        var durationOption = new Option<ulong>(
            name: "--duration",
            description: "The duration of the benchmark in seconds",
            getDefaultValue: () => 0);
        durationOption.AddAlias("-d");

        var maxKeysOption = new Option<ulong>(
            name: "--max-keys",
            description: "The maximum number of keys",
            getDefaultValue: () => 1000000);
        maxKeysOption.AddAlias("-m");

        var reportDelayOption = new Option<ulong>(
            name: "--report-delay",
            description: "The delay between reports in seconds",
            getDefaultValue: () => 10);
        reportDelayOption.AddAlias("-r");

        var sizeOption = new Option<ulong>(
            name: "--size",
            description: "The size of the data",
            getDefaultValue: () => 2048);
        sizeOption.AddAlias("-s");

        var updateOption = new Option<bool>(
            name: "--update",
            description: "Whether to update existing keys",
            getDefaultValue: () => true);
        updateOption.AddAlias("-u");

        var truncateOption = new Option<bool>(
            name: "--truncate",
            description: "Whether to truncate the set before starting",
            getDefaultValue: () => false);
        truncateOption.AddAlias("-t");

        var dockerOption = new Option<bool>(
            name: "--docker",
            description: "Whether running in a Docker environment",
            getDefaultValue: () => false);
        dockerOption.AddAlias("-D");

        var logOption = new Option<bool>(
            name: "--log",
            description: "Whether running in with Logging enabled in Debug mode",
            getDefaultValue: () => false);
        dockerOption.AddAlias("-l");

        rootCommand.AddOption(hostIpOption);
        rootCommand.AddOption(portOption);
        rootCommand.AddOption(writeRatioOption);
        rootCommand.AddOption(concurrencyOption);
        rootCommand.AddOption(durationOption);
        rootCommand.AddOption(maxKeysOption);
        rootCommand.AddOption(reportDelayOption);
        rootCommand.AddOption(sizeOption);
        rootCommand.AddOption(updateOption);
        rootCommand.AddOption(truncateOption);
        rootCommand.AddOption(dockerOption);
        rootCommand.AddOption(logOption);

        rootCommand.SetHandler((InvocationContext context) =>
        {
            var hostIp = context.ParseResult.GetValueForOption(hostIpOption);
            var port = context.ParseResult.GetValueForOption(portOption);
            var writeRatio = context.ParseResult.GetValueForOption(writeRatioOption);
            var concurrency = context.ParseResult.GetValueForOption(concurrencyOption);
            var duration = context.ParseResult.GetValueForOption(durationOption);
            var maxKeys = context.ParseResult.GetValueForOption(maxKeysOption);
            var reportDelay = context.ParseResult.GetValueForOption(reportDelayOption);
            var size = context.ParseResult.GetValueForOption(sizeOption);
            var update = context.ParseResult.GetValueForOption(updateOption);
            var truncate = context.ParseResult.GetValueForOption(truncateOption);
            var docker = context.ParseResult.GetValueForOption(dockerOption);
            var log = context.ParseResult.GetValueForOption(logOption);

            Console.WriteLine($"Starting load with parameters:\n\tHost IP: {hostIp}\n\tPort: {port}\n\tWrite Ratio: {writeRatio}\n\tConcurrency: {concurrency}\n\tDuration: {duration} seconds\n\tMaximum keys: {maxKeys}\n\tReport delay: {reportDelay} seconds\n\tObject size: {size}");

            var runner = new AerospikeBenchmarkRunner(hostIp ?? "127.0.0.1", port, writeRatio, concurrency, duration, maxKeys, reportDelay, size, update, truncate, docker, log);
            runner.Run();
        });

        await rootCommand.InvokeAsync(args);
    }
}