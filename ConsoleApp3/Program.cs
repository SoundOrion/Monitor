using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.CommandLine;
using System.CommandLine.Invocation;
using System.Globalization;
using System.Linq;
using System.Management; // NuGet: System.Management
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.IO;

public class Program
{
    public static async Task<int> Main(string[] args)
    {
        //var hostsOpt = new Option<string>("--hosts", () => "hosts.txt", "ホスト一覧(1行1ホスト)");
        //var servicesOpt = new Option<string?>("--services", description: "共通サービス一覧(1行1サービス)");
        //var mapOpt = new Option<string?>("--map", description: "ホスト別サービスのCSV (Host,Service)。指定時は --services を無視");
        //var degOpt = new Option<int>("--degree", () => 96, "最大並列数");
        //var timeoutOpt = new Option<string>("--timeout", () => "3s", "タイムアウト(例: 3s, 1500ms)");
        //var retriesOpt = new Option<int>("--retries", () => 1, "失敗リトライ回数");
        //var csvOpt = new Option<string?>("--csv", description: "CSV出力先");

        //var root = new RootCommand("複数サービス×複数サーバーを1ホスト1接続で並列監視（入力順プログレス付き）");
        //root.AddOptions(new[] { hostsOpt, servicesOpt, mapOpt, degOpt, timeoutOpt, retriesOpt, csvOpt });

        //root.SetHandler(async (InvocationContext ctx) =>
        //{
            //var hostsPath = ctx.ParseResult.GetValueForOption(hostsOpt)!;
            //var servicesPath = ctx.ParseResult.GetValueForOption(servicesOpt);
            //var mapPath = ctx.ParseResult.GetValueForOption(mapOpt);
            //var degree = ctx.ParseResult.GetValueForOption(degOpt);
            //var timeoutS = ctx.ParseResult.GetValueForOption(timeoutOpt)!;
            //var retries = Math.Max(0, ctx.ParseResult.GetValueForOption(retriesOpt));
            //var csvPath = ctx.ParseResult.GetValueForOption(csvOpt);

            //if (!File.Exists(hostsPath)) { Console.Error.WriteLine($"hostsが見つかりません: {hostsPath}"); ctx.ExitCode = 2; return; }
            //var hosts = ReadLines(hostsPath).ToArray();
            //if (hosts.Length == 0) { Console.Error.WriteLine("hostsが空です"); ctx.ExitCode = 2; return; }

            var map = mapPath != null
                ? BuildMapFromCsv(mapPath, hosts)
                : BuildMapFromServices(hosts, servicesPath);

            if (map.All(kv => kv.Value.Count == 0))
            {
                Console.Error.WriteLine("監視対象サービスがありません。（services/map の指定を確認）");
                /*ctx.ExitCode = 2; */return;
            }

            var timeout = ParseDuration(timeoutS);
            Console.WriteLine($"Hosts: {hosts.Length}, Degree={degree}, Timeout={timeout}, Retries={retries}");
            Console.WriteLine(mapPath != null ? $"Mapping: {mapPath}" : $"Services(all): {servicesPath}");
            Console.WriteLine("-------------------------------------------------------------------------------");

            using var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

            var results = new HostServiceResult[hosts.Length][]; // 各ホストごとに行配列
            var printer = new OrderedHostPrinter(hosts, map);

            await HostSurvey.QueryAsync(
                hosts: hosts,
                hostToServices: map,
                perHostTimeout: timeout,
                maxDegreeOfParallelism: degree,
                retries: retries,
                ct: cts.Token,
                onCompleted: (hostIndex, rows) =>
                {
                    results[hostIndex] = rows;
                    printer.TryPrintHost(results);
                });

            printer.Flush(results);

            // SUMMARY
            Console.WriteLine("\n=== SUMMARY ===");
            var flat = results.Where(r => r != null).SelectMany(r => r!).ToArray();
            foreach (var g in flat.GroupBy(r => r.Status).OrderByDescending(g => g.Count()))
                Console.WriteLine($"{g.Key,-10}: {g.Count()}");

            //if (!string.IsNullOrEmpty(csvPath))
            //{
            //    await CsvWriter.WriteAsync(csvPath!, hosts, results, map);
            //    Console.WriteLine($"\nCSV: {csvPath}");
            //}

            //ctx.ExitCode = 0;
//;        });

//        return await root.InvokeAsync(args)
    }

    static IEnumerable<string> ReadLines(string path)
        => File.ReadAllLines(path)
               .Select(l => l.Trim())
               .Where(l => !string.IsNullOrWhiteSpace(l) && !l.StartsWith("#"))
               .Distinct(StringComparer.OrdinalIgnoreCase);

    static Dictionary<string, List<string>> BuildMapFromServices(string[] hosts, string? servicesPath)
    {
        if (string.IsNullOrEmpty(servicesPath) || !File.Exists(servicesPath))
            return hosts.ToDictionary(h => h, _ => new List<string>(), StringComparer.OrdinalIgnoreCase);

        var services = ReadLines(servicesPath).ToList();
        return hosts.ToDictionary(h => h, _ => new List<string>(services), StringComparer.OrdinalIgnoreCase);
    }

    static Dictionary<string, List<string>> BuildMapFromCsv(string mapPath, string[] hosts)
    {
        var hostSet = new HashSet<string>(hosts, StringComparer.OrdinalIgnoreCase);
        var map = hosts.ToDictionary(h => h, _ => new List<string>(), StringComparer.OrdinalIgnoreCase);

        foreach (var line in ReadLines(mapPath))
        {
            var parts = line.Split(',', StringSplitOptions.TrimEntries);
            if (parts.Length < 2) continue;
            var host = parts[0];
            var svc = parts[1];
            if (!hostSet.Contains(host)) continue; // hosts.txtに無いものは無視
            map[host].Add(svc);
        }
        return map;
    }

    static TimeSpan ParseDuration(string s)
    {
        s = s.Trim().ToLowerInvariant();
        if (s.EndsWith("ms") && int.TryParse(s[..^2], out var ms)) return TimeSpan.FromMilliseconds(ms);
        if (s.EndsWith("s") && double.TryParse(s[..^1], NumberStyles.Any, CultureInfo.InvariantCulture, out var sec)) return TimeSpan.FromSeconds(sec);
        if (double.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture, out var fallbackSec)) return TimeSpan.FromSeconds(fallbackSec);
        throw new ArgumentException($"タイムアウト形式が不正です: {s}");
    }
}

// 出力（入力順でホスト見出し→サービス詳細）
public sealed class OrderedHostPrinter
{
    private readonly string[] _hosts;
    private readonly Dictionary<string, List<string>> _map;
    private int _nextHostIndex = 0;
    private readonly object _gate = new();

    public OrderedHostPrinter(string[] hosts, Dictionary<string, List<string>> map) { _hosts = hosts; _map = map; }

    public void TryPrintHost(HostServiceResult[][] results)
    {
        lock (_gate)
        {
            while (_nextHostIndex < results.Length && results[_nextHostIndex] is not null)
            {
                var host = _hosts[_nextHostIndex];
                var rows = results[_nextHostIndex]!;
                Console.WriteLine($"\n[{_nextHostIndex + 1,4}/{_hosts.Length}] {host}  (services: {rows.Length})");

                foreach (var r in rows)
                {
                    Console.WriteLine($"  - {r.Service,-20} {r.Status,-8}  {r.Detail}");
                }
                _nextHostIndex++;
            }
        }
    }

    public void Flush(HostServiceResult[][] results) => TryPrintHost(results);
}

// 1行：ホスト×サービスの結果
public record HostServiceResult(
    string Host,
    string Service,
    string Status,     // Running / Down / Pending / Timeout / Error / AuthError / NotFound / Unknown
    string Detail,     // State|Status|StartMode|ExitCode|HostCpu=..|HostMem=..|ProcCpu=..|ProcWS=..
    double? HostCpuPercent = null,
    double? HostMemPercent = null,
    double? ProcCpuPercent = null,
    double? ProcWorkingSetMB = null
);

public static class HostSurvey
{
    public static async Task QueryAsync(
        string[] hosts,
        Dictionary<string, List<string>> hostToServices,
        TimeSpan perHostTimeout,
        int maxDegreeOfParallelism,
        int retries,
        CancellationToken ct,
        Action<int, HostServiceResult[]> onCompleted)
    {
        var indexed = hosts.Select((h, i) => (Host: h, Index: i)).ToArray();

        await Parallel.ForEachAsync(indexed, new ParallelOptions
        {
            MaxDegreeOfParallelism = maxDegreeOfParallelism,
            CancellationToken = ct
        },
        async (item, token) =>
        {
            var services = hostToServices[item.Host];
            var rows = await QueryHostWithRetry(item.Host, services, perHostTimeout, retries, token).ConfigureAwait(false);
            onCompleted(item.Index, rows);
        });
    }

    private static async Task<HostServiceResult[]> QueryHostWithRetry(string host, List<string> services, TimeSpan timeout, int retries, CancellationToken ct)
    {
        for (int attempt = 0; ; attempt++)
        {
            var r = await QueryHostOnce(host, services, timeout, ct).ConfigureAwait(false);
            // ホストレベルで Timeout/Error/AuthError のみ再試行、それ以外は即返す
            bool needRetry = r.All(x => x.Status is "Timeout" or "Error" or "AuthError");
            if (!needRetry || attempt >= retries) return r;

            try { await Task.Delay(TimeSpan.FromMilliseconds(250 * Math.Pow(2, attempt)), ct).ConfigureAwait(false); }
            catch (OperationCanceledException) { return r; }
        }
    }

    private static async Task<HostServiceResult[]> QueryHostOnce(string host, List<string> services, TimeSpan timeout, CancellationToken outerCt)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(outerCt);
        cts.CancelAfter(timeout);

        try
        {
            return await Task.Run(() =>
            {
                var scope = new ManagementScope($@"\\{host}\root\cimv2");
                scope.Connect();

                // 監視するサービスが無いホストは空で返す
                if (services.Count == 0)
                    return Array.Empty<HostServiceResult>();

                // Win32_Service を IN句で一括取得
                var inNames = string.Join(",", services.Select(s => $"'{s.Replace("'", "''")}'"));
                var qSvc = new ObjectQuery($"SELECT Name,State,Status,StartMode,ExitCode,ProcessId FROM Win32_Service WHERE Name IN ({inNames})");
                using var sSvc = new ManagementObjectSearcher(scope, qSvc)
                { Options = new System.Management.EnumerationOptions { Timeout = timeout, ReturnImmediately = true, Rewindable = false } };
                using var svcCol = sSvc.Get();

                var svcDict = svcCol.Cast<ManagementObject>().ToDictionary(
                    mo => (string)mo["Name"],
                    mo => mo,
                    StringComparer.OrdinalIgnoreCase);

                // ホストCPU/メモリ（1回）
                double? hostCpu = TryGetHostCpuPercent(scope, timeout);
                double? hostMem = TryGetHostMemPercent(scope, timeout);

                // PID収集 → Procカウンタを IN句で一括
                var pidList = services
                    .Select(s => svcDict.TryGetValue(s, out var mo) ? Convert.ToUInt32(mo["ProcessId"] ?? 0) : 0u)
                    .Where(pid => pid > 0)
                    .Distinct()
                    .ToArray();

                var procCpuDict = TryGetProcsCpu(scope, pidList, timeout);
                var procWsDict = TryGetProcsWS(scope, pidList, timeout);

                // 入力のサービス順に並べる
                var rows = new List<HostServiceResult>(services.Count);
                foreach (var svc in services)
                {
                    if (!svcDict.TryGetValue(svc, out var mo))
                    {
                        rows.Add(new HostServiceResult(host, svc, "NotFound", "service missing", hostCpu, hostMem));
                        continue;
                    }

                    string state = (string)(mo["State"] ?? "Unknown");
                    string status = (string)(mo["Status"] ?? "Unknown");
                    string startMode = (string)(mo["StartMode"] ?? "Unknown");
                    int exitCode = Convert.ToInt32(mo["ExitCode"] ?? 0);
                    uint pid = Convert.ToUInt32(mo["ProcessId"] ?? 0);

                    double? pCpu = (pid > 0 && procCpuDict.TryGetValue(pid, out var c)) ? c : null;
                    double? pWsMB = (pid > 0 && procWsDict.TryGetValue(pid, out var w)) ? w : null;

                    string detail = $"{state}|{status}|StartMode={startMode}|ExitCode={exitCode}"
                                    + (hostCpu is not null ? $"|HostCpu={hostCpu:0.0}%" : "")
                                    + (hostMem is not null ? $"|HostMem={hostMem:0.0}%" : "")
                                    + (pCpu is not null ? $"|ProcCpu={pCpu:0.0}%" : "")
                                    + (pWsMB is not null ? $"|ProcWS={pWsMB:0.0}MB" : "");

                    rows.Add(new HostServiceResult(
                        host, svc, MapState(state), detail, hostCpu, hostMem, pCpu, pWsMB));
                }

                return rows.ToArray();

            }, cts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // ホスト丸ごとタイムアウト → そのホストに割り当てたサービス分、Timeout行で埋める
            return services.Select(s => new HostServiceResult(host, s, "Timeout", "no response within limit")).ToArray();
        }
        catch (UnauthorizedAccessException)
        {
            return services.Select(s => new HostServiceResult(host, s, "AuthError", "access denied")).ToArray();
        }
        catch (Exception ex)
        {
            return services.Select(s => new HostServiceResult(host, s, "Error", ex.GetType().Name)).ToArray();
        }
    }

    private static string MapState(string state) => state switch
    {
        "Running" => "Running",
        "Stopped" or "Paused" => "Down",
        "Start Pending" or "Stop Pending" or "Pause Pending" or "Continue Pending" => "Pending",
        _ => "Unknown"
    };

    // Host CPU
    private static double? TryGetHostCpuPercent(ManagementScope scope, TimeSpan timeout)
    {
        try
        {
            var q = new ObjectQuery("SELECT PercentProcessorTime FROM Win32_PerfFormattedData_PerfOS_Processor WHERE Name='_Total'");
            using var s = new ManagementObjectSearcher(scope, q) { Options = new System.Management.EnumerationOptions { Timeout = timeout, ReturnImmediately = true, Rewindable = false } };
            using var col = s.Get();
            var mo = col.Cast<ManagementObject>().FirstOrDefault();
            return mo is null ? null : Convert.ToDouble(mo["PercentProcessorTime"]);
        }
        catch { return null; }
    }

    // Host Mem
    private static double? TryGetHostMemPercent(ManagementScope scope, TimeSpan timeout)
    {
        try
        {
            var q = new ObjectQuery("SELECT TotalVisibleMemorySize,FreePhysicalMemory FROM Win32_OperatingSystem");
            using var s = new ManagementObjectSearcher(scope, q) { Options = new System.Management.EnumerationOptions { Timeout = timeout, ReturnImmediately = true, Rewindable = false } };
            using var col = s.Get();
            var mo = col.Cast<ManagementObject>().FirstOrDefault();
            if (mo is null) return null;
            double totalKB = Convert.ToDouble(mo["TotalVisibleMemorySize"]);
            double freeKB = Convert.ToDouble(mo["FreePhysicalMemory"]);
            if (totalKB <= 0) return null;
            return (1.0 - (freeKB / totalKB)) * 100.0;
        }
        catch { return null; }
    }

    // Proc CPU%（複数PIDをIN句で）
    private static Dictionary<uint, double> TryGetProcsCpu(ManagementScope scope, uint[] pids, TimeSpan timeout)
    {
        var dict = new Dictionary<uint, double>();
        if (pids.Length == 0) return dict;
        try
        {
            var inPids = string.Join(",", pids);
            var q = new ObjectQuery($"SELECT IDProcess,PercentProcessorTime FROM Win32_PerfFormattedData_PerfProc_Process WHERE IDProcess IN ({inPids})");
            using var s = new ManagementObjectSearcher(scope, q) { Options = new System.Management.EnumerationOptions { Timeout = timeout, ReturnImmediately = true, Rewindable = false } };
            using var col = s.Get();
            foreach (ManagementObject mo in col)
            {
                uint id = Convert.ToUInt32(mo["IDProcess"]);
                if (mo["PercentProcessorTime"] != null)
                    dict[id] = Convert.ToDouble(mo["PercentProcessorTime"]);
            }
        }
        catch { }
        return dict;
    }

    // Proc WS（MB）
    private static Dictionary<uint, double> TryGetProcsWS(ManagementScope scope, uint[] pids, TimeSpan timeout)
    {
        var dict = new Dictionary<uint, double>();
        if (pids.Length == 0) return dict;
        try
        {
            var inPids = string.Join(",", pids);
            var q = new ObjectQuery($"SELECT ProcessId,WorkingSetSize FROM Win32_Process WHERE ProcessId IN ({inPids})");
            using var s = new ManagementObjectSearcher(scope, q) { Options = new System.Management.EnumerationOptions { Timeout = timeout, ReturnImmediately = true, Rewindable = false } };
            using var col = s.Get();
            foreach (ManagementObject mo in col)
            {
                uint id = Convert.ToUInt32(mo["ProcessId"]);
                if (mo["WorkingSetSize"] != null)
                {
                    double bytes = Convert.ToDouble(mo["WorkingSetSize"]);
                    dict[id] = bytes / (1024.0 * 1024.0);
                }
            }
        }
        catch { }
        return dict;
    }
}

// CSV：ホスト順・サービス順で出力
public static class CsvWriter
{
    public static async Task WriteAsync(string path, string[] hosts, HostServiceResult[][] rows, Dictionary<string, List<string>> map)
    {
        var dir = Path.GetDirectoryName(Path.GetFullPath(path));
        if (!string.IsNullOrEmpty(dir) && !Directory.Exists(dir)) Directory.CreateDirectory(dir);

        await using var fs = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.Read);
        await using var sw = new StreamWriter(fs, new UTF8Encoding(encoderShouldEmitUTF8Identifier: true));

        await sw.WriteLineAsync("Host,Service,Status,Detail,HostCpu,HostMem,ProcCpu,ProcWS_MB");

        for (int i = 0; i < hosts.Length; i++)
        {
            var host = hosts[i];
            var list = rows[i] ?? Array.Empty<HostServiceResult>();
            foreach (var r in list)
            {
                string Esc(string s) => (s.Contains(',') || s.Contains('"')) ? $"\"{s.Replace("\"", "\"\"")}\"" : s;
                string F(double? v) => v is null ? "" : v.Value.ToString("0.0", CultureInfo.InvariantCulture);
                await sw.WriteLineAsync($"{Esc(r.Host)},{Esc(r.Service)},{Esc(r.Status)},{Esc(r.Detail)},{F(r.HostCpuPercent)},{F(r.HostMemPercent)},{F(r.ProcCpuPercent)},{F(r.ProcWorkingSetMB)}");
            }
        }
    }
}
