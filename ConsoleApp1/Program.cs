using System;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
class Program
{
    static async Task Main()
    {
        using var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

        var server = new TcpServer(port: 5000);
        await server.StartAsync(cts.Token);

        Console.WriteLine("Press Ctrl+C to stop.");
        try
        {
            // ここでサーバーが動き続ける（他の処理を並行させてもOK）
            await Task.Delay(Timeout.Infinite, cts.Token);
        }
        catch (OperationCanceledException) { /* exit */ }

        await server.StopAsync();
        Console.WriteLine("Server stopped.");
    }
}

public sealed class TcpServer : IAsyncDisposable
{
    private readonly TcpListener _listener;
    private readonly ConcurrentDictionary<EndPoint, TcpClient> _clients = new();
    private readonly TimeSpan _monitorInterval;
    private CancellationTokenSource? _cts;
    private Task? _acceptTask;
    private Task? _monitorTask;

    public TcpServer(int port, TimeSpan? monitorInterval = null)
    {
        _listener = new TcpListener(IPAddress.Any, port);
        _monitorInterval = monitorInterval ?? TimeSpan.FromSeconds(5);
    }

    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _listener.Start();
        Console.WriteLine("Server started...");

        _acceptTask = AcceptLoopAsync(_cts.Token);
        _monitorTask = MonitorLoopAsync(_cts.Token);
        await Task.CompletedTask;
    }

    public async Task StopAsync()
    {
        if (_cts is null) return;
        _cts.Cancel();
        _listener.Stop();

        // Close all clients
        foreach (var kv in _clients)
        {
            try { kv.Value.Close(); } catch { /* ignore */ }
        }

        // Wait loops to finish
        var tasks = new[] { _acceptTask, _monitorTask };
        await Task.WhenAll(tasks.Where(t => t is not null)!);
    }

    public async ValueTask DisposeAsync() => await StopAsync();

    private async Task AcceptLoopAsync(CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                TcpClient client = await _listener.AcceptTcpClientAsync(ct).ConfigureAwait(false);
                client.NoDelay = true; // 低レイテンシにしたい場合
                var ep = client.Client.RemoteEndPoint!;
                _clients[ep] = client;
                Console.WriteLine($"Client connected: {ep}");

                _ = HandleClientAsync(client, ct); // fire-and-forget
            }
        }
        catch (OperationCanceledException) { /* stopping */ }
        catch (ObjectDisposedException) { /* listener stopped */ }
        catch (Exception ex)
        {
            Console.WriteLine($"[AcceptLoop] {ex}");
        }
    }

    private async Task HandleClientAsync(TcpClient client, CancellationToken ct)
    {
        var ep = client.Client.RemoteEndPoint!;
        using NetworkStream stream = client.GetStream();
        byte[] buffer = new byte[4096];

        try
        {
            while (!ct.IsCancellationRequested)
            {
                int read = await stream.ReadAsync(buffer.AsMemory(0, buffer.Length), ct).ConfigureAwait(false);
                if (read == 0) break; // リモートがクリーンに切断
                string msg = Encoding.UTF8.GetString(buffer, 0, read);
                Console.WriteLine($"[{ep}] {msg}");

                // 必要ならエコー
                // await stream.WriteAsync(buffer.AsMemory(0, read), ct).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) { /* stopping */ }
        catch (Exception ex)
        {
            Console.WriteLine($"[HandleClient {ep}] {ex.Message}");
        }
        finally
        {
            // ここにアプリレベルのクリーンアップ
            try { client.Close(); } catch { /* ignore */ }
            _clients.TryRemove(ep, out _);
            Console.WriteLine($"Client disconnected: {ep}");
        }
    }

    private async Task MonitorLoopAsync(CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                foreach (var (ep, client) in _clients)
                {
                    if (!IsConnected(client))
                    {
                        Console.WriteLine($"Detected dead client: {ep}");
                        try { client.Close(); } catch { /* ignore */ }
                        _clients.TryRemove(ep, out _);
                    }
                }

                await Task.Delay(_monitorInterval, ct).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) { /* stopping */ }
        catch (Exception ex)
        {
            Console.WriteLine($"[MonitorLoop] {ex}");
        }
    }

    // 非同期ではないが軽量で十分：Poll(0, Read) && Available == 0 なら切断とみなす
    private static bool IsConnected(TcpClient client)
    {
        try
        {
            if (client is null || !client.Connected) return false;

            var s = client.Client;
            // 受信可能かつキューが空 → FIN 受信済みの可能性が高い
            if (s.Poll(0, SelectMode.SelectRead))
                return s.Available != 0;

            return true;
        }
        catch
        {
            return false;
        }
    }
}
