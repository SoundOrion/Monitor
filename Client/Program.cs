using System;
using System.Buffers.Binary; // ← 追加
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using System;
using System.Threading.Tasks;

class Program
{
    static async Task Main()
    {
        await using var client = new HeartbeatTcpClient("127.0.0.1", 5000);
        await client.ConnectAsync();

        Console.WriteLine("Type messages to send. Ctrl+C to exit.");
        while (true)
        {
            var line = Console.ReadLine();
            if (string.IsNullOrEmpty(line)) continue;
            await client.SendAsync(line);
        }
    }
}

public sealed class HeartbeatTcpClient : IAsyncDisposable
{
    private readonly string _host;
    private readonly int _port;
    private TcpClient? _client;
    private NetworkStream? _ns;
    private StreamReader? _reader;
    private StreamWriter? _writer;
    private readonly CancellationTokenSource _cts = new();

    public HeartbeatTcpClient(string host, int port)
    {
        _host = host;
        _port = port;
    }

    public async Task ConnectAsync()
    {
        // 既存接続があればクリーンアップ（再接続時のため）
        await CleanupAsync();

        _client = new TcpClient();
        await _client.ConnectAsync(_host, _port).ConfigureAwait(false);
        _client.NoDelay = true;

        // OS の TCP KeepAlive（任意）
        TryEnableKeepAlive(_client, 30_000, 5_000);

        _ns = _client.GetStream();
        _reader = new StreamReader(_ns, new UTF8Encoding(false), detectEncodingFromByteOrderMarks: false, bufferSize: 4096, leaveOpen: true);
        _writer = new StreamWriter(_ns, new UTF8Encoding(false)) { AutoFlush = true };

        Console.WriteLine($"Connected to {_host}:{_port}");
        _ = ReceiveLoopAsync(_cts.Token); // fire-and-forget
    }

    public async Task SendAsync(string line)
    {
        if (_writer is null) throw new InvalidOperationException("Not connected.");
        await _writer.WriteLineAsync(line).ConfigureAwait(false);
    }

    private async Task ReceiveLoopAsync(CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var line = await _reader!.ReadLineAsync().WaitAsync(ct).ConfigureAwait(false);
                if (line is null) break;

                if (line == "PING")
                {
                    await _writer!.WriteLineAsync("PONG").ConfigureAwait(false);
                    continue;
                }

                Console.WriteLine($"[Server] {line}");
            }
        }
        catch (OperationCanceledException) { }
        catch (IOException) { }
        catch (Exception ex)
        {
            Console.WriteLine($"[Client Receive] {ex.Message}");
        }

        await ReconnectLoopAsync().ConfigureAwait(false);
    }

    private async Task ReconnectLoopAsync()
    {
        int delayMs = 1000;
        while (!_cts.IsCancellationRequested)
        {
            Console.WriteLine($"Reconnecting in {delayMs} ms...");
            try { await Task.Delay(delayMs, _cts.Token).ConfigureAwait(false); } catch { return; }

            try
            {
                await ConnectAsync().ConfigureAwait(false);
                Console.WriteLine("Reconnected.");
                return;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Reconnect failed: {ex.Message}");
                delayMs = Math.Min(delayMs * 2, 15_000); // 簡易指数バックオフ
            }
        }
    }

    private static void TryEnableKeepAlive(TcpClient client, uint keepAliveTimeMs, uint keepAliveIntervalMs)
    {
        try
        {
            var s = client.Client;
            s.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);

            byte[] inOption = new byte[12];
            BinaryPrimitives.WriteInt32LittleEndian(inOption.AsSpan(0, 4), 1);
            BinaryPrimitives.WriteInt32LittleEndian(inOption.AsSpan(4, 4), unchecked((int)keepAliveTimeMs));
            BinaryPrimitives.WriteInt32LittleEndian(inOption.AsSpan(8, 4), unchecked((int)keepAliveIntervalMs));
            s.IOControl(IOControlCode.KeepAliveValues, inOption, null);
        }
        catch { }
    }

    private async Task CleanupAsync()
    {
        try { _writer?.Dispose(); } catch { }
        try { _reader?.Dispose(); } catch { }
        try { _ns?.Dispose(); } catch { }
        try { _client?.Close(); } catch { }
        await Task.CompletedTask;
        _writer = null; _reader = null; _ns = null; _client = null;
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        await CleanupAsync();
    }
}
