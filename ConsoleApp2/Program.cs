using System;
using System.Buffers.Binary; // ← 追加
using System.Collections.Concurrent;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using System;
using System.Threading;
using System.Threading.Tasks;

class Program
{
    static async Task Main()
    {
        using var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

        var server = new HeartbeatTcpServer(
            port: 5000,
            heartbeatInterval: TimeSpan.FromSeconds(10),
            heartbeatTimeout: TimeSpan.FromSeconds(30));

        await server.StartAsync(cts.Token);

        Console.WriteLine("Server running. Ctrl+C to stop.");
        try { await Task.Delay(Timeout.Infinite, cts.Token); }
        catch (OperationCanceledException) { }

        await server.StopAsync();
        Console.WriteLine("Server stopped.");
    }
}


public sealed class HeartbeatTcpServer : IAsyncDisposable
{
    private readonly TcpListener _listener;
    private readonly ConcurrentDictionary<EndPoint, ClientSession> _clients = new();
    private readonly TimeSpan _heartbeatInterval;
    private readonly TimeSpan _heartbeatTimeout;
    private CancellationTokenSource? _cts;
    private Task? _acceptTask;

    public HeartbeatTcpServer(
        int port,
        TimeSpan? heartbeatInterval = null,
        TimeSpan? heartbeatTimeout = null)
    {
        _listener = new TcpListener(IPAddress.Any, port);
        _heartbeatInterval = heartbeatInterval ?? TimeSpan.FromSeconds(10);
        _heartbeatTimeout = heartbeatTimeout ?? TimeSpan.FromSeconds(30);
    }

    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _listener.Start();
        Console.WriteLine("Server started.");
        _acceptTask = AcceptLoopAsync(_cts.Token);
        await Task.CompletedTask;
    }

    public async Task StopAsync()
    {
        if (_cts is null) return;
        _cts.Cancel();
        _listener.Stop();

        foreach (var (_, s) in _clients)
        {
            try { await s.DisposeAsync(); } catch { /* ignore */ }
        }
        if (_acceptTask is not null)
        {
            try { await _acceptTask.ConfigureAwait(false); } catch { /* ignore */ }
        }
    }

    public async ValueTask DisposeAsync() => await StopAsync();

    private async Task AcceptLoopAsync(CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var client = await _listener.AcceptTcpClientAsync(ct).ConfigureAwait(false);
                client.NoDelay = true;

                // 任意：OSのTCP KeepAlive（早期検出・NAT越えの安定化）
                TryEnableKeepAlive(client, keepAliveTimeMs: 30_000, keepAliveIntervalMs: 5_000);

                var session = new ClientSession(client, _heartbeatInterval, _heartbeatTimeout, RemoveClient, ct);
                _clients[client.Client.RemoteEndPoint!] = session;
                Console.WriteLine($"Client connected: {client.Client.RemoteEndPoint}");
                _ = session.RunAsync(); // fire-and-forget
            }
        }
        catch (OperationCanceledException) { }
        catch (ObjectDisposedException) { }
        catch (Exception ex)
        {
            Console.WriteLine($"[AcceptLoop] {ex}");
        }
    }

    private void RemoveClient(EndPoint ep)
    {
        _clients.TryRemove(ep, out _);
        Console.WriteLine($"Client removed: {ep}");
    }

    private static void TryEnableKeepAlive(TcpClient client, uint keepAliveTimeMs, uint keepAliveIntervalMs)
    {
        try
        {
            var s = client.Client;
            s.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);

            // Windows の詳細設定（12バイト固定: [on(4)][time(4)][interval(4)] little-endian）
            byte[] inOption = new byte[12];
            BinaryPrimitives.WriteInt32LittleEndian(inOption.AsSpan(0, 4), 1); // 有効化
            BinaryPrimitives.WriteInt32LittleEndian(inOption.AsSpan(4, 4), unchecked((int)keepAliveTimeMs));
            BinaryPrimitives.WriteInt32LittleEndian(inOption.AsSpan(8, 4), unchecked((int)keepAliveIntervalMs));

            s.IOControl(IOControlCode.KeepAliveValues, inOption, null);
        }
        catch
        {
            // 未対応OSは黙ってスキップ
        }
    }

    private sealed class ClientSession : IAsyncDisposable
    {
        private readonly TcpClient _client;
        private readonly NetworkStream _ns;
        private readonly StreamReader _reader;
        private readonly StreamWriter _writer;
        private readonly TimeSpan _interval;
        private readonly TimeSpan _timeout;
        private readonly Action<EndPoint> _onClose;
        private readonly CancellationToken _serverCt;

        private DateTime _lastSeenUtc;
        private readonly EndPoint _ep;

        public ClientSession(
            TcpClient client,
            TimeSpan interval,
            TimeSpan timeout,
            Action<EndPoint> onClose,
            CancellationToken serverCt)
        {
            _client = client;
            _ns = client.GetStream();
            _reader = new StreamReader(_ns, new UTF8Encoding(false), detectEncodingFromByteOrderMarks: false, bufferSize: 4096, leaveOpen: true);
            _writer = new StreamWriter(_ns, new UTF8Encoding(false)) { AutoFlush = true };
            _interval = interval;
            _timeout = timeout;
            _onClose = onClose;
            _serverCt = serverCt;
            _lastSeenUtc = DateTime.UtcNow;
            _ep = _client.Client.RemoteEndPoint!;
        }

        public async Task RunAsync()
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(_serverCt);
            var ct = cts.Token;

            var recv = ReceiveLoopAsync(ct);
            var hb = HeartbeatLoopAsync(ct);

            try
            {
                await Task.WhenAny(recv, hb).ConfigureAwait(false);
            }
            finally
            {
                cts.Cancel();
                try { await Task.WhenAll(Suppress(recv), Suppress(hb)).ConfigureAwait(false); } catch { }
                await DisposeAsync();
                _onClose(_ep);
            }
        }

        private async Task ReceiveLoopAsync(CancellationToken ct)
        {
            try
            {
                while (!ct.IsCancellationRequested)
                {
                    // .NET 6+ : ReadLineAsync を WaitAsync でキャンセル可能に
                    var line = await _reader.ReadLineAsync().WaitAsync(ct).ConfigureAwait(false);
                    if (line is null) break; // FIN 受信など

                    _lastSeenUtc = DateTime.UtcNow;

                    if (line == "PING")
                    {
                        await _writer.WriteLineAsync("PONG").ConfigureAwait(false);
                        continue;
                    }

                    if (line == "PONG")
                    {
                        // 心拍応答。特に処理不要
                        continue;
                    }

                    // アプリメッセージ：ここで処理。例はエコー
                    Console.WriteLine($"[{_ep}] {line}");
                    await _writer.WriteLineAsync($"ECHO: {line}").ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) { }
            catch (IOException) { } // 切断等
            catch (Exception ex)
            {
                Console.WriteLine($"[Recv {_ep}] {ex.Message}");
            }
        }

        private async Task HeartbeatLoopAsync(CancellationToken ct)
        {
            try
            {
                while (!ct.IsCancellationRequested)
                {
                    // 送信（サーバー主導）
                    await _writer.WriteLineAsync("PING").ConfigureAwait(false);

                    // タイムアウト判定
                    var until = DateTime.UtcNow + _timeout;
                    while (DateTime.UtcNow < until && !ct.IsCancellationRequested)
                    {
                        await Task.Delay(TimeSpan.FromMilliseconds(200), ct).ConfigureAwait(false);
                        if (DateTime.UtcNow - _lastSeenUtc < _timeout)
                            break;
                    }

                    if (DateTime.UtcNow - _lastSeenUtc >= _timeout)
                    {
                        Console.WriteLine($"[Heartbeat] timeout: {_ep}");
                        break; // 切断
                    }

                    await Task.Delay(_interval, ct).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) { }
            catch (IOException) { }
            catch (Exception ex)
            {
                Console.WriteLine($"[Heartbeat {_ep}] {ex.Message}");
            }
        }

        private static async Task Suppress(Task t)
        {
            try { await t.ConfigureAwait(false); } catch { }
        }

        public async ValueTask DisposeAsync()
        {
            try { _writer.Dispose(); } catch { }
            try { _reader.Dispose(); } catch { }
            try { _ns.Dispose(); } catch { }
            try { _client.Close(); } catch { }
            await Task.CompletedTask;
        }
    }
}
