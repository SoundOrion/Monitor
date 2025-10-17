using System;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;

class Program
{
    static async Task Main()
    {
    }
}

class TcpServer
{
    private readonly TcpListener listener;
    private readonly ConcurrentBag<TcpClient> clients = new();
    private readonly CancellationTokenSource cts = new();

    public TcpServer(int port)
    {
        listener = new TcpListener(IPAddress.Any, port);
    }

    public void Start()
    {
        listener.Start();
        Console.WriteLine("Server started...");

        // 接続受付スレッド
        ThreadPool.QueueUserWorkItem(_ => AcceptLoop());

        // クライアント監視スレッド
        ThreadPool.QueueUserWorkItem(_ => MonitorLoop());
    }

    private void AcceptLoop()
    {
        while (!cts.Token.IsCancellationRequested)
        {
            try
            {
                var client = listener.AcceptTcpClient();
                clients.Add(client);
                Console.WriteLine($"Client connected: {client.Client.RemoteEndPoint}");
                ThreadPool.QueueUserWorkItem(_ => HandleClient(client));
            }
            catch { /* ignore */ }
        }
    }

    private void HandleClient(TcpClient client)
    {
        using var stream = client.GetStream();
        var buffer = new byte[1024];
        try
        {
            while (true)
            {
                int bytesRead = stream.Read(buffer, 0, buffer.Length);
                if (bytesRead == 0) break; // 切断検出
                string msg = Encoding.UTF8.GetString(buffer, 0, bytesRead);
                Console.WriteLine($"[{client.Client.RemoteEndPoint}] {msg}");
            }
        }
        catch { /* 通信エラーなど */ }
        finally
        {
            Console.WriteLine($"Client disconnected: {client.Client.RemoteEndPoint}");
            client.Close();
        }
    }

    private void MonitorLoop()
    {
        while (!cts.Token.IsCancellationRequested)
        {
            foreach (var client in clients)
            {
                if (!IsConnected(client))
                {
                    Console.WriteLine($"Detected dead client: {client.Client.RemoteEndPoint}");
                    client.Close();
                }
            }
            Thread.Sleep(5000); // 5秒ごとにチェック
        }
    }

    private bool IsConnected(TcpClient client)
    {
        try
        {
            if (client == null || !client.Connected) return false;
            if (client.Client.Poll(0, SelectMode.SelectRead))
            {
                return client.Client.Available != 0;
            }
            return true;
        }
        catch
        {
            return false;
        }
    }

    public void Stop()
    {
        cts.Cancel();
        listener.Stop();
        foreach (var client in clients)
            client.Close();
    }
}
