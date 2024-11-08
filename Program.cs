using NATS.Client.KeyValueStore;
using NATS.Net;

await using var client = new NatsClient();
IDistributedLocker locker = new NatsDistributedLocker(client);
Console.WriteLine("Trying...");
await using (await locker.LockOrWait(args.First()))
{
    Console.WriteLine("Became leader");
    Console.ReadLine();
}

public interface IDistributedLocker
{
    Task<IAsyncDisposable> LockOrWait(string key);
    Task<IAsyncDisposable> LockOrGiveUp(string key);
}

public class NatsDistributedLocker(
    NatsClient natsClient
) : IDistributedLocker
{
    private const string LockStoreName = "_locks";

    public async Task<IAsyncDisposable> LockOrGiveUp(string key)
    {
        var kv = await natsClient.CreateKeyValueStoreContext().CreateStoreAsync(LockStoreName);
        ulong rev;
        try
        {
            rev = await kv.CreateAsync(key, "");

            CancellationTokenSource cts = new();
            _ = RefreshLock(cts.Token);
            return new Disposable(async () =>
            {
                Console.WriteLine("Disposing...");
                cts.Cancel();
                await kv.DeleteAsync(key);
            });
        }
        catch (Exception ex) when (ex is NatsKVCreateException or NatsKVWrongLastRevisionException)
        {
            throw new Exception("Lock couldn't be acquired.");
        }

        async Task RefreshLock(CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                await Task.Delay(100_000_000, ct);
                rev = await kv.UpdateAsync(key, "", rev, cancellationToken: ct);
            }
        }
    }

    public async Task<IAsyncDisposable> LockOrWait(string key)
    {
        var kv = await natsClient.CreateKeyValueStoreContext().CreateStoreAsync(LockStoreName);
        ulong rev;
        try
        {
            rev = await kv.CreateAsync(key, "");

            CancellationTokenSource cts = new();
            _ = RefreshLock(cts.Token);
            return new Disposable(async () =>
            {
                Console.WriteLine("Disposing...");
                cts.Cancel();
                await kv.DeleteAsync(key);
            });
        }
        catch (Exception ex) when (ex is NatsKVCreateException or NatsKVWrongLastRevisionException)
        {
            await foreach (var entry in kv.WatchAsync<string>(key))
            {
                if (entry.Operation is not NatsKVOperation.Del)
                    continue;

                return await LockOrWait(key);
            }
            throw new Exception("Couldn't acquire the lock.");
        }

        async Task RefreshLock(CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                await Task.Delay(100_000_000, ct);
                rev = await kv.UpdateAsync(key, "", rev, cancellationToken: ct);
            }
        }
    }

    private class Disposable(Func<ValueTask> dispose) : IAsyncDisposable
    {
        public ValueTask DisposeAsync() => dispose();
    }
}
