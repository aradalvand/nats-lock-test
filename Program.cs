using NATS.Client.KeyValueStore;
using NATS.Net;

await using var client = new NatsClient();
IDistributedLocker locker = new NatsDistributedLocker(client);
Console.WriteLine("Trying...");
await using (var handle = await locker.TryAcquire(args.First()))
{
    if (handle is null) return;
    Console.WriteLine("Became leader");
    Console.ReadLine();
}

public interface IDistributedLocker
{
    /// <summary>
    /// Tries to acquire a given lock, if it fails, it waits until the lock is freed.
    /// The lock has a TTL of 3 seconds and is renewed in the background every second. If the program crashes, the lock will be freed after at most 3 seconds, and can be acquired by another process.
    /// </summary>
    /// <param name="key">A string specifying a name that identifies the lock resource.</param>
    /// <returns>An <see cref="IAsyncDisposable"/> that frees the lock when disposed.</returns>
    Task<IAsyncDisposable> Acquire(string key);

    /// <summary>
    /// Tries to acquire a given lock; returns `null` if the lock isn't free immediately.
    /// The lock has a TTL of 3 seconds and is renewed in the background every second. If the program crashes, the lock will be freed after at most 3 seconds, and can be acquired by another process.
    /// </summary>
    /// <param name="key">A string specifying a name that identifies the lock resource.</param>
    /// <returns>An <see cref="IAsyncDisposable"/> that frees the lock when disposed, or `null` if the lock wasn't acquired.</returns>
    Task<IAsyncDisposable?> TryAcquire(string key);
}

public class NatsDistributedLocker(
    NatsClient natsClient
) : IDistributedLocker
{
    private const string LockStoreName = "_locks";
    private readonly Task<INatsKVStore> _kv = natsClient
        .CreateKeyValueStoreContext()
        .CreateStoreAsync(new NatsKVConfig(LockStoreName)
        {
            MaxAge = TimeSpan.FromSeconds(3),
        })
        .AsTask();

    public async Task<IAsyncDisposable?> TryAcquire(string key)
    {
        var kv = await _kv;
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
            return null;
        }

        async Task RefreshLock(CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                await Task.Delay(1_000, ct);
                rev = await kv.UpdateAsync(key, "", rev, cancellationToken: ct);
            }
        }
    }

    public async Task<IAsyncDisposable> Acquire(string key)
    {
        var kv = await _kv;
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
                if (entry.Operation is not NatsKVOperation.Del) // todo: automatic deletion due to `maxage` doesn't result in a `del` so we won't be notified
                    continue;

                return await Acquire(key);
            }
            throw new Exception("Couldn't acquire the lock.");
        }

        async Task RefreshLock(CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                await Task.Delay(1_000, ct);
                rev = await kv.UpdateAsync(key, "", rev, cancellationToken: ct);
            }
        }
    }

    private class Disposable(Func<ValueTask> dispose) : IAsyncDisposable
    {
        public ValueTask DisposeAsync() => dispose();
    }
}
