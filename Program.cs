using NATS.Client.KeyValueStore;
using NATS.Net;

await using var client = new NatsClient();
IDistributedLocker locker = new NatsDistributedLocker(client);
Console.WriteLine("Trying...");

await using var handle = await locker.Acquire(args.First());
if (handle is null) return;
_ = Task.Run(async () =>
{
    while (true)
    {
        await Task.Delay(1000);
        Console.WriteLine($"Canceled? {handle.CancellationToken.IsCancellationRequested}");
    }
});
Console.WriteLine("Became leader");
Console.ReadLine();

public interface IDistributedLocker
{
    /// <summary>
    /// Tries to acquire a given lock, if it fails, it blocks until the lock is freed.
    /// The lock has a TTL of 3 seconds and is renewed in the background every second. If the program crashes, the lock will be freed after at most 3 seconds, and can be acquired by another process.
    /// </summary>
    /// <param name="key">A string specifying a name that identifies the lock resource.</param>
    /// <returns>An <see cref="ILockHandle"/> that frees the lock when disposed.</returns>
    Task<ILockHandle> Acquire(string key);

    /// <summary>
    /// Tries to acquire a given lock; returns `null` if the lock isn't free immediately.
    /// The lock has a TTL of 3 seconds and is periodically renewed in the background until the lock is disposed. If the program crashes, the lock will be freed after at most 3 seconds, and can be acquired by another process.
    /// </summary>
    /// <param name="key">A string specifying a name that identifies the lock resource.</param>
    /// <returns>An <see cref="ILockHandle"/> that frees the lock when disposed, or `null` if the lock wasn't acquired.</returns>
    Task<ILockHandle?> TryAcquire(string key);
}

public interface ILockHandle : IAsyncDisposable
{
    /// <summary>
    /// If the lease isn't refreshed in time, the next time the refresh is attempted, we'll get an error,
    /// which signals that the lock has been acquired by another process, we need to kill our process once this happens.
    /// Represents a cancellation token that is canceled at that point.
    /// </summary>
    CancellationToken CancellationToken { get; }
}

public class NatsDistributedLocker(
    NatsClient natsClient
) : IDistributedLocker
{
    private const string LockStoreName = "_locks";
    private static readonly TimeSpan Ttl = TimeSpan.FromSeconds(3);
    private readonly Task<INatsKVStore> _kv = natsClient
        .CreateKeyValueStoreContext()
        .CreateStoreAsync(new NatsKVConfig(LockStoreName)
        {
            MaxAge = Ttl,
        })
        .AsTask();

    public async Task<ILockHandle> Acquire(string key)
    {
        var kv = await _kv;
        ulong rev;
        try
        {
            rev = await kv.CreateAsync(key, "");

            CancellationTokenSource cts = new();
            _ = Renew(cts);
            return new Lock(async () =>
            {
                Console.WriteLine("Disposing...");
                cts.Cancel();
                await kv.DeleteAsync(key, new() { Revision = rev });
            }, cts.Token);
        }
        catch (Exception ex) when (ex is NatsKVCreateException or NatsKVWrongLastRevisionException)
        {
            // NOTE: NATS offers almost instantaneous ZooKeeper-like "event-driven waits" via its "watch" mechanism — see https://github.com/madelson/DistributedLock/blob/master/docs/DistributedLock.ZooKeeper.md#:~:text=By%20leveraging%20ZooKeeper%20watches%20under%20the%20hood%2C%20these%20recipes%20allow%20for%20very%20efficient%20event%2Ddriven%20waits%20when%20acquiring.
            await foreach (var entry in kv.WatchAsync<string>(key))
            {
                if (entry.Operation is not NatsKVOperation.Del) // todo: automatic deletion due to `maxage` doesn't result in a `del` so we won't be notified
                    continue;

                return await Acquire(key);
            }
            throw new Exception("Couldn't acquire the lock.");
        }

        async Task Renew(CancellationTokenSource cts)
        {
            while (!cts.Token.IsCancellationRequested)
            {
                // var refreshFrequency = (int)Ttl.TotalMilliseconds / 3;
                var refreshFrequency = 10_000;
                await Task.Delay(refreshFrequency, cts.Token);
                try
                {
                    rev = await kv.UpdateAsync(key, "", rev, cancellationToken: cts.Token);
                }
                catch (NatsKVWrongLastRevisionException ex)
                {
                    Console.WriteLine($"update threw: {ex}");
                    cts.Cancel();
                }
            }
        }
    }

    public async Task<ILockHandle?> TryAcquire(string key)
    {
        throw new NotImplementedException();
        // var kv = await _kv;
        // ulong rev;
        // try
        // {
        //     rev = await kv.CreateAsync(key, "");

        //     CancellationTokenSource cts = new();
        //     _ = RefreshLock(cts.Token);
        //     return new Lock(async () =>
        //     {
        //         Console.WriteLine("Disposing...");
        //         cts.Cancel();
        //         await kv.DeleteAsync(key);
        //     });
        // }
        // catch (Exception ex) when (ex is NatsKVCreateException or NatsKVWrongLastRevisionException)
        // {
        //     return null;
        // }

        // async Task RefreshLock(CancellationToken ct)
        // {
        //     while (!ct.IsCancellationRequested)
        //     {
        //         // var refreshFrequency = (int)Ttl.TotalMilliseconds / 3;
        //         var refreshFrequency = 10_000;
        //         await Task.Delay(refreshFrequency, ct);
        //         rev = await kv.UpdateAsync(key, "", rev, cancellationToken: ct);
        //     }
        // }
    }

    private class Lock(Func<ValueTask> free, CancellationToken ct) : ILockHandle
    {
        public CancellationToken CancellationToken => ct;
        public ValueTask DisposeAsync() => free();
    }
}
