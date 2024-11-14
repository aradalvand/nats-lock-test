using System.Diagnostics;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Client.KeyValueStore;
using NATS.Net;

async Task Main(CancellationToken ct)
{
    await using var client = new NatsClient();
    var locker = new NatsDistributedLocker(client);
    Console.WriteLine("Trying...");

    if (TryFoo(out ct))
    {

    }

    await using var handle = await locker.AcquireOrStandBy(args.First(), () => Main(ct), ct);
    _ = Task.Run(async () =>
    {
        while (!handle.CancellationToken.IsCancellationRequested)
        {
            await Task.Delay(1000, handle.CancellationToken);
            Console.WriteLine($"--- doing work ----");
        }
    }); // see https://stackoverflow.com/a/48971668/7734384
    Console.WriteLine("Became leader");
    Console.ReadLine();
}

bool TryFoo(out CancellationToken foo)
{

}

await Main(default);

public interface ILockHandle : IAsyncDisposable
{
    /// <summary>
    /// If the lock fails to be renewed in time (due to server hanging, or network partitions that make NATS temporarily inaccessible),
    /// the next time the refresh is attempted, we'll get an error if the lock has already been re-acquired by another process,
    /// In such a scenario, the previous leader (this instance) should abort its process.
    /// This property represents a cancellation token that is canceled in the aforementioned point, or when the token passed to called method of `IDistributeLocker` is canceled (making it a superset of that original cancellation token).
    /// </summary>
    CancellationToken CancellationToken { get; }
}

public class NatsDistributedLocker(
    INatsClient natsClient
)
{
    private const string KvStoreName = "locks";
    private static readonly TimeSpan Ttl = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan RenewalFrequency = TimeSpan.FromSeconds(2);
    private readonly Lazy<Task<INatsKVStore>> _kvStore = new(async () =>
        await natsClient
            .CreateKeyValueStoreContext()
            .CreateStoreAsync(new NatsKVConfig(KvStoreName)
            {
                // NOTE: We only really care about the last state of any lock, so we leave the `History` option to its default value which is 1. See https://docs.nats.io/nats-concepts/jetstream/key-value-store/kv_walkthrough#watching-a-k-v-store:~:text=By%20default%2C%20the%20KV%20bucket%20has%20a%20history%20size%20of%20one
                MaxAge = Ttl, // todo: should be set on a per-key basis, preferably — pending https://github.com/nats-io/nats-server/issues/3251#issuecomment-2371195906
            })
    );

    // public async Task<ILockHandle?> TryAcquire(string key, CancellationToken ct = default)
    // {
    //     var acquiredLock = await Acquire(
    //         key, _ => ValueTask.FromResult<ILockHandle?>(null),
    //         ct
    //     );
    //     return acquiredLock;
    // }

    public async Task<ILockHandle> AcquireOrStandBy(string key, Func<Task> operation, CancellationToken ct = default)
    {
        var acquiredLock = await Acquire(key, async kv =>
        {
            // NOTE: NATS offers almost instantaneous ZooKeeper-like "event-driven waits" via its "watch" mechanism — see https://github.com/madelson/DistributedLock/blob/master/docs/DistributedLock.ZooKeeper.md#:~:text=By%20leveraging%20ZooKeeper%20watches%20under%20the%20hood%2C%20these%20recipes%20allow%20for%20very%20efficient%20event%2Ddriven%20waits%20when%20acquiring.
            Console.WriteLine("1");
            await Task.Delay(10000);
            Console.WriteLine("۲");
            // todo: race condition
            await foreach (var entry in kv.WatchAsync<string>(key, opts: new()
            {
                // NOTE: By default, NATS KV stores will keep a history of 1, meaning the last state of the key (including 'DELETE'). We want to set this particular watch configuration to `true` in order to account for a possible race condition in which the key fails to be acquired (hence the invocation of this func), then the current leader releases it (leaving a `Del` marker in the key's history), but then *after* that, we begin watching the key for changes (waiting for it to be deleted, not knowing that it already has been).
                // todo: the only scenario this doesn't cover is when the lock is deleted via the bucket-wide TTL (i.e. `MaxAge`) — (hopefully) pending https://github.com/nats-io/nats-server/issues/3268
                IncludeHistory = true,
            }, cancellationToken: ct))
            {
                Console.WriteLine($"Watch entry: {entry}");
                if (entry.Operation is not NatsKVOperation.Del) // todo: automatic deletion due to `maxage` doesn't result in a `del` so we won't be notified — we are pending https://github.com/nats-io/nats-server/issues/3268
                    continue;

                return await AcquireOrStandBy(key, operation, ct);
            }

            throw new UnreachableException();
        }, operation, ct);
        return acquiredLock!;
    }

    private async Task<ILockHandle?> Acquire(
        string key,
        Func<INatsKVStore, ValueTask<ILockHandle?>> failure,
        Func<Task> operation,
        CancellationToken ct
    )
    {
        var kvStore = await _kvStore.Value;
        ulong revisionNumber;
        try
        {
            revisionNumber = await kvStore.CreateAsync<object?>(key, null, cancellationToken: ct); // NOTE: The cancellation token should only be used for the initial

            var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            _ = Renew(); // todo: is this safe or should we store the task somewhere?
            return new Lock(release: async () =>
            {
                // logger.LogInformation("Releasing lock {Key}...", key);
                cts.Cancel();
                try
                {
                    await kvStore.DeleteAsync(key, new() { Revision = revisionNumber, Purge = false });
                }
                catch { } // NOTE: Delete might throw if the revision number is stale, but that would mean another process has acquired the lock (due to us failing to refresh the lock in time) — but we don't care (hence the "swallowing" of the exception) because our goal of having the lock be released by us will effectively have been achieved.
                // logger.LogInformation("Lock {Key} released.", key);
            }, cts.Token);

            async Task Renew()
            {
                int x = 0;
                while (await RenewalFrequency.WaitAsync(cts.Token))
                {
                    // todo: what if the revision number is stale and this call throws here?
                    Console.WriteLine($"{++x}. renewing lock (rev: {revisionNumber})");
                    // if (x == 5)
                    //     await Task.Delay(10000);
                    try
                    {
                        // NOTE: The NATS internally retries if the NATS server can't be reached, which means we might have to wait here for a long time. There are two approaches in that scenario:
                        // 1. Pessimistic: We cancel the `cts` as soon as `Ttl` is passed via `CancelAfter(Ttl)`. 2. Optimistic: Do not cancel `cts` unless we've received a response from the NATS server explicitly indicating the lock has been lost. I've chosen the latter option.
                        revisionNumber = await kvStore.UpdateAsync<object?>(
                            key: key,
                            value: null,
                            revision: revisionNumber,
                            cancellationToken: cts.Token
                        );
                    }
                    catch (NatsKVWrongLastRevisionException ex)
                    {
                        Console.WriteLine($"Renewing lock failed — aborting operation and retrying: {ex}");
                        cts.Cancel();
                        await operation(); // NOTE: Effectively re-invoking the operation to begin competing for the lock again
                    }
                }
            }
        }
        catch (Exception ex) when (ex is NatsKVCreateException or NatsKVWrongLastRevisionException) // todo: why are we catching `NatsKVWrongLastRevisionException`?
        {
            return await failure(kvStore);
        }
    }

    private class Lock(Func<ValueTask> release, CancellationToken ct) : ILockHandle
    {
        public CancellationToken CancellationToken => ct;
        public ValueTask DisposeAsync() => release();
    }
}

public static class TimeSpanExtensions
{
    /// <summary>
    /// Waits for <paramref name="interval"/> amount of time, then returns `true` or `false` based on whether cancellation has been requested.
    /// Meant to be used with a `while` loop; incorporates a <see cref="CancellationToken"/> with a `Task.Delay`.
    /// </summary>
    public static async Task<bool> WaitAsync(
        this TimeSpan interval,
        CancellationToken ct
    )
    {
        await Task.Delay(interval, ct);
        return !ct.IsCancellationRequested;
    }
}
