# redisLock

A minimal npm package for distributed mutex locks backed by a single Redis instance.

This package is designed for simple Redis setups without Sentinel or Redis Cluster support. It uses Redis key-based locking with a lease/TTL so a lock is automatically released if the process crashes or the lock holder stops renewing it.

## Features

- Simple acquire/release API
- TTL-based lock expiry
- Renew/extend support for long-running work
- Auto-extend support with a background interval
- Safe ownership checks on release
- Works with a standard Redis client

## Installation

```bash
npm install @yasarrkhan/redislock
```

## Usage

```js
const { RedisMutex } = require('@yasarrkhan/redislock');
const { createClient } = require('redis');

(async () => {
  const client = createClient({ url: 'redis://localhost:6379' });
  await client.connect();

  const mutex = new RedisMutex(client, { ttlMs: 30000 });
  const token = await mutex.acquire('my-lock');

  if (token) {
    const stopAutoExtend = mutex.startAutoExtend('my-lock', token, {
      intervalMs: 10000,
      ttlMs: 30000
    });

    try {
      // critical section
      await mutex.renew('my-lock', token, 30000);
    } finally {
      stopAutoExtend();
      await mutex.release('my-lock', token);
    }
  }
})();
```

## Notes

- This package targets single-node Redis deployments.
- For production-grade failover scenarios, consider Redis Sentinel or Redis Cluster.
- The lock is best-effort and relies on the Redis client supplied by the caller.
- This lock is not a correctness boundary. It can reduce concurrent access, but it does not guarantee that two processes will never act on stale state at the same time.
- Even when you use the API correctly, the lock can still fail under GC pauses or other long stop-the-world delays. A process may lose its lease, another process may acquire the lock, and a later `renew` or `extend` call can fail.
- The risky case is when you update application state before discovering that the lock has expired. In that situation, the state change may already be visible even though the lock is no longer valid.
- If you need correctness, use fencing tokens with Redis locks and have downstream writes reject stale tokens.
