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
npm install redislock
```

## Usage

```js
const { RedisMutex } = require('redislock');
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
