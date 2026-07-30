const test = require('node:test');
const assert = require('node:assert/strict');
const { RedisMutex } = require('../lib/index.js');

test('acquire returns a token and release clears it', async () => {
  const store = new Map();
  const client = {
    async set(key, value, options) {
      const existing = store.get(key);
      if (existing && options.NX) {
        return null;
      }
      store.set(key, { value, ttlMs: options.PX });
      return 'OK';
    },
    async eval(script, keys, args) {
      const [key] = keys;
      const [token] = args;
      const current = store.get(key);
      if (!current) {
        return 0;
      }
      if (current.value !== token) {
        return 0;
      }
      store.delete(key);
      return 1;
    }
  };

  const mutex = new RedisMutex(client, { ttlMs: 3000 });
  const token = await mutex.acquire('job:1');

  assert.equal(typeof token, 'string');
  assert.ok(token.length > 0);

  const released = await mutex.release('job:1', token);
  assert.equal(released, true);
  assert.equal(store.has('job:1'), false);
});

test('renew and auto extend keep the lock alive', async () => {
  const store = new Map();
  let renewCalls = 0;
  const client = {
    async set(key, value, options) {
      const existing = store.get(key);
      if (existing && options.NX) {
        return null;
      }
      if (existing && options.XX) {
        if (existing.value !== value) {
          return null;
        }
        store.set(key, { value, ttlMs: options.PX });
        return 'OK';
      }
      store.set(key, { value, ttlMs: options.PX });
      return 'OK';
    },
    async eval(script, keys, args) {
      renewCalls += 1;
      const [key] = keys;
      const [token, ttlMs] = args;
      const current = store.get(key);
      if (!current || current.value !== token) {
        return 0;
      }
      current.ttlMs = Number(ttlMs);
      return 1;
    }
  };

  const mutex = new RedisMutex(client, { ttlMs: 1000, retryDelayMs: 1, maxAttempts: 1 });
  const token = await mutex.acquire('job:2');

  assert.equal(await mutex.renew('job:2', token, 5000), true);

  const stopAutoExtend = mutex.startAutoExtend('job:2', token, { intervalMs: 5, ttlMs: 5000 });
  await new Promise(resolve => setTimeout(resolve, 20));
  stopAutoExtend();

  assert.ok(renewCalls >= 2);
});

test('acquire stops trying after the configured timeout', async () => {
  const store = new Map();
  const client = {
    async set() {
      return null;
    }
  };

  const mutex = new RedisMutex(client, { ttlMs: 1000, retryDelayMs: 5, maxAttempts: 5 });
  const token = await mutex.acquire('job:3', { acquireTimeoutMs: 15 });

  assert.equal(token, null);
});

test('auto extend stops after the configured maximum duration', async () => {
  const store = new Map();
  const client = {
    async set(key, value, options) {
      store.set(key, { value, ttlMs: options.PX });
      return 'OK';
    },
    async eval() {
      return 1;
    }
  };

  const mutex = new RedisMutex(client, { ttlMs: 1000, retryDelayMs: 1, maxAttempts: 1 });
  const token = await mutex.acquire('job:4');
  const stopAutoExtend = mutex.startAutoExtend('job:4', token, { intervalMs: 1, ttlMs: 1000, maxDurationMs: 20 });

  await new Promise(resolve => setTimeout(resolve, 30));
  stopAutoExtend();

  assert.ok(store.has('job:4'));
});
