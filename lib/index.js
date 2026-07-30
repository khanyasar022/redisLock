const crypto = require('node:crypto');

class RedisMutex {
  constructor(client, options = {}) {
    this.client = client;
    this.ttlMs = options.ttlMs || 30000;
    this.retryDelayMs = options.retryDelayMs || 50;
    this.maxAttempts = options.maxAttempts || 5;
    this.backoffMultiplier = options.backoffMultiplier || 2;
    this._autoExtendTimers = new Map();
  }

  async acquire(key, options = {}) {
    const token = crypto.randomUUID();
    const ttlMs = options.ttlMs || this.ttlMs;
    const acquireTimeoutMs = options.acquireTimeoutMs ?? Number.MAX_SAFE_INTEGER;
    const maxAttempts = options.maxAttempts ?? this.maxAttempts;
    const initialRetryDelayMs = options.retryDelayMs ?? this.retryDelayMs;
    const backoffMultiplier = options.backoffMultiplier ?? this.backoffMultiplier;
    const startTime = Date.now();
    const acquired = await this.client.set(
      key,
      token,
      { NX: true, PX: ttlMs }
    );

    if (acquired === 'OK') {
      return token;
    }

    for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
      const elapsed = Date.now() - startTime;
      if (elapsed >= acquireTimeoutMs) {
        return null;
      }

      const delayMs = initialRetryDelayMs * Math.pow(backoffMultiplier, attempt - 1);
      await this.sleep(delayMs);
      const repeated = await this.client.set(
        key,
        token,
        { NX: true, PX: ttlMs }
      );
      if (repeated === 'OK') {
        return token;
      }
    }

    return null;
  }

  async renew(key, token, ttlMs = this.ttlMs) {
    if (!token) {
      return false;
    }

    const result = await this.client.eval(
      'if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("pexpire", KEYS[1], ARGV[2]) else return 0 end',
      [key],
      [token, String(ttlMs)]
    );

    return result === 1;
  }

  startAutoExtend(key, token, options = {}) {
    if (!token) {
      return () => {};
    }

    const intervalMs = options.intervalMs || Math.max(1000, Math.floor(this.ttlMs / 2));
    const ttlMs = options.ttlMs || this.ttlMs;
    const maxDurationMs = options.maxDurationMs ?? 5 * 60 * 1000;
    const startedAt = Date.now();

    const timer = setInterval(async () => {
      if (Date.now() - startedAt >= maxDurationMs) {
        clearInterval(timer);
        if (this._autoExtendTimers.get(key) === timer) {
          this._autoExtendTimers.delete(key);
        }
        return;
      }

      await this.renew(key, token, ttlMs);
    }, intervalMs);

    const existing = this._autoExtendTimers.get(key);
    if (existing) {
      clearInterval(existing);
    }
    this._autoExtendTimers.set(key, timer);

    return () => {
      clearInterval(timer);
      if (this._autoExtendTimers.get(key) === timer) {
        this._autoExtendTimers.delete(key);
      }
    };
  }

  async release(key, token) {
    if (!token) {
      return false;
    }

    const result = await this.client.eval(
      'if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("del", KEYS[1]) else return 0 end',
      [key],
      [token]
    );

    return result === 1;
  }

  sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

module.exports = {
  RedisMutex
};
