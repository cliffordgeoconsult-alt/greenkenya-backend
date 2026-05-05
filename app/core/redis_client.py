# app/core/redis_client.py
import redis
import json
import hashlib
import os

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

redis_client = redis.Redis.from_url(
    REDIS_URL,
    decode_responses=True
)

# ---------- KEY GENERATOR ----------
def make_cache_key(prefix, *args):
    raw = "|".join([str(a) for a in args])
    hashed = hashlib.md5(raw.encode()).hexdigest()
    return f"{prefix}:{hashed}"

# ---------- GET ----------
def cache_get(key):
    data = redis_client.get(key)
    if not data:
        return None
    return json.loads(data)

# ---------- SET ----------
def cache_set(key, value, ttl=3600):
    redis_client.setex(
        key,
        ttl,
        json.dumps(value)
    )