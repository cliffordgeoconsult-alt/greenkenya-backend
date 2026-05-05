from functools import wraps
from app.core.redis_client import cache_get, cache_set, make_cache_key

def redis_cache(prefix, ttl=3600):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):

            key = make_cache_key(prefix, args, kwargs)

            cached = cache_get(key)
            if cached is not None:
                print(f"⚡ REDIS HIT: {prefix}")
                return cached

            result = func(*args, **kwargs)

            cache_set(key, result, ttl)
            print(f"💾 REDIS SET: {prefix}")

            return result

        return wrapper
    return decorator