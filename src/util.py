"""
Various utility routines.
"""
import celery
import redis

def get_redis_con(app: celery.Celery) -> redis.Redis:
    """
    Get the underlying Redis connection, assuming that the Celery application is
    connected to Redis.
    """
    return app.backend.client # type: ignore