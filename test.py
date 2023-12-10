# import redis

# r = redis.Redis(host='localhost', port=6379, decode_responses=True)
# # Working with Redis sets https://redis.io/docs/data-types/sets/
# # https://redis-py.readthedocs.io/en/stable/commands.html

# eles_added = r.sadd('ctx', 'what', 'try')
# print(eles_added)
# # True
# print(r.smembers('ctx'))

import dddb
dddb.video.encode({'in_path': 'test.json', 'out_path': 'test.mp4'})