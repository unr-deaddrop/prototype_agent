[Celery Intro](https://docs.celeryq.dev/en/stable/getting-started/first-steps-with-celery.html#first-steps)

To set up RabbitMQ:
 - `sudo apt-get install rabbitmq-server`
 - `sudo rabbitmq-server` (if needed)

One-time:
- `sudo rabbitmqctl add_user myuser mypassword`
- `sudo rabbitmqctl add_vhost myvhost`
- `sudo rabbitmqctl set_user_tags myuser mytag`
- `sudo rabbitmqctl set_permissions -p myvhost myuser ".*" ".*" ".*"`

Then define tasks in `tasks.py`. The backend is where results get stored; this can be the Django ORM, Redis, or (since we've used RabbitMQ here), RPC.

To start the worker server, run
`celery -A tasks worker --loglevel=INFO`

at which point you can define stuff that imports and runs stuff off of `tasks.py` as desired.

Note that `celery beat` is a different thing from `celery worker`; `beat` is responsible for issuing periodic tasks to `worker`, which is responsible for actually executing the tasks. To start `beat`, run

`celery -A tasks beat`


For redis, just install redis normally and `pip install -U celery[redis]`, then run `redis-server`. So:
- Run `redis-server`
- `celery -A tasks beat --loglevel=INFO`
- `celery -A tasks worker --loglevel=INFO`
- (venv) `python3 src/main.py`

