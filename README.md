## Development
To get the dependencies (Redis and pip installables), simply run `make deps` (preferably in the context of a venv).

To start everything, simply run `make` (or `make all`). This will handle daemonization for you.

In the event you need to kill supervisord, run `make kill`.

## The prototype message format
Derived from P3, all messages are expected to be valid, flat JSON documents with the following structure:
```json

{
	"id": "bd65600d-8669-4903-8a14-af88203add38",
	"message_type": "command_request",
	"initiated_by": "agent", // one of "server", "agent"
	"timestamp": 000000000, // machine UTC timestamp from the Unix epoch
	"data": "data", // Arbitrary binary data as base64
	// Internal agent variables, to be ignored by recipient; may or may not
	// be included in outgoing messages. Any field besides the above may or
	// may not be ignored by another agent or the server.
	"extra":{
		"encoded_msg_path": "local filepath name",
		"decoded_msg_path": "local filepath name"		
	}
}
```

## Project notes
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


So to start and daemonize everything, just run `supervisord` in the root; by default, it will look for `supervisord.conf` in the current directory before looking elsewhere (`/etc/supervisord.conf`). You'll still need to run the app itself.
