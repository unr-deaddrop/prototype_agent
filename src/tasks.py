from pathlib import Path

import dddb
from celery import Celery
import redis

# from celery.schedules import crontab

# Location at 
MESSAGE_DIR = Path('msgs')

app = Celery(
    "tasks", backend="redis://localhost:6379/0", broker="redis://localhost:6379/0"
)

app.conf.timezone = "America/Los_Angeles"

# You can use this to prefix keys, but they'll still have _celery-task-meta-{id} 
# at the end. So there's not really much point unless you're fighting with other 
# applications on the same Redis instance.
# app.conf.result_backend_transport_options = {'global_keyprefix': 'my_prefix_'}

@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    sender.add_periodic_task(5.0, check_new_msgs.s(MESSAGE_DIR), name="check for new messages")

@app.task
def check_new_msgs(msg_path: Path) -> set[Path]:
    """
    Check for new messages.
    
    The filenames of recently processed messages are stored in a Redis set
    whose key is "_agent_meta-msgs". If a filename in the target directory 
    has NOT been seen, it is returned as part of the result, then added to 
    the Redis set.
    """
    # Create new Redis connection (for this worker)
    
    # Perform recursive iteration 