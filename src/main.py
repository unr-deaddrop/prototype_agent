"""
The main process of the application.
"""
from pathlib import Path
import argparse
import logging
import sys
import time

from celery.result import AsyncResult
import redis

from .tasks import app, write_msg
from . import cfg
from . import messages
from . import util

# Default non-sensitive environment path.
DEFAULT_ENV_PATH = Path("./common.env")

# Set up logging
logging.basicConfig(
    handlers=[logging.StreamHandler(sys.stdout)],
    level=logging.DEBUG,
    format="%(filename)s:%(lineno)d | %(asctime)s | [%(levelname)s] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger()

def get_stored_tasks(redis_con: redis.Redis) -> list[AsyncResult]:
    """
    Get all stored tasks in a Redis database.

    This assumes that the prefix is "celery-task-meta-*"; that is, there
    is no Redis prefix that has been manually set, and the default Celery
    prefix is being used.
    """
    # https://stackoverflow.com/questions/72115457/how-can-i-get-results-failures-for-celery-tasks-from-a-redis-backend-when-i-don
    task_results: list[AsyncResult] = []

    # The assumption of celery-task-meta-* is safe for Celery.
    # Additionally, Backend.client is specific to Redis, so we ignore mypy's linting
    # error here.
    for key in redis_con.scan_iter("celery-task-meta-*"):  # type: ignore[attr-defined]
        task_id = str(key).split("celery-task-meta-", 1)[1].replace("'", "")
        task_results.append(AsyncResult(task_id, app=app))
    return task_results

def get_new_msgs(redis_con: redis.Redis, delete_msgs: bool = False) -> list[messages.AgentMessage]:
    """
    Get and reconstruct all messages remaining in the Redis database.
    
    Set `delete_msgs` to `True` to delete each message on retrieval.
    """
    msgs: list[messages.AgentMessage] = []
    
    for key in redis_con.scan_iter(messages.AgentMessage.REDIS_KEY_PREFIX+"*"):
        data = redis_con.hgetall(key)
        msg_obj = messages.AgentMessage.model_validate(data)
        msgs.append(msg_obj)
        logger.debug(f"Retrieved redis key {key} to form the message {msg_obj}")
        
        if delete_msgs:
            logger.info(f"Deleted redis key {key}")
            redis_con.delete(key)
            
    return msgs

def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Creates and verifies .lol files.")
    
    # Access this argument as env_path.
    parser.add_argument(
        "--env",
        "-e",
        default=DEFAULT_ENV_PATH,
        type=Path,
        help="The configuration variables for the agent.",
        required=False,
        dest="env_path"
    )

    return parser.parse_args()

def main(cfg_obj: cfg.Config) -> None:
    """
    Main thread.
    
    Right now, this just checks for new messages placed by Celery into Redis,
    then 
    """
    redis_connected: bool = False
    
    while True:
        try:            
            new_msgs = get_new_msgs(util.get_redis_con(app), delete_msgs=True)
            if not redis_connected:
                logger.info("Successfully connected to Redis")
                redis_connected = True
        except redis.exceptions.ConnectionError:
            logger.warning("Couldn't connect to Redis, retrying in a second")
            time.sleep(1)
            continue
        
        if not new_msgs:
            logger.info("No new messages, sleeping for 5s")
            time.sleep(5)
        
        for msg in new_msgs:
            # Do something with the message using the command module for the agent.
            # For now, let's just reverse the message and write a response message
            # for each one
            new_data = msg.data[::-1]
            
            new_msg = messages.AgentMessage(
                message_type=messages.MessageType.CMD_RESPONSE,
                intiated_by="agent",
                data=new_data
            )
            
            logger.debug(f"Writing new message {new_msg}")

if __name__ == "__main__":
    args = get_args()
    cfg_obj = cfg.Config.from_env_file(args.env_path)
    # Create any required directory structure
    cfg_obj.create_dirs()
    
    main(cfg_obj)
