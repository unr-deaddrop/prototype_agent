from pathlib import Path
from celery import Celery
from celery.utils.log import get_task_logger

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
app = Celery(
    "tasks", backend="redis://localhost:6379/0", broker="redis://localhost:6379/0"
)

app.conf.timezone = "America/Los_Angeles"
app.conf.accept_content = ['pickle', 'json']

# You can use this to prefix keys, but they'll still have _celery-task-meta-{id} 
# at the end. So there's not really much point unless you're fighting with other 
# applications on the same Redis instance.
# app.conf.result_backend_transport_options = {'global_keyprefix': 'my_prefix_'}

@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    cfg_obj = cfg.Config.from_env_file(DEFAULT_ENV_PATH)
    sender.add_periodic_task(5.0, check_new_msgs.s(cfg_obj.MESSAGE_DIR, cfg_obj.DECODED_DIR), name="check for new messages")

# TODO: pickle is unsafe but (on paper) we just want this to work
#
# if someone wants to forge a message they can figure out the key and hmac anyways
@app.task(serializer='pickle')
def check_new_msgs(encoded_msg_dir: Path, decoded_msg_dir: Path) -> list[messages.AgentMessage]:
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
    all_files: list[Path] = []
    for path in encoded_msg_dir.glob("**/*"):
        if path.is_dir():
            continue
        all_files.append(path)
        
    if not all_files:
        logger.info(f"No files found in {encoded_msg_dir}")
        return []
    
    # Check which files are new based on _agent_meta-msgs; add new files to the
    # set referenced by the key.
    new_files = redis_con.smembers(REDIS_FILES_SEEN_KEY)
    redis_con.sadd(REDIS_FILES_SEEN_KEY, *new_files)
    
    # Decode each message and convert to AgentMessage.
    msgs: list[messages.AgentMessage] = []
    for new_file in new_files:
        msg: messages.AgentMessage = messages.AgentMessage.from_encoded_file(new_file, decoded_msg_dir)
        msgs.append(msg)
        
        # Insert AgentMessage as flat dictionary to Redis database
        redis_con.hmset(REDIS_INTERNAL_MSG_PREFIX+str(msg.msg_id), msg.model_dump())
    
    # Return as list of AgentMessage objects (which may or may not be used)
    return msgs

@app.task(serializer='pickle')
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
    
@app.task
def execute_command(msg: messages.AgentMessage) -> messages.AgentMessage:
    """
    Execute a command.
    
    The net result should be a constructed AgentMessage of command_response type,
    as built by the relevant command handler chosen by the command dispatch 
    (which might be this task?).
    """
    #TODO: implement
    