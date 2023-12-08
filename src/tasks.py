from base64 import b64decode, b64encode
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Union, Literal, Optional
import uuid

import dddb
from celery import Celery
import redis

# Paths to directories 
# TODO: Move these to main.py, these are "main thread" level configuration settings
# not Celery's problem
BASE_DIR = Path('msgs')

MESSAGE_DIR = BASE_DIR/Path('raw') # Incoming video storage
DECODED_DIR = BASE_DIR/Path('decoded') # Decoded message storage (as binary)
OUTGOING_DIR = BASE_DIR/Path('outgoing')  # Outgoing video storage

# TODO: rewrite as Pydantic thingy, it already has auto-JSON and auto-dict dumping
@dataclass
class AgentMessage:
    """
    Generic internal class representing a message.
    
    Because Redis only supports flat dictionaries, this is intended as a 
    "helper" class when converting between the Redis database and the Python
    world.
    """
    # The ID (uuidv4) of the message.
    uuid: uuid.UUID
    # The underlying message type.
    message_type: Union[Literal["command_request"], Literal["command_response"]]
    # The source of the message.
    intiated_by: Union[Literal["server"], Literal["agent"]]
    # Timestamp stored inside the message (not the time the message was retrieved).
    # Typically, refers to the time the message was issued.
    timestamp: datetime
    # Whether this message is considered valid; if False, this indicates the
    # associated file did not parse to valid JSON.
    is_valid: bool
    # Underlying message data. Note that the effective max size of this field
    # is about 500 MB, the max size of a redis vaule.
    data: bytes
    # Path to the encoded (video) file associated with this message.
    encoded_msg_path: Path 
    # Path to the decoded binary/text file associated with this message.
    decoded_msg_path: Path
    
    @classmethod
    def from_dict(cls, data: dict) -> "AgentMessage":
        """
        Construct and auto-cast values from a dictionary.
        """
    
    @classmethod
    def from_encoded_file(cls, encoded_filepath: Path) -> "AgentMessage":
        """
        Construct a message from an encoded file.
        
        All files are assumed to be valid JSON. Should a file fail to be decoded
        from JSON, `data` remains uninitialized (None), but `encoded_filepath`
        and `decoded_filepath` are set accordingly.
        """
        pass
    
    def to_encoded_file(self, out_dir: Path) -> None:
        """
        Write this object to an encoded file. 
        
        Sets `encoded_filepath` accordingly. The current value of `encoded_filepath`,
        if any, is ignored.
        """
    
    def to_dict(self) -> dict:
        """
        Convert this object to a dictionary suitable for storage in a Redis database.
        """
        
    def insert_to_redis(self, redis_con: redis.Redis) -> str:
        """
        Insert this message as a dictionary into a Redis database.
        
        The key used is "agent-msg-parsed-{uuid}". Returns the Redis key used.
        """
        

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

def decode_msg(msg_path: Path) -> Path:
    """
    Decode a video-encoded message, writing the result to "{filepath}_decoded"
    without an extension.
    """

@app.task
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
    
    # Perform recursive iteration on the folder
    
    # Decode each to decoded folder, keep track of each decoded filepath
    
    # Assuming each file is JSON, convert to AgentMessage
    
    # Insert AgentMessage to Redis database
    
    # Add to list of Redis keys, return

@app.task
def write_msg(msg: AgentMessage, out_dir: Path) -> None:
    """
    Write a message to a target path as an encoded video.
    """
    
    