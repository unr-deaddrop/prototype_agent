"""
Agent-specific internal message library.

TODO: Note that this message library DOES NOT modularize the protocol handler from
the message dispatch unit. In the future, what *should* happen is that an abstract
protocol handler class should be made that has (exactly?) two methods - one for
decoding and another for encoding - which is called based on the protocol passed into
AgentMessage.
"""

from base64 import b64decode, b64encode
from enum import Enum
from datetime import datetime
from pathlib import Path
from typing import ClassVar, Union, Literal
import binascii
import logging
import json
import uuid

from pydantic import BaseModel, Field, field_serializer, field_validator
import redis

import dddb

logger = logging.getLogger(__name__)


class Base64Encoder(json.JSONEncoder):
    """
    Passthrough conversion of bytes-like objects into a base64 string suitable
    for transmission as a raw text file.

    References:
    - https://stackoverflow.com/questions/37225035/serialize-in-json-a-base64-encoded-data
    """

    # pylint: disable=method-hidden
    def default(self, o):
        if isinstance(o, bytes):
            return b64encode(o).decode()
        return json.JSONEncoder.default(self, o)


class MessageType(str, Enum):
    """
    String enumeration of supported message types.
    """

    CMD_RESPONSE = "command_response"
    CMD_REQUEST = "command_request"


class AgentMessage(BaseModel):
    """
    Generic internal class representing a message.

    Because Redis only supports flat dictionaries, this is intended as a
    "helper" class when converting between the Redis database and the Python
    world.

    TODO: well, since you can store strings, and since we can choose how to serialize
    JSON, why not just store the whole thing as a giant string?

    As a Pydantic model, it supports some convenient classmethods:
    - `model_validate()` constructs the model from a dictionary.
    - `model_validate_json()` accepts a `str` or `bytes`, interprets it as
      JSON, then passes the result to `model_validate()`.
    And some instance methods:
    - `model_dump()` converts the model directly to a dictionary.
    - `model_dump_json()` converts the model to JSON (as `str`).

    These convenience methods from Pydantic make it significantly easier
    to deal with Redis, especially when combined with the validation/serialization
    hook-ins below.

    References:
    - https://docs.pydantic.dev/latest/concepts/models/#helper-functions
    """

    # The prefix used when using this class to directly add itself to a Redis
    # instance. Guarantees all keys with this prefix are representative of a
    # particular message.
    REDIS_KEY_PREFIX: ClassVar[str] = "agent-msg-parsed-"

    # The UUID of the message. If not set at construct time, it is set
    # to a random value (i.e. uuidv4).
    msg_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    # The underlying message type.
    message_type: Union[
        Literal[MessageType.CMD_RESPONSE], Literal[MessageType.CMD_REQUEST]
    ]
    # The source of the message.
    initiated_by: Union[Literal["server"], Literal["agent"]]
    # Timestamp stored inside the message (not the time the message was retrieved).
    # Typically, refers to the time the message was issued. By default, this is
    # the current time (at construct time).
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    # Whether this message is considered valid; if False, this indicates the
    # associated file did not parse to valid JSON.
    is_valid: bool = True
    # Underlying message data. Note that the effective max size of this field
    # is about 500 MB, the max size of a redis value.
    data: bytes | None = None
    # Path to the encoded (video) file associated with this message.
    encoded_msg_path: Path | None = None
    # Path to the decoded binary/text file associated with this message.
    decoded_msg_path: Path | None = None

    @field_validator("data", mode="before")
    @classmethod
    def validate_data(cls, v: str | bytes):
        """
        Before validation, assume that incoming string data is base64-encoded;
        decode it if so.

        If the incoming data is `bytes`, keep it exactly as is.
        """
        if isinstance(v, str):
            try:
                v = b64decode(v, validate=True)
            except binascii.Error:
                pass

        return v

    @field_serializer("data", when_used="json-unless-none")
    def serialize_data(self, data: bytes, _info):
        """
        On JSON serialization, the data field is always a base64-encoded message.
        In all other cases, it is kept as `bytes`.
        """
        return b64encode(data).decode()

    @field_serializer("timestamp", when_used="json-unless-none")
    def serialize_timestamp(self, timestamp: datetime, _info):
        """
        On JSON serialization, the timestamp is always numeric.
        """
        return timestamp.timestamp()

    @classmethod
    def from_encoded_file(
        cls, encoded_filepath: Path, decoded_dir: Path, decoded_suffix: str = ".data"
    ) -> "AgentMessage":
        """
        Construct a message from an encoded file.

        All decoded messages are assumed to be valid JSON. Should a file fail
        to be decoded from JSON, `data` remains uninitialized (None), but
        `encoded_filepath` and `decoded_filepath` are set accordingly.

        `is_valid` is set to False on failure.
        """
        decoded_filename = encoded_filepath.name + decoded_suffix
        out_path = str((decoded_dir / Path(decoded_filename)).resolve())
        # Write result out to disk (since we can't hold it in memory)
        dddb.video.decode(
            {"in_path": str(encoded_filepath.resolve()), "out_path": out_path}
        )

        # Grab result back from disk, interpret as JSON
        with open(out_path, "rb") as fp:
            msg = cls.model_validate_json(fp.read())
            msg.encoded_msg_path = encoded_filepath
            msg.decoded_msg_path = out_path
            return msg

    def to_encoded_file(
        self, decoded_dir: Path, encoded_dir: Path, suffix: str = ".mp4"
    ) -> Path:
        """
        Write this object to an encoded file.

        The name of the encoded file is simply {msg_id}{suffix}, e.g.
        550e8400-e29b-41d4-a716-446655440000.mp4.

        This sets `encoded_filepath` accordingly. The current value of
        `encoded_filepath`, if any, is ignored.

        If the path already exists, this raises a RuntimeError.

        Returns the filepath to the written video.
        """
        decoded_out_path = (
            decoded_dir / (Path(str(self.msg_id)).with_suffix(".data"))
        ).resolve()
        encoded_out_path = (encoded_dir / Path(str(self.msg_id) + suffix)).resolve()
        if decoded_out_path.exists():
            raise RuntimeError(f"A message at {decoded_out_path} already exists!")

        logger.debug(
            f"Writing the following decoded message to {decoded_out_path}: {self}"
        )
        with open(decoded_out_path, "w+") as fp:
            fp.write(self.model_dump_json())

        logger.debug(f"Using dddb to convert {decoded_out_path} to {encoded_out_path}")
        dddb.video.encode(
            {"in_path": str(decoded_out_path), "out_path": str(encoded_out_path)}
        )
        return encoded_out_path

    def get_redis_key(self) -> str:
        """
        Retrive this message's resulting Redis key.
        """
        return self.REDIS_KEY_PREFIX + self.msg_id

    def insert_to_redis(self, redis_con: redis.Redis) -> str:
        """
        Insert this message as a dictionary into a Redis database.

        The key used is "agent-msg-parsed-{uuid}". Returns the Redis key used.
        """
        key = self.get_redis_key()
        redis_con.hset(key, mapping=self.model_dump())
        return key
