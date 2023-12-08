"""
Agent configuration definition and routines.
"""
from pathlib import Path

from dotenv import dotenv_values
from pydantic import BaseModel


class Config(BaseModel):
    """
    Agent-wide configuration definitions. Includes both non-sensitive and
    sensitive configurations set at runtime.
    """
    
    BASE_DIR: Path # Root directory of all messages
    MESSAGE_DIR: Path # Incoming video storage
    # TODO: should there be two separate "intermediate" storage folders?
    DECODED_DIR: Path # Decoded message storage (as unprefixed binary files)
    OUTGOING_DIR: Path # Outgoing video storage
    
    # HMAC_KEY
    # ENCRYPT_KEY
    
    @classmethod
    def from_env_file(cls, cfg_path: Path) -> "Config":
        # TODO: Eventually, this will include a "sensitive" environment
        # file, which is why we're taking this approach instead of just stealing
        # stuff from os.getenv() directly
        return Config.model_validate(dotenv_values(cfg_path))
    
    def create_dirs(self) -> None:
        """
        Create all associated directories.
        """
        # TODO: Perhaps these should be resolved by default?
        self.MESSAGE_DIR.resolve().mkdir(exist_ok=True, parents=True)
        self.DECODED_DIR.resolve().mkdir(exist_ok=True, parents=True)
        self.OUTGOING_DIR.resolve().mkdir(exist_ok=True, parents=True)
    