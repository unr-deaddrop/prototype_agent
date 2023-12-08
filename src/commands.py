"""
All available commands defined by this agent.
"""
from abc import ABC, abstractmethod

class Command(ABC):
    @staticmethod
    @abstractmethod
    def blah(self, arg1):
        raise NotImplementedError