from abc import ABC, abstractmethod

class baseLoadDim(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def load(self):
        pass