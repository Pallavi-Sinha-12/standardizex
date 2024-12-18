from abc import ABC, abstractmethod


class ConfigContract(ABC):

    @abstractmethod
    def generate_template():
        pass

    def validate_config():
        pass
