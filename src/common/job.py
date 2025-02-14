from abc import ABC, abstractmethod


class BaseJob(ABC):
    """Интерфейс для реализации Job'ов"""

    @abstractmethod
    def run(self):
        """Метод запуска job'ы"""
