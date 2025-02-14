from typing import Generator, Any

from src.common.job import BaseJob
from collections import deque

from src.utils.singleton import Singleton


def job_id_gen() -> Generator[int, Any, Any]:
    job_id = 1
    while True:
        yield job_id
        job_id += 1


class Pipeline(metaclass=Singleton):
    """Пайплан для запуска job'ов"""

    def __init__(self, jobs: deque[BaseJob]):
        self.jobs: deque = jobs
        self.job_id_gen = job_id_gen()
        self.job_id = next(self.job_id_gen)

    def run(self):
        while len(self.jobs):
            job = self.jobs.popleft()
            try:
                print(f"Job with {self.job_id} ran")
                job.run()
                print(f"Job with {self.job_id} finished")
            except Exception as exc:
                print(f"Error in job with {self.job_id}: {exc}")

            self.job_id = next(self.job_id_gen)
