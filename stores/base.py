from abc import ABC, abstractmethod
from typing import List

from .task import Task


class TaskLookupError(KeyError):
    def __init__(self, task_id):
        super(TaskLookupError, self).__init__('任务({})不存在'.format(task_id))


class ConflictingIdError(KeyError):
    def __init__(self, task_id):
        super(ConflictingIdError, self).__init__('任务({})已经存在'.format(task_id))


class BaseTaskStore(ABC):
    @abstractmethod
    def add_task(self, task: Task) -> None:
        '''
        :raises ConflictingIdError: 任务已经存在抛异常
        '''

    @abstractmethod
    def lookup_task(self, task_id: str) -> Task:
        '''
        :raises TaskLookupError: 任务不存在抛异常
        '''

    @abstractmethod
    def update_task(self, task: Task) -> None:
        '''
        :raises TaskLookupError: 任务不存在抛异常
        '''

    @abstractmethod
    def remove_task(self, task: Task) -> None:
        pass

    @abstractmethod
    def get_due_tasks(self, now: int, count: int) -> List[Task]:
        '''
        :param int now: 时间戳
        :param int count: 获取任务数量
        '''

    @abstractmethod
    def get_next_run_time(self):
        '''
        :return int or None: 确保返回整数
        '''

    @abstractmethod
    def lock_tasks(self, tasks: List[Task], lock_expire_time: int):
        pass
