import time
from typing import List

from pymongo import ASCENDING, IndexModel, MongoClient
from pymongo.errors import DuplicateKeyError

from .base import BaseTaskStore, ConflictingIdError, TaskLookupError
from .task import Task


class MongoDBTaskStore(BaseTaskStore):
    def __init__(self, db: str = 'python-tasks', collection: str = 'tasks', client: MongoClient = None, **connection_args):
        if not db:
            raise ValueError('db不能为空')
        if not collection:
            raise ValueError('collection不能为空')
        if client:
            self.client = client
        else:
            self.client = MongoClient(**connection_args)

        self.collection = self.client[db][collection]
        idx1 = IndexModel([('next_run_time', ASCENDING), ('deleted_at', ASCENDING)])
        idx2 = IndexModel([('_id', ASCENDING)])
        idx3 = IndexModel([('func_name', ASCENDING)])
        self.collection.create_indexes([idx1, idx2, idx3])

    def add_task(self, task: Task) -> None:
        '''
        :raises ConflictingIdError: 任务已经存在抛异常
        '''
        assert isinstance(task.func_args, str)
        assert isinstance(task.func_kwargs, str)
        try:
            self.collection.insert({
                '_id': task.task_id,
                'func_name': task.func_name,
                'func_args': task.func_args,
                'func_kwargs': task.func_kwargs,
                'sched_times': task.sched_times,
                'max_sched_times': task.max_sched_times,
                'next_run_time': task.next_run_time,
                'incr_step': task.incr_step,
                'created_at': int(time.time()),
                'updated_at': int(time.time()),
                'deleted_at': task.deleted_at,
            })
        except DuplicateKeyError:
            raise ConflictingIdError(task.task_id)

    def lookup_task(self, task_id: str) -> Task:
        document = self.collection.find_one({'_id': task_id})
        if not document:
            return None
        task = Task(**document)
        task.id = task.task_id = document['_id']
        return task

    def update_task(self, task: Task) -> None:
        '''
        :raises TaskLookupError: 任务不存在抛异常
        '''
        assert isinstance(task, Task)
        changes = {
            'sched_times': task.sched_times,
            'updated_at': int(time.time()),
            'next_run_time': task.next_run_time,
            'deleted_at': task.deleted_at
        }
        result = self.collection.update_one({'_id': task.task_id}, {'$set': changes})
        if result and result.modified_count == 0:
            raise TaskLookupError(task.task_id)

    def remove_task(self, task: Task) -> None:
        assert isinstance(task, Task)
        result = self.collection.remove({'_id': task.task_id})
        if result and result['n'] == 0:
            raise TaskLookupError(task.task_id)

    def get_due_tasks(self, now: int, count: int) -> List[Task]:
        '''
        :param int now: 时间戳
        :param int count: 获取任务数量
        '''
        conditions = {'$or': [
            {
                'next_run_time': {'lte': now},
                'deleted_at': 0,
                'max_sched_times': 0
            },
            {
                'next_run_time': {'lte': now},
                'deleted_at': 0,
                'max_sched_times': {'gt': 0},
                '$expr': {'$gte': ['$sched_times', '$max_sched_times']}
            }
        ]}
        conditions = {
            'next_run_time': {'$lte': now},
            'deleted_at': 0,
        }
        documents = self.collection.find(conditions, sort=[('next_run_time', ASCENDING)]).limit(count)
        tasks = []
        for document in documents:
            task = Task(**document)
            task.id = task.task_id = document['_id']
            tasks.append(task)
        return tasks

    def get_next_run_time(self):
        '''
        :return int or None: 确保返回整数
        '''
        document = self.collection.find_one(
            {'deleted_at': 0},
            projection=['next_run_time'],
            sort=[('next_run_time', ASCENDING)]
        )
        return int(document['next_run_time']) if document else None

    def lock_tasks(self, tasks: List[Task], lock_expire_time: int):
        task_ids = [task.task_id for task in tasks]
        self.collection.update_many({'_id': {'$in': task_ids}}, {'$set': {'next_run_time': lock_expire_time}})
