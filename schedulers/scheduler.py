import sys
import json
from stores.base import ConflictingIdError, TaskLookupError
import time
import queue
import logging
import threading
from types import FunctionType
from typing import List
from datetime import datetime

from exceptions import TaskStopException, TaskContinueException

import lock
import stores
from stores.task import Task


class Scheduler(object):

    def __init__(self,
                 max_batch_size=50,
                 min_batch_size=10,
                 task_lock_timeout=30,
                 store: stores.BaseTaskStore = None,
                 lockClass: lock.LockClass = lock.LocalLock,
                 logger: logging.Logger = None):
        '''
        param int max_batch_size: 批次处理任务数量最大值
        param int min_batch_size: 批次处理任务数量最小值
        param int task_lock_timeout: 任务最大运行时长
        '''
        self.max_batch_size = max_batch_size
        self.min_batch_size = min_batch_size
        self._avai_worker_cnt = max_batch_size
        self.task_lock_timeout = task_lock_timeout
        self.store = store
        self._event = threading.Event()
        self._process_tasks_lock = lockClass('process_tasks')
        self._worker_cnt_lock = threading.Lock()
        self._handlers = {}
        self._task_queue = queue.Queue(maxsize=max_batch_size)
        self._terminated_task_cnt = 0
        if logger:
            self._logger = logger
        else:
            self._logger = logging.getLogger('python-tasks')
            self._logger.setLevel('DEBUG')
            hd = logging.StreamHandler()
            hd.setLevel('DEBUG')
            self._logger.addHandler(hd)

    def add_task(self, task: Task):
        try:
            self.store.add_task(task)
        except ConflictingIdError:
            return
        except AssertionError as err:
            self._logger.error('{}'.format(err), exc_info=True)
        self.wakeup()

    def update_task(self, task: Task):
        try:
            self.store.update_task(task)
        except TaskLookupError as err:
            self._logger.error('{}'.format(err), exc_info=True)
            return
        self.wakeup()

    def remove_task(self, task: Task):
        self.store.remove_task(task)
        self.wakeup()

    def _process_tasks(self):
        # 当系统没有任务的时候，默认每5秒轮询一次
        wait_seconds = 5
        if not self._process_tasks_lock.acquire():
            return wait_seconds
        try:
            if self._avai_worker_cnt < self.min_batch_size:
                return wait_seconds
            self._logger.info('当前空闲worker数量为{}, 尝试拉取{}个任务'.format(self._avai_worker_cnt, self._avai_worker_cnt))
            tasks: List[Task] = self.store.get_due_tasks(int(time.time()), self._avai_worker_cnt)
            self.store.lock_tasks(tasks, int(time.time()) + self.task_lock_timeout)
            for task in tasks:
                self._task_queue.put(task)
            if self._avai_worker_cnt >= self.min_batch_size:
                next_run_time = self.store.get_next_run_time()
                if next_run_time is not None:
                    wait_seconds = next_run_time - int(time.time())
            if wait_seconds < 0:
                wait_seconds = 1
        except Exception as err:
            self._logger.error('处理任务失败: {}'.format(err), exc_info=True)
        finally:
            self._process_tasks_lock.release()
        return wait_seconds

    def _worker(self):
        while True:
            task: Task = self._task_queue.get()
            if task.deleted_at > 0:
                sys.exit(1)
            with self._worker_cnt_lock:
                self._avai_worker_cnt -= 1
            task.sched_times += 1
            max_sched_times = task.max_sched_times
            start_at = time.time()
            to_be_continue = True
            args = ()
            kwargs = {}
            try:
                args = json.loads(task.func_args)
                kwargs = json.loads(task.func_kwargs)
                max_sched_times = kwargs.get('max_sched_times', max_sched_times)
            except Exception as err:
                self._logger.error(f'任务{task.id}加载参数报错: {err}', exc_info=True)
            to_be_continue = False if task.sched_times >= max_sched_times and max_sched_times > 0 else True
            try:
                f = self._handlers[task.func_name]
                res = f(*args, **kwargs)
                if res:
                    task.deleted_at = int(time.time())
                else:
                    task.next_run_time = int(time.time()) + task.sched_times * task.incr_step
            except TaskContinueException:
                if to_be_continue:
                    task.next_run_time = int(time.time()) + task.sched_times * task.incr_step
                else:
                    task.deleted_at = int(time.time())
                    self._logger.error('任务{}达到最大处理次数限制: {}'.format(task.id, err), exc_info=True)
            except TaskStopException:
                task.deleted_at = int(time.time())
            except Exception as err:
                if to_be_continue:
                    self._logger.error('处理任务{}失败: {}'.format(task.id, err), exc_info=True)
                    task.next_run_time = int(time.time()) + task.sched_times * task.incr_step
                else:
                    task.deleted_at = int(time.time())
                    self._logger.error('任务{}达到最大处理次数限制: {}'.format(task.id, err), exc_info=True)
            finally:
                end_at = time.time()
                cost = round(end_at - start_at, 2)
                next_run_time = None if task.deleted_at > 0 else datetime.fromtimestamp(task.next_run_time)
                in_seconds = None if task.deleted_at > 0 else task.sched_times * task.incr_step
                self._logger.info('任务({})已经执行({})次, 下次运行时间: {} ({}秒后) -- 耗时{}s'.format(task.id, task.sched_times, next_run_time, in_seconds, cost))
                self.update_task(task)
                with self._worker_cnt_lock:
                    if task.deleted_at > 0:
                        self._terminated_task_cnt += 1
                    self._avai_worker_cnt += 1

    def _start_workers(self):
        for _ in range(self.max_batch_size):
            th = threading.Thread(target=self._worker, daemon=True)
            th.start()

    def wakeup(self):
        self._logger.info('avai_worker_cnt: {}'.format(self._avai_worker_cnt))
        with self._worker_cnt_lock:
            if self._avai_worker_cnt >= self.min_batch_size:
                self._event.set()

    def start(self, background=True):
        self._start_workers()
        th = threading.Thread(target=self._main_loop, name='python-tasks', daemon=True)
        th.start()
        if not background:
            th.join()

    def _main_loop(self):
        wait_seconds = 2
        while True:
            self._event.wait(wait_seconds)
            self._event.clear()
            wait_seconds = self._process_tasks()
            self._logger.info('调度器({})秒后再次唤醒, 终止任务数{}'.format(wait_seconds, self._terminated_task_cnt))

    def register(self, func: FunctionType):
        self._handlers[func.__name__] = func
        return func
