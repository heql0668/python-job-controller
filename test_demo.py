import time
import random

from stores.task import Task
from stores.mongodb import MongoDBTaskStore
from schedulers.scheduler import Scheduler


store = MongoDBTaskStore(db='tasks', collection='python-tasks-demo', host='127.0.0.1', port=31001)
sched = Scheduler(store=store, max_batch_size=50, min_batch_size=10)


@sched.register
def demo():
    sleep_seconds = random.randint(1, 2)
    mod = random.randint(1, 5) % 2
    print(f'demo sleep {sleep_seconds}s, mod: {mod}')
    time.sleep(sleep_seconds)
    return mod == 0


def add_tasks(start: int, end: int):
    for idx in range(start, end):
        task = Task()
        task.task_id = f'demo{idx}'
        task.func_name = 'demo'
        task.incr_step = 1
        task.max_sched_times = random.randint(3, 5)
        sched.add_task(task)


if __name__ == '__main__':
    # add_tasks(1, 101)
    sched.start(background=False)
