class Task:

    def __init__(self, **kwargs):
        self.id = kwargs.get('id')
        self.task_id = kwargs.get('task_id', '')
        self.func_name = kwargs.get('func_name', '')
        self.func_args = kwargs.get('func_args', '[]')
        self.func_kwargs = kwargs.get('func_kwargs', '{}')
        self.sched_times = kwargs.get('sched_times', 0)
        self.max_sched_times = kwargs.get('max_sched_times', 1000)
        self.next_run_time = kwargs.get('next_run_time', 0)
        self.incr_step = kwargs.get('incr_step', 1)
        self.created_at = kwargs.get('created_at', 0)
        self.updated_at = kwargs.get('updated_at', 0)
        self.deleted_at = kwargs.get('deleted_at', 0)
