class TaskContinueException(Exception):
    '''继续调度任务'''


class TaskStopException(Exception):
    '''停止调度任务'''
