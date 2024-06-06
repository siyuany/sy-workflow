# -*- encoding: utf-8 -*-
import concurrent.futures
import enum
import logging
import os
import threading
import time
import uuid
from typing import Any
from typing import Callable
from typing import List
from typing import Optional
import datetime
from typing import Set

_logger = logging.getLogger(__name__)


class TaskStatus(enum.Enum):
  """任务状态枚举类

  - WAITING: 任务等待上游依赖完成，当前不可执行
  - READY: 任务所有上有依赖已完成，处于就绪状态，等待调度
  - RUNNING: 任务当前处于执行状态（包括执行中挂起）
  - DONE: 任务已正常完成
  - ERROR: 任务执行发生异常
  """
  WAITING = 0
  READY = 1
  RUNNING = 2
  DONE = 3
  ERROR = 4


class AsyncTask(object):
  """
  （抽象）任务对象，封装任务执行过程。

  :param dep_tasks: 上游依赖任务列表，列表元素应该为 `AsyncTask` 实例，默认为 None
  :param name: 任务对象名称，默认为 None，此时任务按照实例化顺序编号为 `AsyncTask-N`
  :param retries: 任务执行失败后重试次数，默认值 3
  """

  __seq_no = 0
  __uuid_namespace = uuid.uuid4()

  @classmethod
  def __seq_number(cls):
    cls.__seq_no += 1
    return cls.__seq_no

  def __task_uid_generator(self):
    uid = uuid.uuid5(self.__uuid_namespace, self.name)
    return uid

  def __init__(self,
               dep_tasks: Optional[List] = None,
               name: Optional[str] = None,
               retries: int = 3):
    super(AsyncTask, self).__init__()
    if dep_tasks:
      self.__dep_tasks: List[AsyncTask] = [
          t for t in dep_tasks if isinstance(t, AsyncTask)
      ]
    else:
      self.__dep_tasks: List[AsyncTask] = []
    self._status: TaskStatus = TaskStatus.WAITING
    self.__name = name
    if self.__name is None:
      self.__name = f'{self.__class__.__name__}-{AsyncTask.__seq_number()}'
    self.__uid = self.__task_uid_generator()
    self._result = None
    self.__lock = threading.Lock()
    self.__retries = int(retries)
    if self.__retries <= 0:
      self.__retries = 3

    _logger.debug(
        f'实例化 {self.__class__.__name__} 对象 {self.name}[{self.uid.hex}]')

  def __getstate__(self):
    # pickle interface
    state = self.__dict__.copy()
    state.pop('_AsyncTask__lock')
    return state

  @property
  def name(self):
    """任务名称"""
    return self.__name

  @property
  def uid(self):
    return self.__uid

  def __eq__(self, other):
    if not isinstance(other, AsyncTask):
      return False

    return self.uid == other.uid

  def __hash__(self):
    return hash(self.uid)

  @property
  def result(self):
    """任务执行结果"""
    if self.status not in (TaskStatus.DONE, TaskStatus.ERROR):
      raise RuntimeError('任务状态未完成，无法获取任务结果！')
    else:
      with self.__lock:
        res = self._result
      return res

  @property
  def dep_tasks(self):
    """任务上游依赖"""
    return self.__dep_tasks

  @property
  def status(self):
    """任务状态"""
    with self.__lock:
      return self._status

  def is_ready(self):
    """任务是否已就绪"""
    return self.status == TaskStatus.READY

  def is_done(self):
    """任务是否已完成"""
    return self.status == TaskStatus.DONE

  def update_status(self):
    """
    当Task对象处于 `WAITING` 状态时，检查该对象依赖任务执行状态；
    在上游任务均为 `DONE` 时修改本任务状态为 `READY`
    """
    with self.__lock:
      if self._status == TaskStatus.WAITING:
        dep_tasks = list(
            filter(
                lambda t: t.status not in [TaskStatus.DONE, TaskStatus.ERROR],
                self.dep_tasks))
        if not dep_tasks:
          self._status = TaskStatus.READY
          _logger.debug(f'任务 {self.__name} 状态修改为 {self._status}')
      else:
        raise RuntimeError("该方法仅可对处于等待（WAITING）状态的任务更新任务状态")

  def pre_task(self):
    """
    该方法在任务异步执行前，在主线程同步执行。主要执行内容包括:

    * 判断上游任务是否有失败，若失败，则本任务直接置为失败；否则执行以下流程
    * 执行用户定义的预处理代码
    * 修改任务执行状态
    """
    if list(filter(lambda t: t.status == TaskStatus.ERROR, self.dep_tasks)):
      _logger.error(f'任务 {self.name} 上游任务失败，本任务不再执行')
      with self.__lock:
        self._status = TaskStatus.ERROR
    else:
      _logger.debug(f'执行任务 {self.name} 预处理方法')
      self.preprocess()
      with self.__lock:
        self._status = TaskStatus.RUNNING

  def run(self):
    """
    Task任务对象异步实际执行代码，在用户代码基础上增加了异常处理逻辑
    """
    _logger.debug(f'开始执行任务{self.name}主方法')
    run_count = 0
    while run_count < self.__retries:
      try:
        result = self.process()
        _logger.debug(f'任务 {self.name} 主方法执行完成')
        return result
      except Exception as err:
        run_count += 1

        if run_count >= self.__retries:
          _logger.error(f'任务 {self.name} 主方法执行错误 {repr(err)}')
          raise err
        else:
          _logger.info(
              f'任务 {self.name} 主方法执行错误 {repr(err)}，第 {run_count + 1} 次尝试')

  def post_task(self, future: concurrent.futures.Future):
    """
    该方法在任务完成后，通过回调方式在主进程执行，主要内容执行内容包括：

    * 当任务正常完成后

      * 获取任务执行结果并存储到本任务对象，用户可通过 `self.result` 获取
      * 修改任务状态为 `DONE`

    * 当任务执行错误时

      * 获取任务执行异常并存储到本任务对象，用户可通过 `self.result` 获取
      * 修改任务状态为 `ERROR`

    :param future: 为 `concurrent.futures.Future` 类实例，封装了 `AsyncTask.process` 方法执行结果。
    """
    if self.status == TaskStatus.ERROR:
      _logger.debug(f'任务 {self.name} 执行前失败（上游有失败任务）')
    else:
      _logger.debug(f'执行任务 {self.name} 后处理方法')
      try:
        result = future.result()
        with self.__lock:
          self._result = result
          self._status = TaskStatus.DONE

          # `postprocess` 中可以给 `self._status` 和 `self._result` 重新赋值
          self.postprocess(future)
          _logger.debug(f'任务 {self.name} 后处理方法执行完成')
      except (BaseException, Exception) as err:
        _logger.debug(f'任务 {self.name} 后处理方法，任务运行错误 {repr(err)}')
        with self.__lock:
          self._status = TaskStatus.ERROR
          self._result = err
        raise err

  def preprocess(self):
    """用户重写该方法，该方法为同步执行的预处理流程（注意：不要在此执行耗时任务）。"""

  def process(self) -> Any:
    """
    用户重写该方法，该方法为异步执行的任务。注意，在使用多线程时，推荐在方法内部完成
    异常处理，该方法不要抛出异常，避免异常在进程间传递。通过用户结果约定异常传递，并
    在 `postprocess` 中通过结果识别返回的异常并完成后处理。

    在本方法中可以对 `self._result` 和 `self._status` 重新赋值，调用本方法时已获取
    状态锁，方法内不用加锁。
    """

  def postprocess(self, future: concurrent.futures.Future):
    """
    用户重写该方法，该方法通过回调方式在主进程中执行后处理流程（注意：不要在此执行耗时任务）。

    :param future: 为 `concurrent.futures.Future` 类实例，封装了 `AsyncTask.process` 方法执行结果。
    """


class TaskScheduler(object):
  """
  任务调度器，该调度器使用 `concurrent.futures.Executor` 作为执行器，执行加入到调度器的任务。


  :param executor_type: 并发方式，支持 `thread`（线程）和 `process`（进程）
  :param max_workers: 并发数量，默认为空
  """

  def __init__(self,
               executor_type='thread',
               max_workers: Optional[int] = None,
               **kwargs):
    super().__init__(**kwargs)
    assert executor_type in ['thread', 'process']
    self.__executor_type = executor_type
    if self.__executor_type == 'thread':
      self.__executor = concurrent.futures.ThreadPoolExecutor(
          max_workers=max_workers)
    else:
      self.__executor = concurrent.futures.ProcessPoolExecutor(
          max_workers=max_workers)

    # 待执行任务集
    self.__task_set: Set[AsyncTask] = set()

    # 任务集同步锁
    self.__task_set_lock = threading.Lock()

    # self.__task_set_cond = threading.Condition()

    # 每次任务完成时，均触发提交新任务流程
    self.__task_set_notifier = threading.Semaphore(value=0)

  def submit_ready_task(self):
    """
    在任务调度器所在进程中运行，将状态为 `READY` 的任务提交待执行。注意，调用该方法前需要获取 `task_set_lock`
    对象并加锁，否则会抛出 `RuntimeError` 错误。
    """
    _logger.debug('提交就绪任务')
    if not self.__task_set_lock.locked():
      raise RuntimeError(f'调用本方法时需获取 `TaskScheduler.__task_set_lock` 同步锁')

    if self.__task_set:
      tasks_to_run = []
      for t in self.__task_set:
        if t.status == TaskStatus.WAITING:
          t.update_status()

        if t.is_ready():
          tasks_to_run.append(t)

      for t in tasks_to_run:
        self.__task_set.remove(t)
        self.submit(t)

      _logger.debug(f'已提交执行 {len(tasks_to_run)} 个就绪任务')

  def submit(self, task: AsyncTask) -> None:
    """
    将任务加入到待执行，需要按顺序执行如下内容

    * 执行任务的预处理流程
    * 将任务提交到 `Executor`，并获取对应的 `Future` 对象
    * 获取到的 `Future` 对象中添加回调函数

    :param task: `AsyncTask` 任务
    """
    _logger.debug(f'任务 {task.name} 提交执行')
    task.pre_task()
    future = self.__executor.submit(task.run)
    _logger.debug(f'添加任务 {task.name} 完成及通知回调')
    future.add_done_callback(lambda f: self.task_done_and_notify(f, task))
    _logger.debug(f'任务 {task.name} 提交执行完成')

  def task_done_and_notify(self,
                           future: concurrent.futures.Future = None,
                           task: AsyncTask = None):
    # with self.__task_set_cond:
    #   _logger.debug('任务完成，通知调度后续任务')
    #   self.__task_set_cond.notify_all()
    _logger.debug(f'执行任务 {task.name} 回调')
    task.post_task(future)
    _logger.debug(f'任务 {task.name} 完成，通知调度后续任务')
    self.__task_set_notifier.release()

  def start(self):
    while self.__task_set:
      with self.__task_set_lock:
        self.submit_ready_task()
      # _logger.debug('等待任务完成')
      # with self.__task_set_cond:
      #   self.__task_set_cond.wait(timeout=1)
      #   _logger.debug('收到任务完成通知')

      _logger.debug('等待任务完成通知')
      self.__task_set_notifier.acquire()
      _logger.debug('收到任务完成通知')

    _logger.debug('无待执行任务，等待已提交执行任务完成')
    self.__executor.shutdown(wait=True)

  def add_task(self, task: AsyncTask) -> None:
    """
    将任务及其上游依赖任务加入待执行任务集

    :param task: `AsyncTask` 对象
    """

    def _add_task(_task: AsyncTask):
      # 当任务自身不在待执行任务集，且任务状态不为 `RUNNING`、`DONE`、`ERROR` 三者之一时，
      # 将任务自身及其上游依赖递归加入待执行任务集
      if _task not in self.__task_set and isinstance(
          _task, AsyncTask) and _task.status not in (TaskStatus.RUNNING,
                                                     TaskStatus.DONE,
                                                     TaskStatus.ERROR):
        _logger.debug(f'将 {_task.name} 加入调度')
        self.__task_set.add(_task)
        for t in _task.dep_tasks:
          _add_task(t)

    with self.__task_set_lock:
      _add_task(task)


class SQLExecutionTask(AsyncTask):
  """
  SQL 任务类，该任务用于执行给定的 SQL 代码。其中 `sql_statement` 可以为文本，也可以为
  path-like object。当为后者时，将从指定路径的文件中读取 SQL 代码。
  
  :param connect_fn: Callable，建立数据库连接的函数。数据库连接需要异步建立，建议不
    复用本地（local）建立的数据库连接。
  :param sql_statement: str，SQL 代码文本，或存储 SQL 代码的文件路径。
  """

  def __init__(self,
               connect_fn: Callable,
               sql_statement: str,
               dep_tasks: Optional[List[AsyncTask]] = None,
               name: Optional[str] = None,
               retries: int = 3):
    super().__init__(dep_tasks, name, retries)
    self.__connect_fn = connect_fn
    self.__sql_statement = sql_statement
    self.__is_path = os.path.isfile(sql_statement)

  def process(self) -> Any:
    # 当 sql_statement 为 path-like object 时，读取文件内容
    if self.__is_path:
      with open(self.__sql_statement, 'r') as sql_file:
        sql = '\n'.join(sql_file.readlines())
    else:
      sql = self.__sql_statement

    sqls = list(filter(lambda x: x.strip(), sql.split(';')))

    # 建立数据库连接
    conn = self.__connect_fn()
    for statement in sqls:
      cursor = conn.cursor()
      _logger.debug(f'任务 {self.name} 执行SQL语句: {statement}')
      cursor.execute(statement)
      cursor.close()

    _logger.debug(f'任务 {self.name} 中所有 SQL 语句执行完毕')
    conn.close()


class TimerTask(AsyncTask):

  def __init__(self,
               hour: int = 0,
               minute: int = 0,
               second: int = 0,
               dep_tasks: Optional[List] = None,
               name: Optional[str] = None,
               retries: int = 3):
    super().__init__(dep_tasks, name, retries)
    today = datetime.date.today()
    self.__wake_datetime = datetime.datetime(
        year=today.year,
        month=today.month,
        day=today.day,
        hour=hour,
        minute=minute,
        second=second)

  def process(self) -> Any:
    current_time = datetime.datetime.now()
    if current_time < self.__wake_datetime:
      time_gap = self.__wake_datetime - current_time
      _logger.info(f'休眠 {time_gap} 秒后唤醒')
      time.sleep(time_gap.seconds)

    return True


__all__ = ['AsyncTask', 'SQLExecutionTask', 'TaskStatus', 'TaskScheduler', 'TimerTask']
