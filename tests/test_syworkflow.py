# -*- encoding: utf-8 -*-
import unittest
import syworkflow as wf
import time


class SleepTask(wf.AsyncTask):

  def __init__(self,
               duration=1,
               dep_tasks: wf.List | None = None,
               name: str | None = None,
               retries: int = 3):
    super().__init__(dep_tasks, name, retries)
    self.__duration = duration

  def process(self) -> wf.Any:
    time.sleep(self.__duration)


class TestTask(unittest.TestCase):

  def test_task_equal(self):
    task1 = wf.AsyncTask()
    task2 = wf.AsyncTask()
    self.assertEqual(task1, task1)
    self.assertNotEqual(task1, task2)
    self.assertNotEqual(task1.name, task2.name)

  def test_task_scheduler_p1(self):
    scheduler = wf.TaskScheduler()
    scheduler.start()

  def test_task_scheduler_p2(self):
    task1 = SleepTask(0.1)
    task2 = SleepTask(0.5, dep_tasks=[task1])
    task3 = SleepTask(0.6, dep_tasks=[task1])
    task4 = SleepTask(1.2, dep_tasks=[task2, task3])
    task5 = SleepTask(1.3, dep_tasks=[task2])
    task6 = SleepTask(1.4, dep_tasks=[task3])
    task7 = SleepTask(0.1, dep_tasks=[task4, task5, task6])

    schd = wf.TaskScheduler(executor_type='thread', max_workers=2)
    schd.add_task(task7)
    schd.start()

  def test_task_scheduler_p3(self):
    task1 = SleepTask(0.1)
    task2 = SleepTask(0.5, dep_tasks=[task1])
    task3 = SleepTask(0.6, dep_tasks=[task1])
    task4 = SleepTask(1.2, dep_tasks=[task2, task3])
    task5 = SleepTask(1.3, dep_tasks=[task2])
    task6 = SleepTask(1.4, dep_tasks=[task3])
    task7 = SleepTask(0.1, dep_tasks=[task4, task5, task6])

    schd = wf.TaskScheduler(executor_type='process', max_workers=2)
    schd.add_task(task7)
    schd.start()


if __name__ == '__main__':
  unittest.main()
