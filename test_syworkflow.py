# -*- encoding: utf-8 -*-
import unittest
import syworkflow as wf


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
    scheduler = wf.TaskScheduler()
    task = wf.AsyncTask()
    scheduler.add_task(task)
    scheduler.start()
