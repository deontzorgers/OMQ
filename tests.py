import json
import os
import unittest

from dotenv import load_dotenv
from omq import InvalidJson, NoExactMatch, OMQ, ProcesStatus
from sqlalchemy import create_engine, text


class TestOMW(unittest.TestCase):
	@classmethod
	def setUpClass(cls):
		hostname = os.getenv('OMQ_HOSTNAME')
		cls.db = os.getenv('OMQ_DATABASE')
		db_username = os.getenv('OMQ_USERNAME')
		db_password = os.getenv('OMQ_PASSWORD')
		cls._engine_url = f'mariadb+pymysql://{db_username}:{db_password}@{hostname}?charset=utf8mb4'

		connection = create_engine(cls._engine_url).connect()
		connection.begin()
		connection.execute(text(f"CREATE DATABASE IF NOT EXISTS `{cls.db}` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci */;"))
		connection.commit()
		connection.close()

		cls.omq = OMQ()

	@classmethod
	def tearDownClass(cls):
		connection = create_engine(cls._engine_url).connect()
		connection.begin()
		connection.execute(text(f"use `{cls.db}`"))
		connection.execute(text(f"SET FOREIGN_KEY_CHECKS = 0"))
		connection.execute(text(f"Truncate table `workers`"))
		connection.execute(text(f"Truncate table `messages`"))
		connection.execute(text(f"SET FOREIGN_KEY_CHECKS = 1"))
		# connection.execute(text(f"DROP DATABASE IF EXISTS `{cls.db}`"))
		connection.commit()

	def test_00_test_json_validation(self):
		self.assertTrue(self.omq._is_valid_json("{}"))
		self.assertFalse(self.omq._is_valid_json("{asdf}"))  # prints False
		self.assertTrue(self.omq._is_valid_json('{ "age":100}'))  # prints True
		self.assertFalse(self.omq._is_valid_json("{'age':100 }"))  # prints False
		self.assertTrue(self.omq._is_valid_json("{\"age\":100 }"))  # prints True
		self.assertTrue(self.omq._is_valid_json('{"age":100 }'))  # prints True
		self.assertTrue(self.omq._is_valid_json('{"foo":[5,6.8],"foo":"bar"}'))  # prints True

	def test_01_register_app(self):
		self.assertTrue(self.omq.register_worker('mcepd_runner', 5555))
		self.assertTrue(self.omq.register_worker('cw_runner', 5556))

	def test_02_get_apps(self):
		self.assertEqual(self.omq.get_apps(), ({'name': 'mcepd_runner', 'port': 5555},{'name': 'cw_runner', 'port': 5556}))

	def test_03_get_app(self):
		self.assertEqual(self.omq.get_app('mcepd_runner'), ('mcepd_runner', 5555))
		self.assertEqual(self.omq.get_app('cw_runner'), ('cw_runner', 5556))
		with self.assertRaises(NoExactMatch):
			self.omq.get_app('tf_runner')

	def test_04_send_message(self):
		self.assertTrue(
			self.omq.send_message(
				'cw_runner',
				json.dumps({'package': 'my custom message'}),
			)
		)

		self.assertTrue(
			self.omq.send_message(
				'mcepd_runner',
				json.dumps({'foo': 'bar'}),
				priority=54,
				customer=123,
			)
		)

		self.assertTrue(
			self.omq.send_message(
				'mcepd_runner',
				json.dumps({'not': 'found'}),
				priority=54,
				customer=125,
			)
		)

		self.assertTrue(
			self.omq.send_message(
				'cw_runner',
				json.dumps({'custom': 'body'}),
			)
		)

		self.assertFalse(
			self.omq.send_message(
				'tf_runner',
				json.dumps({'wrong': 'app_name'}),
			)
		)

		with self.assertRaises(InvalidJson):
			self.omq.send_message(
				'mcepd_runner',
				'{oops:fout!}'
			)

	def test_05_get_messages(self):
		id_, msg = self.omq.get_message('mcepd_runner', status=ProcesStatus.QUEUED.value)
		self.assertEqual(id_, 2)
		self.assertEqual(msg.get('priority'), 54)
		self.assertEqual(msg.get('status'), ProcesStatus.QUEUED.value)
		self.assertEqual(msg.get('customer'), 123)
		self.assertEqual(msg.get('message'), {'foo': 'bar'})

		id_, msg = self.omq.get_messages('mcepd_runner', max_results=0)[0]
		self.assertIsNone(id_)
		self.assertIsNone(msg)

		id_, msg = self.omq.get_messages('mcepd_runner', max_results=-567)[0]
		self.assertIsNone(id_)
		self.assertIsNone(msg)

		msgs = self.omq.get_messages('cw_runner')
		id_, msg = msgs[0]
		self.assertEqual(id_, 1)
		self.assertEqual(msg.get('message'), {'package': 'my custom message'})
		self.assertIsNone(msg.get('customer'))

		id_, msg = msgs[1]
		self.assertEqual(id_, 4)
		self.assertEqual(msg.get('message'), {'custom': 'body'})
		self.assertIsNone(msg.get('customer'))

		self.assertEqual(
			len(self.omq.get_messages('mcepd_runner', customer_id=123))
			, 1
		)

		self.assertEqual(
			len(self.omq.get_messages('mcepd_runner'))
			, 2
		)

	def test_06_change_status(self):
		app_name = 'mcepd_runner'
		succesfull_task_id = 2
		missing_task_id = 1

		self.assertFalse(self.omq.starting_proces(app_name, missing_task_id))
		self.assertFalse(self.omq.failed_proces(app_name, missing_task_id))
		self.assertFalse(self.omq.completed_proces(app_name, missing_task_id))
		self.assertFalse(self.omq.requeue_proces(app_name, missing_task_id))


		self.assertTrue(self.omq.starting_proces(app_name, succesfull_task_id))
		id_, msg = self.omq.get_message(app_name, message_id=succesfull_task_id)
		self.assertEqual(msg.get('status'), ProcesStatus.PROGRESS.value)

		self.assertTrue(self.omq.failed_proces(app_name, succesfull_task_id))
		id_, msg = self.omq.get_message(app_name, message_id=succesfull_task_id)
		self.assertEqual(msg.get('status'), ProcesStatus.FAILED.value)

		self.assertTrue(self.omq.completed_proces(app_name, succesfull_task_id))
		id_, msg = self.omq.get_message(app_name, message_id=succesfull_task_id)
		self.assertEqual(msg.get('status'), ProcesStatus.COMPLETED.value)

		self.assertTrue(self.omq.requeue_proces(app_name, succesfull_task_id))
		id_, msg = self.omq.get_message(app_name, message_id=succesfull_task_id)
		self.assertEqual(msg.get('status'), ProcesStatus.QUEUED.value)


if __name__ == '__main__':
	load_dotenv()
	unittest.main()
