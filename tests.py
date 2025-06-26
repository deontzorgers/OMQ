import json
import os
import unittest

from dotenv import load_dotenv
from omq import InvalidJson, NoExactMatch, OMQ
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
		connection.execute(text(f"DROP DATABASE IF EXISTS `{cls.db}`"))
		connection.commit()

	def test_01_register_app(self):
		self.assertTrue(
			self.omq.register_worker('mcepd_runner', 5555)
		)
		self.assertTrue(
			self.omq.register_worker('cw_runner', 5556)
		)

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
				'mcepd_runner',
				json.dumps({'foo': 'bar'}),
			)
		)

		with self.assertRaises(InvalidJson):
			self.omq.send_message(
				'mcepd_runner',
				'{oops:fout!}'
			)


if __name__ == '__main__':
	load_dotenv()
	unittest.main()
