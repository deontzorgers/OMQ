import os

from omq.exceptions import ImproperlyConfigured
from sqlalchemy import create_engine


class Singleton(type):
	_instances = {}
	def __call__(cls, *args, **kwargs):
		if cls not in cls._instances:
			cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
		return cls._instances[cls]


class OMQ(metaclass=Singleton):
	def __init__(self):
		hostname = os.getenv('OMQ_HOSTNAME')
		db = os.getenv('OMQ_DATABASE')
		db_username = os.getenv('OMQ_USERNAME')
		db_password = os.getenv('OMQ_PASSWORD')

		if not (
			True
			and hostname
			and db
			and db_username
			and db_password
		):
			raise ImproperlyConfigured('Database settings ontbreken of zijn niet volledig')

		self.engine = create_engine(
			f'mariadb+pymysql://{db_username}:{db_password}@{hostname}/{db}?charset=utf8mb4'
		)
		self._connection = self.engine.connect()


	def get_message(self, app_name: str) -> tuple[id, dict]:
		pass

	def send_message(self, app_name: str, message: dict) -> bool:
		pass

	def starting_proces(self, message_id: int) -> bool:
		pass

	def failed_proces(self, message_id: int) -> bool:
		pass

	def done_proces(self, message_id: int) -> bool:
		pass

	# def _get_apps(self) -> tuple[tuple[name: str, port: int]]:
	def _get_apps(self) -> tuple[tuple[str, int]]:
		pass

	# def _get_app(self, app_name: str) -> tuple[name: str, port: int]:
	def _get_app(self, app_name: str) -> tuple[str, int]:
		pass
