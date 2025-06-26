import json
import os

from omq.exceptions import ImproperlyConfigured, InvalidJson, NoExactMatch, TooManyRows
from sqlalchemy import create_engine, text


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
		self._create_tables_if_not_exists()

	def _create_tables_if_not_exists(self):
		with self.engine.connect() as connection:
			connection.begin()
			for sql in [
				"""
				CREATE TABLE if not exists `workers` (
					`name` VARCHAR(50) NOT NULL,
					`port` INT NOT NULL,
					UNIQUE INDEX `port` (`port`),
					PRIMARY KEY (`name`)
				)
				COLLATE='utf8mb4_unicode_ci'
				""",
				"""
				CREATE TABLE if not exists `messages` (
					`id` INT(11) NOT NULL AUTO_INCREMENT,
					`destination` VARCHAR(50) NOT NULL COLLATE 'utf8mb4_unicode_ci',
					`priority` SMALLINT(5) UNSIGNED NOT NULL DEFAULT '1',
					`status` ENUM('queued','failed','done') NOT NULL DEFAULT 'queued' COLLATE 'utf8mb4_unicode_ci',
					`creation` DATETIME NOT NULL,
					`run_after` DATETIME NULL,
					`customer` INT(11) NULL DEFAULT NULL,
					`message` LONGTEXT NOT NULL COLLATE 'utf8mb4_bin',
					PRIMARY KEY (`id`) USING BTREE,
					INDEX `FK__workers` (`destination`) USING BTREE,
					CONSTRAINT `FK__workers` FOREIGN KEY (`destination`) REFERENCES `workers` (`name`) ON UPDATE NO ACTION ON DELETE NO ACTION,
					CONSTRAINT `message` CHECK (json_valid(`message`))
				)
				COLLATE='utf8mb4_unicode_ci'
				ENGINE=InnoDB
				"""
			]:
				connection.execute(text(sql))
			connection.commit()

	def _is_valid_json(self, message: str) -> bool:
		try:
			json.loads(message)
		except ValueError as e:
			return False
		return True

	def get_message(self, app_name: str) -> tuple[id, dict]:
		pass

	def get_messages(self, app_name: str) -> tuple[tuple[id, dict]]:
		pass

	def send_message(
			self,
			app_name: str,
			message: str,
			priority=1,
			run_after=None,
			customer=None
	) -> bool:
		if not self._is_valid_json(message):
			raise InvalidJson(message)

		sql = """
			insert into `messages`(`destination`, `priority`, `status`, `creation`, `run_after`, `customer`, `message`)
			values(:app_name, :priority, 'queued', now(), :run_after, :customer, :message)
		"""
		with self.engine.connect() as connection:
			connection.begin()
			connection.execute(text(sql), {
				'app_name': app_name,
				'message':message,
				'priority': priority,
				'run_after': run_after,
				'customer': customer,
			})
			connection.commit()
			return True

	def starting_proces(self, message_id: int) -> bool:
		pass

	def failed_proces(self, message_id: int) -> bool:
		pass

	def done_proces(self, message_id: int) -> bool:
		pass

	# def _get_apps(self) -> tuple[tuple[name: str, port: int]]:
	def get_apps(self) -> tuple[dict]:
		sql = "select name, port from `workers`"
		with self.engine.connect() as connection:
			resultset = connection.execute(text(sql))
			return tuple(resultset.mappings().all())

	# def _get_app(self, app_name: str) -> tuple[name: str, port: int]:
	def get_app(self, app_name: str) -> tuple[str, int]:
		sql = "select name, port from `workers` where name = :app_name"
		with self.engine.connect() as connection:
			resultset = connection.execute(text(sql), {'app_name': app_name})
			if resultset.rowcount != 1:
				raise NoExactMatch(f'Expected to find one row, however there are {resultset.rowcount} rows found.')
			return tuple(resultset.first())

	def register_worker(self, app_name: str, notify_port: int) -> bool:
		sql = "insert into `workers`(`name`, `port`) values(:app_name, :notify_port)"
		with self.engine.connect() as connection:
			connection.begin()
			connection.execute(text(sql), {'app_name': app_name, 'notify_port': notify_port})
			connection.commit()
		return True
