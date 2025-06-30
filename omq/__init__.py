import logging
import json
import os
import socket
import typing

from enum import Enum
from omq.exceptions import ImproperlyConfigured, InvalidArgument, InvalidJson, NoAppFound, NoExactMatch, TooManyRows
from sqlalchemy.exc import IntegrityError
from sqlalchemy import create_engine, text
from sqlalchemy.engine.row import Row

logger = logging.getLogger(__name__)

class ProcesStatus(Enum):
	QUEUED = 'queued'
	FAILED = 'failed'
	PROGRESS = 'in progres'
	COMPLETED = 'completed'


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
					`hostname` VARCHAR(150) NOT NULL,
					`port` INT NOT NULL,
					UNIQUE INDEX `port` (`port`, `hostname`),
					PRIMARY KEY (`name`)
				)
				COLLATE='utf8mb4_unicode_ci'
				""",
				"""
				CREATE TABLE if not exists `messages` (
					`id` INT(11) NOT NULL AUTO_INCREMENT,
					`destination` VARCHAR(50) NOT NULL COLLATE 'utf8mb4_unicode_ci',
					`priority` SMALLINT(5) UNSIGNED NOT NULL DEFAULT '1',
					`status` ENUM('queued','in progres','failed','completed') NOT NULL DEFAULT 'queued' COLLATE 'utf8mb4_unicode_ci',
					`creation` DATETIME NOT NULL,
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

	def _notify(self, host: str, port: int, message: str):
		with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
			server_address = (host, int(port))
			sock.sendto(message.encode(), server_address)

	def _row_to_dict(self, row: Row) -> dict:
		id_, priority, status, creation, customer, message = row
		return {
			'id': id_,
			'priority': priority,
			'status': status,
			'creation': creation,
			'customer': customer,
			'message': json.loads(message),
		}

	def get_messages(
		self,
		app_name: str,
		message_id=-1,
		status=None,
		max_results=20,
		customer_id=None
	) -> typing.Union[
		tuple[tuple[id, dict]],
		tuple[tuple[None, None]],
	]:
		if max_results < 1:
			return ((None, None),)

		sql_parameters = {'app_name': app_name}
		filters = ''
		if message_id > 0:
			sql_parameters.update({'message_id': message_id})
			filters += 'and `id` = :message_id'

		if status:
			status_ = status
			if isinstance(status, ProcesStatus):
				status_ = status.value

			sql_parameters.update({'status': status_})
			filters += 'and `status` = :status'

		if customer_id:
			sql_parameters.update({'customer_id': customer_id})
			filters += 'and customer = :customer_id'

		sql = f"""
			select
				`id`,
				`priority`,
				`status`,
				`creation`,
				`customer`,
				`message`
			 from
			 	`messages`
			where
				`destination` = :app_name
				{filters}
			order by
				priority desc, creation desc
			limit {min(int(max_results), 50)}
		"""
		with self.engine.connect() as connection:
			resultset = connection.execute(text(sql), sql_parameters)
			if resultset.rowcount == 0:
				return ((None, None),)

			if max_results == 1 or resultset.rowcount == 1:
				row = resultset.first()
				return ((row[0], self._row_to_dict(row)),)

			return *(
				(row[0], self._row_to_dict(row))
				for row in resultset.all()
			),

	def get_message(
			self,
			app_name: str,
			message_id=-1,
			status=None,
			customer_id=None
	) -> typing.Union[
		tuple[id, dict],
		tuple[None, None],
	]:
		return self.get_messages(
			app_name,
			message_id,
			status=status,
			max_results=1,
			customer_id=customer_id,
		)[0]

	def send_message(
			self,
			app_name: str,
			message: str,
			priority=1,
			customer=None
	) -> bool:
		if not self._is_valid_json(message):
			raise InvalidJson(message)

		try:
			host, port = self.get_app(app_name)
		except NoAppFound as naf:
			logger.error(naf)
			return False

		sql = """
			insert into `messages`(`destination`, `priority`, `status`, `creation`, `customer`, `message`)
			values(:app_name, :priority, 'queued', now(), :customer, :message)
		"""
		with self.engine.connect() as connection:
			try:
				connection.begin()
				connection.execute(text(sql), {
					'app_name': app_name,
					'message':message,
					'priority': priority,
					'customer': customer,
				})
				connection.commit()
				self._notify(host, port, message)
			except IntegrityError as ie:
				logger.error(ie._sql_message())
				return False

			return True

	def _set_status(self, app_name: str, message_id: int, status: ProcesStatus):
		if not isinstance(status, ProcesStatus):
			raise InvalidArgument('Invalid argument provided, should be one of the ProcesStatus enumerations.')

		sql = """
			update `messages` set `status` = :status
			where
				`destination` = :app_name
				and `id` = :message_id
		"""
		with self.engine.connect() as connection:
			connection.begin()
			result = connection.execute(text(sql), {
				'status': status.value,
				'app_name': app_name,
				'message_id': int(message_id),
			})
			connection.commit()
			if result.rowcount == 1:
				return True

		logger.error(NoExactMatch(f'Could not update message. Message with ID \'{message_id}\' not found for \'{app_name}\''))
		return False

	def requeue_proces(self, app_name: str, message_id: int) -> bool:
		return self._set_status(app_name, message_id, ProcesStatus.QUEUED)

	def starting_proces(self, app_name: str, message_id: int) -> bool:
		return self._set_status(app_name, message_id, ProcesStatus.PROGRESS)

	def failed_proces(self, app_name: str, message_id: int) -> bool:
		return self._set_status(app_name, message_id, ProcesStatus.FAILED)

	def completed_proces(self, app_name: str, message_id: int) -> bool:
		sql = """
			delete from `messages`
			where
				`destination` = :app_name
				and `id` = :message_id
		"""
		with self.engine.connect() as connection:
			connection.begin()
			result = connection.execute(text(sql), {
				'app_name': app_name,
				'message_id': int(message_id),
			})
			connection.commit()
			if result.rowcount == 1:
				return True

		logger.error(NoExactMatch(f'Could not delete message. Message with ID \'{message_id}\' not found for \'{app_name}\''))
		return False

	def get_apps(self) -> tuple[dict]:
		sql = "select name, port from `workers`"
		with self.engine.connect() as connection:
			resultset = connection.execute(text(sql))
			return tuple(resultset.mappings().all())

	def get_app(self, app_name: str) -> tuple[str, int]:
		sql = "select hostname, port from `workers` where name = :app_name"
		with self.engine.connect() as connection:
			resultset = connection.execute(text(sql), {'app_name': app_name})

			if resultset.rowcount == 0:
				raise NoAppFound(f'The worker {app_name} does not exists or did not register.')
			elif resultset.rowcount > 1:
				raise NoExactMatch(f'Expected to find one row, however there are {resultset.rowcount} rows found.')

			hostname, port = resultset.first()
			return hostname, port

	def register_worker(self, app_name: str, hostname: str, notify_port: int) -> bool:
		try:
			sql = "insert into `workers` (`name`, `hostname`, `port`) values(:app_name, :hostname, :notify_port)"
			with self.engine.connect() as connection:
				connection.begin()
				connection.execute(text(sql), {'app_name': app_name, 'hostname': hostname, 'notify_port': notify_port})
				connection.commit()
			return True
		except IntegrityError as ie:
			logger.error(ie._sql_message())
		return False

	def unregister_worker(self, app_name: str, hostname: str, notify_port: int) -> bool:
		sql = "delete from `workers` where `name` = :app_name and `hostname` = :hostname and `port` = :notify_port"
		with self.engine.connect() as connection:
			connection.begin()
			connection.execute(text(f"SET FOREIGN_KEY_CHECKS = 0"))
			connection.execute(text(sql), {'app_name': app_name, 'hostname': hostname, 'notify_port': notify_port})
			connection.execute(text(f"SET FOREIGN_KEY_CHECKS = 1"))
			connection.commit()
		return True
