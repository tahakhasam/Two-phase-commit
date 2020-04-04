import os
import asyncio
import logging
from constants import Constants
from database_connectivity import DatabaseConnection


class Participant:

	def __init__(self: object, address: str, 
		fail_safe_addr: str = None, timeout: int=30) -> object:
		"""
		This constructor sets up all required address and
		preliminary configurations.
		"""
		self.SERVER_ADDRESS = (address, 8005)
		self.FAIL_SAFE_ADDRESS = (fail_safe_addr, 8006)
		self.protocols = Constants()
		self.timeout = timeout
		self.database_connector = DatabaseConnection()
		self.set_up_logger()

	def set_up_logger(self: object) -> None:
		""" Sets up loggers both for Console as well as File. """
		self.logger = logging.getLogger(f'participant')
		self.logger.setLevel(logging.INFO)
		console_handler = logging.StreamHandler()
		file_handler = logging.FileHandler('participant.log', 'w+')
		console_handler.setLevel(logging.INFO)
		file_handler.setLevel(logging.INFO)
		console_format = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
		file_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
		console_handler.setFormatter(console_format)
		file_handler.setFormatter(file_format)
		self.logger.addHandler(console_handler)
		self.logger.addHandler(file_handler)

	async def perform_actions(self: object):
		"""
		Main Coroutine which connects to main coordinator
		and also handles the database querying. 
		"""
		try:
			# Connection to Main Coordinator Phase
			self.logger.info('Attempting to connect Main Coordinator at host: {} at port: {}'.format(*self.SERVER_ADDRESS))
			reader, writer = await asyncio.open_connection(*self.SERVER_ADDRESS)
			self.logger.info('Connected to host: {} at port: {}'.format(*self.SERVER_ADDRESS))
		
			# PREPARE phase
			data = await asyncio.wait_for(reader.read(1024), timeout=self.timeout)
			self.logger.info('Received {} from host: {} at port: {}'.format(data.decode(), *self.SERVER_ADDRESS))
			if data == self.protocols.PREPARE:
				self.database_connector.prepare()

			# RECEIVING transaction query
			data = await asyncio.wait_for(reader.read(2048), timeout=self.timeout)
			result = self.database_connector.insert_values(data.decode())
			self.logger.warning('Sending {} to host: {} at port: {}'.format(result.decode(), *self.SERVER_ADDRESS))
			writer.write(result)
			await writer.drain()
			
			# GLOBAL_COMMIT or GLOBAL ABORT Phase 
			data = await asyncio.wait_for(reader.read(1024), timeout=self.timeout)
			self.logger.warning('Received {} from host: {} at port: {}'.format(data.decode(), *self.SERVER_ADDRESS))
			await self.commit_or_rollback(reader, writer, data, self.SERVER_ADDRESS)
		except asyncio.TimeoutError as err:
			self.logger.error(f'Main Coordinator timed out.')
			writer.close()
			await writer.wait_closed()
			await self.perform_actions_failsafe()
		except ConnectionRefusedError:
			self.logger.error('Unable to connect to intended server.')
			await self.perform_actions_failsafe()
		except:
			self.logger.error('Unknown error occured.')
			self.logger.warning('Shutting down participant.')


	async def commit_or_rollback(self: object, reader: asyncio.StreamReader,
		writer: asyncio.StreamWriter, data: str, address: tuple) -> None:
		"""
		This coroutine responsible for either rollback
		or commit.
		"""
		if data == self.protocols.GLOBAL_COMMIT:
			self.database_connector.commit()
			self.logger.info(f'commit complete.')
			writer.write(self.protocols.SUCCESSFUL_COMMIT)
			await writer.drain()

		elif data == self.protocols.GLOBAL_ABORT:
			self.database_connector.rollback()
			self.logger.warning(f'rollback complete.')
			self.logger.info('')
			writer.write(self.protocols.SUCCESSFUL_ABORT)
			await writer.drain()
		
		else: 
			self.logger.error('Unrecognized protocol.')


	async def perform_actions_failsafe(self: object) -> None:
		"""
		This coroutine responsible to handle communication with
		Fail Safe Coordinator.
		"""
		try:
			self.logger.info('Attempting to connect to Fail Safe Coordinator at host: {} at port: {}'.format(*self.FAIL_SAFE_ADDRESS))
			reader, writer = await asyncio.open_connection(*self.FAIL_SAFE_ADDRESS)
			self.logger.info('Connected to host: {} at port: {}'.format(*self.FAIL_SAFE_ADDRESS))
			writer.write(b'Participant acknowledgement.')
			await writer.drain()
			data = await asyncio.wait_for(reader.read(1024), timeout=self.timeout)
			await self.commit_or_rollback(reader, writer, data, self.FAIL_SAFE_ADDRESS)

		except asyncio.TimeoutError as err:
			self.logger.error(f'Fail Safe timed out.')
		except ConnectionRefusedError:
			self.logger.error('Unable to connect to intended server.')
		except:
			self.logger.error(f'Unknown error occured')
		finally:
			self.logger.warning(f'Shutting down participant.')


# For Testing purposes
if __name__ == '__main__':
	ob = Participant(('127.0.0.1', 8005), ('127.0.0.1', 8006))
	try:
		asyncio.run(ob.perform_actions())
	except KeyboardInterrupt:
		ob.logger.error('Participant interrupted.')