import asyncio
import logging
import sys
from database_connectivity import DatabaseConnection
from constants import Constants


class Coordinator:

	def __init__(self: object, max_connections: int, fail_safe_address: str) -> object:
		""" 
		This constructor sets up all the required address 
		and preliminary configurations.
		"""
		self.SECONDARY_SERVER_ADDRESS = ('127.0.0.1', 9000)
		self.SERVER_ADDRESS = ('', 8005)
		self.FAIL_SAFE_ADDRESS = (fail_safe_address, 8006)
		self.max_connections = max_connections
		self.clients = {}
		self.connected_clients = 0
		self.commit = 0
		self.protocols = Constants()
		self.set_up_logger()
	
	def set_up_logger(self: object) -> None:
		""" Sets up to loggers One for Console, One for File. """
		self.logger = logging.getLogger('main-coordinator')
		self.logger.setLevel(logging.INFO)
		console_handler = logging.StreamHandler()
		file_handler = logging.FileHandler('main_coordinator.log', 'w+')
		console_handler.setLevel(logging.INFO)
		file_handler.setLevel(logging.INFO)
		console_format = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
		file_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
		console_handler.setFormatter(console_format)
		file_handler.setFormatter(file_format)
		self.logger.addHandler(console_handler)
		self.logger.addHandler(file_handler)

	async def perform_actions(self: object, reader: asyncio.StreamReader,
		writer: asyncio.StreamWriter) -> None:
		"""
		Main Coroutine which connects to all participants as well as 
		handles communication with Fail safe Coordinator.
		"""
		try:
			address = writer.get_extra_info('peername')
			self.clients[address] = (reader, writer)
			self.connected_clients += 1
			self.logger.info('Connected to host: {} at port: {}'.format(*address))

			await self.waiting_state()
			# PREPARE phase
			await asyncio.sleep(3)
			await self.send_protocol(writer, self.protocols.PREPARE, address)

			# SEND transaction query
			await asyncio.sleep(3)
			await self.send_protocol(writer, self.transaction.encode(), address)
			
			# VOTE_COMMIT or VOTE_ABORT phase
			await asyncio.sleep(3)
			message = await self.received_reply(reader, address)

			if message == self.protocols.VOTE_COMMIT:
				self.commit += 1
				await asyncio.sleep(5)
			
			# GLOBAL_ABORT phase
			if message == self.protocols.VOTE_ABORT:
				await self.broadcast(self.protocols.GLOBAL_ABORT)
				writer.close()
				await writer.wait_closed()
				self.connected_clients -= 1
				del self.clients[address]
			
			# GLOBAL_COMMIT phase
			if self.commit == self.connected_clients:
				await self.send_protocol(self.fail_safe_stream[1], 
					self.protocols.DECIDED_TO_COMMIT, self.FAIL_SAFE_ADDRESS)
				data = await self.received_reply(self.fail_safe_stream[0], address)
				if data == self.protocols.RECORDED_COMMIT:
					await self.broadcast(self.protocols.GLOBAL_COMMIT)
					self.commit = 0

			# COMPLETION phase
			data = await reader.read(1024)
			self.logger.info('Received {} from host: {} at port: {}'.format(data.decode(), *address))
			if data == self.protocols.SUCCESSFUL_COMMIT or data == self.protocols.SUCCESSFUL_ABORT:
				writer.close()
				await writer.wait_closed()
				self.connected_clients -= 1
				del self.clients[address]

		except ConnectionRefusedError:
			self.logger.error('Connection Error with host: {} at port: {}'.format(*address))
		except:
			self.logger.error('Unknown error occured.')

	async def send_protocol(self: object, writer: asyncio.StreamWriter, 
		message: str, address: tuple) -> None:
		""" This coroutine sends protocol to participant. """
		writer.write(message)
		await writer.drain()
		self.logger.info('Sent {} to host: {} at port: {}'.format(message.decode(), *address))

	async def received_reply(self: object, reader: asyncio.StreamReader, 
		address: tuple) -> None:
		""" This coroutine receives reply from respective participants """
		message = await reader.read(1024)
		self.logger.info('Received {} from host: {} at port: {}'.format(message.decode(), *address))
		return message

	async def waiting_state(self: object):
		"""
		This coroutine is a supporting function that is used 
		to await for connection from all participants.
		"""
		while True:
			if self.connected_clients == self.max_connections:
				return

			await asyncio.sleep(1)

	async def connect_to_failsafe(self:object) -> None:
		""" This coroutine is used to connect to all fail safe coordinator. """
		try:
			self.logger.info('Attempting to connect to fail_safe_server.')
			reader, writer = await asyncio.open_connection(*self.FAIL_SAFE_ADDRESS, 
				local_addr=self.SECONDARY_SERVER_ADDRESS)
			self.logger.info('Connected to Fail Safe Coordinator.')
			self.fail_safe_stream = (reader, writer)
		except OSError:
			self.logger.error('Unable to connect to Fail Safe Coordinator.')
			sys.exit(0)

	async def broadcast(self: object, protocol: str) -> None:
		""" This coroutine is used to broadcast global protocols. """
		for reader, writer in self.clients.values():
			writer.write(protocol)
			await writer.drain()
			self.logger.info(f'Sent {protocol.decode()} to all connected participants')

	async def start_connections(self: object) -> None:
		"""
		This coroutine creates server and sets up connection to all participants and
		Fail Safe Coordinator. 
		"""
		self.server = await asyncio.start_server(self.perform_actions,*self.SERVER_ADDRESS)
		self.employee_name = input('Enter employee_name : ')
		self.salary = input(f'Enter salary of {self.employee_name} : ')
		self.transaction = DatabaseConnection.add_transaction %(self.employee_name, self.salary)
		async with self.server:
			await self.connect_to_failsafe()
			self.logger.info('Awaiting connection from participants.')
			await self.server.serve_forever()

# For Testing Purposes
if __name__ == '__main__':
	ob = Coordinator(2,'127.0.0.1')
	try:
		asyncio.run(ob.start_connections())
	except KeyboardInterrupt:
		ob.logger.error('Server Stopped')
