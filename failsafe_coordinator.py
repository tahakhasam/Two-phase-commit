import asyncio
import logging
from constants import Constants

class FailSafeCoordinator:

	def __init__(self: object, address: str, max_conn: int) -> object:
		"""
		This constructor sets up all required address and
		preliminary configurations.
		"""
		self.SERVER_ADDRESS = (address, 9000)
		self.FAIL_SAFE_ADDRESS = ('', 8006)
		self.protocols = Constants()
		self.set_up_logger()
		self.max_conn = max_conn
		self.connected_clients = 0
		self.commit = False
		self.clients = {}

	def set_up_logger(self: object) -> None:
		""" Sets up loggers both for Console as well as File. """
		self.logger = logging.getLogger('failsafe-coordinator')
		self.logger.setLevel(logging.INFO)
		console_handler = logging.StreamHandler()
		file_handler = logging.FileHandler('failsafe_coordinator.log', 'w+')
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
		Main Coroutine handles connection with 
		Main Coordinator as well as participants.
		"""
		try:
			address = writer.get_extra_info('peername')
			if address == self.SERVER_ADDRESS:
				self.logger.info('Connected to Main Coordinator.')
				await self.communicating_with_server(reader, writer)

			if address != self.SERVER_ADDRESS:
				self.connected_clients += 1
				self.clients[address] = (reader, writer)
				await self.communicating_with_participants(reader, writer, address)
		except:
			self.logger.error('Unknown error occured.')

	async def communicating_with_server(self: object, reader: asyncio.StreamReader, 
		writer: asyncio.StreamWriter) -> None:
		"""
		This coroutine communicates with Main Coordinator.
		"""
		self.logger.info('Awaiting communication from Main Coordinator.')
		data = await reader.read(1024)
		
		if data == self.protocols.DECIDED_TO_COMMIT:
			self.logger.info(f'Received {data.decode()} from Main Coordinator.')
			self.commit = True
			writer.write(self.protocols.RECORDED_COMMIT)
			self.logger.info(f'Sent {self.protocols.RECORDED_COMMIT.decode()} to Main Coordinator.')
			await writer.drain()
			writer.close()
			await writer.wait_closed()

	async def communicating_with_participants(self: object, reader: asyncio.StreamReader, 
		writer: asyncio.StreamWriter, address: tuple) -> None:
		"""
		This coroutine communicates with all Participants.
		"""
		self.logger.info('Awaiting connection from all participants.')
		await self.waiting_state()
		
		if self.commit:
			await self.broadcast(self.protocols.GLOBAL_COMMIT)
			return 

		if not self.commit:
			await self.broadcast(self.protocols.GLOBAL_ABORT)
			return

		data = await reader.read(1024)
		self.logger.info('Received {} from host: {} at port: {}'.format(data.decode(), *address))
		if data == self.protocols.SUCCESSFUL_COMMIT or data == self.protocols.SUCCESSFUL_ABORT:
			self.logger.warning('Closing stream of host: {} at port: '.format(*address))
			writer.close()
			await writer.wait_closed()

	async def broadcast(self: object, message: str) -> None:
		"""
		Broadcasts GLOBAL protocols.
		"""
		self.logger.info(f'Sending {message.decode()} to all connected participants.')
		for reader, writer in self.clients.values():
			writer.write(message)
			await writer.drain()

	async def waiting_state(self: object) -> None:
		"""
		Waits from connection from all participants.
		"""
		while True:
			if self.connected_clients == self.max_conn:
				return
			await asyncio.sleep(1)

	async def start_connections(self: object) -> None:
		""" Responsible for all connections. """
		self.server = await asyncio.start_server(self.perform_actions, *self.FAIL_SAFE_ADDRESS)
		async with self.server:
			self.logger.info('Awaiting connection')
			await self.server.serve_forever()

# For Testing purposes
if __name__ == '__main__':
	ob = FailSafeCoordinator('127.0.0.1', 2)
	try:
		asyncio.run(ob.start_connections())
	except KeyboardInterrupt:
		ob.logger.error('Fail safe stopped')







