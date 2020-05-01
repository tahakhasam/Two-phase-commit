import logging

class CommonBaseClass:
	def set_up_logger(self, logger_name):
		""" Sets up to loggers One for Console, One for File. """
		self.logger = logging.getLogger(logger_name)
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