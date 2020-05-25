import mysql.connector
import getpass
from constants import Constants
from mysql.connector import errorcode

class DatabaseConnection:
	database = 'transaction_data'

	config = {
		'user': 'taha',
		'password': 'Freezehell!23',
		'host': '127.0.0.1',
		'database': database,
		'raise_on_warnings': True 
	}

	table = (
		"CREATE TABLE transaction_table ("
		"	transid INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT,"
		"	name VARCHAR(50),"
		"	salary DECIMAL(9,2)"
		")")

	def __init__(self):
		""" Connects to database. """
		while True:
			try:	
				self.conn = mysql.connector.connect(**type(self).config)
				break	

			except mysql.connector.Error as err:
				if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
					print('Username or password is incorrect.')
					self.reconfigure()
				elif err.errno == errorcode.ER_BAD_DB_ERROR:
					print('Database does not exist')
					self.create_database()
				else:
					print(err)

	def reconfigure(self):
		""" Used for reconfiguring database connection details. """
		print('Re-enter your Details: ')
		user = input('Enter username : ')
		password = getpass.getpass(prompt='Enter your password : ')
		type(self).config = {
			'user': user,
			'password': password,
			'host': '127.0.0.1',
			'database': type(self).database,
			'raise_on_warnings': True
		}

	def create_database(self):
		""" Creates database. """
		print('Creating database: ')
		try:
			temp_config = type(self).config.copy()
			print(type(self).config)
			del temp_config['database']
			print(temp_config)
			conn = mysql.connector.connect(**temp_config)
			cursor = conn.cursor()
			cursor.execute('CREATE DATABASE transaction_data')
			conn.close()

		except mysql.connector.Error as err:
			print(err.msg)

	def create_table(self):
		""" Creates table. """
		try:
			self.cursor.execute(f"USE {type(self).database}")
			self.cursor.execute(type(self).table)
		
		except mysql.connector.Error as err:
			if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
				print('table already exists.')
			else:
				print(err.msg)
		else:
			print('Table created.')

	def insert_values(self: object, query:str) -> None:
		""" Inserts Value to the table. """
		try:
			self.cursor.execute(query)
			return Constants().VOTE_COMMIT
		except mysql.connector.Error as err:
			print(err.msg)
			return Constants().VOTE_ABORT

	def prepare(self):
		""" Prepares for database processes. """
		self.cursor = self.conn.cursor()
		self.create_table()

	def commit(self):
		""" Commits to database. """
		try:
			self.conn.commit()
		except mysql.connector.Error as err:
			print(err.msg)
		finally:
			self.conn.close()

	def rollback(self):
		""" Rollbacks the inserted data. """
		try:
			self.conn.rollback()
		except mysql.connector.Error as err:
			print(err.msg)
		finally:
			self.conn.close()


