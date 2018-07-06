import json
import time
import arrow
import pandas as pd
import sqlalchemy
from sqlalchemy.orm.session import sessionmaker

import re
import os
from pprint import pprint
from itertools import chain
from collections import defaultdict


class EventTableGetter:
	
	"""
	class to connect to venue tables and get all useful data
	"""
	def __init__(self, **kwargs):

		self.EVENT_TBL = 'DWSales.dbo.event_dim'
		self.SAVETO_DIR = 'collected_events'

	def start_session(self, sqlcredsfile):

		print('starting sqlalchemy session...', end='')

		sql_creds = json.load(open(sqlcredsfile))

		sql_keys_required = set('user user_pwd server port db_name'.split())

		if sql_keys_required != set(sql_creds):
			raise KeyError(f'SQL Credentials are incomplete! The following keys are missing: '
				f'{", ".join([k for k in sql_keys_required - set(sql_creds)])}')

		# generate a Session object
		self._SESSION = sessionmaker(autocommit=True)
		self._ENGINE = sqlalchemy.create_engine(f'mssql+pymssql://{sql_creds["user"]}:{sql_creds["user_pwd"]}@{sql_creds["server"]}:{sql_creds["port"]}/{sql_creds["db_name"]}')
		self._SESSION.configure(bind=self._ENGINE)
		# create a session
		self.sess = self._SESSION()

		print('ok')
		
		return self

	def close_session(self):

		self.sess.close()

		print('closed sqlalchemy session...')

		return self


	def exists(self, tab):
		"""
		check if a table tab exists; return 1 if it does or 0 otherwise
		"""
		return self.sess.execute(f""" IF OBJECT_ID(N'{tab}', N'U') IS NOT NULL
											SELECT 1
										ELSE
											SELECT 0
										  """).fetchone()[0]

	def count_rows(self, tab):
		"""
		count how many rows in table tab
		"""
		return self.sess.execute(f'SELECT COUNT (*) FROM {tab};').fetchone()[0]
				

	def get_events(self):

		if not self.exists(self.EVENT_TBL):
			raise Exception(f'table {self.EVENT_TBL} doesn\'t exist!')
		else:
			print(f'table {self.EVENT_TBL} exists...')	

		self.events_ = pd.read_sql(f"""
								SELECT pk_event_dim,
											primary_show_desc, performance_time, title_who, title_where, title_when, 
												title1, title2, title3, title4, title5, title6
								FROM {self.EVENT_TBL};				
								""", self._ENGINE)

		print(f'collected columns from the event table: {len(self.events_):,} rows')

		return self

	def save(self, tofile=None):

		if not os.path.exists(self.SAVETO_DIR):
			os.mkdir(self.SAVETO_DIR)

		if not tofile:
			file_ = f'events_{arrow.utcnow().to("Australia/Sydney").format("YYYYMMDD")}.csv.gz'
		else:
			file_ = tofile

		self.events_.to_csv(self.SAVETO_DIR + '/' + file_, sep='\t', index=False, compression='gzip')

		print(f'saved to a tab-separated file {file_} in {self.SAVETO_DIR}')

		return self

if __name__ == '__main__':

	t = EventTableGetter().start_session('creds/rds.txt').get_events().close_session().save()
	