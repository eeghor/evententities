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

		self.NEWEVENT_DIR = 'new_events'
		self.OLDEVENT_DIR = 'old_events'
		self.OLDEVENT_FILE = 'old_events.txt'

		self.REQ_DIRS = [self.NEWEVENT_DIR, self.OLDEVENT_DIR]	

		for d in self.REQ_DIRS:
			if not os.path.exists(d):
				os.mkdir(d)

	def start_session(self, rds_creds_):

		print('starting sqlalchemy session...', end='')

		sql_keys_required = set('user user_pwd server port db_name'.split())

		sql_creds = json.load(open(rds_creds_))

		if sql_keys_required != set(sql_creds):
			raise KeyError(f'RDS SQL Credentials are incomplete! The following keys are missing: '
				f'{", ".join([k for k in sql_keys_required - set(sql_creds)])}')

		self._ENGINE = sqlalchemy.create_engine(f'mssql+pymssql://{sql_creds["user"]}:{sql_creds["user_pwd"]}'
													f'@{sql_creds["server"]}:{sql_creds["port"]}/{sql_creds["db_name"]}')
		self._SESSION = sessionmaker(autocommit=True, bind=self._ENGINE)

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

	def get_column(self, table_, column_, type_=None, distinct_=None):
		"""
		grab a single column COLUMN_ from table TABLE_ as type TYPE_ (pick DISTINCT_=True for distinct values)

		type_ can be one of the following: 
		
			bigint, int, smallint, tinyint, bit, decimal, numeric, money, smallmoney, 
			float, real, datetime, smalldatetime, char, varchar, text, nchar, nvarchar, 
			ntext, binary, varbinary, or image

		"""
		cast_part = f'CAST ({column_} as {type_})' if type_ else column_

		return {_[0] for _ in self.sess.execute(f'SELECT {"DISTINCT" if distinct_ else ""} {cast_part} FROM {table_};').fetchall()}
				
	def find_new_events(self):
		"""
		check which pks currently in the event table are new, i.e. not on the list of old pks yet and return them 
		"""
		current_pks = self.get_column(table_=self.EVENT_TBL, column_='pk_event_dim', type_='nvarchar', distinct_=True) 

		try: 
			old_event_pks = {l.strip() for l in open(f'{self.OLDEVENT_DIR}/{self.OLDEVENT_FILE}').readlines() if l.strip()}
		except:
			old_event_pks = set()

		self.NEW_EVENT_PKS = current_pks - old_event_pks

		print(f'found {len(self.NEW_EVENT_PKS):,} new events...')

		return self


	def get_events(self):
		"""
		download relevant columns for the events with primary keys that we are interested in
		"""

		if not self.NEW_EVENT_PKS:
			print('no new events today...')
			return self

		pks_ = """pk_event_dim primary_show_desc performance_time title_who title_where title_when
					title1 title2 title3 title4 title5 title6""".split()

		if not self.exists(self.EVENT_TBL):
			raise Exception(f'table {self.EVENT_TBL} doesn\'t exist!')
		else:
			print(f'table {self.EVENT_TBL} exists...')	

		# https://docs.microsoft.com/en-us/sql/sql-server/maximum-capacity-specifications-for-sql-server

		if len(self.NEW_EVENT_PKS) < 10000:

			self.events_ = pd.read_sql(f"""
								SELECT {','.join(pks_)}
								FROM {self.EVENT_TBL} WHERE pk_event_dim in ({', '.join(self.NEW_EVENT_PKS)});				
								""", self._ENGINE)
		else:

			self.events_ = pd.read_sql(f"""
								SELECT {','.join(pks_)}
								FROM {self.EVENT_TBL};				
								""", self._ENGINE).query(f"pk_event_dim in ({', '.join(self.NEW_EVENT_PKS)})")

		print(f'collected {len(self.events_):,} rows')

		return self

	def save(self, tofile=None):

		if not tofile:
			file_ = f'events_{arrow.utcnow().to("Australia/Sydney").format("YYYYMMDD")}.csv.gz'
		else:
			file_ = tofile

		self.events_.to_csv(self.NEWEVENT_DIR + '/' + file_, sep='\t', index=False, compression='gzip')

		print(f'saved to a tab-separated file {file_} in {self.NEWEVENT_DIR}')

		return self

if __name__ == '__main__':

	t = EventTableGetter().start_session('creds/rds.txt').find_new_events().get_events()

	t.close_session()

	t.save()	