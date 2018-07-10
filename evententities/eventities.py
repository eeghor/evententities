from collections import defaultdict
import pandas as pd
import json
import arrow
from weakref import WeakKeyDictionary
import string
import re
import os
import enchant
from artistnormaliser import ArtistNameNormaliser
from typing import NamedTuple

import time
import sqlalchemy
from sqlalchemy.orm.session import sessionmaker

import jellyfish
import itertools

class Artist(NamedTuple):

	name: str
	words_in_name: float
	uncommon_words_in_name: float
	popularity: float
	award_winner: float
	performed_in_australia: float
	possibly_dead: float
	score: float=0


class String:

	"""
	descriptor that requires property to be a string; for explanation of descriptors
	see http://nbviewer.jupyter.org/urls/gist.github.com/ChrisBeaumont/5758381/raw/descriptor_writeup.ipynb
	"""

	def __init__(self, value_default):

		# value_default is the default value any String instance will be initialized with; 
		# it could be None or something else

		self.value = value_default
		self.data = WeakKeyDictionary()

	def __get__(self, instance, owner):

		# instance is the instance (say, x) that calls get on an String-instance property d: like x.d
		# owner: this is effectively the Event class that "owns" this descriptor (because descriptor
		# instances will have to be **class** variables)

		return self.data.get(instance, self.value)

	def __set__(self, instance, value):

		if not isinstance(value, str):
			self.data[instance] = None
		else:
			self.data[instance] = value

class Event:
	"""
	representation of a basic event
	"""
	description = String('')
	entertainment_type = String('')

	def __init__(self, event_id, timestamp=None, description=None, entertainment_type=None):

		# event id comes from an event table, it's a label we assign and do nothing with
		self._ev_id = event_id
		self._timestamp = timestamp
		# description is a string containing basic info about an event
		self.description = description
		# entertainment_type is a high-level event type like sports or music or something
		self.entertainment = entertainment_type
		# every event instance will have its features (labels)
		self._labels = defaultdict()
	
	@property
	def ev_id(self):
		return self._ev_id

	@ev_id.setter
	def ev_id(self, value):
		if isinstance(value, int):
			self._ev_id = value

	@property
	def timestamp(self):
		return self._timestamp

	@timestamp.setter
	def timestamp(self, value):
		if isinstance(value, str):
			self._timestamp = value

	def show(self):

		print()
		print(f'ID:')
		print(f'{" ":>18}{self._ev_id}')
		print(f'DESCRIPTION:')
		print(f'{" ":>18}{self.description}')
		print('LABELS:')
		print()
		for lab in self._labels:
			print(f'{lab:>16}: {", ".join(self._labels[lab])}')
		print()
		print(f'ENTERTAINMENT TYPE: {self.entertainment}')
		print()

	def get_type(self):
		"""
		decide what event type it is based on labels
		"""
		words_in_descr_ = set(self.description.lower().split())
		descr_norm_ = ' '.join(self.description.lower().split())

		conditions = {'concert': any([{'artists', 'promoters'} < set(self._labels),
										('artists' in self._labels) and ({'guest', 'featuring', 'feat', 'with', 
											'headline', 'presents', 'vinyl', 'cd', 'tour'} & words_in_descr_),
												'doors open' in descr_norm_]),

					  'special interest': {'boxers', 'psychics', 'life_coaches', 'motivational_speakers'} & set(self._labels),
					  'sport': any([len(self._labels.get('teams', [])) == 2,
										{'sport_venues', 'sport_names'} < set(self._labels)]),
					  'circus': 'circuses' in self._labels}

		for tp in conditions:

			if conditions[tp]:
				self.entertainment = tp
				break

		return self

	def to_json(self):

		return {**{'event_id': self._ev_id, 'type': self.entertainment}, **{l: list(self._labels[l]) for l in self._labels}}
	

class EventFeatureFactory(ArtistNameNormaliser):
	
	"""
	class to connect to venue tables and get all useful data
	"""
	def __init__(self, reset_tracking=False):

		self.EVENT_TBL = 'DWSales.dbo.event_dim'

		self.NEWEVENT_DIR = os.path.join(os.path.curdir, 'new_events')
		self.OLDEVENT_DIR = os.path.join(os.path.curdir, 'old_events')
		self.OLDEVENT_FILENAME = 'old_events.txt'
		self.OLDEVENT_FILE = os.path.join(self.OLDEVENT_DIR, self.OLDEVENT_FILENAME)

		if reset_tracking:
			try:
				os.remove(self.OLDEVENT_FILE)
				print(f'reset event primary keys tracking - deleted {self.OLDEVENT_FILE}..')
			except:
				pass

		self.JSON_DIR = os.path.join(os.path.curdir, 'features')
		self.JSON_FILENAME = 'features.json'
		self.JSON_FILE = os.path.join(self.JSON_DIR, self.JSON_FILENAME)

		self.REQ_DIRS = [self.NEWEVENT_DIR, self.OLDEVENT_DIR, self.JSON_DIR]	

		for d in self.REQ_DIRS:
			if not os.path.exists(d):
				os.mkdir(d)

		self.spell_checker = enchant.Dict("en_US")

		self.DATA_DIR = os.path.join(os.path.curdir, 'data')

		# geo

		self.GEO_DIR = os.path.join(self.DATA_DIR, 'geo')

		self._countries, self._suburbs = [json.load(open(os.path.join(self.GEO_DIR, f + '.json'))) 
			for f in ['countries', 'suburbs']]

		# sports

		self.SPORTS_DIR = 'sports'

		self._teams, self._sport_names, self._tournaments, \
			self._tournament_types, self._sponsors, self._sport_venues = \
				[json.load(open(os.path.join(self.DATA_DIR, self.SPORTS_DIR, f + '.json'))) 
			for f in ['teams', 
						'sport-names', 
							'tournaments',
								'tournament-types',
									'sponsors',
										'sport-venues']]
		# music

		self._promoters = json.load(open(os.path.join(self.DATA_DIR, 'data_promoters.json')))
		self._music_venues = json.load(open(os.path.join(self.DATA_DIR, 'data_music-venues.json')))

		self._artists = json.load(open(os.path.join(self.DATA_DIR, 'data_artists.json')))
		self._major_music_genres = json.load(open(os.path.join(self.DATA_DIR, 'data_major-music-genres.json')))

		self._dead_bands = json.load(open(os.path.join(self.DATA_DIR, 'dead_bands.json')))

		self._award_winners = [self.normalize(a) for a in json.load(open(os.path.join(self.DATA_DIR, 'award_winners.json')))]

		self._artists_popular = {self.normalize(a) for a in open(os.path.join(self.DATA_DIR, 'top_artists.txt')).readlines() if a.strip()}

		self._aus_gig_artists = {self.normalize(a) for a in open(os.path.join(self.DATA_DIR, 'aus_gig_artists.txt')).readlines() if a.strip()}

		self._venue_types = json.load(open(os.path.join(self.DATA_DIR, 'data_venue-types.json')))

		# musicals

		self.MUSICAL_DIR = 'musical'

		self._musicals = json.load(open(os.path.join(self.DATA_DIR, self.MUSICAL_DIR, 'musicals.json')))

		# opera

		self.OPERA_DIR = 'opera'

		self._opera_singers = json.load(open(os.path.join(self.DATA_DIR, self.OPERA_DIR, 'singers.json')))

		# comedy

		self.COMEDY_DIR = 'comedy'

		self._comedians = json.load(open(os.path.join(self.DATA_DIR, self.COMEDY_DIR, 'comedians.json')))

		# circus

		self.CIRCUS_DIR = 'circus'

		self._circuses = json.load(open(os.path.join(self.DATA_DIR, self.CIRCUS_DIR, 'circus.json')))

		# special interests

		self.SPECIAL_DIR = 'special'

		self._life_coaches, self._boxers, self._psychics, self._motivational_speakers = [json.load(open(os.path.join(self.DATA_DIR, self.SPECIAL_DIR, f + '.json'))) 
												for f in ['life_coaches', 'boxers', 'psychics', 'motivational_speakers']]

		self.COMPANY_DIR = 'companies'

		self._companies = json.load(open(os.path.join(self.DATA_DIR, self.COMPANY_DIR, 'companies.json')))

		self.MOVIE_DIR = 'movie'

		self._movies = json.load(open(os.path.join(self.DATA_DIR, self.MOVIE_DIR, 'movies.json')))

		self._festivals = json.load(open(os.path.join(self.DATA_DIR, 'data_festivals.json')))
		
		self._purchase_types = json.load(open(os.path.join(self.DATA_DIR, 'data_purchase-types.json')))

		

		self._NES = {'suburbs': self._suburbs, 
					 'musicals': self._musicals, 
					 'artists': self._artists, 
					 'movies': self._movies,
					 'promoters': self._promoters, 
					 'opera_singers': self._opera_singers,
					 'countries': self._countries, 
					 'companies': self._companies,
					 'teams': self._teams,
					 'sport_names': self._sport_names, 
					 'venue_types': self._venue_types,
					 'sport_venues': self._sport_venues,
					 'major_music_genres': self._major_music_genres, 
					 'music_venues': self._music_venues,
					 'festivals': self._festivals,
					 'tournament_types': self._tournament_types,
					 'tournaments': self._tournament_types, 
					 'sponsors': self._sponsors,
					 'purchase_types': self._purchase_types, 
					 'comedians': self._comedians,
					 'life_coaches': self._life_coaches,
					 'boxers': self._boxers,
					 'psychics': self._psychics,
					 'circuses': self._circuses,
					 'motivational_speakers': self._motivational_speakers}

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
			old_event_pks = {l.strip() for l in open(self.OLDEVENT_FILE).readlines() if l.strip()}
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

	def find_matches(self, st, items):
		"""
		generic matcher: find items in string st
		"""

		found = set()

		for c in items:

			if ' ' + c + ' ' in ' ' + st.lower() + ' ':
				found.add(c)

		return found

	def get_event_time(self, st):
		"""
		find a time stamp in string st and see if it's morning, afternoon or evening
		"""
		# we expect to have time stamps like '2013-05-05 12:30:45'

		weekdays = {i: d for i, d in zip([i for i in range(7)], 
							['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday'])}

		ts = arrow.get(st, 'YYYY-MM-DD HH:mm:ss')
		hour = ts.hour

		return (weekdays[ts.weekday()], 'morning' if (5 <= hour <= 11) else 
					'afternoon' if (12 <= hour < 18) else 
						'evening' if (18 <= hour < 21) else 'night')

	def find(self, st, what):
		"""
		find something that is available in an alphabetical dictionary in the string
		"""

		assert what in self._NES, f'unfortunately, {what} is not supported'

		_s = self.normalize(st)

		dk = self._NES[what]

		if not _s:
			return None

		found = set()
		words = _s.split()

		for i, w in enumerate(words):

			_l1 = w[0]

			if _l1.isalpha() and (_l1 in dk):

				_cands = {s for s in dk[_l1] if len(s.split()) <= len(words[i:])}

				if _cands:
					found.update(self.find_matches(_s, _cands))
			else:
				continue

		return found if found else None

	def rank_artists(self, artist_list):
		"""
		which artist candidates on the list artist_list are more likely to be artist?
		"""

		MAX_ART = 3   # return up to 3 top ranked artists

		bonuses = {'words_in_name': 0.5,    # per extra word
						'uncommon_words_in_name': 1,   # multiplier
							'popularity': 2,
								'award_winner': 1,
									'performed_in_australia': 0.5,
										'possibly_dead': -1}   


		criteria = {'words_in_name': lambda x: bonuses['words_in_name']*(len(x.split()) - 1),
					'uncommon_words_in_name': lambda x: bonuses['uncommon_words_in_name']*(1 - sum([(self.spell_checker.check(x) or self.spell_checker.check(x.title())) 
															for w in x.split()])/len(x.split())),
					'popularity': lambda x: bonuses['popularity'] if x in self._artists_popular else 0,
					'award_winner': lambda x: bonuses['award_winner'] if x in self._award_winners else 0,
					'performed_in_australia': lambda x: bonuses['performed_in_australia'] if x in self._aus_gig_artists else 0,
					'possibly_dead': lambda x: bonuses['possibly_dead'] if x in self._dead_bands[x[0]] else 0}

		scores_ = [a._replace(score=sum([a.words_in_name, a.uncommon_words_in_name, a.popularity, a.performed_in_australia,
							a.possibly_dead]))
						 for a in [Artist(name=a, **{c: criteria[c](a) for c in criteria}) for a in artist_list]]

		return [_.name for _ in sorted(scores_, key=lambda x: x.score, reverse=True) if _.score > 0][:MAX_ART]

	def rank_countries(self, countries):
		"""
		decide what countries in countries are worth keeping
		"""

		if not isinstance(countries, list):
			_ = list(countries)
		else:
			_ = countries

		list_out = [c for c in _ if (len(c) > 3) or (c in ['aus', 'nz', 'png', 'usa', 'us', 'uk'])]

		return list_out if list_out else None

	def find_teams(self, cands, s, m=None):
		"""
		find what teams are mentioned in event description s; only up to 2 teams can be returned!
		if need more, lift restrictions below
		"""

		max_words_in_team = len(max(cands, key=lambda _: len(_.split())).split())
		print('max_words_in_team=', max_words_in_team)
		words_in_string = len(s.split()) 
		print('words_in_string=', words_in_string)

		print('len(cands)=', len(cands))

		# set to store matched teams
		if not m:
			m = set()

		# stoppage conditions - pay attention because this method is called recursively
		if (len(m) > 1) or (not max_words_in_team) or (not words_in_string) or (max_words_in_team > words_in_string):
			return m

		its = itertools.tee(iter(s.split()), max_words_in_team)

		print('its=', its)

		# move some iterators ahead
		for i, _ in enumerate(range(max_words_in_team)):
			if i > 0:
				for x in range(i):
					next(its[i], None)  # i moves ahead by i - 1
		
		possible_matches = set()
																
		for p in zip(*its):
			possible_matches.add(' '.join(p))

		print('possible_matches=',possible_matches)

		cands_to_remove = set()
		pms_to_remove = set()
		
		# vary levenshtein distance from 0 (exact match) to 2 
		for lev in range(3):

			for team in cands:

				if not lev:

					if team in possible_matches:
						print('found ', team)
						m.add(team)
						print('now m=', m)
						
						if len(m) > 1:
							return m
						
						cands_to_remove.add(team)
						pms_to_remove.add(team)
						
						# remove matched team from description
						s = ' '.join(s.replace(team, ' ').split())
				else:

					for pm in possible_matches:

						if jellyfish.levenshtein_distance(team,pm) == lev:
							print('found ', team)
							m.add(team)
							print('m=',m)
							
							if len(m) > 1:
								print('returning m!')
								return m

							print('seting candidates to remove..')
							cands_to_remove.add(team)
							pms_to_remove.add(pm)

			# remove detected candidates from list of candidates
			# and do same for possible matches
			print('updating candidates...')
			cands = cands - cands_to_remove
			possible_matches = possible_matches - pms_to_remove

		print('special case...')
		# cover the case when some teams have long names and hence are often mentioned by a shortened name
		if max_words_in_team > 1: 
																
			new_cands = set()
																
			for c in cands:

				if len(c.split()) == max_words_in_team:
					if max_words_in_team == 2:
						for v in c.split():
							if not self.spell_checker.check(v):
								new_cands.add(v)
					else:

						for cm in itertools.combinations(c.split(), max_words_in_team - 1):
							new_cands.add(' '.join(cm))
						if max_words_in_team > 2:
							new_cands.add(''.join([x[0] for x in c.split()]))
			
			cands = {c for c in cands if not len(c.split()) == max_words_in_team} | new_cands

			print('calling find_teams again with')
			print('len(cands)=', len(cands))
			print('s=', s)
			print('len(m)=', len(m))
			m.update(self.find_teams(cands, s, m))

		print('end of method returning m=', m)
		
		return m if m else None


	def get_labels(self, s):
		"""
		extract all labels from description s
		"""

		labels_ = dict()

		for what in self._NES:

			fnd_ = self.find(s, what)

			if fnd_:

				if what == 'artists':
					fnd_ = self.rank_artists(fnd_)
				elif what == 'countries':
					fnd_ = self.rank_countries(fnd_)

				if fnd_:
					labels_.update({what: fnd_})

		return labels_

	def get_features(self):

		# column names to be parts of description
		descr_cols = list(self.events_.columns[1:])
		
		pks_processed = []

		evs_processed = []

		for i, event in enumerate(self.events_.iloc[:1000].iterrows(),1):

			e = Event(event_id=event[1]['pk_event_dim'],
					description= ' ' .join([str(event[1][c]) for c in descr_cols]))

			e._labels = self.get_labels(e.description)

			e.get_type()
			e.show()

			if e._labels.get('sport_venues', None) and len(e._labels.get('teams', []) < 2):
				e._labels['teams'] = self.find_teams({t for l in self._teams for t in self._teams[l]}, e.description)

			
			pks_processed.append(event[1]['pk_event_dim'])
			evs_processed.append(e.to_json())
		
		with open(self.OLDEVENT_FILE, 'a') as f:
			for k in pks_processed:
				f.write(f'{k}\n')

		try:
			evs_processed = {**evs_processed, **json.load(open(self.JSON_FILE))}
		except:
			pass

		json.dump(evs_processed, open(self.JSON_FILE,'w'))

		print(f'done. produced features for {len(pks_processed)} new event primary keys...')

if __name__ == '__main__':

	t_st = time.time()

	eff = EventFeatureFactory(reset_tracking=True)

	tms = {t for l in eff._teams for t in eff._teams[l]}

	print(eff.find_teams(tms, '233/544/1 __DSxfskjn Al Ahly vs  Sydny fc ANZ stadium shandong luneng'.lower()))

	# eff = EventFeatureFactory(reset_tracking=True) \
	# 		.start_session('creds/rds.txt') \
	# 		.find_new_events() \
	# 		.get_events() \
	# 		.close_session() \
	# 		.save()	\
	# 		.get_features()

	print('elapsed time: {:.0f} min {:.0f} sec'.format(*divmod(time.time() - t_st, 60)))
	

	
	
	
	

	

	

	

	
	
	

