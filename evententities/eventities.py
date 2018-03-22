from collections import defaultdict
from itertools import chain
import pandas as pd
import json
import arrow
from weakref import WeakKeyDictionary
import string
import re
import os
import time

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
	

class EventFeatureFactory:
	"""
	extract entities from event description
	"""
	def __init__(self):

		self._ev_id = None
		self._description = None
		self._labels = defaultdict()

		self.DATA_DIR = os.path.join(os.path.curdir, 'data')

		# abbreviations
		self._abb, self._state_abb, self._sport_abb = [json.load(open(os.path.join(self.DATA_DIR, f))) 
			for f in ['data_abbreviations.json', 
						'data_state-abbreviations.json', 
							'data_sport-abbreviations.json']]
		# sports
		self._teams, self._sport_names, self._tournaments, self._tournament_types, self._sponsors = [json.load(open(os.path.join(self.DATA_DIR, f))) 
			for f in ['data_teams.json', 
						'data_sport-names.json', 
							'data_tournaments.json',
								'data_tournament-types.json',
									'data_sponsors.json']]
		
		self._musicals = json.load(open(os.path.join(self.DATA_DIR, 'data_musicals.json')))
		self._venue_types = json.load(open(os.path.join(self.DATA_DIR, 'data_venue-types.json')))

		self._countries =  json.load(open(os.path.join(self.DATA_DIR, 'data_countries.json')))
		self._suburbs = json.load(open(os.path.join(self.DATA_DIR, 'data_suburbs.json')))
		
		self._promoters = json.load(open(os.path.join(self.DATA_DIR, 'data_promoters.json')))
		self._artists = json.load(open(os.path.join(self.DATA_DIR, 'data_artists.json')))
		self._major_music_genres = json.load(open(os.path.join(self.DATA_DIR, 'data_major-music-genres.json')))

		self._comedians = json.load(open(os.path.join(self.DATA_DIR, 'data_comedians.json')))
		self._opera_singers = json.load(open(os.path.join(self.DATA_DIR, 'data_opera-singers.json')))

		self._companies = json.load(open(os.path.join(self.DATA_DIR, 'data_companies.json')))

		self._movies = json.load(open(os.path.join(self.DATA_DIR, 'data_movies.json')))
		
		self._purchase_types = json.load(open(os.path.join(self.DATA_DIR, 'data_purchase-types.json')))

		self._NES = {'suburbs': self._suburbs, 'musicals': self._musicals, 
					 'artists': self._artists, 'movies': self._movies,
					 'promoters': self._promoters, 'opera_singers': self._opera_singers,
					 'countries': self._countries, 'teams': self._teams,
					 'sport_names': self._sport_names, 'venue_types': self._venue_types,
					 'major_music_genres': self._major_music_genres, 'tournament_types': self._tournament_types,
					 'purchase_types': self._purchase_types, 'comedians': self._comedians}

	def _deabbreviate(self, st):
		"""
		unfold abbreviations in string st
		"""
		_st = st
		# first replace full state names by abbreviations;
		for s in self._state_abb:
			_st = _st.replace(s, self._state_abb[s])
		# other dictionaries map single-word abbreviations so we can just do split
		return ' '.join([self._abb.get(self._sport_abb.get(_, _),_) for _ in _st.strip().split()])

	def _normalize(self, st):
		"""
		normalize string st
		"""
		# firstly, lower case, replace separators with white spaces, remove all non-alphanumeric 
		# and single white space between words - this applies to all

		if not st:
			return None

		_st = st
		_st = _st.lower()
		_st = _st.replace('-',' ').replace('_',' ').replace(".",' ').replace('/',' ').replace('&', 'and')

		_st = ''.join([w for w in _st if w.isalnum() or w.isspace()])

		if not _st:
			return None

		_st = ' '.join(_st.split())

		# remove 'a', 'the', 'and', '&'
		_st = ' '.join([w for w in _st.split() if w not in ['the', 'a', 'an']])

		_st = self._deabbreviate(_st)

		return _st if _st else None

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

		_s = self._normalize(st)

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

if __name__ == '__main__':

	e = Event('123ddf')
	e.description = """0222ds TOMORROW performance by MAGDA OLIVERO plus (** Arsenal at ANZ stadium, 
						NZ -- 20/01/2015 : gordon 2064"""
	
	eff = EventFeatureFactory()
	
	t0 = time.time()

	for i in range(10000):
		for _ in eff._dic:
			e._labels[_] = eff.find(e.description, _)

	print(f'time: {time.time() - t0} sec')
	
	print(e._labels)

