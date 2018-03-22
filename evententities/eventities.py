from collections import defaultdict
from itertools import chain
import pandas as pd
import json
import arrow
from weakref import WeakKeyDictionary
import string
import re
import os

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

	def __init__(self, event_id, description, entertainment_type):

		# event id comes from an event table, it's a label we assign and do nothing with
		self._ev_id = event_id
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

class EventFeatureFactory:
	"""
	extract entities from event description
	"""
	def __init__(self):

		self._ev_id = None
		self._description = None
		self._labels = defaultdict()

		self.DATA_DIR = os.path.join(os.path.curdir, 'data')

		# load data
		self._teams = json.load(open(os.path.join(self.DATA_DIR, 'data_teams.json')))
		self._sport_abb = json.load(open(os.path.join(self.DATA_DIR, 'data_sport-abbreviations.json')))
		self._abb = json.load(open(os.path.join(self.DATA_DIR, 'data_abbreviations.json')))
		self._sport_names = {_.strip() for _ in open(os.path.join(self.DATA_DIR, 'data_sport-names.txt')).readlines() if _.strip()}
		self._musicals = json.load(open(os.path.join(self.DATA_DIR, 'data_musicals.json')))
		self._venue_types = {_.strip() for _ in open(os.path.join(self.DATA_DIR, 'data_venue_types.txt')).readlines() if _.strip()}
		self._countries =  json.load(open(os.path.join(self.DATA_DIR, 'data_countries.json')))
		self._suburbs = json.load(open(os.path.join(self.DATA_DIR, 'data_suburbs.json')))
		self._state_abb = json.load(open(os.path.join(self.DATA_DIR, 'data_state-abbreviations.json')))
		self._abb = json.load(open(os.path.join(self.DATA_DIR, 'data_abbreviations.json')))
		self._promoters = json.load(open(os.path.join(self.DATA_DIR, 'data_promoters.json')))
		self._comedians = json.load(open(os.path.join(self.DATA_DIR, 'data_comedians.json')))
		self._opera_singers = json.load(open(os.path.join(self.DATA_DIR, 'data_opera-singers.json')))
		self._companies = json.load(open(os.path.join(self.DATA_DIR, 'data_companies.json')))
		self._movies = json.load(open(os.path.join(self.DATA_DIR, 'data_movies.json')))
		self._tournaments = json.load(open(os.path.join(self.DATA_DIR, 'data_tournaments.json')))
		self._tournament_types = {_.strip() for _ in open(os.path.join(self.DATA_DIR, 'data_tournament-types.txt')).readlines() if _.strip()}
		self._sponsors = json.load(open(os.path.join(self.DATA_DIR, 'data_sponsors.json')))
		self._purchase_types = {_.strip() for _ in open(os.path.join(self.DATA_DIR, 'data_purchase-types.txt')).readlines() if _.strip()}
		self._mojor_music_genres = {_.strip() for _ in open(os.path.join(self.DATA_DIR, 'data_major_music-genres.txt')).readlines() if _.strip()}
		self._artists = json.load(open(os.path.join(self.DATA_DIR, 'data_artists.json')))

		self.re_punct = re.compile('[' + '|'.join([re.escape(p) for p in string.punctuation]) + ']')

		self.labels = defaultdict()

	def _deabbreviate(self, st):
		"""
		unfold abbreviations in string st
		"""
		# first replace full state names by abbreviations;
		for s in self._state_abb:
			st = st.replace(s, self._state_abb[s])
		# other dictionaries map single-word abbreviations so we can just do split
		return ' '.join([self._abb.get(self._sport_abb.get(_, _),_) for _ in st.strip().split()])

	def _list_to_alph_dict(self, lst):

		d = defaultdict()

		# remove the, a
		for l in lst:
			_ = l.replace('the ','').replace('a ','').replace('"','').strip()
			if len(_) > 1:
				_l1 = _[0]
				if _l1 in d:
					d[_l1].append(_)
				else:
					d[_l1] = [_]
		for i in d:
			d[i] = sorted(d[i])

		return d


	def _unfold_states(self, st):

		for ab, s in self.state_abbr.items():
			st = st.replace(s, ab)

		return st

	def _unfold_cities(self, st):
		
		for cit in self.city_variants:
			for alt in self.city_variants[cit]:
				st = re.sub(r'\b{}\b'.format(alt), cit, st.lower())

		return st

	def _unfold_teams(self, st):

		st = re.sub(r'\b{}\b'.format(self.sport_variants[cn]), cn, st.lower())

		return st
	
	def _remove_punctuation(self, st):

		if not isinstance(st, str):
			return None

		_ = [_ for _ in self.re_punct.sub(' ', st).split() if len(_) > 0]

		return ' '.join(_) if _ else _

	def _remove_nonalphanum(self, st):

		if not isinstance(st, str):
			return None

		_ = ''.join([w for w in st if w.isalnum() or w.isspace()]).strip()

		return ''.join(_) if len(_) > 0 else None

	def _remove_nonalpha(self, st):

		if not isinstance(st, str):
			return None

		_ = ''.join([w for w in st if w.isalpha() or w.isspace()]).strip()

		return ''.join(_) if len(_) > 0 else None

	def _remove_articles(self, st):

		if not isinstance(st, str):
			return None

		_ = [w for w in st.lower().split() if w not in {'and', 'the', 'a'}]

		return ' '.join(_) if _ else None

	def _normalize_title(self, title_str):
		"""
		assuming title_str is a title such as a movie title or play title, do specialized normalization
		"""
		title_str = title_str.lower()

		if '(' in title_str:
			p1, p2 = title_str.split('(')
			_ = ' '.join((p1 + p2.split(')')[-1]).split())
			title_str = _ if len(_) > 0 else None

		if (title_str) and (len(title_str) > 0):
			title_str = title_str.replace('-',' ').strip()
		else:
			return None

		if (title_str) and (len(title_str) > 0):
			title_str = self._remove_punctuation(self._remove_nonalphanum(title_str))
		else:
			return None

		title_str = self._remove_articles(title_str)

		return title_str


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

	def get_suburb(self, st):
		"""
		find any suburbs in the string
		"""
		_s = self._remove_nonalpha(st.lower())

		if not _s:
			return None

		found = set()

		for i, w in enumerate(_s.split()):

			_l1 = w[0]

			if _l1.isalpha():

				_cands = {s['name'] for s in self._suburbs[_l1] if len(s['name'].split()) <= len(_s[i:])}

				if _cands:
					found.update(self.find_matches(_s, _cands))
			else:
				continue

		return found if found else None

	def normalize_countries(self):

		self._countries = {_c for _c in {self._remove_nonalphanum(self._remove_punctuation(c.lower())) 
								for c in self._countries} if _c}
		return self

	def normalize_musicals(self):

		self._musicals = self._list_to_alph_dict({_c for _c in {self._normalize_title(c) for c in self._musicals} if _c})

		return self

	def normalize_movies(self):

		self.movies = self._list_to_alph_dict({_c for _c in {self._normalize_title(c) for c in self.movies} if _c})

		return self

	def normalize_artists(self):

		for c in self.artists:
			self.artists[c] = sorted([_ for _ in [self._remove_articles(_c) for _c in self.artists[c]] if _ and len(_) > 0])

		return self

	def get_countries(self, st):

		self.labels['countries'] = self.find_matches(self._remove_nonalphanum(self._remove_punctuation(st)), self._countries)

		return self

	def get_venue_types(self, st):

		self.labels['venue_types'] = self.find_matches(self._remove_nonalphanum(self._remove_punctuation(st)), self._venue_types)

		return self

	def get_sports_abbreviations(self, st):

		self.labels['sports_abbrev'] = self.find_matches(self._remove_nonalphanum(self._remove_punctuation(st)), self.sport_abbr)

		return self

	def get_tournament_type(self, st):

		self.labels['tournament_types'] = self.find_matches(self._remove_nonalphanum(self._remove_punctuation(st)), self.tournament_types)

		return self

	def get_comedians(self, st):

		self.labels['comedians'] = self.find_matches(self._remove_nonalpha(self._remove_punctuation(st)), self.comedians)

		return self

	def get_opera_singers(self, st):

		self.labels['opera_singers'] = self.find_matches(self._remove_nonalpha(self._remove_punctuation(st)), self.opera_singers)

		return self

	# def get_musicals(self, st):

	# 	self.labels['musicals'] = self.find_matches(self._normalize_title(st), self._musicals)

	# 	return self

	def get_musicals(self, st):
		"""
		find any suburbs in the string
		"""

		found = set()

		for i, w in enumerate(st.lower().split()):

			_l1 = w[0]

			if _l1 in self._musicals:
				
				_cands = {s for s in self._musicals[_l1] if len(s.split()) <= len(st[i:])}

				if _cands:
					found.update(self.find_matches(st, _cands))

		self.labels['musicals'] = found if found else None

	# def get_movies(self, st):

	# 	self.labels['movies'] = self.find_matches(self._normalize_title(st), self.movies)

	# 	return self

	def get_movies(self, st):
		"""
		find any suburbs in the string
		"""

		found = set()

		for i, w in enumerate(st.lower().split()):

			_l1 = w[0]

			if _l1 in self.movies:
				
				_cands = {s for s in self.movies[_l1] if len(s.split()) <= len(st[i:])}

				if _cands:
					found.update(self.find_matches(st, _cands))

		self.labels['movies'] = found if found else None

	def get_artists(self, st):
		"""
		find any artists in the string
		"""

		found = set()

		for i, w in enumerate(st.lower().split()):

			_l1 = w[0]

			if _l1 in self.artists:
				
				_cands = {s for s in self.artists[_l1] if len(s.split()) <= len(st[i:])}

				if _cands:
					found.update(self.find_matches(st, _cands))

		self.labels['artists'] = found if found else None

if __name__ == '__main__':

	e = Event('123ddf', 'some event', 'sports')

	e.description = """AustRalia 938artarmon vs New. Zealand @##and also laos.. steve davislim** pub league-- 
	darlinghurst nz vs kuwait concert hall polo cricket  2012-03-21 21:10:00 fyrom aziz ansari00 show oval
	 3th final jam    bugsy malone taxi driver   0 young frankenstein  depeche Mode and also (*) butterfly eFFect
	"""

	eff = EventFeatureFactory()
	# .build_dict(what='teams').build_dict(what='sport-abbreviations')

	print(eff._deabbreviate('this new south wales team played in bris yesterday'))

	# print(eff.artists)

	# eff.find_matches(e.description, eff._countries)
	# print(f'this starts {eff.get_event_time(e.description)}')

	# print(eff.get_suburb(e.description))
	# print(f'was: {e.description}')
	# print(f'now: {eff._remove_punctuation(e.description)}')

	# eff.get_countries(e.description)
	# eff.get_suburb(e.description)
	# eff.get_venue_types(e.description)
	# eff.get_tournament_type(e.description)
	# eff.get_comedians(e.description)
	# eff.get_opera_singers(e.description)
	# eff.get_musicals(e.description)
	# eff.get_movies(e.description)
	# eff.get_artists(e.description)

	print(eff.labels)

