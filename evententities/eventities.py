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

		self.DATA_DIR = os.path.join(os.path.curdir, 'data')
		self._teams = defaultdict()
		self.PREFIXES = {'teams': 'tm_'}

		self._ev_id = None
		self._description = None
		self._labels = defaultdict()

		"""
		[
  			{
  			  "name": "afghanistan",
  			  "other_names": null,
  			  "abbr": "afg"
  			},
		"""
		countries = json.load(open('/Users/ik/Data/country-abbreviations/countries.json','r'))

		self._countries =  {_ for _ in chain([' '.join([w for w in c['name'].lower().split() if w not in {'and', '&', 'the'}]) 
																										for c in countries], 
														[v for c in countries if c['other_names'] for v in c['other_names']],
														[c['abbr'] for c in countries])}
		# print(self._countries)

		self._venue_types = {'club', 'centre', 'center', 'studio', 'stadium', 'grounds', 'park', 'pub', 
							 'school', 'bar', 'oval', 'showgrounds', 'court', 'arena', 'zoo', 'gallery', 'museum', 
							 'garden', 'gardens', 'theatre', 'theater', 'lodge', 'field', 'complex', 'cafe', 'church',
								'cathedral', 'house'}

		"""
		"a": [
    			{
    			  "name": "aarons pass",
    			  "state": "nsw",
    			  "postcode": 2850
    			}
		"""
		self._suburbs = json.load(open('/Users/ik/Data/suburbs-and-postcodes/aus_suburbs_auspost_APR2017.json','r'))

		self.state_abbr = {'nsw': 'new south wales', 
							'vic': 'victoria',
							'tas': 'tasmania',
							'sa': 'south australia',
							'wa': 'western australia',
							'act': 'australian capital territory',
							'nt': 'northern territory',
							'qld': 'queensland'}

		self.city_variants = {'sydney': ['syd'], 
								'melbourne': ['mel', 'melb'],
								'brisbane': ['bris', 'brisb'],
								'gold coast': ['gc'],
								'adelaide': ['adel'],
								'canberra': ['canb'],
								'mount': ['mt']}

		self._promoters = {line.lower().strip() for line in open('/Users/ik/Data/promoters/promoters.txt','r').readlines()}
		# print(self._promoters)

		# sport names are keys
		_sports = json.load(open('/Users/ik/Data/sports/sports-identifiers/sports-identifiers.json','r'))

		self.sports = set(_sports)
		# sports abbreviations
		self.sport_abbr = {a for s in _sports if 'abbreviations' in _sports[s] for a in _sports[s]['abbreviations'] }
		# tournaments
		self.tournaments = {n for s in _sports for c in _sports[s]['competitions'] for n in _sports[s]['competitions'][c]}

		self.tournament_types = set('cup championship test trophy tour tournament series league games premiership race'.split())

		self.sport_sponsors = {sp for s in _sports if 'sponsors' in _sports[s] for sp in _sports[s]['sponsors']}
		
		self.sport_teams = {p for s in _sports if ('nrl' in s) or ('afl' in s) for p in _sports[s]['key_participants']}

		self.comedians = {line.lower().strip() for line in open('/Users/ik/Data/comedians/comedians_1515.txt','r').readlines()}
		
		self.opera_singers = {line.lower().strip() for line in open('/Users/ik/Data/opera-singers/opera_singers_1873.txt','r').readlines()}

		self.purchase_types = 'fee merchandise parking upsell'.split()

		self.musicals = set(pd.read_csv('/Users/ik/Data/musicals/musicals_.csv')['name'])

		self.movies = {m.split(',')[0].split('(')[0].lower().strip() for m in set(pd.read_csv('/Users/ik/Data/movies/movies.csv')['title'])}

		self.major_genres = 'rock pop jazz soul funk folk blues'.split()

		self.companies = set(pd.read_csv('/Users/ik/Data/businesses/ASXListedCompanies.csv', skiprows=3).iloc[:, 0].str.lower())

		self.event_types = """
							fanfare fireworks grandstand occurrence pageantry panoply representation shine showboat
  							showing sight splash view anniversary commemoration competition fair feast gala
  							holiday carnival entertainment festivities fete fiesta jubilee merrymaking trear
  							bazar celebration display exhibit festival market pageant
  							show centennial occasion spectacle act concert portrayal production burlesque
  							ceremony gig matinee recital rehearsal revue rigmarole rite special
  							spectacle stunt stage circus celebration barbecue amusement entertainment 
  							prom soiree function ball banquet festivity feast reception fun get-together 
  							cocktails luncheon occasion pageant parade appearance spectacle presentation
  							display exposition occurrence showing panoply manifestation fanfare pageantry
  							expo showboat
							""".split()

		self.sport_variants = {'united': 'utd', 'city': 'cty', 'fc': 'football club'}

		self.artists = json.load(open('/Users/ik/Data/music/artist_names/artists_.json','r'))

		# print(self.sport_teams)
		# print(f'total teams: {len(self.sport_teams)}')

		self.re_punct = re.compile('[' + '|'.join([re.escape(p) for p in string.punctuation]) + ']')

		self.labels = defaultdict()

	def build_dict(self, what):

		if what == 'teams':

			dir_start = '/'.join([self.DATA_DIR, what])

			for p in os.walk(top=dir_start):

				if p[2]:

					pth = p[0]
					file_name = p[2].pop()	
	
					if file_name.startswith(self.PREFIXES[what]):
						
						_, _, _, sport, country, league = pth.split('/')
		
						for team in open(os.path.join(pth, file_name),'r').readlines():
		
							if len(team.strip()) > 1:
								self._teams[team.strip()] = {'sport': sport, 'country': country, 'league': league}

		return self

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

		self._musicals = self._list_to_alph_dict({_c for _c in {self._normalize_title(c) for c in self.musicals} if _c})

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

	eff = EventFeatureFactory().build_dict(what='teams')

	# print(eff._teams)

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

