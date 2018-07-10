import re
from itertools import chain
from collections import OrderedDict

class BaseNormaliser:
	"""
	this class has methods useful no matter what you normalize
	"""
	def __init__(self):
		pass

	def normalize(self, s):

		# lower case, replace separators and quotes with white spaces, remove all brackets
		# then all spelled numbers to numbers, then and -> &, make all white spaces single and strip

		s = self.deabbreviate(s.lower())

		return re.sub(r'\s{2,}', ' ', self.spelledout_numbers_to_numbers(re.sub(r'[\[\]\{\}\(\)]','', 
						re.sub(r'[_\-:;/.,\"\`\']', ' ', s.lower()))).replace(' and ',' & ')).strip()


	def spelledout_numbers_to_numbers(self, s):
		"""
		returns string s where all spelled out numbers between 0 and 99 are
		converted to numbers
		"""
		numbers_1to9 = 'one two three four five six seven eight nine'.split() 
		mappings_1to9 = {t[0]: str(t[1]) 
							   for t in zip(numbers_1to9, range(1,10))}
		
		mappings_10to19 = {t[0]: str(t[1]) 
							   for t in zip("""ten eleven twelve thirteen fourteen fifteen 
											  sixteen seventeen eighteen nineteen""".split(), range(10,20))}
		
		numbers_20to90 = 'twenty thirty forty fifty sixty seventy eighty ninety'.split()
		mappings_20to90 = {t[0]: str(t[1]) 
							   for t in zip(numbers_20to90, range(20,100,10))}
		
		# produce numbers like twenty one, fifty seven, etc.
		numbers_21to99 = [' '.join([s,p]) for s in numbers_20to90 for p in numbers_1to9]
		
		"""
		create an ordered dictionary mapping spelled numbers to numbers in
		digits; note that the order is important because we want to search
		for spelled numbers starting from the compound ones like twenty two,
		then try to find the rest
		"""
		
		od = OrderedDict({t[0]:t[1] 
							for t in zip(numbers_21to99, 
										 # create a list [21,22,..,29,31,..,39,41,..,99]
										 [_ for _ in chain.from_iterable([[str(_) for _ in range(int(d)*10 + 1,int(d+1)*10)] 
											   for d in range(2,10)])])})
		od.update(mappings_20to90)
		od.update(mappings_10to19)
		od.update(mappings_1to9)
		
		for w_ in od:
			  s = re.sub(r'\b' + w_ + r'\b', od[w_], s)
		
		return s

	def deabbreviate(self, st):
		"""
		unfold abbreviations in string st
		"""
		abbrs = {'gws': 'greater western sydney giants',
				 'gwsg': 'greater western sydney giants',
				 'afl': 'australian football league',
				 'nrc': 'national rugby championship',
				 'nrl': 'national rugby league',
				 'syd': 'sydney',
				 'mel': 'melbourne',
				 'melb': 'melbourne',
				 'bris': 'brisbane',
				 'brisb': 'brisbane',
				 'gc': 'gold coast',
				 'adel': 'adelaide',
				 'canb': 'canberra',
				 'mt': 'mount',
				 'utd': 'united',
				 'cty': 'city',
				 'football club': 'fc',
				 'snr': 'senior',
				 'jr': 'junion',
				 'nsw': 'new south wales' ,
				 'vic': 'victoria',
				 'tas' : 'tasmania',
				 'sa': 'south australia',
				 'wa': 'western australia',
				 'act': 'australian capital territory',
				 'nt': 'northern territory',
				 'qld': 'queensland',
				 'champs': 'championships', 
				 'champ': 'championship', 
				 'soc': 'society',
				 'ent': 'entertainment',
				 'intl': 'international', 
				 'int': 'international', 
				 'aust': 'australian'}

		# first replace full state names by abbreviations;
		for ab in abbrs:
			st = re.sub(r'\b' + ab + r'\b', abbrs[ab], st)

		return st


class ArtistNameNormaliser(BaseNormaliser):

	def __init__(self):
		pass
	
	def normalize(self, name):
		"""
		return a normalized artist name
		"""

		# label emojis, specifically :) and :( as @artist, then apply 
		# base normalization

		name = super().normalize(re.sub(r'\s*:[\(\)]\s*',' @artist ', name))
		
		# if now name is ? it may be an artist, so label as @artist
		if name.strip() in {'?','...'}:
			return '@artist'
		
		# fix ! - remove if at the end of a word, otherwise replace with i
		name = re.sub(r'\!+$','', re.sub(r'\!+(?=[^\b\w])','', name)).replace('!','i')
		
		# remove the and a
		name = re.sub(r'^(the|a)\s+','', name)
		 
		# remove multiple white spaces
		name = re.sub(r'\s{2,}', ' ', name).strip()
		
		return name

if __name__ == '__main__':

	an = ArtistNameNormaliser()

	print(an.deabbreviate('BRIS concert and afl show'))

	print(an.normalize('the    rolling_stonES! :) ? ...      twenty two --GWS play sydNEY fc in bris qld'))
