import re
from itertools import chain
from collections import OrderedDict


class ArtistNameNormaliser:

	def __init__(self):
		pass


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
		
		numbers_20to90 = 'twenty thirty fourty fifty sixty seventy eighty ninety'.split()
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
	
	def normalise_name(self, name):
		"""
		return a normalized artist name
		"""
		
		name = name.lower()
		
		# \s matches any whitespace character; 
		# this is equivalent to the class [ \t\n\r\f\v]
		wsp = re.compile(r'\s{2,}')
		
		emoji = ':) :('.split()
		
		# if its only ? it may be an ArtistCollector
		if name.strip() in {'?','...'}:
			return '@artist'
		
		# label emojis
		name = re.sub(r'\s*:[\(\)]\s*',' @artist ', name)
		
		# replace separators and quotes with white spaces
		name = re.sub(r'[_\-:;/.,\"\`\']', ' ', name) 
		
		# fix ! - remove if at the end of a word, otherwise replace with i
		name = re.sub(r'\!+(?=[^\b\w])','', name)
		name = re.sub(r'\!+$','', name)
		
		name = name.replace('!','i')
		
		# remove all brackets and hyphens
		name = re.sub(r'[\[\]\{\}\(\)]','', name) 
		
		# remove the and a
		name = re.sub(r'^(the|a)\s+','', name)
	
		# remove multiple white spaces
		name = wsp.sub(' ',name)
		
		# spelled numbers to numbers
		name = self.spelledout_numbers_to_numbers(name)
		
		# replace and with &
		name = name.replace(' and ',' & ')
		 
		# remove multiple white spaces
		name = wsp.sub(' ',name)
		
		# finally, strip
		name = name.strip()
		
		return name
