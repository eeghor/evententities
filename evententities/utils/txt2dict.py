import sys
from collections import defaultdict
import json

text_file = sys.argv[1]

dik = defaultdict(list)

lines = {line.lower().strip() for line in open(text_file).readlines() if line.strip()}

print(f'found {len(lines)} in {text_file}')

if lines:
	for l in lines:
		dik[l[0]].append(l)

json.dump(dik, open(''.join(text_file.split('.')[:-1]) + '.json', 'w'))

