# evententities
Extract entities from live event description

## Live event descriptions
Although this package is intended to help extracting features from live event descriptions, it’s simply looking for certain types of entities in a string. These can then be used to produce dictionary-based features for machine learning models. 

## Entities
Given the live entertainment context, we are currently interested in extracting the following:

* artists
	* we search through over 180,000 artists features on Spotify
* Australian suburbs
	* we use a comprehensive list of suburbs on par with that by Australia Post
* promoters
	* both the Australian and International promoters; we make sure to cover the world top100 and a large number of small time ones operating in Australia
* countries
	* all world countries along with their alternative names and the official 3-letter abbreviations
* sport names
	* we are primarily focused on the sports visible in Australia 
* musicals
	* as featured on Wikipedia: Broadway, West End and other musicals
* movies
	* movie names as in the [MovieLens](https://grouplens.org/datasets/movielens/) datasets
* opera singers
	* course: Wikipedia
* sport teams
	* team names across major Australian team sports and, of course, domestic and international soccer
* sport names
* sport sponsors (Australian sports only)
* major music genres
* venue types
* tournament types
* purchase types
* comedians (stand-up)
	* source: [Wikipedia](https://en.wikipedia.org/wiki/List_of_stand-up_comedians) list
* companies (optional)
	* source: [ASX official list](https://www.asx.com.au/asx/research/listedCompanies.do)
