# last updated 2015-01-21 toby

# converts the 4 letter semtype code
# into the full name (human readable)

import sys
import util

def get_semtypes():
	info = dict()
	for line in util.read_file("/home/toby/semmed/info/", "SRDEF.txt"):
		vals = line.split('|')
		assert len(vals) == 11, "Too many columns in SRDEF line {0}!".format(vals[8])

		if vals[0] == "STY":
			info[vals[8]] = (vals[2], vals[4])

	return info

info = get_semtypes()

def get_semtype(key):
	return info[key] if key in info else ()

if __name__ == "__main__":
	print get_semtype(sys.argv[1])
