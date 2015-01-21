# last updated 2015-01-21 toby

def read_file(floc, fname):
	with open(floc + fname, "r") as file:
		for line in file:
			line = line.rstrip('\n')
			yield line
