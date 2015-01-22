import mysql.connector
from useful import const
import time
from collections import defaultdict

from useful import util

def load_morbidmap():
	dmim_cuis = dict()
	gene_ids = dict()

	file_loc = "/home/toby/semmed/data/converted_morbidmap.txt"
	with open(file_loc, "r") as file:
		cur_dmim = "";
		for i, line in enumerate(file):
			line = line.rstrip('\n')
			line = line.lstrip('\t')

			# skip the dmims with no cuis...
			# since there's no way to look it up anyways
			if i % 2 == 0: #dmim
				vals = line.split('|')
				if len(vals) == 1: # no cuis
					cur_dmim = ""
					continue
				else:
					cur_dmim = vals[0]
					dmim_cuis[cur_dmim] = vals[1:]
			else: #gene ids
				if not cur_dmim: # previous line had no cuis
					continue
				else:
					gene_ids[cur_dmim] = line.split('|')

	return (dmim_cuis, gene_ids)

def get_semmed_tuples():
	cnx = mysql.connector.connect(database = "semmeddb",
		**const.DB_INFO)

	if cnx.is_connected():
		cur = cnx.cursor()

		query = ("SELECT DISTINCT PID, SID, PMID, s_cui, s_name, predicate, o_cui, o_name "
			"FROM PREDICATION_AGGREGATE "
			"WHERE s_type IN ('gngm', 'aapp') "
			"AND o_type IN ('dsyn', 'neop', 'cgab', 'mobd');")

		cur.execute(query)
		with open("semmed_fps.txt", "w") as out:
			for row in cur:
				out.write("{0}#{1}#{2}#{3}#{4}#{5}#{6}#{7}\n".format(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7]))

		cur.close()

	cnx.close()

def get_cached_info():
	semmed_tuples = set()
	name = dict()
	identifiers = defaultdict(list)

	for line in util.read_file("/home/toby/semmed/", "semmed_fps.txt"):
		vals = line.split('#')

#		o_cui are all C1234567 (no gene ids)
		sub_ids = vals[3].split('|')
		sub_names = vals[4].split('|')

		semmed_tuples |= set([(val, vals[6]) for val in sub_ids])

		for val in sub_ids:
			identifiers[(val, vals[6])].append((vals[5], vals[0], vals[1], vals[2]))

		name[vals[6]] = vals[7] # object
		for sub, s_name in zip(sub_ids, sub_names):
			name[sub] = s_name


	return (semmed_tuples, name, identifiers)

def main():

	dmim_cuis, gene_ids = load_morbidmap()

	all_omim_cuis = set()
	for dmim, cuis in dmim_cuis.items():
		all_omim_cuis |= set(cuis)

	omim_tuples = set() # 8456 items
	for dmim, cuis in dmim_cuis.items():
		omim_tuples |= set([(gid, cui) for cui in cuis for gid in gene_ids[dmim]])

	semmed_tuples, name, semmed_info = get_cached_info()

	filtered_semmed_tuples = set([pair for pair in semmed_tuples if pair[1] in all_omim_cuis])

	sem_only = filtered_semmed_tuples - omim_tuples

	gid_sem_only = set([pair for pair in sem_only if pair[0][0] != 'C'])

	print len(gid_sem_only)


	temp = [(pair, len(semmed_info[pair])) for pair in gid_sem_only]

	temp = sorted(temp, key = lambda x: x[1], reverse = True)

	with open("sem_only_set.txt", "w") as out:
		for outer_pair in temp:
			sub = outer_pair[0][0]
			obj = outer_pair[0][1]

			out.write("{0}|{1}\n".format(sub, obj))
			out.write("{0}|{1}\n".format(name[sub], name[obj]))
			out.write(str(len(semmed_info[(sub, obj)])) + "\n")
			for pred in semmed_info[(sub, obj)]:
				out.write("\t{0}\n".format(pred))

			out.write("\n")


if __name__ == "__main__":
	main()
	print "done"
