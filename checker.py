import sys
sys.path.append("/home/toby/grader/preprocess/")

from parse_morbidmap import parse_morbidmap
import convert

import mysql.connector
from useful import const

def get_semmed_genes(cui):
	cnx = mysql.connector.connect(database = "semmeddb",
		**const.DB_INFO)

	ans = [] # all genes associated with or causes this disease cui
	if cnx.is_connected():
		print "Querying", cui

		cur = cnx.cursor()
		query = ("SELECT DISTINCT s_cui "
			"FROM PREDICATION_AGGREGATE "
			"WHERE o_cui = %s AND s_cui NOT REGEXP '^C.{7}$' "
			"AND predicate IN ('ASSOCIATED_WITH', 'CAUSES');")

		cur.execute(query, (cui, ))
		ans = [row[0] for row in cur]

		cur.close()
	cnx.close()
	return ans

def main():
	genes = parse_morbidmap()
#	dmims that we will try to find in semmeddb
	testset = [dmim for dmim, gmims in genes.items() if len(gmims) > 10]

#	convert these dmims to CUIs
	dmim_cuis = map(convert.dmim_to_cui, testset)

	for dmim, cuis in zip(testset, dmim_cuis):
		print "dmim", dmim
		print "cuis", cuis

		semmed_data = [get_semmed_genes(cui) for cui in cuis]

#		parse the rows into unique gene_ids
		uniq_gene_ids = set()
		for data_set in semmed_data:
			for row in data_set:
				vals = row.split('|')
				gene_ids = [val for val in vals if val[0] != 'C']
				uniq_gene_ids |= set(gene_ids)

		with open(str(dmim) + ".txt", "w") as out:
			for gene_id in uniq_gene_ids:
				out.write(gene_id + "\n")

if __name__ == "__main__":
	main()
