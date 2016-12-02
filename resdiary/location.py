import csv

with open( 'dataset1.csv', 'rb' ) as fileA:
	reader = csv.reader(fileA)
	next( reader, None )
	locations = {}	
	for row in reader:
		locations[ row[1] ] = [ row[4], row[5] ]



