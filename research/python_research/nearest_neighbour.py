import csv
import math

def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees).
    """

    # Convert to radians 
    lon1, lat1, lon2, lat2 = map(math.radians, [lon1, lat1, lon2, lat2])
    
    # Haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a)) 
    km = 6367 * c
    return km

def main():

	restaurantDict = {}

	with open( 'Restaurant.csv', 'rb' ) as f:
   		reader = csv.reader(f)

   		# Skip the headers
   		next( reader, None ) 
   		
   		for row in reader:
   			# { id : (lon, lat) }
   			restaurantDict[ row[0] ] = ( float(row[5]), float(row[4]) )

   	keyList = restaurantDict.keys()   	
   	k0 = restaurantDict[ keyList[0] ]


   	minKey = restaurantDict[ keyList[1] ]
   	
   	minDistance = haversine( k0[0], k0[1], minKey[0], minKey[1] )

   	keyList.remove( keyList[0] )

   	for key in keyList:
   		if haversine( k0[0], k0[1], restaurantDict[key][0], restaurantDict[key][1] ) < minDistance:
   			minDistance = haversine( k0[0], k0[1], restaurantDict[key][0], restaurantDict[key][1] )
   			minKey = key

   	print minDistance
   	print minKey
   	print restaurantDict[minKey]
   	print k0


if __name__ == "__main__":
	main()
