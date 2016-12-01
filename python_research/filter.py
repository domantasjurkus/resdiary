import csv
from collections import Counter
from dateutil.parser import parse
import numpy as np
import matplotlib.pyplot as plt
from sklearn.covariance import EllipticEnvelope

# Detects outliers using based on frequency, which is defined as
# number_of_reservations / (difference_between_first_and_last_visit_in_seconds+1)
with open('../data/Repeat diners in Glasgow, Oct 2016 - Sheet1.csv', 'rb') as file:
	reader = csv.reader(file)
	next(reader, None) # skip the headers
        
        # read the data while keeping track of important information
        first_visit = {}
        last_visit = {}
        count = Counter()
	for row in reader:
                diner_id, _, restaurant_id, visit_time, covers, _ = row;
                visit_time = parse(visit_time)
                first_visit[diner_id] = (min(first_visit[diner_id], visit_time)
                                         if diner_id in first_visit else visit_time)
                last_visit[diner_id] = (max(last_visit[diner_id], visit_time)
                                        if diner_id in last_visit else visit_time)
                count[diner_id] += 1

        # calculate actual frequencies
        frequencies = []
        for diner_id in count:
                # +1 just to avoid division by 0; each number is enclosed by a
                # list because our outlier detection algorithm expects a list
                # of arrays
                frequencies.append([count[diner_id] /
                                    ((last_visit[diner_id] -
                                      first_visit[diner_id]).seconds + 1.0)])
        frequencies = np.array(frequencies)
        
        # train the model, find the outliers and plot the comparison box plot
        model = EllipticEnvelope()
        model.fit(frequencies)
        prediction = model.predict(frequencies)
        filtered = [frequencies[i][0] for i in range(len(frequencies))
                    if prediction[i] == 1]
        plt.boxplot([frequencies, filtered],
                    labels=['original ({:d})'.format(len(frequencies)),
                            'filtered ({:d})'.format(len(filtered))])
        plt.title('Outlier Detection ({:.3f}%)'.format(
                100.0 * (len(frequencies) - len(filtered)) / len(frequencies)))
        plt.ylabel('frequency')
        plt.show()
