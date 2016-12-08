import csv
from collections import defaultdict
from sets import Set

import script

# filter out half of the data from BookingFull.csv to Booking.csv and store the
# remaining restaurant IDs
answers = defaultdict(Set)
with open('data/BookingFull.csv', 'rb') as full_file, open('data/Booking.csv',
                                                           'wb') as partial_file:
    reader = csv.reader(full_file)
    writer = csv.writer(partial_file)

    writer.writerow(next(reader)) # move the headers

    # read all the data and separate by diner id
    bookings = defaultdict(list)
    for row in reader:
        bookings[row[0]].append(row)

    for personal_bookings in bookings.values():
        num_bookings = len(personal_bookings)
        if num_bookings < 2:
            # not enough bookings to use this user for evaluation
            continue
        first_test_index = num_bookings / 2
        personal_bookings.sort(key=lambda r: r[3]) # sort according to visit time
        # write half of the data to the other file
        for booking in personal_bookings[:first_test_index]:
            writer.writerow(booking)
        # store restaurant IDs from the remaining rows
        for booking in personal_bookings[first_test_index + 1:]:
            answers[booking[0]].add(booking[2])

script.generate_recommendations()
right = 0
total = 0
with open('data/Recommendations.csv', 'rb') as recommendations_file:
    reader = csv.reader(recommendations_file)
    for row in reader:
        total += 1
        if row[1] in answers[row[0]]:
            right += 1
print '{:.1f}% of the recommendations were good.'.format(100.0 * right / total)
