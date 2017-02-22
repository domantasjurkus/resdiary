var fs = require('fs');
var parse = require('csv-parse');
var parse = require('csv-parse/lib/sync');
var pg = require('pg');

var config = {
    user: 'team_i', //env var: PGUSER
    database: 'dev', //env var: PGDATABASE
    password: '', //env var: PGPASSWORD
    host: 'team-i-dev.cuslwdgfxmvv.eu-west-1.rds.amazonaws.com', // Server hosting the postgres database
    port: 5432, //env var: PGPORT
    max: 10, // max number of clients in the pool
    idleTimeoutMillis: 30000, // how long a client is allowed to remain idle before being closed
};

/* A module for reading and writing data from the CSV files. Takes a Diner ID and sends the
   recommended restaurants to the res object. */
module.exports = {
    getRandomUserSync: function () {
        var recommendationsFile = fs.readFileSync('./src/data/Recommendations.csv', 'utf8');
        var recs = parse(recommendationsFile, {});
        return recs[Math.floor(Math.random() * recs.length)][0];

    },
    getRecommendationsSync: function (userId) {
        var recommendationsFile = fs.readFileSync('./src/data/Recommendations.csv', 'utf8');
        var restaurantsFile = fs.readFileSync('./src/data/Restaurant.csv', 'utf8');
        var recommendations = parse(recommendationsFile, {});
        var restaurants = parse(restaurantsFile, { columns: true });

        // filter out the relevant recommendations
        return recommendations.filter(function (recommendation) {
            return recommendation[0] == userId;
        }).map(function (recommendation) {
            // find the information about each restaurant
            return restaurants.find(function (restaurant) {
                return restaurant['RestaurantId'] ==
                    recommendation[1];
            });
        })
    },
    getRecentlyVisitedSync: function (userId) {
        var bookingsFile = fs.readFileSync('./src/data/Booking.csv', 'utf8');
        var restaurantsFile = fs.readFileSync('./src/data/Restaurant.csv', 'utf8');
        var bookings = parse(bookingsFile, {});
        var restaurants = parse(restaurantsFile, { columns: true });

        // filter out the relevant bookings
        return bookings.filter(function (booking) {
            return booking[0] == userId;
        }).map(function (booking) {
            console.log("Before Filter - Name " + booking[1] + " Visit Time: " + booking[3]);

            var rest = restaurants.find(function (restaurant) {
                return restaurant['RestaurantId'] == booking[2];
            });

            console.log("After Filter - Name " + booking[1] + " Visit Time: " + booking[3]);
            //rest.Booking = booking;
            bk = booking[3]; //Visit Date 
            sc = (booking[5]=="NULL")? "Didn't Review" : booking[5]; //Review Score (if available)

            console.log("Rest booking value time " + booking[3])
            console.log(rest);
            return {Restaurant:rest,Score:sc,BookingTime:bk};
        })

    },
    getRecommendations: function (userId, res) {

        // read two files
        fs.readFile('./src/data/Recommendations.csv', 'utf8', function (err1, recommendationsFile) {
            fs.readFile('./src/data/Restaurant.csv', 'utf8', function (err2, restaurantsFile) {
                // parse the files
                parse(recommendationsFile, {}, function (err3, recommendations) {
                    parse(restaurantsFile, { columns: true }, function (err4, restaurants) {
                        // filter out the relevant recommendations
                        return res.send(recommendations.filter(function (recommendation) {
                            return recommendation[0] == userId;
                        }).map(function (recommendation) {
                            // find the information about each restaurant
                            return restaurants.find(function (restaurant) {
                                return restaurant['RestaurantId'] ==
                                    recommendation[1];
                            });
                        }));
                    });
                });
            });
        });
    },
    getRecentlyVisited: function (userId) {
        // read two files
        fs.readFile('./src/data/Booking.csv', 'utf8', function (err1, bookingsFile) {
            fs.readFile('./src/data/Restaurant.csv', 'utf8', function (err2, restaurantsFile) {
                // parse the files
                parse(bookingsFile, {}, function (err3, bookings) {
                    parse(restaurantsFile, { columns: true }, function (err4, restaurants) {
                        // filter out the relevant bookings
                        return res.send(bookings.filter(function (booking) {
                            return booking[0] == userId;
                        }).map(function (booking) {
                            // find the information about each restaurant
                            return restaurants.find(function (restaurant) {
                                return restaurant['Restaurant Id'] ==
                                    booking[2];
                            });
                        }));
                    });
                });
            });
        });
    },
    readCSV: function (id, res) {
        // read two files
        fs.readFile('./src/data/Recommendations.csv', 'utf8', function (err1, recommendationsFile) {
            fs.readFile('./src/data/Restaurant.csv', 'utf8', function (err2, restaurantsFile) {
                // parse the files
                parse(recommendationsFile, {}, function (err3, recommendations) {
                    parse(restaurantsFile, { columns: true }, function (err4, restaurants) {
                        // filter out the relevant recommendations
                        res.send(recommendations.filter(function (recommendation) {
                            return recommendation[0] == id;
                        }).map(function (recommendation) {
                            // find the information about each restaurant
                            return restaurants.find(function (restaurant) {
                                return restaurant['RestaurantId'] ==
                                    recommendation[1];
                            });
                        }));
                    });
                });
            });
        });
    },
    readDB: function (id, res) {
        var client = new pg.Client(config);
        // connect to our database
        client.connect(function (err) {
            if (err) throw err;

            // TODO: this database should not have a different format from our CSV data
            // execute a query on our database
            // If you want a parameter then use 
            // client.query('SELECT * from resdiary.recommendations where user_id = $1', ['344561'], function (err, result) {
            client.query(`SELECT rec.restaurant_id,rest.restaurant_name,rest.town,rest.price_point 
                    FROM resdiary.recommendations rec 
                    LEFT JOIN resdiary.restaurants rest 
                    ON rec.restaurant_id = rest.restaurant_id WHERE user_id = $1`, [id], function (err, result) {
                    if (err) throw err;

                    // just print the result to the console
                    console.log(result.rows);

                    // disconnect the client
                    client.end(function (err) {
                        if (err) throw err;
                    });
                    res.send(result.rows);
                });
        });
    }
};
