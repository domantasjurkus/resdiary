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

// Match the restaurants id to its cuisine type then perfrom a cuisine type lookup
// Could be extended to return all cuisine types as it currently only returns 1
function getResCuisine (resId) {
        var restaurantsFile = fs.readFileSync('./data/Restaurant.csv', 'utf8');
        var restaurants = parse(restaurantsFile, { columns: true });

        var cuisineFile = fs.readFileSync('./data/RestaurantCuisineTypes.csv', 'utf8');
        var cuisines = parse(cuisineFile, { columns: true });

        var cuisineTypeFile = fs.readFileSync('./data/CuisineTypes.csv', 'utf8');
        var cuisineTypes = parse(cuisineTypeFile, { columns: true });

        var cuisineID = cuisines.find(function (tempResID){
                return tempResID['RestaurantId'] == resId ;
            });

        var cuisineT = "Data Not Provided";

        if (cuisineID !== undefined){
                cuisineT = cuisineTypes.find(function (tempResID){
                return tempResID[' Id'] == cuisineID['CuisineTypeId'];
                });
                
                cuisineT = cuisineT['Name'];                
        }
        return cuisineT;
}

/* A module for reading and writing data from the CSV files. Takes a Diner ID and sends the
   recommended restaurants to the res object. */
module.exports = {
    /* Added by Dom - single route for all recommendation CSV files */
    getRecommendations: function (userId, recType) {
        var recommendationsFile = fs.readFileSync('./data/recs_'+recType+'.csv', 'utf8');
        var restaurantsFile = fs.readFileSync('./data/Restaurant.csv', 'utf8');
        var recommendations = parse(recommendationsFile, {});
        var restaurants = parse(restaurantsFile, { columns: true });

        // filter out the relevant recommendations
        return recommendations.filter(function (recommendation) {
        if (recommendation[0] == userId) {
            return recommendation[0] == userId;
        }
        }).map(function (recommendation) {
            // find the information about each restaurant
            var rest = restaurants.find(function (restaurant) {
                return restaurant['RestaurantId'] ==
                    recommendation[1];
            });
            var cuisineT = getResCuisine(recommendation[1]);

            return {
                Restaurant: rest,
                RecScore: recommendation[2],
                CuisineType: cuisineT,
                PricePoint: rest['PricePoint']
            };
        })
    },

    /*getRandomUserSync: function () {
        //var recommendationsFile = fs.readFileSync('./data/Recommendations_ALSExplicit_Demo.csv', 'utf8');
        var recommendationsFile = fs.readFileSync('./data/recs_demo.csv', 'utf8');
        var recs = parse(recommendationsFile, {});
        return recs[Math.floor(Math.random() * recs.length)][0];

    },*/
    getRandomUsersSync: function(num) {
        // Get users from the System recommender
        // These will surely have plenty of recommendations to show
        var bookingsFile = fs.readFileSync('./data/recs_system.csv', 'utf8');
        var bookings = parse(bookingsFile, {}); 
        var users = [];
        for (i=0; i<num; i++) {
            var id = bookings[Math.floor(Math.random() * bookings.length)][0];
            users.push(id)
            console.log("User id", id);
        }
        return users;
    },
    /*getRecommendationsContentSync: function (userId) {
        //var recommendationsFile = fs.readFileSync('./data/Recommendations_Content_Based.csv', 'utf8');
        var recommendationsFile = fs.readFileSync('./data/recs_cuisinetype.csv', 'utf8');
        var restaurantsFile = fs.readFileSync('./data/Restaurant.csv', 'utf8');
        var recommendations = parse(recommendationsFile, {});
        var restaurants = parse(restaurantsFile, { columns: true });

        // filter out the relevant recommendations
        return recommendations.filter(function (recommendation) {
            return recommendation[0] == userId;
        }).map(function (recommendation) {
            // find the information about each restaurant
            var rest = restaurants.find(function (restaurant) {
                return restaurant['RestaurantId'] ==
                    recommendation[1];
            });
            var cuisineT = getResCuisine(recommendation[1]);

            return {Restaurant:rest,RecScore:recommendation[2],CuisineType:cuisineT};
        })
    },*/
    /*getRecommendationsAlsSync: function (userId) {
        //var recommendationsFile = fs.readFileSync('./data/Recommendations_ALSExplicit_Demo.csv', 'utf8');
        var recommendationsFile = fs.readFileSync('./data/recs_explicitals.csv', 'utf8');

        var restaurantsFile = fs.readFileSync('./data/Restaurant.csv', 'utf8');
        var recommendations = parse(recommendationsFile, {});
        var restaurants = parse(restaurantsFile, { columns: true });

        // filter out the relevant recommendations
        return recommendations.filter(function (recommendation) {
            return recommendation[0] == userId;
        }).map(function (recommendation) {
            // find the information about each restaurant
            var rest = restaurants.find(function (restaurant) {
                return restaurant['RestaurantId'] ==
                    recommendation[1];
            });
            var cuisineT = getResCuisine(recommendation[1]);

            return {Restaurant:rest,RecScore:recommendation[2],CuisineType:cuisineT};
        })
    },*/
    getRecentlyVisitedSync: function (userId) {
        var bookingsFile = fs.readFileSync('./data/bookings_demo.csv', 'utf8');
        var restaurantsFile = fs.readFileSync('./data/Restaurant.csv', 'utf8');
        var bookings = parse(bookingsFile, {});
        var restaurants = parse(restaurantsFile, { columns: true });

        /* Booking file columns:
            [0] user id
            [1] restaurant name
            [2] restaurant id
            [3] visit time
            [4] covers
            [5] review score
        */

        // Filter out the relevant bookings
        return bookings.filter(function (booking) {
            return booking[0] == userId;

        // Ensure restaurant can be found
        }).filter(function (booking) {
            return restaurants.find(function (restaurant) {
                return restaurant['RestaurantId'] == booking[2];
            });

        }).map(function (booking) {

            var rest = restaurants.find(function (restaurant) {
                return restaurant['RestaurantId'] == booking[2];
            });

            var cuisineT = getResCuisine(booking[2]);

            bk = "Not available"; //booking[3];                             // Visit Date 
            sc = (booking[5]=="NULL")? "Didn't Review" : booking[5];        // Review Score (if available)

            return {
                Restaurant: rest,
                Score: sc,
                BookingTime: bk,
                CuisineType: cuisineT,
                PricePoint: rest['PricePoint']
            };
        })
    },

    // Need recently visited for google map lat & lon plotting - individual restaurant page
    getRecentlyVisitedCoord: function (userId) {
        var bookingsFile = fs.readFileSync('./data/bookings_demo.csv', 'utf8');
        var restaurantsFile = fs.readFileSync('./data/Restaurant.csv', 'utf8');
        var bookings = parse(bookingsFile, {});
        var restaurants = parse(restaurantsFile, { columns: true });

        // filter out the relevant bookings
        return bookings.filter(function (booking) {
            return booking[0] == userId;
        }).map(function (booking) {

            var rest = restaurants.find(function (restaurant) {
                return restaurant['RestaurantId'] == booking[2];
            });

            var resId = rest['Restaurant Id'];
            var lat = rest["Lat"];
            var lon = rest["Lon"];

            return {Restaurant:resId,Lat:lat,Lon:lon};
        })
    },

    getRecommendedRes: function (resId) {
        var restaurantsFile = fs.readFileSync('./data/Restaurant.csv', 'utf8');
        var restaurants = parse(restaurantsFile, { columns: true });

        var rest = restaurants.find(function (restaurant) {
            return restaurant['RestaurantId'] == resId;
        });
        return {Restaurant:rest};
    },

    getResCuisine: function (resId) {
        var restaurantsFile = fs.readFileSync('./data/Restaurant.csv', 'utf8');
        var restaurants = parse(restaurantsFile, { columns: true });

        var cuisineFile = fs.readFileSync('./data/RestaurantCuisineTypes.csv', 'utf8');
        var cuisines = parse(cuisineFile, { columns: true });

        var cuisineTypeFile = fs.readFileSync('./data/CuisineTypes.csv', 'utf8');
        var cuisineTypes = parse(cuisineTypeFile, { columns: true });

        var cuisineID = cuisines.find(function (tempResID){
                return tempResID['RestaurantId'] == resId ;
            });

        var cuisineT = "Data Not Provided";

        if (cuisineID !== undefined){
                cuisineT = cuisineTypes.find(function (tempResID){
                return tempResID[' Id'] == cuisineID['CuisineTypeId'];
                });
                
                cuisineT = cuisineT['Name'];                
        }
        return {CuisineType:cuisineT};
    },

    getRecAlsScoreSync: function (userId, resId) {
        //var recommendationsFile = fs.readFileSync('./data/Recommendations_ALSExplicit_Demo.csv', 'utf8');
        var recommendationsFile = fs.readFileSync('./data/recs_explicitals.csv', 'utf8');

        var restaurantsFile = fs.readFileSync('./data/Restaurant.csv', 'utf8');
        var recommendations = parse(recommendationsFile, {});
        var restaurants = parse(restaurantsFile, { columns: true });

        var res = null;

        res = recommendations.find(function (recommendation) {
            return recommendation[0] == userId && recommendation[1] == resId;
        });

        if (res === undefined){
            return {ScoreContent:null};
        } else {
            return {ScoreContent:res[2]};
        }
    },

    getRecContentScoreSync: function (userId, resId) {
        //var recommendationsFile = fs.readFileSync('./data/Recommendations_Content_Based.csv', 'utf8');
        var recommendationsFile = fs.readFileSync('./data/recs_cuisinetype.csv', 'utf8');

        var restaurantsFile = fs.readFileSync('./data/Restaurant.csv', 'utf8');
        var recommendations = parse(recommendationsFile, {});
        var restaurants = parse(restaurantsFile, { columns: true });

        var res = recommendations.find(function (recommendation) {
            return recommendation[0] == userId && recommendation[1] == resId;
        });

        if (res === undefined){
            return {ScoreContent:null};
        } else {
            return {ScoreContent:res[2]};
        }
    },

/*    getRecommendations: function (userId, res) {

        // read two files
        fs.readFile('./data/Recommendations.csv', 'utf8', function (err1, recommendationsFile) {
            fs.readFile('./data/Restaurant.csv', 'utf8', function (err2, restaurantsFile) {
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
        fs.readFile('./data/Booking.csv', 'utf8', function (err1, bookingsFile) {
            fs.readFile('./data/Restaurant.csv', 'utf8', function (err2, restaurantsFile) {
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
*/
    readCSV: function (id, res) {
        // read two files
        fs.readFile('./data/Recommendations.csv', 'utf8', function (err1, recommendationsFile) {
            fs.readFile('./data/Restaurant.csv', 'utf8', function (err2, restaurantsFile) {
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
