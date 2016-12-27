var fs = require('fs');
var parse = require('csv-parse');
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

/* A module for reading and writing data. Takes a Diner ID and sends the
   recommended restaurants to the res object. */
module.exports = {
    readCSV: function(id, res) {
        // read two files
        fs.readFile('./data/Recommendations.csv', 'utf8', function(err1, recommendationsFile) {
            fs.readFile('./data/Restaurant.csv', 'utf8', function(err2, restaurantsFile) {
                // parse the files
                parse(recommendationsFile, {}, function(err3, recommendations) {
                    parse(restaurantsFile, {columns: true}, function(err4, restaurants) {
                        // filter out the relevant recommendations
                        res.send(recommendations.filter(function(recommendation) {
                            return recommendation[0] == id;
                        }).map(function(recommendation) {
                            // find the information about each restaurant
                            return restaurants.find(function(restaurant) {
                                return restaurant['RestaurantId'] ==
                                    recommendation[1];
                            });
                        }));
                    });
                });
            });
        });
    },
    readDB: function(id, res) {
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
