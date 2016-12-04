var express = require('express');
var app = express();
var http = require('http').Server(app);
var parse = require('csv-parse');
var fs = require('fs');

// app.use(express.static(__dirname + '/public'));
app.use('/js', express.static(__dirname + '/js'));
app.use('/css', express.static(__dirname + '/css'));
// app.use('/img', express.static(__dirname + '/img'));

app.get('/', function(req, res) {
    res.sendFile('views/demo.html', { root: __dirname });
});

// API ports
app.get('/recs/:id', function(req, res) {

    var id;
    if   (!req.params.id) id = 0;
    else id = req.params.id;

    // read two files
    fs.readFile('./recommendations.csv', 'utf8', function(err1, recommendationsFile) {
        fs.readFile('./Restaurant.csv', 'utf8', function(err2, restaurantsFile) {
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
});

var port = process.env.PORT||3000;
http.listen(port, function() {
    console.log('listening on *:3000');
});


