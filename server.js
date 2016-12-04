var express = require('express');
var app = express();
var http = require('http').Server(app);
var parse = require('csv-parse');
var fs = require('fs');
var pythonShell = require('python-shell');

app.use('/js', express.static(__dirname + '/js'));
app.use('/css', express.static(__dirname + '/css'));
app.use('/img', express.static(__dirname + '/img'));

app.get('/', function(req, res) {
    res.sendFile('views/demo.html', { root: __dirname });
});

/* 
    Possibly have the demo as the / for Wednesday.
    Later change / to index and have the existing / new user split as discussed.
    Also can have temporary links to RND dev work from index.
 */
app.get('/index', function(req, res){
    res.sendFile('views/index.html', {root: __dirname});
});

app.get('/netflix-hover-horizontal', function(req, res){
	res.sendFile('views/netflix-hover-horizontal.html', {root: __dirname});
});

app.get('/netflix-hover-vertical', function(req, res){
    res.sendFile('views/netflix-hover-vertical.html', {root: __dirname});
});

/* API ports */
app.get('/recs/:id', function(req, res) {

    var id;
    if   (!req.params.id) id = 0;
    else id = req.params.id;

    /* Read two files */
    fs.readFile('./data/Recommendations.csv', 'utf8', function(err1, recommendationsFile) {
        fs.readFile('./data/Restaurant.csv', 'utf8', function(err2, restaurantsFile) {
            /* Parse the files */
            parse(recommendationsFile, {}, function(err3, recommendations) {
                parse(restaurantsFile, {columns: true}, function(err4, restaurants) {
                    /* Filter out the relevant recommendations */
                    res.send(recommendations.filter(function(recommendation) {
                        return recommendation[0] == id;
                    }).map(function(recommendation) {
                        /* Find the information about each restaurant */
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

/* Call Python script to generate recommendation CSV automatically */
// pythonShell.run('script.py', function (err) {
//	if (err) throw err;
//	console.log('Recommendation script success');
// });

var port = process.env.PORT||3000;
http.listen(port, function() {
    console.log('listening on *:3000');
});


