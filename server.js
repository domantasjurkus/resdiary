var express = require('express');
var app = express();
var path = require('path');
var http = require('http').Server(app);
var data = require('./data');
var csv = require('csvtojson');

app.use('/js', express.static(__dirname + '/js'));
app.use('/css', express.static(__dirname + '/css'));
app.use('/img', express.static(__dirname + '/img'));

app.set('views', './views')
app.set('view engine', 'ejs')

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

/*New demo (23/02) links go here */
app.get('/new_demo/',function(req,res){
    res.sendFile('views/new_demo_index.html', {root: __dirname});
})

app.get('/new_demo/user/:id',function(req,res){
    var id = req.params.id; //Grab the ID 

    var visited = data.getRecentlyVisitedSync(id || 0); //Gets the recently visited
    var recs = data.getRecommendationsSync(id || 0); //Gets the recommendations (and possibly reasons)

    res.json({ userId: id, recent: visited, recommendations: recs})

    //res.render('new_demo_user', { userId: id, recent: visited, recommendations: recs})
})

/* API ports */
app.get('/recs/:id', function(req, res) {
    data.readCSV(req.params.id || 0, res);
});

app.get('/recs/recent/:id',function(req,res){
    data.readCSV(req.params.id)
})

app.get('/recs/recommendations/:id',function(req,res){
    data.readCSV(req.params.id)
})

// Return all generated recommendations as a JSON
app.get('/data', function(req, res) {
	var data = [];
	var filepath = path.join(__dirname, 'src/data/Recommendations.csv')

	csv().fromFile(filepath).on('json', function(jsonObj) {
	    data.push(jsonObj);
	    
	}).on('done', function(error) {
	    res.json(data);
	})
})

var port = process.env.PORT || 3000;
http.listen(port, function() {
    console.log('listening on *:3000');
});
