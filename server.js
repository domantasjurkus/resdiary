var express = require('express');
var app = express();
var http = require('http').Server(app);

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

	// TODO: fetch actual recommendations for user id
	var recs = require('./stub-recommendations');

	res.json(recs);
});

var port = process.env.PORT||3000;
http.listen(port, function() {
	console.log('listening on *:3000');
});


