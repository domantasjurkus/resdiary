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

/*
	Python and JavaScript talking to one another
	Possible method 2: http://www.sohamkamani.com/blog/2015/08/21/python-nodejs-comm/
*/
	
var spawn = require('child_process').spawn,
	py = spawn('python', ['compute_input.py']),
    data = [1,2,3,4,5,6,7,8,9],
    dataString = '';

py.stdout.on('data', function(data){
  	dataString += data.toString(); 
});

py.stdout.on('end', function(){
	console.log('Sum of numbers = HERE =',dataString);
});

py.stdin.write(JSON.stringify(data));
py.stdin.end();

