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

var port = process.env.PORT||3000;
http.listen(port, function() {
	console.log('listening on *:3000');
});
