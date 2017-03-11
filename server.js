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
    res.redirect('/new_demo/'); 		/* Redirect to '/new_demo/' to generate random selection of users */
});

/* Demo homepage which displays random selection of users */
app.get('/new_demo/',function(req,res){
    var users = data.getRandomUsersSync(4);
    
    var user_pics = [   "res_user_" + String(Math.floor(Math.random() * 3) + 1) + ".jpg",
                        "res_user_" + String(Math.floor(Math.random() * 3) + 1) + ".jpg", 
                        "res_user_" + String(Math.floor(Math.random() * 3) + 1) + ".jpg",
                        "res_user_" + String(Math.floor(Math.random() * 3) + 1) + ".jpg"
                    ];
/*
    var users = {
        user1:data.getRandomUserSync(),
        user2:data.getRandomUserSync(),
        user3:data.getRandomUserSync(),
        user4:data.getRandomUserSync()
    };
*/
    // res.json({ ids: users, genders: user_pics })
    res.render('new_demo_index', { ids: users, genders: user_pics })
    
})

/* User specific recommendations and dining history */
app.get('/new_demo/user/:id',function(req,res){
    var id = req.params.id; 												// Grab the ID 

    var visited = data.getRecentlyVisitedSync(id || 0); 					// Gets the recently visited
    var recsALS = data.getRecommendationsAlsSync(id || 0); 					// Gets the recommendations (and possibly reasons)
    var recsContent = data.getRecommendationsContentSync(id || 0); 			// Gets content-based recommendations
    
    // console.log(recsALS);

    recsALS.sort(function(a, b) {
        return parseFloat(b.RecScore) - parseFloat(a.RecScore);
    });
    recsContent.sort(function(a, b) {
        return parseFloat(b.RecScore) - parseFloat(a.RecScore);
    });

    //res.json({ userId: id, recent: visited, contentBased:recsContent, als:recsALS})
    res.render('new_demo_user', { userId: id, recent: visited, contentBased:recsContent, als:recsALS })
})

/* User specific recommendations and dining history */
app.get('/new_demo/user/:id/:resId',function(req,res){

    var id = req.params.id; 												// Grab the user ID 
   	var resId = req.params.resId;											// Grab restraunt ID

    var visited = data.getRecentlyVisitedCoord(id || 0); 					// Gets the user's recently visited restaurants

    // console.log(visited);

    var rest = data.getRecommendedRes(resId);							// Gets the specific restaurant info

    res.render('new_demo_res', { userId: id, restaurantId: resId, recent: visited, restaurant: rest })
})

/* API ports */
app.get('/recs/:id', function(req, res) {
    data.readCSV(req.params.id || 0, res);
});

/* Return all generated recommendations as a JSON */
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
