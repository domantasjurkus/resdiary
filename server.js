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

    var recentlyVisited = data.getRecentlyVisitedSync(id || 0); 			// Gets the recently visited
    //var recsALS = data.getRecommendationsAlsSync(id || 0); 					// Gets the recommendations (and possibly reasons)
    //var recsContent = data.getRecommendationsContentSync(id || 0); 			// Gets content-based recommendations
    
    var recommenders = [
        {name: "explicitals", label: "Explicit ALS", data: data.getRecommendations(id || 0, "explicitals")},
        {name: "implicitals", label: "Implicit ALS", data: data.getRecommendations(id || 0, "implicitals")},
        {name: "cuisinetype", label: "Cuisine Type", data: data.getRecommendations(id || 0, "cuisinetype")},
        {name: "pricepoint",  label: "Price Point",  data: data.getRecommendations(id || 0, "pricepoint")},
        {name: "system",      label: "System",       data: data.getRecommendations(id || 0, "system")}
    ]
    recommenders.forEach(function(r) {
        r.data.sort(function(a, b) {
            return parseFloat(b.RecScore) - parseFloat(a.RecScore);
        });
        console.log(r.data.length, r.name, "recommendations");
    });

    /*recsALS.sort(function(a, b) {
        return parseFloat(b.RecScore) - parseFloat(a.RecScore);
    });
    recsContent.sort(function(a, b) {
        return parseFloat(b.RecScore) - parseFloat(a.RecScore);
    });*/

    //res.json({ userId: id, recent: visited, contentBased:recsContent, als:recsALS})
    res.render('new_demo_user', {
        userId: id,
        recent: recentlyVisited,
        //contentBased: recsContent,
        //als: recsALS,
        recommenders: recommenders
    });
})

/* User specific recommendations and dining history */
app.get('/new_demo/user/:id/:resId',function(req,res){

    var id = req.params.id; 												// Grab the user ID 
   	var resId = req.params.resId;											// Grab restraunt ID

    var visited = data.getRecentlyVisitedCoord(id || 0); 					// Gets the user's recently visited restaurants
    var rest = data.getRecommendedRes(resId);							    // Gets the specific restaurant info
    var cus = data.getResCuisine(resId);                                    // Get the cuisine type of the restaurant

    var scoreALS = data.getRecAlsScoreSync(id, resId);
    var scoreContent = data.getRecContentScoreSync(id, resId);
    
    //console.log(scoreALS);
    //console.log(scoreContent);
    // console.log(visited);
    // console.log(cus);

    //res.json({userId: id, restaurantId: resId, recent: visited, restaurant: rest})
    res.render('new_demo_res', { userId: id, restaurantId: resId, recent: visited, restaurant: rest })
})


var port = process.env.PORT || 3000;
http.listen(port, function() {
    console.log('listening on *:3000');
});
