$(document).ready(function() {
	
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

	var userIds = getUserIds();

	userIds.forEach(function(id) {
		var li = $('<li><a href="#" class="user-id">'+id+'</a></li>');

		// Populate/update recommendations list on dropdown select
		li.on('click', function() {
			$("#rec-list").html('');
			var id = $(this).children().html();
			var recs = getRecommendations(id);

			recs.forEach(function(rec) {

				// TODO: remove when actual recommendations are given
				if (rec.id != parseInt(id)) return;

				// TODO: use a templating module and move HTML into another file
				var entry = $('<img class="rec-image" src="https://resdiary.blob.core.windows.net/uploads/uk/3349/images/1430/Portal/Logo/img9216.png"><p class="rec-label">'+rec.name+'</p><hr class="rec-divider" />');
				$("#rec-list").append(entry);
			});
		});

		$("#user-id-ul").append(li);
	});

	// TODO: replace with API call
	function getUserIds() {
		return [0,1,2];
	}

	// TODO: replace with API call
	function getRecommendations(id) {

		// Data will look something like this
		return [{
			id: 0,
			name: "Di Maggio's Airdrie",
			town: "Airdrie",
			img: "",
			PricePoint: 2,
			lat: 55.8598369,
			lng: -3.9994152
		},
		{
			id: 1,
			name: "Barolo Grill",
			town: "Glasgow",
			img: "",
			PricePoint: null,
			lat: 55.8603952,
			lng: -4.255553
		},
		{
			id: 2,
			name: "Dakota Edinburgh",
			town: "Edinburgh",
			img: "",
			PricePoint: 1,
			lat: 55.98386,
			lng: -3.40117
		}]
	}
});
