$(document).ready(function() {
    
	var userIds = getUserIds();

	userIds.forEach(function(id) {
		$("#user-id-ul").append('<li><a href="#">'+id+'</a></li>');
	});

	// TODO: replace with API call
	function getUserIds() {
		return [2,3,5,8,];
	}

	// Data looks like this:
	var data = {
		id: 3466,
		name: "Di Maggio's Airdrie",
		town: "Airdrie",
		img: "",
		PricePoint: 2, // Can be NULL
		lat: 55.8598369,
		lng: -3.9994152
	}

});