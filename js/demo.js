$(document).ready(function() {
    
	var userIds = getUserIds();
	userIds.forEach(function(id) {
		$('#user-id-ul').append('<li><a href="#">'+id+'</a></li>');
	});

	// TODO: replace with API call
	function getUserIds() {
		return [2,3,5,8,];
	}

});