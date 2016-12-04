$(document).ready(function() {
	
	var userIds = getUserIds();

	userIds.forEach(function(id) {
		var li = $('<li><a href="#" class="user-id">'+id+'</a></li>');

		// Populate/update recommendations list on dropdown select
		li.on('click', function() {
			$("#rec-list").html('');
			var id = $(this).children().html();
			var recs = getRecommendations(id);
		});

		$("#user-id-ul").append(li);
	});

	// TODO: replace with API call
	function getUserIds() {
		return [6589241,34162351,13539951];
	}

	// TODO: replace with API call
	function getRecommendations(id) {
		$.ajax({
			url: "/recs/"+id,
			success: function(data) {
				drawRecommendations(id, data)
			}
		})
	}

	function drawRecommendations(id, recs) {
		recs.forEach(function(rec) {
			// TODO: use a templating module and move HTML into another file
			var entry = $('<img class="rec-image" src="https://resdiary.blob.core.windows.net/uploads/uk/3349/images/1430/Portal/Logo/img9216.png"><p class="rec-label">'+rec.Name+'</p><hr class="rec-divider" />');
			$("#rec-list").append(entry);
		});
	}
});
