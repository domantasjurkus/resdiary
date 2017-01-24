$(document).ready(function() {
    
    $.ajax({
        url: '/data',
        success: function(data) {
            drawDropdown(data);
        }, error: function() {
            console.log("Error calling /data");
        }
    });

    function drawDropdown(data) {
        var userIds = data.map(function(object) {
            return object.userID
        })

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
    }  

    function getRecommendations(id) {
        $.ajax({
            url: "/recs/"+id,
            success: function(data) {
                drawRecommendations(id, data);
            }
        })
    }

    function drawRecommendations(id, recs) {
        recs.forEach(function(rec) {
            // TODO: use a templating module and move HTML into another file
            console.log(rec);
            var entry = $('<div class="rec-div"><img class="rec-image" src="https://resdiary.blob.core.windows.net/uploads/uk/3349/images/1430/Portal/Logo/img9216.png"><p class="rec-label">'+rec.Name+'</p><p class="small">'+rec.Town+'</p><p class="small">Price point: '+rec.PricePoint+'</p><hr class="rec-divider"/></div>');
            $("#rec-list").append(entry);
        });
    }

});
