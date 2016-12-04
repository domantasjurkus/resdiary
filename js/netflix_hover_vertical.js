var row = document.getElementById('row');
var tiles = document.getElementsByClassName('tile');
var hoverTile = document.getElementById('hover');

/* Move the tiles left and right */
hoverTile.addEventListener('mouseover', function() {
	for (var i = 0; i < 2; i++) {
		var tile = tiles[i];
		tile.classList.add('shiftUp');
  	}
  
	for (var i = 3; i < tiles.length; i++) {
		var tile = tiles[i];
		tile.classList.add('shiftDown');
  	}
});

/* Return the tiles to original positions */
hoverTile.addEventListener('mouseout', function() {
	for (var i = 0; i < 2; i++) {
		var tile = tiles[i];
		tile.classList.remove('shiftUp');
  	}
  
	for (var i = 3; i < tiles.length; i++) {
		var tile = tiles[i];
		tile.classList.remove('shiftDown');
  	}
});
