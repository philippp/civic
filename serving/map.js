globals = {};

function loadEvictions() {
    $.getJSON('/reports/all_2013_now.json', function(data) {
        var items = [];
        globals.evictions = {};
        $.each(data, function(idx, val) {
	    var locationkey = val["longitude"]+"-"+val["latitude"];
	    if (!(locationkey in globals.evictions)) {
		globals.evictions[locationkey] = [];
	    };
	    globals.evictions[locationkey].push(val);
        });

	var grantees = {};
	globals.top_grantees = [];
	$.each(globals.evictions, function(key, vals) {
	    for (var i = 0; i < vals.length; i++) {
		if (!('grantees' in vals[i])) {
		    continue;
		}
		for (var j = 0; j < vals[i]['grantees'].length; j++) {
		    var grantee = vals[i]['grantees'][j];
		    if (!(grantee in grantees)) {
			grantees[grantee] = 0;
		    }
		    grantees[grantee]++;
		}
	    }
	});
	$.each(grantees, function(key, val) {
	    globals.top_grantees.push([key, val]);
	});
	globals.top_grantees.sort(function(a, b) {return b[1] - a[1]});
	for (var i = 0; i < globals.top_grantees.length; i++) {
	    var grantee_str = globals.top_grantees[i][0] + ": " + globals.top_grantees[i][1];
	    var grantee_cell = $("<li></li>").text(grantee_str);
	    grantee_cell.click((function(name) { return function(){
		renderEvictions("grantees", function(grantees) {
		    for (var j = 0; j < grantees.length; j++) {
			if (grantees[j] == name) {
			    return true;
			}
		    }
		    return false;
		}); 
	    }; })(globals.top_grantees[i][0]));
	    $("#top-grantees").append(grantee_cell);
	}
    });
}

function renderEvictions(criteria_field, criteria_lambda) {
    var bounds = new google.maps.LatLngBounds();
    $.each(globals.evictions, function(key, vals) {
	var marker = null;
	var info_pieces = {};
	info_pieces.addresses = [];
	for (var i = 0; i < vals.length; i++) {
	    if (!(criteria_field in vals[i])) {
		continue;
	    }
	    var marker_latlng = new google.maps.LatLng(
		vals[i]["latitude"], vals[i]["longitude"]);
	    bounds.extend(marker_latlng);
	    if (criteria_lambda(vals[i][criteria_field])) {
		if (!marker) {
		    marker =  new google.maps.Marker({
			position: marker_latlng,
			map: globals["map"],
			title: vals[i]["address"]
		    });
		    globals.markers.push(marker);
		}
		info_pieces.date = vals[i]["date"];
		info_pieces.addresses.push(vals[i]["address"]);
	    }
	}
	var contentString = "<p>"+info_pieces.date+"</p>";
	for (var i = 0; i < info_pieces.addresses.length; i++) {
	    contentString += ("<p>" + info_pieces.addresses[i] + "</p>");
	}
	var infowindow = new google.maps.InfoWindow({
	    content: contentString
	});
	if (marker != null) {
	    google.maps.event.addListener(marker, 'click', function() {
		infowindow.open(globals.map, marker);
	    });
	}
    });
    globals.map.fitBounds(bounds);
}

function clearMarkers() {
    for (var i = 0; i < globals.markers.length; i++) {
	globals.markers[i].setMap(null);
    }
    globals.markers = [];
}

function initialize() {
    var mapOptions = {
        center: new google.maps.LatLng(37.7833, -122.43),
        zoom: 13
    };
    globals.markers = [];
    globals.map = new google.maps.Map(
	document.getElementById("map-canvas"),
        mapOptions);
    loadEvictions();
}
google.maps.event.addDomListener(window, 'load', initialize);
