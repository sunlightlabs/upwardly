var jsloader = function(d, s, id, url) {
	var js, fjs = d.getElementsByTagName(s)[0];
	if (d.getElementById(id)) return;
	js = d.createElement(s);
	js.id = id;
	js.src = url;
	fjs.parentNode.insertBefore(js, fjs);
};

var probablyNotMobile = $(document).width() >= 960;

$(function(){
	
	$("input").placehold();

	if (probablyNotMobile) {
		jsloader(document, 'script', 'chartbeat-js', 'http://static.chartbeat.com/js/chartbeat.js');
	}

});
