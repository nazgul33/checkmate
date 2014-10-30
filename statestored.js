var needle = require('needle');
var cheerio = require('cheerio')

var subscribers_impalad = [];
var subscribers_catalogd = [];

String.prototype.startsWith = function(s)
{
   if( this.lastIndexOf(s, 0) === 0 ) return true;
   return false;
}

var cb_impalad, cb_catalogd;

var parse_subscribers = function($) {
    var table = $('table')
    var th = $(table).find('th');
    var col_id, col_addr;

    // find colume number of Id, Address
    for (var i=0; i<th.length; i++) {
        if ( $(th[i]).text() == 'Id' ) {
            col_id = i;
        } else if ( $(th[i]).text() == 'Address' ) {
            col_addr = i;
        }
    }

    // clear subscriber list
    subscribers_impalad = [];
    subscribers_catalogd = [];

    // parse tr's
    $(table).find('tr').each( function() {
        var td = $(this).find('td');
        var id = $(td[col_id]).text();
        var addr = $(td[col_addr]).text();
        var addronly = addr.substring(0, addr.lastIndexOf(':'));

        if (id.startsWith('impalad')) {
            subscribers_impalad.push(addronly);
        } else if (id.startsWith('catalog-server')) {
            subscribers_catalogd.push(addronly);
        }
    });

    if (cb_impalad) cb_impalad(subscribers_impalad);
    if (cb_catalogd) cb_catalogd(subscribers_catalogd);
    // console.log( subscribers_impalad );
    // console.log( subscribers_catalogd );
}

var update_subscribers = function(url) {
    // console.log('updating subscribers from', url);
    needle.get(url, function (error, response) {
        if (!error && response.statusCode == 200) {
            parse_subscribers( cheerio.load(response.body) );
        }
    });
}

exports.getImpaladInstances = function() {
    return subscribers_impalad;
}

exports.getCatalogdInstances = function() {
    return subscribers_impalad;
}

var subscribers_url = '';

exports.init = function( _cb_impalad, _cb_catalogd ) {
    cb_impalad = _cb_impalad;
    cb_catalogd = _cb_catalogd;

    update_subscribers(subscribers_url);
    setInterval( update_subscribers, 10000, subscribers_url );
}

exports.update_subscribers_url = function (url) {
    subscribers_url = url;
}
