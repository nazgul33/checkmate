var needle = require('needle');
var jsdom = require('jsdom');
var url = require('url');
var moment = require('moment');
// var statestored = require('./statestored.js');

var in_flight_jobs = {};

function QueryJob( $, tr ) {
    var self = this;
    this.user = null;
    this.default_db = null;
    this.statement = null;
    this.query_type = null;
    this.start_time = null;
    this.backend_progress = null;
    this.state = null;
    this.n_rows_fetched = null;

    this.query_id = null;

    this.last_update = null;

    this.parseHtmlRow = function( $, tr ) {
        var td = $(tr).find('td');

        if ( td.length > 0 ) {
            this.user = $(td[0]).text();
            this.default_db = $(td[1]).text();
            this.statement = $(td[2]).text();
            this.query_type = $(td[3]).text();
            this.start_time = moment($(td[4]).text());
            this.backend_progress = $(td[5]).text();
            this.state = $(td[6]).text();
            this.n_rows_fetched = $(td[7]).text();

            urlobj = url.parse( $(td[8]).find('a').attr('href'), true, false);
            this.query_id = urlobj.query['query_id'];

            this.last_update = moment();
        }
    }

    this.parseHtmlRow( $, tr );
};

var parse_impalad_jobs = function(err, window) {
    var $ = window.jQuery;
    var table_queries = $('table')[0];

    // process queries
    $(table_queries).find('tr').each( function() {
        var queryjob = new QueryJob($, this);
        var query_id = queryjob.query_id;

        if ( query_id ) {
            if ( !in_flight_jobs[query_id] ) {
                console.log( 'new query', query_id );
                in_flight_jobs[query_id] = queryjob;
            }
            else {
                console.log( 'existing query', query_id );
                if ( in_flight_jobs[query_id].backend_progress == queryjob.backend_progress ) {
                    console.log( '**** QUERY', query_id, 'NO PROGRESS!' );
                }
                in_flight_jobs[query_id] = queryjob;
            }
        }
    });

    // ignore locations, complete queries
    // var table_locations = $('table')[1];
    // var table_complete = $('table')[2];

    window.close();
}

var server_state = {};
var server_list = [];
var server_port = 25000;

var update_impalad = function() {
    // console.log('processing impalad server list', server_list);
    for (var server in server_list) {
        var url = 'http://' + server_list[server] + ':' + server_port + '/queries';
        // console.log('updating impalad jobs from', url);
        needle.get(url, function (error, response) {
            if (!error && response.statusCode == 200) {
                jsdom.env({
                    html: response.body,
                    scripts: ['./jquery-1.11.1.min.js'],
                    done: parse_impalad_jobs,
                });
            }
        });
    }
}

var timer_update_impalad = null;

exports.update_server_list = function(new_server_list) {
    var now = moment();
    var need_update = false;
    // update server state map
    for ( var idx in new_server_list ) {
        var key = new_server_list[idx];
        if ( key in new_server_list ) {
            server_state[key].updated = now;
        }
        else {
            server_state[key] = {};
            server_state[key].updated = now;
            server_state[key].first_seen = now;
        }
    }
    // examine each other for missing server
    for ( var idx in new_server_list ) {
        if ( server_list.indexOf(new_server_list[idx]) < 0 ) {
            need_update = true;
        }
    }
    for ( var idx in server_list ) {
        if ( new_server_list.indexOf(server_list[idx]) < 0 ) {
            need_update = true;
        }
    }

    if (need_update) {
        console.log( "------ NEW SERVER LIST:", new_server_list );

        // if (timer_update_impalad) {
        //     clearTimeout(timer_update_impalad);
        //     timer_update_impalad = null;
        // }
        server_list = new_server_list;

        // setTimeout( update_impalad, 0 );
        // timer_update_impalad = setInterval( update_impalad, 5000 );
    }
}

exports.update_server_port = function(new_server_port) {
    server_port = new_server_port;
}

exports.get_server_state = function() {
    return server_state;
}

exports.init = function() {
    // server_list = [ 'bdbc-r1n006', 'bdbc-r1n007', 'bdbc-r1n008',
    //                 'bdbc-r2n006', 'bdbc-r2n007', 'bdbc-r2n008',
    //                 'bdbc-r3n006', 'bdbc-r3n007', 'bdbc-r3n008' ];
    // update_impalad(server_list, port);
    timer_update_impalad = setInterval( update_impalad, 5000 );
}
