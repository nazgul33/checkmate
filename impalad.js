var needle = require('needle');
var cheerio = require('cheerio')
var url = require('url');
var moment = require('moment');

var in_flight_jobs = {};

function QueryJob( $, tr ) {
    var self = this;
    this.user = null;
    this.default_db = null;
    this.statement = null;
    this.query_type = null;
    // epoch milisec
    this.start_time = null;
    this.backend_progress = null;
    this.state = null;
    this.n_rows_fetched = null;

    this.query_id = null;

    // epoch milisec
    this.last_update = null;
    this.check = false;
    this.server = null;

    this.stall = false;
    this.parseHtmlRow = function( $, tr ) {
        var td = $(tr).find('td');

        if ( td.length > 0 ) {
            this.user = $(td[0]).text();
            this.default_db = $(td[1]).text();
            this.statement = $(td[2]).text();
            this.query_type = $(td[3]).text();
            this.start_time = moment($(td[4]).text()).valueOf();
            this.backend_progress = $(td[5]).text();
            this.state = $(td[6]).text();
            this.n_rows_fetched = $(td[7]).text();

            urlobj = url.parse( $(td[8]).find('a').attr('href'), true, false);
            this.query_id = urlobj.query['query_id'];

        }
    }

    this.parseHtmlRow( $, tr );
};

var updating_server_list = [];

var parse_impalad_jobs = function(server, $) {
    var table_queries = $('table')[0];
    var now = moment().valueOf();
    // process queries
    $(table_queries).find('tr').each( function() {
        var queryjob = new QueryJob($, this);
        var query_id = queryjob.query_id;
        if ( query_id ) {
            queryjob.server = server;
            queryjob.check = true;
            if ( !in_flight_jobs[query_id] ) {
                queryjob.last_update = now;
                console.log( 'new query', query_id );
                in_flight_jobs[query_id] = queryjob;
            }
            else {
                console.log( 'existing query', query_id );
                if ( (in_flight_jobs[query_id].backend_progress == queryjob.backend_progress) &&
                    (in_flight_jobs[query_id].n_rows_fetched == queryjob.n_rows_fetched) ) {
                    console.log( '**** QUERY', query_id, 'NO PROGRESS!' );
                    queryjob.stall = true;
                }
                queryjob.last_update = now;
                in_flight_jobs[query_id] = queryjob;
            }
        }
    });

    if (updating_server_list.length > 0) {
        var server_idx = updating_server_list.indexOf(server);
        if (server_idx >= 0) {
            updating_server_list.splice(server_idx, 1);
        }
        if (updating_server_list.length == 0) {
            // all servers processed : remove disappeared queries
            var id_to_remove = [];
            for (var id in in_flight_jobs) {
                if (in_flight_jobs[id].check == false) {
                    id_to_remove.push(id);
                }
            }

            for (var idx in id_to_remove) {
                console.log('removing query ' + id_to_remove[idx]);
                delete in_flight_jobs[id_to_remove[idx]];
            }

            // DEBUG: make sure removed properly
            for (var idx in id_to_remove) {
                if ( in_flight_jobs.hasOwnProperty(id_to_remove[idx]) ) {
                    console.log('query id ' + id_to_remove[idx] + ' is not removed properly');
                }
            }
        }
    }
}

var server_state = {};
var server_list = [];
var server_port = 25000;

var update_impalad = function() {
    // clone server_list to prevent server_list from being erased;
    updating_server_list = server_list.slice(0);

    // clear check field
    for (var id in in_flight_jobs) {
        in_flight_jobs[id].check = false;
    }

    for (var idx in server_list) {
        var url = 'http://' + server_list[idx] + ':' + server_port + '/queries';
        // console.log('updating impalad jobs from', url);
        needle.get(url, function (error, response) {
            if (!error && response.statusCode == 200) {
                var server = response.req._headers.host.split(':')[0];
                parse_impalad_jobs( server, cheerio.load(response.body) );
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
        // capitalize first.
        new_server_list[idx] = new_server_list[idx].toLowerCase();

        var key = new_server_list[idx];
        if ( key in server_state ) {
            server_state[key].updated = now.valueOf();
        }
        else {
            server_state[key] = {};
            server_state[key].updated = now.valueOf();
            server_state[key].first_seen = now.valueOf();
        }
    }

    // examine each other for missing server
    for ( var idx in new_server_list ) {
        if ( server_list.indexOf(new_server_list[idx]) < 0 ) {
            need_update = true;
            console.log(new_server_list[idx] + ' newly appeared');
        }
    }
    for ( var idx in server_list ) {
        if ( new_server_list.indexOf(server_list[idx]) < 0 ) {
            need_update = true;
            console.log(new_server_list[idx] + ' disappeared');
        }
    }

    if (need_update) {
        console.log( "------ OLD SERVER LIST\n", server_list );
        console.log( "------ NEW SERVER LIST\n", new_server_list );

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

exports.get_queries = function() {
    return in_flight_jobs;
}

exports.init = function() {
    // server_list = [ 'bdbc-r1n006', 'bdbc-r1n007', 'bdbc-r1n008',
    //                 'bdbc-r2n006', 'bdbc-r2n007', 'bdbc-r2n008',
    //                 'bdbc-r3n006', 'bdbc-r3n007', 'bdbc-r3n008' ];
    // update_impalad(server_list, port);
    timer_update_impalad = setInterval( update_impalad, 5000 );
}
