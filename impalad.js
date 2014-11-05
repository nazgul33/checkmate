var needle = require('needle');
var cheerio = require('cheerio')
var url = require('url');
var moment = require('moment');
var datahub = require('./datahub.js');

var datahub_client;

String.prototype.startsWith = function(s)
{
   if( this.lastIndexOf(s, 0) === 0 ) return true;
   return false;
}

function ImpalaRunningQuery(server, user, default_db, statement, query_type, start_time, backend_progress, state, n_rows_fetched, query_id) {
    this.user = user;
    this.default_db = default_db;
    this.statement = statement;
    this.query_type = query_type;
    this.start_time = start_time; // epoch milisec
    this.backend_progress = backend_progress;
    this.state = state;
    this.n_rows_fetched = n_rows_fetched;
    this.query_id = query_id;

    this.last_update = null; // epoch milisec
    this.check = false; // clear this value before refreshing.
                        // this field is used to remove this query from running query list.
                        // if this field is false after refresh, this query is not running anymore.

    this.server = server; // an instance of ImpalaServer
    this.stall = false;
}

function ImpaladServer(cluster, name, port, timestamp) {
    this.cluster = cluster;

    this.server_name = name;
    this.web_port = port;

    this.running_queries = {};

    this.updated = this.first_seen = timestamp;
    this.state = 'online';
}

/*
options = {
    statestored_hostname:"dicc-tm003"
    statestored_web_port:25010;
    subscribers_update_interval:30000;
    impalad_web_port:25000;
    impalad_update_interval:5000;
}
*/

function ImpalaCluster(name, options) {
    this.cluster_name = name;
    this.cluster_options = options;
    this.cluster_options.statestored_web_port = options.statestored_web_port | 25010;
    this.cluster_options.subscribers_update_interval = options.subscribers_update_interval | 30000;
    this.cluster_options.impalad_web_port = options.impalad_web_port | 25000;
    this.cluster_options.impalad_update_interval = options.impalad_update_interval | 5000;

    // temporary server list to determine server update is complete or not
    this.impalad_updating = 0;
    this.impalad_servers = {}; // { servername: instanceOf(ImpaladServer), ... }
    this.impalad_update_timer = null;

    this.statestored_state = 'offline';
    this.statestored_update_timer = null;
}

ImpaladServer.prototype.getRunningQueryFromTr = function($, tr) {
    var td = $(tr).find('td');

    if ( td.length < 9 ) {
        return null;
    }

    return new ImpalaRunningQuery( this,
                                    $(td[0]).text(),
                                    $(td[1]).text(),
                                    $(td[2]).text(),
                                    $(td[3]).text(),
                                    moment($(td[4]).text()).valueOf(),
                                    $(td[5]).text(),
                                    $(td[6]).text(),
                                    $(td[7]).text(),
                                    url.parse( $(td[8]).find('a').attr('href'), true, false).query['query_id']);
}

ImpaladServer.prototype.getRunningQueriesFromHtml = function($) {
    var server = this;
    var table_queries = $('table')[0];
    var now = moment().valueOf();
    // process queries
    $(table_queries).find('tr').each( function() {
        var running_query = server.getRunningQueryFromTr($, this);  // 'this' is a 'tr'
        if ( running_query == null ) return;

        var query_id = running_query.query_id;
        running_query.check = true;
        running_query.last_update = now;
        if ( server.running_queries[query_id] ) {
            console.log( 'existing query', query_id );
            if ( (server.running_queries[query_id].backend_progress == running_query.backend_progress) &&
                (server.running_queries[query_id].n_rows_fetched == running_query.n_rows_fetched) ) {
                console.log( '**** QUERY', query_id, 'NO PROGRESS!' );
                running_query.stall = true;
            }
        }
        else {
            running_query.last_update = now;
            console.log( 'new query', query_id );
        }
        server.running_queries[query_id] = running_query;
    });
}

ImpalaCluster.prototype.finishUpdatingImpalad = function () {
    if (this.impalad_updating <= 0) {
        console.log('BUG: finishUpdatingImpalad() w/ impalad_updating' + this.impalad_updating);
        return;
    }

    this.impalad_updating--;
    if (this.impalad_updating > 0) return;

    // all servers processed : remove disappeared queries
    for (var name in this.impalad_servers) {
        var impalad = this.impalad_servers[name];
        var query_id_list = [];
        for (var id in impalad.running_queries) {
            if (impalad.running_queries[id].check == false) {
                query_id_list.push(id);
            }
        }

        for (var i=0; i < query_id_list.length; i++) {
            console.log('removing query ' + query_id_list[i]);
            delete impalad.running_queries[query_id_list[i]];
        }
    }

    var cluster = { 'name': this.cluster_name }
    var running_queries = [];
    var serverlist = [];
    for ( var server_name in this.impalad_servers ) {
        var server = this.impalad_servers[server_name];
        for ( var query_id in server.running_queries ) {
            var q = server.running_queries[query_id];
            running_queries.push({
                user: q.user,
                default_db: q.default_db,
                statement: q.statement,
                query_type: q.query_type,
                start_time: q.start_time,
                backend_progress: q.backend_progress,
                state: q.state,
                n_rows_fetched: q.n_rows_fetched,
                query_id: q.query_id,
                last_update: q.last_update,
                stall: q.stall,

                server_name: q.server.server_name,
                server_port: q.server.web_port,
                server_state: q.server.state,
            });
        }
        serverlist.push({
            'server_name': server.server_name,
            'server_state': server.state
        });
    }
    cluster['cluster'] = { 'running_queries': running_queries, 'servers': serverlist };

    datahub_client.send( {'type':'cluster', 'value':cluster } );
    // console.log('cluster ' + this.cluster_name + ' updated.')
}

ImpaladServer.prototype.updateRunningQueries = function() {
    var impalad = this;
    // clear check field for all running queries
    for (var query_id in impalad.running_queries) {
        impalad.running_queries[query_id].check = false;
    }

    var url = 'http://' + impalad.server_name + ':' + impalad.web_port + '/queries';
    // console.log('updating impalad jobs from', url);
    needle.get(url, { timeout: 3000 }, function (error, response) {
        if (!error && response.statusCode == 200) {
            var server = response.req._headers.host.split(':')[0];
            impalad.getRunningQueriesFromHtml( cheerio.load(response.body) );
        }
        // finish update for this server
        impalad.cluster.finishUpdatingImpalad();
    });
}

ImpalaCluster.prototype.updateImpalad = function () {
    // this is safe only because impalad_servers has no prototype.
    this.impalad_updating = Object.keys(this.impalad_servers).length;

    // update running queries connecting impalad's web interface
    for (var server_name in this.impalad_servers) {
        var impalad = this.impalad_servers[server_name];
        impalad.updateRunningQueries();
    }
}

ImpalaCluster.prototype.updateImpaladList = function(new_impalad_list) {
    var now = moment().valueOf();
    var need_update = false;

    // examine/update each other for missing server
    for ( var i=0; i < new_impalad_list.length; i++ ) {
        var server_name = new_impalad_list[i] = new_impalad_list[i].toLowerCase();
        var impalad = null;
        if ( server_name in this.impalad_servers ) {
            impalad = this.impalad_servers[server_name];
            impalad.updated = now;
            if (impalad.state != 'online') {
                // console.log(this.cluster_name + ': impalad "' + server_name + '" online');
                impalad.state = 'online';
            }
        }
        else {
            need_update = true;
            impalad =  new ImpaladServer( this, server_name, this.cluster_options.impalad_web_port, now );
            this.impalad_servers[server_name] = impalad;
            // console.log(this.cluster_name + ': impalad "' + server_name + '" added');
        }
    }

    // TODO: determine what to do, if server is disappeared from server list
    for ( var server_name in this.impalad_servers ) {
        if ( new_impalad_list.indexOf(server_name) < 0 ) {
            console.log(this.cluster_name + ': impalad "' + server_name + '" offline');
            this.impalad_servers[server_name].state = 'offline';

            // queries in offline server will be removed when the server comes back online
        }
    }

    // if timer is not active yet, we should start it
    if (this.impalad_update_timer == null) {
        need_update = true;
    }

    if (need_update) {
        console.log(this.cluster_name + ': impalad update timer is being started');

        if (this.impalad_update_timer) {
            clearInterval(this.impalad_update_timer);
        }
        setTimeout( this.updateImpalad.bind(this), 0 );
        this.impalad_update_timer = setInterval( this.updateImpalad.bind(this), this.cluster_options.impalad_update_interval | 5000 );
    }
}

ImpalaCluster.prototype.getServersFromHtml = function($) {
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

    var subscribers_impalad = [];
    var subscribers_catalogd = [];

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

    return { impalad: subscribers_impalad, catalogd: subscribers_catalogd };
}

ImpalaCluster.prototype.updateSubscribers = function() {
    var cluster = this;
    var opt = this.cluster_options;
    var url = 'http://' + opt.statestored_hostname + ':' + opt.statestored_web_port + '/subscribers';
    needle.get(url, { timeout: 3000 }, function (error, response) {
        if (!error && response.statusCode == 200) {
            var servers = cluster.getServersFromHtml( cheerio.load(response.body) );
            cluster.statestored_state = 'online';
            cluster.updateImpaladList(servers.impalad);
            // currently we're not interested in catalogd.
            // cluster.updateCatalogdList(servers.catalogd);
        }
        else {
            cluster.statestored_state = 'offline';
        }
    });
}

ImpalaCluster.prototype.startUpdating = function() {
    if (this.statestored_update_timer) {
        clearInterval(this.statestored_update_timer);
    }
    setTimeout( this.updateSubscribers.bind(this), 0 );
    this.statestored_update_timer = setInterval( this.updateSubscribers.bind(this), this.cluster_options.subscribers_update_interval | 30000 );
}

var impala_clusters = {};

exports.init = function(cluster_descriptions) {
    datahub_client = new datahub.client('impalas');
    for ( var cluster_name in cluster_descriptions ) {
        var cluster = new ImpalaCluster(cluster_name, cluster_descriptions[cluster_name]);
        impala_clusters[cluster_name] = cluster;
        console.log('CLUSTER: ' + cluster_name);
        cluster.startUpdating();
    }
}


exports.get_clusters = function() {
    return impala_clusters;
}