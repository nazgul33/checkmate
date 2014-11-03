var impala = require('./impalad.js');

function get_impala_running_queries( cluster_name ) {
    var cluster = impala.get_clusters()[cluster_name];
    if (cluster) {
        var running_queries = [];
        for ( var server_name in cluster.impalad_servers ) {
            var server = cluster.impalad_servers[server_name];
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
                running_queries.push( q );
            }
        }
        return running_queries;
    }
    return null;
}

exports.pass = function( path, req, res ) {
    var query = req.query;

    switch (path[0]) {
        // case 'impalad_status': {
        //     var impalad_status = impalad.get_server_state();
        //     return res.end( JSON.stringify( { "status":"ok", "value":impalad_status } ) );
        // }
        case 'running_queries': {
            console.log('WEB API : running_queries', query);
            var running_queries = null;
            if (query.cluster) {
                running_queries = get_impala_running_queries(query.cluster);
            }
            if (running_queries) {
                return res.end( JSON.stringify( { "status":"ok", "value":running_queries } ) );
            }
            return res.end( JSON.stringify( { "status":"error", "msg": 'invalid cluster name ' + query.cluster } ) );
        }
        case 'clusters': {
            console.log('WEB API : clusters', query);
            var clusters = impala.get_clusters();
            var name_list = [];
            for (var name in clusters) {
                name_list.push(name);
            }
            return res.end( JSON.stringify( { "status":"ok", "value":name_list } ) );
        }
    }
    res.end('{"type":"error", "msg":"api error (unknown api ' + path[0] + ')"}');
}