var datahub = require('./datahub.js');

exports.pass = function( path, req, res ) {
    var query = req.query;

    switch (path[0]) {
        // case 'impalad_status': {
        //     var impalad_status = impalad.get_server_state();
        //     return res.end( JSON.stringify( { "status":"ok", "value":impalad_status } ) );
        // }
        case 'cluster': {
            var cluster = null;
            if (query.cluster) {
                cluster = datahub.getCluster(query.cluster);
            }
            if (cluster) {
                console.log('API : cluster(' + query.cluster + ') : ok');
                return res.end( JSON.stringify( { "status":"ok", "value":cluster } ) );
            }
            console.log('API : cluster(' + query.cluster + ') : failed');
            return res.end( JSON.stringify( { "status":"error", "msg": 'invalid cluster name ' + query.cluster } ) );
        }
        case 'cluster_names': {
            var clusters = JSON.stringify( { "status":"ok", "value":datahub.getClusterNames() } );
            console.log('API : cluster_names ' + clusters);
            return res.end( clusters );
        }
    }
    res.end('{"type":"error", "msg":"api error (unknown api ' + path[0] + ')"}');
}