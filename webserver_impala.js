var impalad = require('./impalad.js');

exports.pass = function( path, req, res ) {
    var query = req.query;

    switch (path[0]) {
        case 'impalad_status': {
            var impalad_status = impalad.get_server_state();
            return res.end( JSON.stringify( { "status":"ok", "value":impalad_status } ) );
        }
        case 'impalad_queries': {
            var impalad_queries = impalad.get_queries();
            return res.end( JSON.stringify( { "status":"ok", "value":impalad_queries } ) );
        }
    }
    res.end('{"type":"error", "msg":"api error (unknown api ' + path[0] + ')"}');
}