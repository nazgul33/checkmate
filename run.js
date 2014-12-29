var cluster = require('cluster');

console.log('starting up healthchecker...');

if (cluster.isMaster) {
    var Common = require('./lib/common.js');
    var datahub = require('./datahub.js');
    var webserver = require('./webserver.js');
    var opt = Common.getopts([
        ['w', 'webport=ARG', 'port number for web interface (default: 8080)'],
        ['h', 'help', 'display this help'],
    ]);
    var webserver_port = opt.options['webport'] || 8080;

    datahub.init( function() {
        console.log('datahub online!');
        console.log('starting webserver on port ' + webserver_port);
        webserver.init(webserver_port);

        var child_list = [ 'impala' ];
        var workers = {};
        for (var i=0; i<child_list.length; i++) {
            cluster.fork({
                'HCCHILD': child_list[i],
            });
        }
        cluster.on('exit', function(worker, code, signal) {
            console.log('worker ' + worker.process.pid + ' died');
        });
    });
}
else {
    var config = require('./config.js');
    switch (process.env.HCCHILD) {
    case 'impala': 
        {
            var impala = require('./impalad.js');
            impala.init( config.impala );
        }
        break;
    default:
        console.log('environment variable HCCHILD not valid : ' + process.env.HCCHILD);
    }
}
