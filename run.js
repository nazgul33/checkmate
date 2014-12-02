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
    switch (process.env.HCCHILD) {
    case 'impala': 
        {
            var impala = require('./impalad.js');
            var mail_recipient_list = [ 'taewook@sk.com', 'jungyup.lee@sk.com', 'steven.han@sk.com', 'gounna@sk.com'];
            var sms_recipient_list = [ '01036875275', '01098954723', '01086807083', '01071578092' ];
            var impala_cluster_descriptions = {
                test: {
                    impala_version:'2.0.0',
                    statestored_hostname:"dicc-tm003",
                    statestored_web_port:25010,
                    subscribers_update_interval:60000,
                    impalad_web_port:25000,
                    impalad_update_interval:30000,
                    mail_recipients:mail_recipient_list,
                    sms_recipients:sms_recipient_list },
                daas: {
                    impala_version:'1.4.1',
                    statestored_hostname:"dicc-m003",
                    statestored_web_port:25010,
                    subscribers_update_interval:60000,
                    impalad_web_port:25000,
                    impalad_update_interval:30000,
                    mail_recipients:mail_recipient_list,
                    sms_recipients:sms_recipient_list },
                eda: {
                    impala_version:'1.4.1',
                    statestored_hostname:"dicc-m002",
                    statestored_web_port:25010,
                    subscribers_update_interval:60000,
                    impalad_web_port:25000,
                    impalad_update_interval:30000,
                    mail_recipients:mail_recipient_list,
                    sms_recipients:sms_recipient_list },
                eda2: {
                    impala_version:'2.0.0',
                    statestored_hostname:"dicc-m004",
                    statestored_web_port:25010,
                    subscribers_update_interval:60000,
                    impalad_web_port:25000,
                    impalad_update_interval:30000,
                    mail_recipients:mail_recipient_list,
                    sms_recipients:sms_recipient_list },
            };

            impala.init( impala_cluster_descriptions );
        }
        break;
    default:
        console.log('environment variable HCCHILD not valid : ' + process.env.HCCHILD);
    }
}