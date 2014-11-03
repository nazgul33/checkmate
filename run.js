var impala = require('./impalad.js');
var webserver = require('./webserver.js');

impala_cluster_descriptions = {
    test: {
        statestored_hostname:"dicc-tm003",
        statestored_web_port:25010,
        subscribers_update_interval:10000,
        impalad_web_port:25000,
        impalad_update_interval:5000 },
    daas: {
        statestored_hostname:"dicc-m003",
        statestored_web_port:25010,
        subscribers_update_interval:10000,
        impalad_web_port:25000,
        impalad_update_interval:5000 },
    eda: {
        statestored_hostname:"dicc-m002",
        statestored_web_port:25010,
        subscribers_update_interval:10000,
        impalad_web_port:25000,
        impalad_update_interval:5000 },
};

impala_cluster_descriptions = {
};

impala.init( impala_cluster_descriptions );

webserver.init(8080);