var impalad = require('./impalad.js');
var statestored = require('./statestored.js');
var webserver = require('./webserver.js');

statestored.update_subscribers_url('http://dicc-tm003:25010/subscribers');
// statestored.update_subscribers_url('http://dicc-m004:25010/subscribers');
impalad.update_server_port(25000);

statestored.init( impalad.update_server_list );
impalad.init();

webserver.init(25090);