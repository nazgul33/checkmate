var libnet = require('net');
var fs = require('fs');
var strbuf = require('./lib/stringBuffer.js');

var socket_path = '/tmp/.clustermonitor.sock';

var datahub_clients = {};
var client_id = 0;

// client representation on clientside
function ClusterSender(name) {
    this.sock = null;
    this.clientName = name || 'noname'+client_id.toString();
    this.sendQ = [];
    this.connecting = false;

    client_id++;

    this.onData = function(chunk) {
        // eat up data from server by doing nothing;
    };

    this.onError = function(e) {
        console.log('cluster sender error ', e);
        // socket will call onClose() for us.
    };

    this.onClose = function() {
        console.log('cluster sender connection closed');
        this.sock = null;
    };

    this.send = function(obj) {
        if (this.sock == null || this.connecting == true) {
            this.sendQ.push(obj);
            if (!this.connecting) this.connect();
        }
        else {
            this.sendQueue();
            this.sock.write( JSON.stringify(obj) + '\n\n', 'UTF-8');
            return true;
        }
    };

    this.sendQueue = function() {
        while (this.sendQ.length>0) {
            var obj = this.sendQ.shift();
            this.sock.write( JSON.stringify(obj) + '\n\n', 'UTF-8');
        }
    };

    this.connect = function() {
        var self = this;

        if (this.connecting) return;
        self.connecting = true;
        self.sock = libnet.connect(socket_path, function() {
            self.connecting = false;
            self.send( {'type':'id', 'value':self.clientName} );
            self.sendQueue();
        });
        self.sock.on('data', self.onData.bind(self));
        self.sock.on('error', self.onError.bind(self));
        self.sock.once('close', self.onClose.bind(self));
    };

}

// client representation on serverside
function ClientConnection() {
    var cSock = this;
    var buffer = new strbuf.StringBuffer();
    var clientName = '__noname__';
    var cid = client_id.toString();
    var clusters = {};
    client_id++;

    this.clientClosed = function() {
        console.log('socket closed : (name=' + clientName + ')');
        if (cid in datahub_clients) {
            delete datahub_clients[cid];
        }
    };

    this.onData = function(chunk) {
        if ( buffer.append(chunk) ) {
            if ( buffer.runObj(this.jsonRead.bind(this)) ) {
                return true;
            }
            else {
                console.log('ERROR!! data obj error. killing connection. (name=' + clientName + ')');
            }
        }
        else {
            console.log('ERROR!! data error. killing connection. (name=' + clientName + ')');
        }
        cSock.end();
        return false;
    };

    this.jsonRead = function(obj) {
        switch (obj.type) {
        case 'id':
            clientName = obj.value;
            console.log('new socket connection : ' + clientName);
            if (cid in datahub_clients) {
                console.log('BUGBUG!!! ' + clientName + ' already registered.');
                // TODO: WHAT TO DO? this case?
            }
            datahub_clients[cid] = this;
            break;
        case 'clusters':
            clusters = obj.value;
            console.log('client ' + clientName + 'sending clusters..');
            for (var cluster_name in clusters) {
                console.log('  cluster ' + cluster_name + ' : ' + clusters[cluster_name].running_queries.length + ' running queries');
                console.log('  cluster ' + cluster_name + ' : ' + clusters[cluster_name].servers.length + ' known servers');
            }
            break;
        case 'cluster':
            if (obj.value.name && obj.value.cluster) {
                if (obj.value.name in clusters) {
                    var cq = clusters[obj.value.name].completed_queries;
                    clusters[obj.value.name] = obj.value.cluster;
                    clusters[obj.value.name].completed_queries = cq;
                }
                else {
                    clusters[obj.value.name] = obj.value.cluster;
                    clusters[obj.value.name].completed_queries = [];
                }

                // var cn=obj.value.name;
                // var cl=obj.value.cluster;
                // console.log('client ' + clientName + 'updated cluster ' + cn);
                // console.log('  ' + cl.running_queries.length + ' running queries');
                // console.log('  ' + cl.servers.length + ' known servers');

                // TODO: logging of stalled queries
            }
            break;
        case 'completed_queries':
            if (obj.value.cluster in clusters) {
                // TODO: logging of completed queries
                var c = clusters[obj.value.cluster];
                var cq = c.completed_queries || [];
                var cq_retired = [];
                var cqs_org = cq.length;
                cq = cq.concat(obj.value.completed_queries);

                // sort by start_time / descending order
                cq.sort( function(a, b) { return b.start_time - a.start_time; } );
                if (cq.length > 100) {
                    cq_retired = cq.splice(100, cq.length-100);
                }
                c.completed_queries = cq; // because of concat(), cq is a different array obj

                console.log('rcv_cq() c:' + obj.value.cluster + ' o:' + cqs_org
                 + ' +:' + obj.value.completed_queries.length
                 + ' -:' + cq_retired.length
                 + ' s:' + cq.length);
            }
            break;
        default:
            console.log('BUG! unknown datahub client cmd', obj.type);
            break;
        }
    };

    this.getClusters = function() {
        return clusters;
    };

    this.setTimeout(0);
    this.bufferSize = 1024*8;
    this.setEncoding('UTF-8');
    this.setNoDelay(true);
    this.setKeepAlive(true);

    this.once('close', this.clientClosed.bind(this));
    this.once('end', function() {cSock.end();}); // peer ended connection
    // this.once('timeout', function() {console.log('timeout');self.destroy();});
    this.once('error', function(e) {console.log('error', e);}); // net lib에서 close()해줌. .end()나 .destroy()부를 필요가 없다.

    this.on('data', this.onData.bind(this));

    console.log('new client connection');
}

var connectionListener = function(c) {
    ClientConnection.apply(c, []);
}

var serverSocket = null;

exports.getSocketPath = function() {
    return socket_path;
}

// cb : called when socket server is ready.
exports.init = function(cb) {
    function serverListen() {
        sock = libnet.createServer(connectionListener);
        sock.maxConnections = 32;

        sock.on('error', function(e) {
            console.log('ERROR!! server listen error', e);
            if (e.code == 'EADDRINUSE') {
                console.log('Address in use, retrying...');
                setTimeout(function () {
                    serverListen();
                }, 1000);
            }
        });

        fs.unlink(socket_path);
        sock.listen(socket_path, 1024, function() {
            serverSocket = sock;
            cb();
        });
    }

    console.log('starting datahub server...');
    serverListen();
}

exports.client = ClusterSender;

exports.getClusterNames = function() {
    var tmp_clusters = [];
    for (var cid in datahub_clients) {
        var client_clusters = datahub_clients[cid].getClusters();
        for (var cluster_name in client_clusters) {
            tmp_clusters.push(cluster_name);
        }
    }
    return tmp_clusters;
}

exports.getCluster = function(cluster_name) {
    for (var cid in datahub_clients) {
        var client_clusters = datahub_clients[cid].getClusters();
        if (cluster_name in client_clusters) {
            return client_clusters[cluster_name];
        }
    }
    console.log('ERROR!! getCluster: cluster ' + cluster_name + ' not found.');
    return null;
}

exports.getRunningQueries = function(cluster_name) {
    var cluster = exports.getCluster(cluster_name);
    if (cluster) {
        return cluster.running_queries || [];
    }
    return [];
}

exports.getServers = function(cluster_name) {
    var cluster = exports.getCluster(cluster_name);
    if (cluster) {
        return cluster.servers || [];
    }
    return [];
}