var HTTP = require('http');
var URL = require('url');
var Connect = require('connect');
var assetManager = require('connect-assetmanager');
var assetHandler = require('connect-assetmanager-handlers');
var Utils = require('./utils.js');

var assetManagerGroups = {
    'iphc.js': {
        'route': /\/static\/js\/iphc\.js/,
        'path': __dirname+'/public/',
        'dataType': 'javascript',
        'files': [
            'js/jquery-1.11.1.min.js',
            'bootstrap/js/bootstrap.min.js',
            'bootstrap/js/bootstrap-select.min.js',
        ]
    },
    'iphc.css': {
        'route': /\/static\/css\/iphc\.css/,
        'path': __dirname+'/public/',
        'dataType': 'css',
        'files': [
            'bootstrap/css/bootstrap.min.css',
            'bootstrap/css/bootstrap-select.min.css',
            'css/iphc.css',
        ],
        'preManipulate': {
            // Regexp to match user-agents including MSIE.
            'MSIE': [
                assetHandler.yuiCssOptimize
                , assetHandler.fixVendorPrefixes
                , assetHandler.fixGradients
                , assetHandler.stripDataUrlsPrefix
            ],
            // Matches all (regex start line)
            '^': [
                assetHandler.yuiCssOptimize
                , assetHandler.fixVendorPrefixes
                , assetHandler.fixGradients
                //, assetHandler.replaceImageRefToBase64(root)
            ]
        }
    }
};

var assetsManagerMiddleware = assetManager(assetManagerGroups);

var serveStatic = require('serve-static');
var bodyParser = require('body-parser');
var connectTimeout = require('connect-timeout');
var cookieParser = require('cookie-parser');
var ModuleImpala = require('./webserver_impala.js');
//var qs = require('qs');
//var serve_favicon = require('serve-favicon');

var api_version = 'v1'

var app = Connect()
// .use(serve_favicon('path to favicon.ico'))
// .use(Connect.logger('dev'))
.use(assetsManagerMiddleware)
.use(serveStatic(__dirname+'/public'))
.use(bodyParser.urlencoded({extended: true}))
.use(connectTimeout(1000*10))
.use(bodyParser.json())
.use(cookieParser())
.use(function(req, res) {
    var spec = null;
    try{
        spec = URL.parse(req.url);
        var path = (spec.pathname || '/').split('/');
        if (path && path.length > 0) path.shift();

        req.query = Utils.extend({}, true, req.query || {}, req.body || {});

        res.setHeader('Content-Type', 'application/json');
        if (path[0] != 'api') {
            return res.end('{"type":"error", "msg":"api path error"}');
        }
        if (path[1] != api_version) {
            return res.end('{"type":"error", "msg":"api version error"}');
        }
        var p = path.slice(3);
        switch (path[2]) {
            case 'impala':
                ModuleImpala.pass(p, req, res);
                break;
            default:
                res.end('{"type":"error", "msg":"api path error (unknown module ' + path[2] + ')"}');
                break;
        }
    }catch(e) {
        console.log(spec, req.query);
        process.stdout.write(e.stack);
        res.statusCode = 500;
        //res.setHeader('Content-Type', 'application/json');
        res.end('<html><head><title>Internal Error</title></head><body><H1>Internal Error</H1><div style="white-space:pre-wrap">' + e.stack + '</div></body>');
    }
});

exports.init = function(server_port) {
    HTTP.createServer(app).listen(server_port);
}
