exports.getopts = function(opt) {
	return require('node-getopt').create(opt).bindHelp().parseSystem();
};
