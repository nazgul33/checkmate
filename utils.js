var ___str___ = require('./string.js');

exports.extend = (function() {
    var toString = Object.prototype.toString,
    obj = '[object Object]';

    return function extend( deep /*, obj1, obj2, obj3 */ ) {
        // take first argument, if its not a boolean
        var args = arguments,
        i = deep === true ? 1 : 0,
                key = null,
                target = args[i];

        for ( ++i; i < args.length; ++i ) {
            for (key in args[i]) {
                if ( deep === true &&
                        target[key] &&
                        // if not doing this check you may end in
                        // endless loop if using deep option
                        toString.call(args[i][key]) === obj &&
                        toString.call(target[key]) === obj ) {

                    extend( deep, target[key], args[i][key] );
                } else {
                    target[key] = args[i][key];
                }
            }
        }

        return target;
    };
})();

if (typeof String.prototype.endsWith !== 'function') {
    String.prototype.endsWith = function(suffix) {
        return this.indexOf(suffix, this.length - suffix.length) !== -1;
    };
}