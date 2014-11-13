/* DataBuffer */

exports.StringBuffer = function(data) {
    var buf = data || '';

    this.get = function() {
        return buf;
    };

    this.append = function(data) {
        if (typeof data == 'undefined' || data.toString().length <= 0) {
            console.log('WARNING!! empty data fed.');
            return false;
        }

        buf += data;

        if (buf.length + data.toString().length > 1024*1024) {
            console.log('WARNING!! DataBuffer getting too large (>1MB).');
            return false;
        }

        return true;
    };

    this.getLine = function() {
        var idx = buf.indexOf('\n\n');
        if (idx < 0) return null;
        var line = buf.substr(0, idx+1);
        buf = buf.substr(idx+1);
        return line;
    };

    this.runLine = function(cb) {
        while (1) {
            var line = this.getLine();
            if (line === null)
                return true;

            try{
                if (cb) cb(line);
            }catch(e) {
                return false;
            }
        }
    };

    this.getObj = function() {
        var line = this.getLine();
        if (line === null)
            return null;
        try{
            return JSON.parse(line.trim());
        }catch(e) {
            return false;
        }
    };

    this.runObj = function(cb) {
        while (1) {
            var obj = this.getObj();
            if (obj === null)
                return true;
            else if (!obj)
                return false;

            try{
                if (cb) cb(obj);
            }catch(e) {
                return false;
            }
        }
    };
}

