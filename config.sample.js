var ml = [ 'test1@my.domain.com', 'test2@my.domain.com'];
var impala_config = {
    test1: {
        impala_version:'1.4.1',
        statestored_hostname:"tm001",
        statestored_web_port:25010,
        subscribers_update_interval:60000,
        impalad_web_port:25000,
        impalad_update_interval:30000,
        mail_recipients:ml,
        sms_recipients:null },
    test2: {
        impala_version:'2.0.0',
        statestored_hostname:"m002",
        statestored_web_port:25010,
        subscribers_update_interval:60000,
        impalad_web_port:25000,
        impalad_update_interval:30000,
        mail_recipients:ml,
        sms_recipients:null },
};

exports.impala = impala_config;
