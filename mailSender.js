var nodemailer = require('nodemailer');
var smtpTransport = require('nodemailer-smtp-transport');

function sendMail(recipients, subject, html, text) {
    var options = {
        host: '10.40.30.85',
        port: 25,
        connectionTimeout: 5000
    };

    var tr = nodemailer.createTransport( smtpTransport( options ) );
    // var tr = nodemailer.createTransport( options );

    for (var idx = 0; idx < recipients.length; idx++) {
        var mailop = {
            from: 'CheckMate <checkmate@sk.com>',
            to: recipients[idx],
            subject: subject,
            html: html? html:'',
            text: text? text:'',
        };
        tr.sendMail(mailop, function(error, info) {
            if (error) {
                console.log('sendMail returned error', error);
            }
            else {
                console.log('mail sent :' + info.response);
            }
        });
    }
}

module.exports = function(recipients, subject, html, text) {
    sendMail(recipients, subject, html, text);
}

// sendMail(['steven.han@sk.com'], 'test subject', '<p>test html</p>', 'test text');
