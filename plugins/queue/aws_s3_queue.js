// aws_s3_queue

// documentation via: haraka -c /Users/thihara/workspace/haraka_veritone -h plugins/aws_s3_queue.timeout

// Put your plugin code here
// type: `haraka -h Plugins` for documentation on how to create a plugin

var AWS = require("aws-sdk"), zlib = require("zlib"),
    util = require('util'), async = require("async"), Transform = require('stream').Transform;

exports.register = function () {
    this.logdebug("Initializing AWS S3 Queue");

    var config = this.config.get("aws_s3_queue.json");
    this.logdebug("Config loaded : "+util.inspect(config));

    AWS.config.update({
        accessKeyId: config.accessKeyId, secretAccessKey: config.secretAccessKey, region: config.region
    });
    
    this.s3Bucket = config.s3Bucket;
    
    this.zipBeforeUpload = config.zipBeforeUpload;
    this.fileExtension = config.fileExtension;
    this.copyAllAddresses = config.copyAllAddresses;
    this.validDomains = config.validDomains;
};

exports.hook_queue = function (next, connection) {
    var plugin = this;

    var transaction = connection.transaction;
    var emailTo = transaction.rcpt_to;
    
    var gzip = zlib.createGzip();
    var transformer = plugin.zipBeforeUpload ? gzip : new TransformStream();
    var body = transaction.message_stream.pipe(transformer);
    
    var s3 = new AWS.S3();
    
    var addresses = plugin.copyAllAddresses ? transaction.rcpt_to : transaction.rcpt_to[0];

    async.each(addresses, function (address, eachCallback) {
        var key = address.user + "@" + address.host + "/" + transaction.uuid + plugin.fileExtension;

        var params = {
            Bucket: plugin.s3Bucket,
            Key: key,
            Body: body
        };

        s3.upload(params).on('httpUploadProgress', function (evt) {
            plugin.logdebug("Uploading file... Status : " + util.inspect(evt));
        }).send(function (err, data) {
            plugin.logdebug("S3 Send response data : " + util.inspect(data));
            eachCallback(err);
        });
    }, function (err) {
        if (err) {
            plugin.logerror(err);
            next();
        } else {
            next(OK, "Email Accepted.");
        }
    });
};

exports.shutdown = function () {
    this.loginfo("Shutting down queue plugin.");
};

//Dummy transform stream to help with a haraka issue. Can't use message_stream as a S3 API parameter without this.
var TransformStream = function() {
    Transform.call(this);
};
util.inherits(TransformStream, Transform);

TransformStream.prototype._transform = function(chunk, encoding, callback) {
    this.push(chunk);
    callback();
};