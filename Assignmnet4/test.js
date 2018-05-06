const AWS = require('aws-sdk');
const s3 = new AWS.S3();
const fs = require("fs")

//Function for processing/cleaning a main data file on load to an S3 bucket
exports.handler = (event, context) => {
    //basic s3 connection object
    let s3 = new AWS.S3();
    //variable to extract the key of the triggering object
    let EventObject = event.Records[0].s3.object.key;
    //variable to extract the name of the bucket the triggering object is in
    let EventBucket = event.Records[0].s3.bucket.name;
    //bucket and object parameters
    let eventParams = {Bucket: EventBucket, Key: EventObject};
    //Set up the read stream to connect to the given object
    let stream = s3.getObject(eventParams).createReadStream();
    //Set up the bucket to write to
    var writeBucket = "governetdataclean";
    //Set up the key to write to
    var writeKey = "testing";
    //create the 'chunks' list, which we'll fill with the chunks of the downloaded object at it comes in
    let chunks = [];
    //when data hits the readstream, add it to the chunks list
    stream.on('data', (chunk) => {
        chunks.push(chunk) 
    });
    //when the stream is completed, concatonate the chunks together and return as a string, replacing commas with tabs
    stream.on('end', () => { 
        //concatonate the buffered data to a string, and process it by repalcing the pipes with tabs
        var data = ((Buffer.concat(chunks).toString()).replace(/\|/g,"\t"))
        //set up the params for the given item you want to write
        var writeParams = {
            Bucket: writeBucket, 
            Key: writeKey, 
            Body: data }
        //write the object to the bucket
        s3.putObject(writeParams, function(err, data) {
            if (err) console.log(err, err.stack); // an error occurred
            else context.done(null, data) 
        });
    });
};
