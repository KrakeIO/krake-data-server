AWS             = require 'aws-sdk'
fs              = require 'fs'
Q               = require 'q'
S3              = require('aws-sdk').S3
UnescapeStream  = require 'unescape-stream'

class S3Backup
  constructor : ( aws_access_key, aws_secret, aws_region, @bucket_name )->
    AWS.config.update { 
      accessKeyId : aws_access_key
      secretAccessKey : aws_secret
      region : aws_region
    }    
    @s3bucket = new AWS.S3(
      params:
        Bucket: @bucket_name 
    )

  getS3CacheStream : ( task_key, file_name, path_to_file, content_type )->
    deferred = Q.defer()
    @cacheExist( task_key, file_name )
      .then ( cache_exists )=> # When S3 cache exists
        if cache_exists
          console.log "[S3Backup] #{new Date()} \t\tReturned download stream"      
          download_stream_obj = @getDownloadStreamObject task_key, file_name
          deferred.resolve download_stream_obj
        else
          console.log "[S3Backup] #{new Date()} \t\tGenerating S3 cache"
          download_stream_obj = @streamUpload task_key, file_name, path_to_file
          deferred.resolve download_stream_obj

    deferred.promise

  # Description:
  #   given a task_key, file_name, path_to_file, content_type
  # 
  # Params:
  #   task_key:String     - the string that matches the task_key of the data source
  #   file_name:String    - the hash of the query performed on the data source
  #   path_to_file:String - the absolute path to the file in which the temporary cache is stored
  #   content_type:String - the HTTP content type of the data to be archived on S3
  #     Supported types:
  #       "application/json; charset=utf-8", "text/csv; charset=utf-8", "text/html; charset=utf-8"
  # 
  # Returns:      
  #   Promise:Object
  #     Success:Function 
  #       
  #
  streamUpload: ( task_key, file_name, path_to_file )->
    deferred = Q.defer()
    unescape = new UnescapeStream()

    body = fs.createReadStream( path_to_file ).pipe(unescape);
    s3obj = new AWS.S3(
      params: 
        Bucket: @bucket_name
        Key: task_key + "/" + file_name 
    )

    s3obj.upload({ Body: body })
      .on('httpUploadProgress', (evt) => 
        console.log(evt)

      ).send( (err, data) => 
        console.log(err, data) 
        download_stream_obj = @getDownloadStreamObject( task_key, file_name  )
        deferred.resolve download_stream_obj
      )

    deferred.promise

  getDownloadStreamObject: ( task_key, file_name )->
    params =
      Key: task_key + "/" + file_name

    @s3bucket.getObject(
      params
    ).createReadStream()

  # Description:
  #   given a task_key and file_name checks if the corresponding S3 object exists
  # 
  # Param:
  #   task_key:String     - the string that matches the task_key of the data source
  #   file_name:String    - the hash of the query performed on the data source  
  # 
  # Returns:      
  #   Promise:Object
  #
  cacheExist: ( task_key, file_name )->
    deferred = Q.defer()
    params =
      Key: task_key + "/" + file_name

    @s3bucket.headObject( 
      params, 
      (err, res) ->
        if (err)
          console.log "it does not exist"
          deferred.resolve false
        else
          console.log "it exists"
          deferred.resolve true
    )
    deferred.promise

module.exports = S3Backup

if !module.parent

  access_key  = process.env['AWS_ACCESS_KEY']
  secret_key  = process.env['AWS_SECRET_KEY']
  region      = process.env['AWS_S3_REGION'] 
  bucket_name = process.env['AWS_S3_BUCKET']

  sb = new S3Backup( access_key, secret_key, region, bucket_name )
  # sb.getS3CacheStream(
  #   "n468_4e93c2fc0dd9aceadf57e0756571232aeses", 
  #   "n468_4e93c2fc0dd9aceadf57e0756571232aeses_664df84dc3b97df6dac430ab49fa5e09.json", 
  #   "/tmp/krake_data_cache/n468_4e93c2fc0dd9aceadf57e0756571232aeses_664df84dc3b97df6dac430ab49fa5e09.json", 
  #   "application/json; charset=utf-8"
  # ).then ( s3_down_stream )->
  #   console.log("Printing out the object that we sent to S3")
  #   s3_down_stream.pipe(process.stdout)
  task_key = "n473_35c2e0826e57195a24ed01752436b65eeses"
  file_name = "n473_35c2e0826e57195a24ed01752436b65eeses_423a711741650c2749769f9c295950a1.json"
  path_to_file = "/Users/garyjob/Applications/krake_data/helpers/batch_test_file.json"
  content_type = "json"

  sb.streamUpload( task_key, file_name, path_to_file )
    .then ( s3_down_stream )=>
      console.log "[CacheController] #{new Date()} \t\treturning generated S3 cache stream "

    .catch ( err )=>
      console.log "[CacheController] #{new Date()} \t\terror occurred generating s3 cache "
