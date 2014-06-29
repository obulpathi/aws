import boto
from boto.s3 import *
from boto.s3.connection import *
from boto.exception import S3ResponseError
from boto.services.start_service import find_class
from boto.services.start_service import get_userdata

## Comment-in this method to test the script outside of EC2
#def get_userdata(params):
#  params['module_name'] = '@my-bucket,MultiCommandService.py,MultiCommandService'
#  params['class_name'] = 'MultiCommandService'


# Download Service implementation from S3
def download_service(service_in_s3, data):
  # Parse bucket name, object key, and target module file
  paths = service_in_s3.split(',')
  bucket_name = paths[0]
  key = paths[1]
  module_name = paths[2]
  out_filename = module_name + '.py'

  print 'Downloading service implementation from S3: %s/%s => %s' % \
    (bucket_name, key, out_filename)

  successful = False
  num_tries = 0

  # Download service module from S3
  s3_conn = S3Connection(data['aws_access_key_id'], \
                         data['aws_secret_access_key'])
  bucket = s3_conn.create_bucket(bucket_name)
  k = Key(bucket)
  k.key = key
  while not successful and num_tries < 5:
    try:
      num_tries += 1
      k.get_contents_to_filename(out_filename)
      successful = True
    except S3ResponseError, e:
      print 'Failed to download service from S3: \n%s' % e
      time.sleep(5)

  # Local name of downloaded service module
  return module_name


def main():
  print 'Bootstrapping service'

  # Default paramaters, will be overridden by EC2 User Data values
  params = {'module_name' : 'MultiCommandService',
        'class_name' : 'MultiCommandService',
        'notify_email' : None,
        'input_queue_name' : None,
        'output_queue_name' : None,
        'log_queue_name' : None,
        'working_dir' : None,
        'keypair' : None,
        'on_completion' : 'shutdown',
        'ami' : None}

  # Obtain startup information from EC2 User Data
  get_userdata(params)

  # If module is remote, download from S3
  if params['module_name'][0] == '@':
    s3_path = params['module_name'][1:]
    params['module_name'] = download_service(s3_path, params)

  # Create service object and run it
  print 'Loading service class %s in module %s' % \
    (params['class_name'], params['module_name'])
  cls = find_class(params)
  s = cls()
  s.run()


if __name__ == "__main__":
  main()
