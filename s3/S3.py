#!/usr/bin/env python
"""
Sample Python code for the O'Reilly book "Using AWS Infrastructure Services"
by James Murty.

This code was written for Python version 2.4.4 or greater, and requires the
XML/XPath library "lxml" version 1.3.6 or greater (see
http://codespeak.net/lxml/).

The S3 module implements the REST API of the Amazon Simple Storage Service.
"""

from AWS import AWS
import re
import urllib, urlparse
import types
import os
import md5, base64
import time
import string

class S3(AWS):

  S3_ENDPOINT = 's3.amazonaws.com'
  XML_NAMESPACE = 'http://s3.amazonaws.com/doc/2006-03-01/'

  def is_valid_dns_name(self, bucket_name):
    # Ensure bucket name is within length constraints
    if not bucket_name or len(bucket_name) > 63 or len(bucket_name) < 3:
      return False

    # Only lower-case letters, numbers, '.' or '-' characters allowed
    if not re.search(r'^[a-z0-9][a-z0-9.-]+$', bucket_name): return False

    # Cannot be an IP address (must contain at least one lower-case letter)
    if not re.search(r'.*[a-z].*', bucket_name): return False

    # Components of name between '.' characters cannot start or end with
    # '-', and cannot be empty
    for fragment in bucket_name.split('.'):
      if (re.search(r'^-.*', fragment) or re.search(r'.*-$', fragment) or
          len(fragment) == 0): return False

    return True


  def generate_s3_url(self, bucket_name='', object_key='', parameters={}):
    # Decide between the default and sub-domain host name formats
    if self.is_valid_dns_name(bucket_name):
      hostname = "%s.%s" % (bucket_name, self.S3_ENDPOINT)
    else:
      hostname = self.S3_ENDPOINT

    # Build an initial secure or non-secure URI for the end point.
    if self.secure_http:
      url = 'https://' + hostname
    else:
      url = 'http://' + hostname

    # Include the bucket name in the URI except for alternative hostnames
    if len(bucket_name) > 0 and hostname == self.S3_ENDPOINT:
      url += '/' + urllib.quote(bucket_name)

    # Add object name component to URI if present
    if len(object_key) > 0:
      url += '/' + urllib.quote(object_key)

    # Ensure URL includes at least a slash in the path, if nothing else
    path = urlparse.urlparse(url)[2]
    if (len(path) == 0):
      url += "/";

    # Add request parameters to the URI.
    query = ''
    for param_name in parameters:
      if len(query) > 0: query += '&'

      if not parameters[param_name]:
        query += param_name
      else:
        query += param_name + '=' + \
          urllib.quote(str(parameters[param_name]))

    if len(query) > 0: url += '?' + query

    return url


  def list_buckets(self):
    url = self.generate_s3_url()
    response = self.do_rest('GET', url)
    xmldoc = self.parse_xml(response.read())

    buckets = []
    for node in self.xpath_list('//ns:Buckets/ns:Bucket', xmldoc):
      buckets.append({
        'name': self.xpath_value('ns:Name', node),
        'creation_date': self.xpath_value(u'ns:CreationDate', node)})

    return {
      'owner_id': self.xpath_value(u'//ns:Owner/ns:ID', xmldoc),
      'display_name': self.xpath_value(u'//ns:DisplayName', xmldoc),
      'buckets': buckets}


  def create_bucket(self, bucket_name, bucket_location='US'):
    url = self.generate_s3_url(bucket_name)

    if bucket_location != 'US':
      # Build XML document to set bucket's location
      config_xml_text = \
        "<CreateBucketConfiguration xmlns='" + self.XML_NAMESPACE + "'>" + \
          "<LocationConstraint>" + \
            bucket_location + \
          "</LocationConstraint>" + \
        "</CreateBucketConfiguration>"

      # Send a PUT request with the configuration XML
      response = self.do_rest('PUT', url, config_xml_text,
                              {'Content-Type': 'application/xml'})
    else:
      # If the bucket is located in the US, no configuration is required
      response = self.do_rest('PUT', url)

    return True


  def delete_bucket(self, bucket_name):
    url = self.generate_s3_url(bucket_name)
    response = self.do_rest('DELETE', url)
    return True


  def get_bucket_location(self, bucket_name):
    url = self.generate_s3_url(bucket_name, parameters={'location': None})
    response = self.do_rest('GET', url)
    xmldoc = self.parse_xml(response.read())
    return self.xpath_value('//ns:LocationConstraint', xmldoc)


  def list_objects(self, bucket_name, parameters={}):
    is_truncated = True

    objects = []
    prefixes = []

    while (is_truncated):
      url = self.generate_s3_url(bucket_name, parameters=parameters)
      response = self.do_rest('GET', url)
      xmldoc = self.parse_xml(response.read())

      for node in self.xpath_list('//ns:Contents', xmldoc):
        objects.append({
          'key': self.xpath_value('ns:Key', node),
          'size': self.xpath_value('ns:Size', node),
          'last_modified': self.xpath_value('ns:LastModified', node),
          'etag': self.xpath_value('ns:ETag', node),
          'owner_id': self.xpath_value('ns:Owner/ns:ID', node),
          'owner_name': self.xpath_value('ns:Owner/ns:DisplayName', node)})

      for node in self.xpath_list('//ns:CommonPrefixes/ns:Prefix', xmldoc):
        prefixes.append(node.text)

      # Determine whether listing is truncated
      is_truncated = self.xpath_value('//ns:IsTruncated', xmldoc) == 'true'

      # Set the marker parameter to the NextMarker if it is available,
      # otherwise set it to the last key name in the listing
      if self.xpath_value('//ns:NextMarker', xmldoc):
        parameters['marker'] = self.xpath_value('//ns:NextMarker', xmldoc)
      elif self.xpath_value('//ns:Contents/ns:Key', xmldoc):
        # The lxml XPath implementation doesn't support the 'last()'
        # function, so we must manually find the last Key.
        parameters['marker'] = objects[len(objects) - 1]['key']
      else:
        parameters['marker'] = None

    return {'bucket_name': bucket_name, 'objects': objects,
            'prefixes': prefixes}


  def create_object(self, bucket_name, object_key, data, headers={},
                    metadata={}, policy='private'):
    # The Content-Length header must always be set when data is uploaded.
    if not data:
      headers['Content-Length'] = '0'
    elif type(data) == types.FileType:
      headers['Content-Length'] = os.stat(data.name)[6]
    else:
      headers['Content-Length'] = len(data)

    # Calculate an md5 hash of the data for upload verification
    if data:
      if type(data) == types.FileType:
        m = md5.new()
        bytes = data.read(8192)
        while len(bytes) > 0:
          m.update(bytes)
          bytes = data.read(8192)
        data.seek(0) # Reset file's read position
      else:
        m = md5.new(data)
      headers['Content-MD5'] = base64.b64encode(m.digest())

    # Set the canned policy, may be: 'private', 'public-read',
    # 'public-read-write', 'authenticated-read'
    headers['x-amz-acl'] = policy

    # Set an explicit content type if none is provided
    if not headers.has_key('Content-Type'):
      headers['Content-Type'] = 'application/octet-stream'

    # Convert metadata items to headers using the S3 metadata
    # header name prefix.
    for metadata_name in metadata:
      headers['x-amz-meta-%s' % metadata_name] = metadata[metadata_name]

    url = self.generate_s3_url(bucket_name, object_key)
    response = self.do_rest('PUT', url, data, headers)
    return True


  def delete_object(self, bucket_name, object_key):
    url = self.generate_s3_url(bucket_name, object_key)
    response = self.do_rest('DELETE', url)
    return True


  def get_object(self, bucket_name, object_key, headers={}, out_file=None):
    url = self.generate_s3_url(bucket_name, object_key)
    response = self.do_rest('GET', url, headers=headers)

    metadata = {}
    headers = {}

    # Find metadata headers.
    response_headers = dict(response.getheaders())
    for header_name in response_headers:
      if re.search(r'^x-amz-meta', header_name):
        metadata[header_name[11:]] = response_headers[header_name]

    object = {
      'key': object_key,
      'etag': response_headers['etag'],
      'last_modified': response_headers['last-modified'],
      'size': response_headers['content-length'],
      'metadata': metadata
    }

    if out_file:
      # Download data
      bytes = response.read(8192)
      while len(bytes) > 0:
        out_file.write(bytes)
        bytes = response.read(8192)
      out_file.close()
    else:
      object['body'] = response.read()

    return object


  def get_object_metadata(self, bucket_name, object_key, headers={}):
    url = self.generate_s3_url(bucket_name, object_key)
    response = self.do_rest('HEAD', url, headers=headers)

    metadata = {}
    headers = {}

    # Find metadata headers.
    response_headers = dict(response.getheaders())
    for header_name in response_headers:
      if re.search(r'^x-amz-meta', header_name):
        metadata[header_name[11:]] = response_headers[header_name]

    object = {
      'key': object_key,
      'etag': response_headers['etag'],
      'last_modified': response_headers['last-modified'],
      'size': response_headers['content-length'],
      'metadata': metadata
    }

    return object


  def get_logging(self, bucket_name):
    url = self.generate_s3_url(bucket_name, parameters={'logging': None})
    response = self.do_rest('GET', url)
    xmldoc = self.parse_xml(response.read())

    if self.xpath_list('//ns:LoggingEnabled', xmldoc):
      return {
        'target_bucket': self.xpath_value('//ns:TargetBucket', xmldoc),
        'target_prefix': self.xpath_value('//ns:TargetPrefix', xmldoc)}
    else:
      return None


  def set_logging(self, bucket_name, target_bucket, target_prefix=''):
    url = self.generate_s3_url(bucket_name, parameters={'logging': None})

    logging_xml_text = "<BucketLoggingStatus xmlns='" + self.XML_NAMESPACE + "'>"
    if target_bucket:
      logging_xml_text += \
        "<LoggingEnabled>" + \
            "<TargetBucket>" + target_bucket + "</TargetBucket>" + \
            "<TargetPrefix>" + target_prefix + "</TargetPrefix>" + \
        "</LoggingEnabled>"
    logging_xml_text += "</BucketLoggingStatus>";

    # Send a PUT request with the logging XML
    response = self.do_rest('PUT', url, logging_xml_text,
                            {'Content-Type': 'application/xml'})
    return True


  def get_acl(self, bucket_name, object_key=''):
    url = self.generate_s3_url(bucket_name, object_key, parameters={'acl': None})
    response = self.do_rest('GET', url)
    xmldoc = self.parse_xml(response.read())

    grants = []

    for node in self.xpath_list('//ns:Grant', xmldoc):
      grantee_node = self.xpath_list('ns:Grantee', node)[0]

      grant = {'type':
        grantee_node.get('{http://www.w3.org/2001/XMLSchema-instance}type'),
        'permission': self.xpath_value('ns:Permission', node)}
      if grant['type'] == 'Group':
        grant['uri'] = self.xpath_value('ns:URI', grantee_node)
      else:
        grant['id'] = self.xpath_value('ns:ID', grantee_node)
        grant['display_name'] = self.xpath_value('ns:DisplayName', grantee_node)

      grants.append(grant)

    return {
      'owner_id': self.xpath_value('//ns:Owner/ns:ID', xmldoc),
      'owner_name': self.xpath_value('//ns:Owner/ns:DisplayName', xmldoc),
      'grants': grants}


  def set_acl(self, owner_id, bucket_name, object_key='', grants=[]):
    url = self.generate_s3_url(bucket_name, object_key, parameters={'acl': None})

    acl_xml_text = \
      "<AccessControlPolicy xmlns='" + self.XML_NAMESPACE + "'>" + \
        "<Owner><ID>" + owner_id + "</ID></Owner>" + \
        "<AccessControlList>"
    for grantee_id, permission in grants:
      acl_xml_text += \
          "<Grant>" + \
            "<Permission>" + permission + "</Permission>" + \
            "<Grantee xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' "
      # Grantee may be of type email, group, or canonical user
      if re.search(r'@', grantee_id):
        # Email grantee
        acl_xml_text += "xsi:type='AmazonCustomerByEmail'>" + \
              "<EmailAddress>" + grantee_id + "</EmailAddress>"
      elif re.search(r'://', grantee_id):
        # Group grantee
        acl_xml_text += "xsi:type='Group'>" + \
              "<URI>" + grantee_id + "</URI>"
      else:
        # Canonical user grantee
        acl_xml_text += "xsi:type='CanonicalUser'>" + \
              "<ID>" + grantee_id + "</ID>"
      acl_xml_text += "</Grantee></Grant>"

    acl_xml_text += "</AccessControlList></AccessControlPolicy>"

    # Send a PUT request with the ACL XML
    response = self.do_rest('PUT', url, acl_xml_text,
                            {'Content-Type': 'application/xml'})
    return True


  def set_canned_acl(self, policy, bucket_name, object_key=''):
    url = self.generate_s3_url(bucket_name, object_key, parameters={'acl': None})
    # Send a PUT request with the policy in an x-amz-acl header
    response = self.do_rest('PUT', url, headers={'x-amz-acl': policy})
    return True


  def get_torrent(self, bucket_name, object_key, out_file):
    url = self.generate_s3_url(bucket_name, object_key, parameters={'torrent': None})
    response = self.do_rest('GET', url)

    # Download torrent file data
    bytes = response.read(8192)
    while len(bytes) > 0:
      out_file.write(bytes)
      bytes = response.read(8192)
    out_file.close()
    return True


  def get_signed_uri(self, method, expires, bucket_name, object_key,
                     parameters={}, headers={}, is_virtual_host=False):
    headers['Date'] = str(expires)
    url = self.generate_s3_url(bucket_name, object_key, parameters)
    signature = self.generate_rest_signature(method, url, headers)

    if self.secure_http:
      signed_uri = 'https://'
    else:
      signed_uri = 'http://'

    scheme, host, path, params, query, fragment = urlparse.urlparse(url)

    if is_virtual_host:
      signed_uri += bucket_name
    else:
      signed_uri += host

    parameters['Signature'] = signature
    parameters['Expires'] = str(expires)
    parameters['AWSAccessKeyId'] = self.aws_access_key

    signed_uri += path + '?'
    if query: signed_uri += query + '&'

    signed_uri += \
      "Signature=" + urllib.quote(signature) + \
      "&Expires=" + str(expires) + \
      "&AWSAccessKeyId=" + self.aws_access_key

    return signed_uri


  def build_post_policy(self, expiration_time, conditions):
    if type(expiration_time) != time.struct_time:
      raise 'Policy document must include a valid expiration Time object'
    if type(conditions) != types.DictionaryType:
      raise 'Policy document must include a valid conditions Hash object'

    # Convert conditions object mappings to condition statements
    conds = []
    for name in conditions:
      test = conditions[name]

      if not test:
        # A nil condition value means allow anything.
        conds.append('["starts-with", "$%s", ""]' % name)
      elif type(test) == types.StringType:
        conds.append('{"%s": "%s"}' % (name, test))
      elif type(test) == types.ListType:
        conds.append('{"%s": "%s"}' % (name, string.join(test, ',')))
      elif type(test) == types.DictionaryType:
        operation = test['op']
        value = test['value']
        conds.append('["%s", "$%s", "%s"]' % (operation, name, value))
      elif type(test) == types.SliceType:
        conds.append('["%s", %i, %i]' % (name, test.start, test.stop))
      else:
        raise 'Unexpected value type for condition "%s": %s' % \
          (name, type(test))

    return '{"expiration": "%s",\n"conditions": [%s]}' % \
      (time.strftime(self.ISO8601, expiration), string.join(conds, ','))


  def build_post_form(self, bucket_name, key, expiration = None,
                      conditions = None, extra_fields = {}, text_input = None):
    fields = []

    # Form is only authenticated if a policy is specified.
    if expiration or conditions:
      # Generate policy document
      policy = self.build_post_policy(expiration, conditions)
      if self.debug_mode:
        print 'POST Policy\n===========\n%s\n\n' % policy

      # Add the base64-encoded policy document as the 'policy' field
      policy_b64 = base64.b64encode(policy)
      fields.append('<input type="hidden" name="policy" value="%s">' % policy_b64)

      # Add the AWS access key as the 'AWSAccessKeyId' field
      fields.append('<input type="hidden" name="AWSAccessKeyId" value="%s">' % \
                    self.aws_access_key)

      # Add signature for encoded policy document as the 'AWSAccessKeyId' field
      signature = self.generate_signature(policy_b64)
      fields.append('<input type="hidden" name="signature" value="%s">' % signature)

    # Include any additional fields
    print 'Fields: %s' % extra_fields
    for n in extra_fields:
      v = extra_fields[n]

      if not v:
        # Allow users to provide their own <input> fields as text.
        fields.append(n)
      else:
        fields.append('<input type="hidden" name="%s" value="%s">' % (n, v))

    # Add the vital 'file' input item, which may be a textarea or file.
    if text_input:
      # Use the text_input option which should specify a textarea or text
      # input field. For example:
      # '<textarea name="file" cols="80" rows="5">Default Text</textarea>'
      fields.append(text_input)
    else:
      fields.append('<input name="file" type="file">')

    # Construct a sub-domain URL to refer to the target bucket. The
    # HTTPS protocol will be used if the secure HTTP option is enabled.
    if self.secure_http:
      url = 'https'
    else:
      url = 'http'
    url += '://%s.s3.amazonaws.com/' % bucket_name

    # Construct the entire form.
    form = """
      <form action="%s" method="post" enctype="multipart/form-data">
        <input type="hidden" name="key" value="%s">
        %s
        <br>
        <input type="submit" value="Upload to Amazon S3">
      </form>
    """ % (url, key, string.join(fields, '\n'))

    if self.debug_mode:
      print 'POST Form\n=========\n%s\n' % form

    return form



if __name__ == '__main__':
  s3 = S3(debug_mode=True)

  # print s3.list_buckets()

  # print s3.create_bucket('test.location', 'EU')

  # print s3.get_bucket_location('test.location')

  # print s3.delete_bucket('test.location')

  # print s3.list_objects('using-aws', {'delimiter': '/'})

  # print s3.create_object('using-aws', 'Testing/WebPage.html',
  #            data = '<html><body>This is a <b>Web Page</b></body></html>',
  #            # data = open('/path/to/WebPage.html', 'rb'),
  #            headers = {'Content-Type': 'text/html'},
  #            metadata = {'Description': 'this is just a description'})

  #
  # get_object as string (content is available in the body dictionary item)
  # print s3.get_object("using-aws", "WebPage.html")

  # get_object to a File
  # file = open('/path/to/WebPage.html', 'wb')
  # print s3.get_object("using-aws", "WebPage.html", out_file = file)

  # print s3.get_object_metadata("using-aws", "WebPage.html")

  # print s3.delete_object("using-aws", "WebPage.html")

  # print s3.get_logging('using-aws')

  # print s3.set_logging('using-aws', 'using-aws')

  # print s3.get_acl('using-aws', 'WebPage.html')

  # grants = [
  #  ('your_email@address.com','FULL_CONTROL'),
  #  ('http://acs.amazonaws.com/groups/global/AllUsers','READ')]
  # print s3.set_acl(
  #  'your_s3_owner_id',
  #  'using-aws', 'WebPage.html', grants)

  # print s3.set_canned_acl('public-read', 'using-aws', 'WebPage.html')

  # file = open('/path/to/WebPage.html.torrent', 'wb')
  # s3.get_torrent('using-aws', 'WebPage.html', file)

  # params = {'acl': None}
  # time_in_thirty_secs = int(time.time()) + 30
  # print s3.get_signed_uri('GET', time_in_thirty_secs, 'using-aws',
  #                         'WebPage.html', params, {}, False)

  #fields = {
  #  'acl' : 'public-read',
  #  'Content-Type' : 'image/jpeg',
  #  'success_action_redirect' : 'http://localhost/post_upload'
  #  }
  #
  #conditions = {
  #  'bucket' : 'using-aws',
  #  'key' : 'uploads/images/pic.jpg',
  #  'acl' : 'public-read',
  #  'Content-Type' : 'image/jpeg',
  #  'success_action_redirect' : 'http://localhost/post_upload',
  #  'content-length-range' : slice(10240, 204800)
  #}
  #expiration = time.gmtime(time.time() + 60 * 5)  # Policy expires in 5 minutes
  #
  #print s3.build_post_form('using-aws', 'uploads/images/pic.jpg',
  #                         expiration, conditions, fields)
