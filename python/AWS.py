#!/usr/bin/env python
"""
Sample Python code for the O'Reilly book "Using AWS Infrastructure Services"
by James Murty.

This code was written for Python version 2.4.4 or greater, and requires the
XML/XPath library "lxml" version 1.3.6 or greater (see
http://codespeak.net/lxml/).

The AWS module includes HTTP messaging and utility methods that handle
communication with Amazon Web Services' REST or Query APIs. Service
client implementations are built on top of this module.
"""

import os, string
import hmac, sha
import base64
import time, calendar
import httplib, urllib, urlparse
import re
import types

# XML handling is performed using the lxml library
from lxml import etree

class AWS:
  ISO8601 = '%Y-%m-%dT%H:%M:%SZ'
  RFC822 = '%a, %d %b %Y %H:%M:%S GMT'

  # Your Amazon Web Services Access Key credential.
  aws_access_key = None

  # Your Amazon Web Services Secret Key credential.
  aws_secret_key = None

  # Enable debugging messages? When this value is true, debug logging
  # messages describing AWS communication messages are printed to standard
  # output.
  debug_mode = False

  # Use only the Secure HTTP protocol (HTTPS)? When this value is true, all
  # requests are sent using HTTPS. When this value is false, standard HTTP
  # is used.
  secure_http = True

  # The approximate difference in the current time between your computer and
  # Amazon's servers, measured in seconds.
  #
  # This value is 0 by default. Use the current_time() method to obtain the
  # current time with this offset factor included, and the adjust_time()
  # method to calculate an offset value for your computer based on a
  # response from an AWS server.
  time_offset = 0

  # Initialize AWS and set the service-specific variables: aws_access_key,
  # aws_secret_key, debug_mode, and secure_http.
  def __init__(self,
               aws_access_key=os.environ['AWS_ACCESS_KEY'],
               aws_secret_key=os.environ['AWS_SECRET_KEY'],
               debug_mode=False, secure_http=True):
    self.aws_access_key = aws_access_key
    self.aws_secret_key = aws_secret_key
    self.debug_mode = debug_mode
    self.secure_http = secure_http


  # Generates an AWS signature value for the given request description.
  # The result value is a HMAC signature that is cryptographically signed
  # with the SHA1 algorithm using your AWS Secret Key credential. The
  # signature value is Base64 encoded before being returned.
  #
  # This method can be used to sign requests destined for the REST or
  # Query AWS API interfaces.
  def generate_signature(self, request_description):
    my_hmac = hmac.new(self.aws_secret_key, request_description, sha)
    return base64.b64encode(my_hmac.digest())


  # Converts a minimal set of parameters destined for an AWS Query API
  # interface into a complete set necessary for invoking an AWS operation.
  #
  # Normal parameters are included in the resultant complete set as-is.
  #
  # Indexed parameters are converted into multiple parameter name/value
  # pairs, where the name starts with the given parameter name but has a
  # suffix value appended to it. For example, the input mapping
  # 'Name' => ['x','y'] will be converted to two parameters,
  # 'Name.1' => 'x' and 'Name.2' => 'y'.
  def build_query_parameters(self, api_version, signature_version,
                             parameters={}, indexed_parameters={}):
    # Set mandatory query parameters
    my_params = {
      'Version': api_version,
      'SignatureVersion': signature_version,
      'AWSAccessKeyId': self.aws_access_key
      }

    # Use current time as timestamp if no date/time value is already set
    if not 'Timestamp' in parameters and not 'Expires' in parameters:
      my_params['Timestamp'] = time.strftime(self.ISO8601,
                                             self.current_time())

    # Merge parameters provided with defaults after removing
    # any parameters without a value.
    for param_name in parameters:
      if (parameters[param_name] != None):
        my_params[param_name] = str(parameters[param_name])

    # Add any indexed parameters as ParamName.1, ParamName.2, etc
    for param_name in indexed_parameters:
      if (indexed_parameters[param_name] == None): continue

      suffix = 1
      for value in indexed_parameters[param_name]:
        my_params[param_name + '.%s' % suffix] = value
        suffix += 1

    return my_params


  # Sends a GET or POST request message to an AWS service's Query API
  # interface and returns the response result from the service. This method
  # signs the request message with your AWS credentials.
  #
  # If the AWS service returns an error message, this method will throw a
  # ServiceException describing the error.
  def do_query(self, method, url, parameters):
    scheme, host, path, params, query, fragment = urlparse.urlparse(url)

    # Ensure the URL is using Secure HTTP protocol if the flag is set
    if (self.secure_http):
      conn = httplib.HTTPSConnection(host, 443)
      url = urlparse.urlunparse(('https',
                                 host, path, params, query, fragment))
    else:
      conn = httplib.HTTPConnection(host, 80)
      url = urlparse.urlunparse(('http',
                                 host, path, params, query, fragment))

    # Generate request description and signature by:
    # - sorting parameters into alphabtical order ignoring case
    sorted_param_names = list(parameters)
    sorted_param_names.sort(lambda x,y: cmp(x.lower(), y.lower()))

    # - merging the original parameter names and values in a string
    #   in order, and without any extra separator characters
    request_description = ''
    for param_name in sorted_param_names:
      request_description += param_name + parameters[param_name]

    # - signing the resultant request description
    signature = self.generate_signature(request_description)

    # - adding the signature to the URL as the parameter 'Signature'
    parameters['Signature'] = signature

    if (path == ''): path = '/'

    if (method == 'GET'):
      path += '?%s' % urllib.urlencode(parameters)
      if (self.debug_mode):
        self.debug_request(method, url + '?' + urllib.urlencode(parameters))

      conn.request(method, path)
    elif (method == 'POST'):
      headers = {'Content-type': 'application/x-www-form-urlencoded; charset=utf-8'}
      if (self.debug_mode):
        self.debug_request(method, url, parameters, headers)

      conn.request(method, path, urllib.urlencode(parameters), headers)
    else:
      raise 'Invalid HTTP Query method: %s' % method

    response = conn.getresponse()

    if (self.debug_mode): self.debug_response(response)

    if (response.status < 200 or response.status >= 300):
      raise ServiceException(response, self)

    return response


  # Generates a request description string for a request destined for a REST
  # AWS API interface, and returns a signature value for the request.
  #
  # This method will work for any REST AWS request, though it is intended
  # mainly for the S3 service's API and handles special cases required for
  # this service.
  def generate_rest_signature(self, method, url, headers):
    # Set mandatory Date header if it is missing
    if not headers.has_key('Date'):
      headers['Date'] = time.strftime(self.RFC822, self.current_time())

    # Describe main components of REST request. If Content-MD5
    # or Content-Type headers are missing, use an empty string
    request_desc = '%s\n' % method
    if headers.has_key('Content-MD5'):
      request_desc += headers['Content-MD5']
    request_desc += '\n'
    if headers.has_key('Content-Type'):
      request_desc += headers['Content-Type']
    request_desc += '\n%s\n' % headers['Date']

    # Find any x-amz-* headers and convert the header name to lower case.
    amz_headers = {}
    for header_name in headers:
      if re.search(r'^x-amz-', header_name):
        amz_headers[header_name.lower()] = headers[header_name]

    # Append x-maz-* headers to the description string in alphabetic
    # order.
    sorted_amz_header_names = list(amz_headers)
    sorted_amz_header_names.sort()
    for amz_header_name in sorted_amz_header_names:
      request_desc += '%s:%s\n' % \
        (amz_header_name, amz_headers[amz_header_name])

    req_desc_path = ''

    # Handle special case of S3 alternative hostname URLs. The bucket
    # portion of alternative hostnames must be included in the request
    # description's URL path.
    scheme, host, path, params, query, fragment = urlparse.urlparse(url)
    if not host == 's3.amazonaws.com' and not host == 'queue.amazonaws.com':
      s3_subdomain_match = re.match(r'(.*).s3.amazonaws.com$', host)
      if (s3_subdomain_match):
        req_desc_path = '/%s' % s3_subdomain_match.group(1)
      else:
        req_desc_path = '/%s' % host

      # For alternative hosts, the path must end with a slash
      # if there is no object in the path.
      if req_desc_path == '': req_desc_path += '/'

    # Append the request's URL path to the description
    req_desc_path += path

    # Ensure the request description's URL path includes at least a slash.
    if len(req_desc_path) == 0:
      request_desc += '/'
    else:
      request_desc += req_desc_path

    # Append special S3 parameters to request description
    for param in query.split('&'):
      if param in ('acl', 'torrent', 'logging', 'location'):
        request_desc += '?' + param

    if self.debug_mode:
      print 'REQUEST DESCRIPTION\n======='
      print re.sub(r'\n', '\\\\n\n', request_desc)
      print

    # Generate signature
    return self.generate_signature(request_desc)


  # Sends a GET, HEAD, DELETE or PUT request message to an AWS service's
  # REST API interface and returns the response result from the service. This
  # method signs the request message with your AWS credentials.
  #
  # If the AWS service returns an error message, this method will throw a
  # ServiceException describing the error. This method also includes support
  # for following Temporary Redirect responses (with HTTP response
  # codes 307).
  def do_rest(self, method, url, data=None, headers={}):
    # Generate request description and signature, and add to the request
    # as the header 'Authorization'
    signature = self.generate_rest_signature(method, url, headers)
    headers['Authorization'] = 'AWS %s:%s' % \
      (self.aws_access_key, signature)

    # Ensure the Host header is always set
    headers['Host'] = urlparse.urlparse(url)[1]

    redirect_count = 0
    while (redirect_count < 5): # Repeat requests after a Temporary Redirect
      scheme, host, path, params, query, fragment = urlparse.urlparse(url)

      # Ensure the URL is using Secure HTTP protocol if the flag is set
      if (self.secure_http):
        conn = httplib.HTTPSConnection(host, 443)
        url = urlparse.urlunparse(('https',
                                   host, path, params, query, fragment))
      else:
        conn = httplib.HTTPConnection(host, 80)
        url = urlparse.urlunparse(('http',
                                   host, path, params, query, fragment))

      # Uploads via the PUT method get special treatment
      if method == 'PUT':
        # Tell service to confirm the request message is valid before
        # it accepts data. Confirmation is indicated by a 100
        # (Continue) message
        headers['Expect'] = '100-continue'

        # Ensure HTTP content-length header is set to the correct value
        if not headers.has_key('Content-Length'):
          if not data:
            headers['Content-Length'] = '0'
          elif type(data) == types.FileType:
            headers['Content-Length'] = os.stat(data)
          else:
            headers['Content-Length'] = len(data)

        if self.debug_mode:
          self.debug_request(method, url, {}, headers, data)

        # Start the request
        conn.putrequest(method, path + '?' + query)
        for header_name in headers:
          conn.putheader(header_name, headers[header_name])
        conn.endheaders()

        if type(data) == types.FileType:
          # Upload data in chunks
          bytes = data.read(8192)
          while len(bytes) > 0:
            conn.send(bytes)
            bytes = data.read(8192)
        elif data:
          # Upload in-memory data object all at once
          conn.send(data)
      else:
        if self.debug_mode:
          self.debug_request(method, url, {}, headers)

        conn.request(method, path + '?' + query, None, headers)

      response = conn.getresponse()

      if (self.debug_mode): self.debug_response(response)

      if response.status == 307:
        # Automatically follow Temporary Redirects
        url = dict(response.getheaders())['location']
        conn.close()
        redirect_count += 1
      elif (response.status < 200 or response.status >= 300):
        raise ServiceException(response, self)
      else:
        return response


  # Prints detailed information about an HTTP request message to standard
  # output.
  def debug_request(self, method, url, parameters={}, headers={}, data=None):
    print 'REQUEST\n======='
    print 'Method : %s' % method

    # Print URI
    pieces = url.split('&')
    print 'URI    : %s' % pieces[0]
    for piece in pieces[1:]:
      print '\t &%s' % piece

    # Print Headers
    if (len(headers) > 0):
      print 'Headers:'
      for header_name in headers:
        print '  %s=%s' % (header_name, headers[header_name])

    # Print Query Parameters
    if (len(parameters) > 0):
      print 'Query Parameters:'
      for param_name in parameters:
        print '  %s=%s' % (param_name, parameters[param_name])

    # Print Request Data
    if data:
      print 'Request Body Data:'
      if headers['Content-Type'] == 'application/xml':
        # Pretty-print XML data (done by parse_xml method)
        self.parse_xml(data)
      else:
        print data
      print


  # Prints detailed information about an HTTP response message to standard
  # output.
  def debug_response(self, response):
    print '\nRESPONSE\n========'
    print 'Status : %s %s' % (response.status, response.reason)

    # Print Headers
    print 'Headers:'
    for header in response.getheaders():
      print '  %s=%s' % header

    # We cannot print the response body here in the Python implementation
    # as the HTTP response object's data stream cannot be reset, and
    # therefore cannot be read multiple times. Instead, the response body
    # will be printed by the parse_xml method below that will be used in
    # most cases to convert the response data into an XML Document.


  # Returns the current date and time, adjusted according to the time
  # offset between your computer and an AWS server (as set by the
  # adjust_time method.
  def current_time(self):
    return time.gmtime(time.time() - self.time_offset)


  # Sets a time offset value to reflect the time difference between your
  # computer's clock and the current time according to an AWS server. This
  # method returns the calculated time difference and also sets the
  # timeOffset variable in AWS.
  #
  # Ideally you should not rely on this method to overcome clock-related
  # disagreements between your computer and AWS. If you computer is set
  # to update its clock periodically and has the correct timezone setting
  # you should never have to resort to this work-around.
  def adjust_time(self):
    # Connect to an AWS server to obtain response headers.
    response = urllib.urlopen('http://aws.amazon.com/')
    time_now = time.time()

    # Retrieve the time according to AWS, based on the Date header
    aws_time = calendar.timegm(
      time.strptime(response.info()['Date'], self.RFC822))

    # Calculate the difference between the current time according to AWS,
    # and the current time according to your computer's clock.
    time_offset = aws_time - time_now

    if (self.debug_mode):
      print 'Time offset for AWS requests: %f seconds' % time_offset

    return time_offset


  ########################################################################
  # The class methods below this point are specific to the Python
  # implementation of the AWS clients. They provide convenient short-cuts
  # for performing XML parsing and XPath queries using the lxml library
  # (a Python binding for the libxml2 C library).
  ########################################################################

  def parse_xml(self, xml_text):
    xmldoc = etree.fromstring(xml_text)
    # Pretty-print XML document
    print 'Body:\n%s\n' % etree.tostring(xmldoc, pretty_print=True)
    return xmldoc

  def xpath_list(self, query, xml_object):
    return xml_object.xpath(query, namespaces={'ns':self.XML_NAMESPACE})

  def xpath_node(self, query, xml_object):
    node_list = self.xpath_list(query, xml_object)
    if not node_list or len(node_list) == 0:
      return None
    else:
      return node_list[0]

  def xpath_value(self, query, xml_object):
    node_list = self.xpath_list(query, xml_object)
    if not node_list or len(node_list) == 0:
      return None
    else:
      return node_list[0].text


# An exception object that captures information about an AWS service error.
class ServiceException(Exception):
  message = ''
  error_xml = None
  error_text = None

  def __init__(self, response, service):
    # Add the HTTP status code and message to a descriptive message
    self.message = 'HTTP Error: %s - %s' % (response.status, response.reason)
    self.error_text = response.read()

    # If an AWS error message is available, add its code and
    # message to the overall descriptive message.
    if (re.search(r'<\?xml', self.error_text)):
      self.error_xml = etree.fromstring(self.error_text)

      self.message += ', AWS Error: %s - %s' % \
                (service.xpath_value('//ns:Code', self.error_xml),
                 service.xpath_value('//ns:Message', self.error_xml))
    self.args = (self.message, self.error_xml)
