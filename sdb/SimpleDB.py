#!/usr/bin/env python
"""
Sample Python code for the O'Reilly book "Using AWS Infrastructure Services"
by James Murty.

This code was written for Python version 2.4.4 or greater, and requires the
XML/XPath library "lxml" version 1.3.6 or greater (see
http://codespeak.net/lxml/).

The SimpleDB module implements the Query API of the Amazon SimpleDB service.
"""

from AWS import AWS
import types
import time
import re

class SimpleDB(AWS):

  ENDPOINT_URI = 'https://sdb.amazonaws.com/'
  API_VERSION = '2007-11-07'
  SIGNATURE_VERSION = '1'

  HTTP_METHOD = 'POST' # 'GET'

  XML_NAMESPACE = 'http://sdb.amazonaws.com/doc/2007-11-07/'

  prior_box_usage = 0
  total_box_usage = 0


  def do_sdb_query(self, params):
    response = self.do_query(self.HTTP_METHOD, self.ENDPOINT_URI, params)
    xmldoc = self.parse_xml(response.read())

    self.prior_box_usage = float(self.xpath_value('//ns:BoxUsage', xmldoc))
    self.total_box_usage += self.prior_box_usage

    return xmldoc


  def list_domains(self, max_domains = 100):
    more_domains = True
    next_token = None
    domain_names = []

    while more_domains:
      params = self.build_query_parameters(
        self.API_VERSION, self.SIGNATURE_VERSION,
        {'Action' : 'ListDomains',
         'MaxNumberOfDomains' : max_domains,
         'NextToken' : next_token})

      xmldoc = self.do_sdb_query(params)

      for node in self.xpath_list('//ns:DomainName', xmldoc):
        domain_names.append(node.text)

      # If we receive a NextToken element, perform a follow-up operation
      # to retrieve the next set of domain names.
      next_token = self.xpath_value('//ns:NextToken', xmldoc)
      more_domains = next_token != None

    return domain_names


  def create_domain(self, domain_name):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action' : 'CreateDomain',
       'DomainName' : domain_name})

    self.do_sdb_query(params)
    return True


  def delete_domain(self, domain_name):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action' : 'DeleteDomain',
       'DomainName' : domain_name
      })

    self.do_sdb_query(params)
    return True


  def build_attribute_params(self, attributes={}, replace=False):
    attribute_params = {}
    index = 0

    for attrib_name in attributes:
      attrib_value = attributes[attrib_name]

      if type(attrib_value) != types.ListType:
        attrib_value = [attrib_value]

      for value in attrib_value:
        attribute_params['Attribute.%i.Name' % index] = attrib_name
        if value != None:
          # Automatically encode attribute values
          value = self.encode_attribute_value(value)
          attribute_params['Attribute.%i.Value' % index] = value

        # Add a Replace parameter for the attribute if the replace flag is set
        if replace:
          attribute_params['Attribute.%i.Replace' % index] = 'true'
        index += 1

    return attribute_params


  def get_attributes(self, domain_name, item_name, attribute_name = None):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action' : 'GetAttributes',
       'DomainName' : domain_name,
       'ItemName' : item_name,
       'AttributeName' : attribute_name})

    xmldoc = self.do_sdb_query(params)

    attributes = {}
    for attribute_node in self.xpath_list('//ns:Attribute', xmldoc):
      attr_name = self.xpath_value('ns:Name', attribute_node)
      value = self.xpath_value('ns:Value', attribute_node)

      # Automatically decode attribute values
      value = self.decode_attribute_value(value)

      # An empty attribute value is an empty string, not nil.
      if not value:
        value = ''

      if attr_name in attributes:
        attributes[attr_name].append(value)
      else:
        attributes[attr_name] = [value]

    if attribute_name:
      # If a specific attribute was requested, return only the values array
      # for this attribute.
      if not attribute_name in attributes:
        return []
      else:
        return attributes[attribute_name]
    else:
      return attributes


  def put_attributes(self, domain_name, item_name, attributes, replace=False):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action' : 'PutAttributes',
       'DomainName' : domain_name,
       'ItemName' : item_name})

    attrib_params = self.build_attribute_params(attributes, replace)
    for param_name in attrib_params:
      params[param_name] = attrib_params[param_name]

    self.do_sdb_query(params)
    return True


  def delete_attributes(self, domain_name, item_name, attributes={}):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action' : 'DeleteAttributes',
       'DomainName' : domain_name,
       'ItemName' : item_name})

    attrib_params = self.build_attribute_params(attributes)
    for param_name in attrib_params:
      params[param_name] = attrib_params[param_name]

    self.do_sdb_query(params)
    return True


  def query(self, domain_name, query_expression = None,
            max_items = 100, fetch_all = True):
    more_items = True
    next_token = None
    item_names = []

    while more_items:
      params = self.build_query_parameters(
        self.API_VERSION, self.SIGNATURE_VERSION,
        {'Action' : 'Query',
         'DomainName' : domain_name,
         'QueryExpression' : query_expression,
         'MaxNumberOfItems' : max_items,
         'NextToken' : next_token})

      xmldoc = self.do_sdb_query(params)

      for item_name_node in self.xpath_list('//ns:ItemName', xmldoc):
        item_names.append(item_name_node.text)

      next_token = self.xpath_value('//ns:NextToken', xmldoc)
      more_items = fetch_all and next_token

    return item_names


  def encode_boolean(self, value):
    if value:
      return '!b'
    else:
      return '!B'


  def decode_boolean(self, value_str):
    if value_str == '!B':
      return False
    elif value_str == '!b':
      return True
    else:
      raise 'Cannot decode boolean from string: %s' % value_str


  def encode_date(self, value):
    return "!d" + time.strftime(self.ISO8601, value)


  def decode_date(self, value_str):
    if value_str[0:2] == '!d':
      return time.strptime(value_str[2:], self.ISO8601)
    else:
      raise 'Cannot decode date from string: %s' % value_str


  def encode_integer(self, value, max_digits=18):
    upper_bound = (10 ** max_digits)

    if value >= upper_bound or value < -upper_bound:
      raise 'Integer %i is outside encoding range (-%i to %i)' % \
        (value, upper_bound, (upper_bound - 1))

    format = '%%0%dd' % max_digits
    if value < 0:
      return "!I" + format % (upper_bound + value)
    else:
      return "!i" + format % value


  def decode_integer(self, value_str):
    if value_str[0:2] == '!I':
      # Encoded value is a negative integer
      max_digits = len(value_str) - 2
      upper_bound = (10 ** max_digits)

      return int(value_str[2:]) - upper_bound
    elif value_str[0:2] == '!i':
      # Encoded value is a positive integer
      return int(value_str[2:])
    else:
      raise 'Cannot decode integer from string: %s' % value_str


  def encode_float(self, value, max_exp_digits=2, max_precision_digits=15):
    exp_midpoint = (10 ** max_exp_digits) / 2

    # This is a horrible string-based hack to work out the fraction and
    # exponent comoponents of a float.
    str = ('%%.%ie' % (max_precision_digits - 1)) % value
    if value >= 0:
      fraction = str[0] + str[2:(max_precision_digits + 1)]
      exponent = int(str[(max_precision_digits + 2):])
      if exponent != 0:
        exponent += 1
    else:
      fraction = str[1] + str[3:(max_precision_digits + 2)]
      exponent = int(str[(max_precision_digits + 3):]) + 1

    if exponent >= exp_midpoint or exponent < -exp_midpoint:
      raise 'Exponent %i is outside encoding range (-%i to %i)' % \
        (exponent, exp_midpoint, (exp_midpoint - 1))

    # The zero value is a special case, for which the exponent must be 0
    if value == 0:
      exponent = -exp_midpoint

    exp_format = '%%0%dd' % max_exp_digits
    fraction_format = '%%0%dd' % max_precision_digits

    if value >= 0:
      return '!f%s!%s' % ((exp_format % (exp_midpoint + exponent)), fraction)
    else:
      fraction_upper_bound = (10 ** max_precision_digits)
      diff_fraction = fraction_upper_bound - int(fraction)
      return '!F%s!%s' % (exp_format % (exp_midpoint - exponent),
        fraction_format % diff_fraction)


  def decode_float(self, value_str):
    prefix = value_str[0:2]

    if prefix != '!f' and prefix != '!F':
      raise 'Cannot decode float from string: %s' % value_str

    matches = re.findall(r'![fF]([0-9]+)!([0-9]+)', value_str)
    exp_str, fraction_str = matches[0]

    max_exp_digits = len(exp_str)
    exp_midpoint = (10 ** max_exp_digits) / 2
    max_precision_digits = len(fraction_str)

    if prefix == '!F':
      sign = -1
      exp = exp_midpoint - int(exp_str)

      fraction_upper_bound = (10 ** max_precision_digits)
      fraction = fraction_upper_bound - int(fraction_str)
    else:
      sign = 1
      exp = int(exp_str) - exp_midpoint

      fraction = int(fraction_str)

    return sign * float('0.%i' % fraction) * (10 ** exp)


  def encode_attribute_value(self, value):
    if value == True or value == False:
      return self.encode_boolean(value)
    elif type(value) == time.struct_time:
      return self.encode_date(value)
    elif type(value) == types.IntType:
      return self.encode_integer(value)
    elif type(value) == types.FloatType:
      return self.encode_float(value)
    else:
      # No type-specific encoding is available, so we simply convert
      # the value to a string.
      return str(value)


  def decode_attribute_value(self, value_str):
    if not value_str:
      return ''

    # Check whether the '!' flag is present to indicate an encoded value
    if value_str[0] != '!':
      return value_str

    prefix = value_str[0:2].lower()
    if prefix == '!b':
      return self.decode_boolean(value_str)
    elif prefix == '!d':
      return self.decode_date(value_str)
    elif prefix == '!i':
      return self.decode_integer(value_str)
    elif prefix == '!f':
      return self.decode_float(value_str)
    else:
      return value_str


if __name__ == '__main__':
  sdb = SimpleDB(debug_mode=True)

  print sdb.list_domains()

  #print sdb.create_domain('test-domain')

  #attribs = \
  #  {'Name' : 'Tomorrow Never Knows',
  #   'Artist' : 'The Beatles',
  #   'Time' : '177',
  #   'Album' : ['Revolver','The Beatles Box Set']}
  #sdb.put_attributes('test-domain', 'TestItem', attribs)

  #print sdb.put_attributes('test-domain', 'TestItem', {'Time' : '2:57'})

  #print sdb.put_attributes('test-domain', 'TestItem', {'Time' : '2:57'}, True)

  #print sdb.get_attributes('test-domain', 'TestItem')

  #print sdb.get_attributes('test-domain', 'TestItem', 'Time')

  #print sdb.delete_attributes('test-domain', 'TestItem', {'Album' : 'Revolver'})

  #print sdb.delete_attributes('test-domain', 'TestItem', {'Album' : None})

  #print sdb.delete_attributes('test-domain', 'TestItem')

  #results = sdb.query('stocks', "['Code' = 'AAPL']", max_items = 250)
  #print results
  #print len(results)

  #results = sdb.query('stocks', "['Date' > '2007-06-05T00:00:00Z']")
  #print results
  #print len(results)

  #print sdb.delete_domain('test-domain')

  #print 'Prior usage: %f' % sdb.prior_box_usage
  #print 'Total usage: %f' % sdb.total_box_usage

  #print 'Encoded booleans: True=%s, False=%s' % \
  #  (sdb.encode_boolean(True), sdb.encode_boolean(False))
  #print 'Decoded booleans: !b=%s, !B=%s' % \
  #  (sdb.decode_boolean('!b'), sdb.decode_boolean('!B'))

  #enc_date = sdb.encode_date(time.gmtime(time.time()))
  #print 'Encoded Date: %s' % enc_date
  #print 'Decoded Date: %s' % sdb.decode_date(enc_date)

  #max_digits = 2
  #print sdb.encode_integer(7, max_digits)
  #print sdb.decode_integer('!i07')
  #print sdb.encode_integer(25, max_digits)
  #print sdb.decode_integer('!i25')
  #print sdb.encode_integer(-3, max_digits)
  #print sdb.decode_integer('!I97')
  #print sdb.encode_integer(-100, max_digits)
  #print sdb.decode_integer('!I00')

  #print sdb.encode_float(0.0)
  #print sdb.decode_float('!f50!000000000000000')
  #print sdb.encode_float(12345678901234567890)
  #print '%15f' % sdb.decode_float('!f70!123456789012346')
  #print sdb.encode_float(0.12345678901234567890)
  #print '%.15f' % sdb.decode_float('!f50!123456789012346')
  #print sdb.encode_float(-12345678901234567890)
  #print '%15f' % sdb.decode_float('!F30!876543210987654')
  #print sdb.encode_float(-0.12345678901234567890)
  #print '%.15f' % sdb.decode_float('!F50!876543210987654')

  #print sdb.encode_attribute_value(False)
  #print sdb.decode_attribute_value('!B')
  #print sdb.encode_attribute_value(time.gmtime())
  #print sdb.decode_attribute_value('!d2008-01-25T05:32:39Z')
  #print sdb.encode_attribute_value(43.54)
  #print sdb.decode_attribute_value('!f52!435400000000000')
  #print sdb.encode_attribute_value(8637)
  #print sdb.decode_attribute_value('!i000000000000008637')
