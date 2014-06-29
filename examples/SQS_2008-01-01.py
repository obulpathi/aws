#!/usr/bin/env python
"""
Sample Python code for the O'Reilly book "Using AWS Infrastructure Services"
by James Murty.

This code was written for Python version 2.4.4 or greater, and requires the
XML/XPath library "lxml" version 1.3.6 or greater (see
http://codespeak.net/lxml/).

The SQS module implements the Query API of the Amazon Simple Queue Service.
"""

from AWS import AWS
import base64
import re

class SQS(AWS):

  ENDPOINT_URI = 'https://queue.amazonaws.com/'
  API_VERSION = '2008-01-01'
  SIGNATURE_VERSION = '1'

  HTTP_METHOD = 'POST' # 'GET'

  XML_NAMESPACE = 'http://queue.amazonaws.com/doc/2008-01-01/'


  def list_queues(self, queue_name_prefix = None):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'ListQueues',
       'QueueNamePrefix': queue_name_prefix})

    response = self.do_query(self.HTTP_METHOD, self.ENDPOINT_URI, params)
    xmldoc = self.parse_xml(response.read())

    queue_urls = []
    for node in self.xpath_list('//ns:QueueUrl', xmldoc):
      queue_urls.append(node.text)
    return queue_urls


  def create_queue(self, queue_name, visibility_timeout_secs = None):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'CreateQueue',
       'QueueName': queue_name,
       'DefaultVisibilityTimeout': visibility_timeout_secs})

    response = self.do_query(self.HTTP_METHOD, self.ENDPOINT_URI, params)
    xmldoc = self.parse_xml(response.read())

    return self.xpath_value('//ns:QueueUrl', xmldoc)


  def delete_queue(self, queue_url):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'DeleteQueue'})

    response = self.do_query(self.HTTP_METHOD, queue_url, params)
    return True


  def get_queue_attributes(self, queue_url, attribute_name='All'):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'GetQueueAttributes',
       'AttributeName': attribute_name})

    response = self.do_query(self.HTTP_METHOD, queue_url, params)
    xmldoc = self.parse_xml(response.read())

    attributes = []
    for node in self.xpath_list('//ns:Attribute', xmldoc):
      attributes.append({
        self.xpath_value('ns:Name', node):
          # All currently supported attributes have integer values
          int(self.xpath_value('ns:Value', node))})

    return attributes


  def set_queue_attribute(self, queue_url, attribute_name, value):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'SetQueueAttributes',
       'Attribute.Name': attribute_name,
       'Attribute.Value': str(value)})

    response = self.do_query(self.HTTP_METHOD, queue_url, params)
    return True


  def send_message(self, queue_url, message_body, encode_body=True):
    if encode_body: message_body = base64.b64encode(message_body)

    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'SendMessage',
       'MessageBody': message_body})

    response = self.do_query(self.HTTP_METHOD, queue_url, params)
    xmldoc = self.parse_xml(response.read())

    return {
      'id': self.xpath_value('//ns:MessageId', xmldoc),
      'md5': self.xpath_value('//ns:MD5OfMessageBody', xmldoc)}


  def receive_messages(self, queue_url, maximum=10,
                       visibility_timeout_secs=None, decode_body=True):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'ReceiveMessage',
       'MaxNumberOfMessages': str(maximum)})

    if visibility_timeout_secs:
       params['VisibilityTimeout'] = str(visibility_timeout_secs)

    response = self.do_query(self.HTTP_METHOD, queue_url, params)
    xmldoc = self.parse_xml(response.read())

    messages = []
    for node in self.xpath_list('//ns:Message', xmldoc):
      message_body = self.xpath_value('ns:Body', node)
      if decode_body: message_body = base64.b64decode(message_body)

      messages.append({
        'id': self.xpath_value('ns:MessageId', node),
        'body': message_body,
        'md5': self.xpath_value('ns:MD5OfBody', node),
        'receipt': self.xpath_value('ns:ReceiptHandle', node)
        })
    return messages


  def delete_message(self, queue_url, receipt_handle):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'DeleteMessage',
       'ReceiptHandle': receipt_handle})

    response = self.do_query(self.HTTP_METHOD, queue_url, params)
    return True


if __name__ == '__main__':
  sqs = SQS(debug_mode=True)

  print sqs.list_queues()

  # print sqs.create_queue('test-queue')
  # queue_url = 'http://queue.amazonaws.com/test-queue'

  # print sqs.set_queue_attribute(queue_url, 'VisibilityTimeout', 15);

  # print sqs.get_queue_attributes(queue_url)

  # print sqs.send_message(queue_url, 'This is just a test message')

  # print sqs.receive_messages(queue_url, 10, visibility_timeout_secs=60)
  # receipt_handle = \
  #   'Euvo62/1nlJPzOOkUJD+zQFGq5AeZcQ7ZB2kXAz1GAqed30vrpx5jzOnQzNQtzy66Hf' \
  #   + 'RMWUPb3WmyZWBCAKWqwVnGzikxbX1FHDmtCHk9VEiQLwZ8RtqzLrlfaoWyt2o4AvN' \
  #   + 'tRR0wDoF1W7WFvRX9oBDAleiEAn4jcn9J5a2rAxJx2LofUCXWzXl8LIZMOxDXANxT' \
  #   + 'nmjf1t0UjFhSEVS3mrvN2941LZB6L3pdxqa82abBZSaiBf6TA=='

  # print sqs.delete_message(queue_url, receipt_handle)

  # print sqs.delete_queue(queue_url)
