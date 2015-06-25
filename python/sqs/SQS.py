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
  API_VERSION = '2007-05-01'
  SIGNATURE_VERSION = '1'

  HTTP_METHOD = 'POST' # 'GET'

  XML_NAMESPACE = 'http://queue.amazonaws.com/doc/2007-05-01/'


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


  def delete_queue(self, queue_url, force_deletion = False):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'DeleteQueue',
       'ForceDeletion': str(force_deletion)})

    response = self.do_query(self.HTTP_METHOD, queue_url, params)
    return True


  def get_queue_attributes(self, queue_url, attribute_name='All'):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'GetQueueAttributes',
       'Attribute': attribute_name})

    response = self.do_query(self.HTTP_METHOD, queue_url, params)
    xmldoc = self.parse_xml(response.read())

    attributes = []
    for node in self.xpath_list('//ns:AttributedValue', xmldoc):
      attributes.append({
        self.xpath_value('ns:Attribute', node):
          # All currently supported attributes have integer values
          int(self.xpath_value('ns:Value', node))})
    return attributes


  def set_queue_attribute(self, queue_url, attribute_name, value):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'SetQueueAttributes',
       'Attribute': attribute_name,
       'Value': str(value)})

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

    return self.xpath_value('//ns:MessageId', xmldoc)


  def peek_message(self, queue_url, message_id, decode_body=True):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'PeekMessage',
       'MessageId': message_id})

    response = self.do_query(self.HTTP_METHOD, queue_url, params)
    xmldoc = self.parse_xml(response.read())

    message_id = self.xpath_value('//ns:MessageId', xmldoc)
    message_body = self.xpath_value('//ns:MessageBody', xmldoc)

    if decode_body: message_body = base64.b64decode(message_body)

    return {'id': message_id, 'body': message_body}


  def receive_messages(self, queue_url, maximum=10,
                       visibility_timeout_secs=None, decode_body=True):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'ReceiveMessage',
       'NumberOfMessages': str(maximum)})

    if visibility_timeout_secs:
       params['VisibilityTimeout'] = str(visibility_timeout_secs)

    response = self.do_query(self.HTTP_METHOD, queue_url, params)
    xmldoc = self.parse_xml(response.read())

    messages = []
    for node in self.xpath_list('//ns:Message', xmldoc):
      message_id = self.xpath_value('ns:MessageId', node)
      message_body = self.xpath_value('ns:MessageBody', node)

      if decode_body: message_body = base64.b64decode(message_body)
      messages.append({'id': message_id, 'body': message_body})
    return messages


  def delete_message(self, queue_url, message_id):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'DeleteMessage',
       'MessageId': message_id})

    response = self.do_query(self.HTTP_METHOD, queue_url, params)
    return True


  def change_message_visibility(self, queue_url, message_id,
                                visibility_timeout_secs):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'ChangeMessageVisibility',
       'MessageId': message_id,
       'VisibilityTimeout': str(visibility_timeout_secs)})

    response = self.do_query(self.HTTP_METHOD, queue_url, params)
    return True


  def list_grants(self, queue_url, permission=None, grantee=None):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'ListGrants'})

    if permission: params['Permission'] = permission

    if grantee:
      if re.search(r'@', grantee):
        params['Grantee.EmailAddress'] = grantee
      else:
        params['Grantee.ID'] = grantee

    response = self.do_query(self.HTTP_METHOD, queue_url, params)
    xmldoc = self.parse_xml(response.read())

    grants = []
    for node in self.xpath_list('//ns:GrantList', xmldoc):
      grants.append({
        'permission': self.xpath_value('ns:Permission', node),
        'id': self.xpath_value('ns:Grantee/ns:ID', node),
        'display_name': self.xpath_value('ns:Grantee/ns:DisplayName', node)
        })
    return grants


  def add_grant(self, queue_url, grantee, permission):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'AddGrant',
       'Permission': permission})

    if re.search(r'@', grantee):
      params['Grantee.EmailAddress'] = grantee
    else:
      params['Grantee.ID'] = grantee

    response = self.do_query(self.HTTP_METHOD, queue_url, params)
    return True


  def remove_grant(self, queue_url, grantee, permission):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'RemoveGrant',
       'Permission': permission})

    if re.search(r'@', grantee):
      params['Grantee.EmailAddress'] = grantee
    else:
      params['Grantee.ID'] = grantee

    response = self.do_query(self.HTTP_METHOD, queue_url, params)
    return True


if __name__ == '__main__':
  sqs = SQS(debug_mode=True)

  print sqs.list_queues()

  # print sqs.create_queue('test123')
  # queue_url = 'http://queue.amazonaws.com/A1MU5FWLQSN7CU/test123'

  # print sqs.delete_queue(queue_url, force_deletion=True)

  # print sqs.set_queue_attribute(queue_url, 'VisibilityTimeout', 15);

  # print sqs.get_queue_attributes(queue_url)

  # print sqs.send_message(queue_url, 'This is just a test message')
  # msg_id = '0TSSB045TNR0EX9YGS58|4KQ1JAZG57TB83V9PWW0|K6KT2VSF12BR9S52HCC0'

  # print sqs.peek_message(queue_url, msg_id)

  # print sqs.receive_messages(queue_url, 2, 60, True)

  # print sqs.change_message_visibility(queue_url, msg_id, 0)

  # print sqs.delete_message(queue_url, msg_id)

  # print sqs.list_grants(queue_url, permission='FULLCONTROL', grantee='your_email@address.com')

  # print sqs.add_grant(queue_url, 'your_email@address.com', 'RECEIVEMESSAGE')

  # print sqs.remove_grant(queue_url, 'your_email@address.com', 'RECEIVEMESSAGE')
