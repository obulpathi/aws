#!/usr/bin/env python
"""
Sample Python code for the O'Reilly book "Using AWS Infrastructure Services"
by James Murty.

This code was written for Python version 2.4.4 or greater, and requires the
XML/XPath library "lxml" version 1.3.6 or greater (see
http://codespeak.net/lxml/).

The EC2 module implements the Query API of the Amazon Elastic Compute Cloud
service.
"""

from AWS import AWS
import base64

class EC2(AWS):

  ENDPOINT_URI = 'https://ec2.amazonaws.com/'
  API_VERSION = '2007-08-29'
  SIGNATURE_VERSION = '1'

  HTTP_METHOD = 'POST' # 'GET'

  XML_NAMESPACE = 'http://ec2.amazonaws.com/doc/2007-08-29/'


  def parse_reservation(self, res_node):
    reservation = {
      'id': self.xpath_value('ns:reservationId', res_node),
      'owner_id': self.xpath_value('ns:ownerId', res_node)}

    groups = []
    for group_node in self.xpath_list('ns:groupSet/ns:item/ns:groupId', res_node):
      groups.append(group_node.text)

    instances = []
    for item_node in self.xpath_list('ns:instancesSet/ns:item', res_node):
      instance = {
        'id': self.xpath_value('ns:instanceId', item_node),
        'image_id': self.xpath_value('ns:imageId', item_node),
        'state': self.xpath_value('ns:instanceState/ns:name', item_node),
        'private_dns': self.xpath_value('ns:privateDnsName', item_node),
        'public_dns': self.xpath_value('ns:dnsName', item_node),
        'type': self.xpath_value('ns:instanceType', item_node),
        'launch_time': self.xpath_value('ns:launchTime', item_node),
        'reason': self.xpath_value('ns:reason', item_node),
        'key_name': self.xpath_value('ns:keyName', item_node),
        'launch_index': self.xpath_value('ns:amiLaunchIndex', item_node)}

      instance['product_codes'] = []
      for prod_code_node in self.xpath_list('ns:productCodes/ns:item/ns:productCode', item_node):
        instance['product_codes'].append(prod_code_node.text)

      instances.append(instance)

    reservation['groups'] = groups
    reservation['instances'] = instances
    return reservation


  def describe_instances(self, instance_ids=[]):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'DescribeInstances'},
      {'InstanceId': instance_ids})

    response = self.do_query(self.HTTP_METHOD, self.ENDPOINT_URI, params)
    xmldoc = self.parse_xml(response.read())

    reservations = []
    for node in self.xpath_list('//ns:reservationSet/ns:item', xmldoc):
      reservations.append(self.parse_reservation(node))
    return reservations


  def describe_keypairs(self, keypair_names=[]):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'DescribeKeyPairs'},
      {'KeyName': keypair_names})

    response = self.do_query(self.HTTP_METHOD, self.ENDPOINT_URI, params)
    xmldoc = self.parse_xml(response.read())

    keypairs = []
    for node in self.xpath_list('//ns:keySet/ns:item', xmldoc):
      keypairs.append({
        'name': self.xpath_value('ns:keyName', node),
        'fingerprint': self.xpath_value('ns:keyFingerprint', node)})
    return keypairs


  def create_keypair(self, keypair_name, autosave=True):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'CreateKeyPair',
       'KeyName': keypair_name})

    response = self.do_query(self.HTTP_METHOD, self.ENDPOINT_URI, params)
    xmldoc = self.parse_xml(response.read())

    keypair = {
      'name': self.xpath_value('//ns:keyName', xmldoc),
      'fingerprint': self.xpath_value('//ns:keyFingerprint', xmldoc),
      'material': self.xpath_value('ns:keyMaterial', xmldoc)}

    if autosave:
      # Locate key material and save to a file named after the keyName
      keypair_file = open('%s.pem' % keypair['name'], 'w')
      keypair_file.write(keypair['material'])
      keypair_file.close()
      keypair['file_name'] = keypair_file.name

    return keypair


  def delete_keypair(self, keypair_name):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'DeleteKeyPair',
       'KeyName': keypair_name})

    response = self.do_query(self.HTTP_METHOD, self.ENDPOINT_URI, params)
    xmldoc = self.parse_xml(response.read())
    return True


  def describe_images(self, image_ids=[], owner_ids=[], executable_by=None):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'DescribeImages',
       'ExecutableBy': executable_by},
      {'ImageId': image_ids,
       'Owner': owner_ids})

    response = self.do_query(self.HTTP_METHOD, self.ENDPOINT_URI, params)
    xmldoc = self.parse_xml(response.read())

    images = []
    for node in self.xpath_list('//ns:imagesSet/ns:item', xmldoc):
      image = {
        'id': self.xpath_value('ns:imageId', node),
        'location': self.xpath_value('ns:imageLocation', node),
        'state': self.xpath_value('ns:imageState', node),
        'is_public': 'true' == self.xpath_value('ns:isPublic', node),
        'owner_id': self.xpath_value('ns:imageOwnerId', node)}

      image['product_codes'] = []
      for prod_code_node in self.xpath_list('ns:productCodes/ns:item/ns:productCode', node):
        image['product_codes'].append(prod_code_node.text)

      images.append(image)

    return images


  def run_instances(self, image_id, keypair_name=None, security_groups=[],
                    user_data='', minimum=1, maximum=1,
                    instance_type='m1.small', addressing_type='public'):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'RunInstances',
       'ImageId': image_id,
       'MinCount': str(minimum),
       'MaxCount': str(maximum),
       'KeyName': keypair_name,
       'InstanceType': instance_type,
       'AddressingType': addressing_type,
       'UserData': base64.b64encode(user_data)},
      {'SecurityGroup': security_groups})

    response = self.do_query(self.HTTP_METHOD, self.ENDPOINT_URI, params)
    xmldoc = self.parse_xml(response.read())
    return self.parse_reservation(xmldoc)


  def get_console_output(self, instance_id):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'GetConsoleOutput',
       'InstanceId': instance_id})

    response = self.do_query(self.HTTP_METHOD, self.ENDPOINT_URI, params)
    xmldoc = self.parse_xml(response.read())

    return {
      'instance_id': self.xpath_value('//ns:instanceId', xmldoc),
      'timestamp': self.xpath_value('//ns:timestamp', xmldoc),
      'output': base64.b64decode(self.xpath_value('//ns:output', xmldoc))}


  def reboot_instances(self, instance_ids):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'RebootInstances'},
      {'InstanceId': instance_ids})

    response = self.do_query(self.HTTP_METHOD, self.ENDPOINT_URI, params)
    xmldoc = self.parse_xml(response.read())
    return True


  def terminate_instances(self, instance_ids):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'TerminateInstances'},
      {'InstanceId': instance_ids})

    response = self.do_query(self.HTTP_METHOD, self.ENDPOINT_URI, params)
    xmldoc = self.parse_xml(response.read())

    instances = []
    for item_node in self.xpath_list('//ns:instancesSet/ns:item', xmldoc):
      instances.append({
        'id': self.xpath_value('ns:instanceId', item_node),
        'state': self.xpath_value('ns:shutdownState/ns:name', item_node),
        'previous_state': self.xpath_value('ns:previousState/ns:name', item_node)})
    return instances


  def describe_security_groups(self, group_names=[]):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'DescribeSecurityGroups'},
      {'GroupName': group_names})

    response = self.do_query(self.HTTP_METHOD, self.ENDPOINT_URI, params)
    xmldoc = self.parse_xml(response.read())

    groups = []
    for group_node in self.xpath_list('//ns:securityGroupInfo/ns:item', xmldoc):
      group = {
        'name': self.xpath_value('ns:groupName', group_node),
        'description': self.xpath_value('ns:groupDescription', group_node),
        'owner_id': self.xpath_value('ns:ownerId', group_node),
        'grants': []}

      for grant_node in self.xpath_list('ns:ipPermissions/ns:item', group_node):
        grant = {
          'ip_protocol': self.xpath_value('ns:ipProtocol', grant_node),
          'from_port': self.xpath_value('ns:fromPort', grant_node),
          'to_port': self.xpath_value('ns:toPort', grant_node),
          'cidr_range': self.xpath_value('ns:ipRanges/ns:item/ns:cidrIp', grant_node),
          'groups': []}

        for perm_group_node in self.xpath_list('ns:groups/ns:item', grant_node):
          grant['groups'].append({
            'name': self.xpath_value('ns:groupName', perm_group_node),
            'user_id': self.xpath_value('ns:userId', perm_group_node)})

        group['grants'].append(grant)

      groups.append(group)

    return groups


  def create_security_group(self, group_name, group_description):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'CreateSecurityGroup',
       'GroupName': group_name,
       'GroupDescription': group_description})

    response = self.do_query(self.HTTP_METHOD, self.ENDPOINT_URI, params)
    return True


  def delete_security_group(self, group_name):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'DeleteSecurityGroup',
       'GroupName': group_name})

    response = self.do_query(self.HTTP_METHOD, self.ENDPOINT_URI, params)
    return True


  def authorize_ingress_by_cidr(self, group_name, ip_protocol, from_port,
                                to_port, cidr_range='0.0.0.0/0'):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'AuthorizeSecurityGroupIngress',
       'GroupName': group_name,
       'IpProtocol': ip_protocol,
       'FromPort': str(from_port),
       'ToPort': str(to_port),
       'CidrIp': cidr_range})

    response = self.do_query(self.HTTP_METHOD, self.ENDPOINT_URI, params)
    return True


  def revoke_ingress_by_cidr(self, group_name, ip_protocol, from_port,
                                to_port, cidr_range='0.0.0.0/0'):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'RevokeSecurityGroupIngress',
       'GroupName': group_name,
       'IpProtocol': ip_protocol,
       'FromPort': str(from_port),
       'ToPort': str(to_port),
       'CidrIp': cidr_range})

    response = self.do_query(self.HTTP_METHOD, self.ENDPOINT_URI, params)
    return True


  def authorize_ingress_by_group(self, group_name, source_group_name,
                                 source_group_owner_id):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'AuthorizeSecurityGroupIngress',
       'GroupName': group_name,
       'SourceSecurityGroupName': source_group_name,
       'SourceSecurityGroupOwnerId': source_group_owner_id})

    response = self.do_query(self.HTTP_METHOD, self.ENDPOINT_URI, params)
    return True


  def revoke_ingress_by_group(self, group_name, source_group_name,
                              source_group_owner_id):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'RevokeSecurityGroupIngress',
       'GroupName': group_name,
       'SourceSecurityGroupName': source_group_name,
       'SourceSecurityGroupOwnerId': source_group_owner_id})

    response = self.do_query(self.HTTP_METHOD, self.ENDPOINT_URI, params)
    return True


  def register_image(self, image_location):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'RegisterImage',
       'ImageLocation': image_location})

    response = self.do_query(self.HTTP_METHOD, self.ENDPOINT_URI, params)
    xmldoc = self.parse_xml(response.read())
    return self.xpath_value('//ns:imageId', xmldoc)


  def deregister_image(self, image_id):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'DeregisterImage',
       'ImageId': image_id})

    response = self.do_query(self.HTTP_METHOD, self.ENDPOINT_URI, params)
    return True


  def describe_image_attribute(self, image_id, attribute_name='launchPermission'):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'DescribeImageAttribute',
       'ImageId': image_id,
       'Attribute': attribute_name})

    response = self.do_query(self.HTTP_METHOD, self.ENDPOINT_URI, params)
    xmldoc = self.parse_xml(response.read())

    attribute = {'image_id': self.xpath_value('//ns:imageId', xmldoc)}

    if len(self.xpath_list('//ns:launchPermission', xmldoc)) > 0:
      attribute['groups'] = []
      for node in self.xpath_list('//ns:launchPermission/ns:item/ns:group', xmldoc):
        attribute['groups'].append(node.text)

      attribute['user_ids'] = []
      for node in self.xpath_list('//ns:launchPermission/ns:item/ns:userId', xmldoc):
        attribute['user_ids'].append(node.text)
    elif len(self.xpath_list('//ns:productCodes', xmldoc)) > 0:
      attribute['product_codes'] = []
      for node in self.xpath_list('//ns:productCodes/ns:item', xmldoc):
        attribute['product_codes'].append(node.text)

    return attribute


  def modify_image_attribute(self, image_id, attribute_name, operation,
                             attribute_values_dict):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'ModifyImageAttribute',
       'ImageId': image_id,
       'Attribute': attribute_name,
       'OperationType': operation},
      attribute_values_dict)

    response = self.do_query(self.HTTP_METHOD, self.ENDPOINT_URI, params)
    return True


  def reset_image_attribute(self, image_id, attribute_name):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'ResetImageAttribute',
       'ImageId': image_id,
       'Attribute': attribute_name})

    response = self.do_query(self.HTTP_METHOD, self.ENDPOINT_URI, params)
    return True


  def confirm_product_instance(self, product_code, instance_id):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'ConfirmProductInstance',
       'ProductCode': product_code,
       'InstanceId': instance_id})

    response = self.do_query(self.HTTP_METHOD, self.ENDPOINT_URI, params)
    xmldoc = self.parse_xml(response.read())
    return self.xpath_value('//ns:ownerId', xmldoc)





if __name__ == '__main__':
  ec2 = EC2(debug_mode=True)

  print ec2.describe_instances()

  # print ec2.describe_keypairs()

  # print ec2.create_keypair('testing', True)

  # print ec2.delete_keypair('testing2')

  # print ec2.describe_images(owner_ids=['amazon'])

  # print ec2.run_instances('ami-2bb65342', keypair_name='my-private-key',
  #                         security_groups=['web'], user_data='Just testing')
  # instance_id = 'i-3d4bbf54'

  # print ec2.get_console_output(instance_id)

  # print ec2.reboot_instances([instance_id])

  # print ec2.terminate_instances([instance_id])

  # print ec2.create_security_group('Test', 'Just testing')

  # print ec2.describe_security_groups(['Test'])

  # print ec2.authorize_ingress_by_cidr('Test', 'tcp', 1234, 1234, '0.0.0.0/0')

  # print ec2.revoke_ingress_by_cidr("Test", 'tcp', 1234, 1234, '0.0.0.0/0')

  # print ec2.authorize_ingress_by_group('Test', 'web', '123456789012')

  # print ec2.revoke_ingress_by_group('Test', 'web', '123456789012')

  # print ec2.delete_security_group('Test')

  # print ec2.register_image('oreilly-aws/ami-fedora7-base-i386.manifest.xml')
  # image_id = 'ami-86d93cef'

  # print ec2.deregister_image(image_id)

  # print ec2.describe_image_attribute('ami-1eca2f77', 'launchPermission')

  # print ec2.modify_image_attribute('ami-1eca2f77', 'launchPermission', 'add',
  #                                  {'UserId': ['123456789012', '987654321098'],
  #                                   'UserGroup': ['all']})

  # print ec2.reset_image_attribute('ami-1eca2f77', 'launchPermission')

  # print ec2.confirm_product_instance('1234', 'i-1234')
