#!/usr/bin/env python
"""
Sample Python code for the O'Reilly book "Using AWS Infrastructure Services"
by James Murty.

This code was written for Python version 2.4.4 or greater, and requires the
XML/XPath library "lxml" version 1.3.6 or greater (see
http://codespeak.net/lxml/).

The FPS module implements the Query API of the Amazon Flexible Payments
Service.
"""

from AWS import AWS
import time, base64
import urllib, urlparse, cgi

# XML handling is performed using the lxml library
from lxml import etree

class FPS(AWS):

  ENDPOINT_URI = 'https://fps.sandbox.amazonaws.com/'
  PIPELINE_URI = 'https://authorize.payments-sandbox.amazon.com/cobranded-ui/actions/start'
  API_VERSION = '2007-01-08'
  SIGNATURE_VERSION = '1'

  HTTP_METHOD = 'POST' # 'GET'

  XML_NAMESPACE = 'http://fps.amazonaws.com/doc/2007-01-08/'

  # Uses the do_query method defined in AWS to sends a GET or POST request
  # message to the FPS service's Query API interface and returns the response
  # result from the service.
  #
  # This method performs additional checking of the response from the FPS
  # service to check for error results, which may not be detected by the more
  # generic do_query method in AWS.
  def do_fps_query(self, parameters):
    response = self.do_query(self.HTTP_METHOD, self.ENDPOINT_URI, parameters)
    xmldoc = self.parse_xml(response.read())

    if 'Success' != self.xpath_value('Status', xmldoc):
      raise FpsServiceException(fps, xmldoc)

    return xmldoc

  ###
  # Methods to parse XML document elements into data structures
  ###

  def parse_amount(self, amount_node):
    if amount_node:
      return {
        'amount': self.xpath_value('Amount', amount_node),
        'currency_code': self.xpath_value('CurrencyCode', amount_node)}
    else:
      return None


  def parse_transaction_response(self, trans_node):
    trans_response = {
      'id': self.xpath_value('TransactionId', trans_node),
      'status': self.xpath_value('Status', trans_node),
      'status_detail': self.xpath_value('StatusDetail', trans_node),
      'token_usage': []}

    for usage_node in self.xpath_list('NewSenderTokenUsage', trans_node):
      trans_response['token_usage'].append(
        self.parse_token_usage_limit(usage_node))

    return trans_response


  def parse_token_usage_limit(self, usage_node):
    usage = {
      'last_reset_timestamp': self.xpath_value(
        'LastResetTimeStamp', usage_node)}

    if self.xpath_node('Amount', usage_node):
      usage['amount'] = self.parse_amount(
        self.xpath_node('Amount', usage_node))
      usage['last_reset_amount'] = self.parse_amount(
        self.xpath_node('LastResetAmount', usage_node))
    else:
      usage['count'] = int(self.xpath_value('Count', usage_node))
      usage['last_reset_count'] = int(self.xpath_value(
        'LastResetCount', usage_node))

    return usage


  def parse_transaction(self, trans_node):
    transaction = {
      'id': self.xpath_value('TransactionId', trans_node),
      'caller_transaction_date': self.xpath_value(
        'CallerTransactionDate', trans_node),

      'date_received': self.xpath_value('DateReceived', trans_node),
      'transaction_amount': self.parse_amount(
        self.xpath_node('TransactionAmount', trans_node)),
      'fees_amount': self.parse_amount(
        self.xpath_node('Fees', trans_node)),
      'operation': self.xpath_value('Operation', trans_node),
      'payment_method': self.xpath_value('PaymentMethod', trans_node),
      'status': self.xpath_value('Status', trans_node),
      'status_detail': self.xpath_value('StatusDetail', trans_node),

      'caller_name': self.xpath_value('CallerName', trans_node),
      'sender_name': self.xpath_value('SenderName', trans_node),
      'recipient_name': self.xpath_value('RecipientName', trans_node),
      'caller_token_id': self.xpath_value('CallerTokenId', trans_node),
      'sender_token_id': self.xpath_value('SenderTokenId', trans_node),
      'recipient_token_id': self.xpath_value('RecipientTokenId', trans_node),
      'error_code': self.xpath_value('ErrorCode', trans_node),
      'error_message': self.xpath_value('ErrorMessage', trans_node),
      'metadata': self.xpath_value('Metadata', trans_node),
      'original_transaction_id': self.xpath_value(
        'OriginalTransactionId', trans_node),
      'date_completed': self.xpath_value('DateCompleted', trans_node),
      'balance': self.parse_amount(
        self.xpath_node('Balance', trans_node)),
      'transaction_parts': [],
      'related_transactions': [],
      'status_history': [],
      'token_usage': []}

    for part_node in self.xpath_list('TransactionParts', trans_node):
      transaction['transaction_parts'].append({
        'account_id': self.xpath_value('AccountId', part_node),
        'role': self.xpath_value('Role', part_node),
        'name': self.xpath_value('Name', part_node),
        'instrument_id': self.xpath_value('InstrumentId', part_node),
        'description': self.xpath_value('Description', part_node),
        'reference': self.xpath_value('Reference', part_node),
        'fee_paid': self.parse_amount(
          self.xpath_node('FeePaid', part_node))
      })

    for status_node in self.xpath_list('StatusHistory', trans_node):
      transaction['status_history'].append({
        'status': self.xpath_value('Status', status_node),
        'date': self.xpath_value('Date', status_node),
        'amount': self.parse_amount(
          self.xpath_value('Amount', status_node))
        })

    for related_node in self.xpath_list('RelatedTransactions', trans_node):
      transaction['related_transactions'].append(
        self.xpath_value('TransactionId', related_node))

    for usage_node in self.xpath_list('NewSenderTokenUsage', trans_node):
      transaction['token_usage'].append(
        self.parse_token_usage_limit(usage_node))

    return transaction


  def parse_token(self, token_node):
    return {
      'id': self.xpath_value('TokenId', token_node),
      'old_id': self.xpath_value('OldTokenId', token_node),
      'status': self.xpath_value('Status', token_node),
      'caller_installed': self.xpath_value('CallerInstalled', token_node),
      'date_installed': self.xpath_value('DateInstalled', token_node),
      'caller_ref': self.xpath_value('CallerReference', token_node),
      'type': self.xpath_value('TokenType', token_node),
      'friendly_name': self.xpath_value('FriendlyName', token_node),
      'payment_reason': self.xpath_value('PaymentReason', token_node)}


  ###
  # Methods for generating Co-Branded UI Pipeline request URIs.
  ###

  # Generates generic Co-Branded UI Pipeline request URIs.
  def generate_pipeline_url(self, pipeline, return_url, parameters={}):
    # Set mandatory parameters
    my_params = {
      'callerKey': self.aws_access_key,
      'pipelineName': pipeline,
      'returnURL': return_url}

    # Add any extra parameters, ignoring those with a null value
    for param_name in parameters:
      if parameters[param_name]:
        my_params[param_name] = str(parameters[param_name])

    # Build CBUI pipeline URI with sorted parameters.
    my_params_sorted = list(my_params)
    my_params_sorted.sort()
    query = ''
    for param_name in my_params_sorted:
      if len(query) > 0: query += '&'
      query += '%s=%s' % (param_name,
                          urllib.quote(my_params[param_name], safe=''))

    # Sign Pipeline URI
    pipeline_path = urlparse.urlparse(self.PIPELINE_URI)[2]
    request_desc = pipeline_path + '?' + query
    signature = self.generate_signature(request_desc)
    query += '&awsSignature=%s' % urllib.quote(signature)

    return self.PIPELINE_URI + "?" + query


  def get_url_for_single_use_sender(self, caller_ref, amount, return_url,
                                    method=None, reason=None,
                                    recipient_token=None, can_reserve=None):
    params = {
      'callerReference': caller_ref,
      'transactionAmount': amount,
      'paymentReason': reason,
      'paymentMethod': method,
      'recipientToken': recipient_token,
      'reserve': can_reserve}
    return self.generate_pipeline_url('SingleUse', return_url, params)


  def get_url_for_multi_use_sender(self, caller_ref, global_amount_limit, return_url,
      method=None, reason=None, recipient_tokens=None, amount_limit_type=None,
      amount_limit_value=None, validity_start=None, validity_expiry=None, usage_limits=[]):
    params = {
      'callerReference': caller_ref,
      'globalAmountLimit': global_amount_limit,
      'paymentReason': reason,
      'paymentMethod': method,
      'recipientTokenList': recipient_tokens,
      'amountType': amount_limit_type,
      'transactionAmount': amount_limit_value,
      'validityStart': validity_start,
      'validityExpiry': validity_expiry}

    limit_suffix = 1
    for usage_limit in usage_limits:
      params['usageLimitType%i' % limit_suffix] = usage_limit['type']
      params['usageLimitValue%i' % limit_suffix] = usage_limit['value']
      params['usageLimitPeriod%i' % limit_suffix] = usage_limit['period']
      limit_suffix += 1

    return self.generate_pipeline_url('MultiUse', return_url, params)


  def get_url_for_recurring_sender(self, caller_ref, amount, recurring_period,
      return_url, method=None, reason=None, recipient_token=None,
      validity_start=None, validity_expiry=None):
    params = {
      'callerReference': caller_ref,
      'transactionAmount': amount,
      'recurringPeriod': recurring_period,
      'paymentReason': reason,
      'paymentMethod': method,
      'recipientToken': recipient_token,
      'validityStart': validity_start,
      'validityExpiry': validity_expiry}

    return self.generate_pipeline_url('Recurring', return_url, params)


  def get_url_for_recipient(self, caller_ref, caller_ref_refund,
                            recipient_pays_fees, return_url,
                            method=None, validity_start=None, validity_expiry=None):
    params = {
      'callerReference': caller_ref,
      'callerReferenceRefund': caller_ref_refund,
      'recipientPaysFee': str(recipient_pays_fees),
      'paymentMethod': method,
      'validityStart': validity_start,
      'validityExpiry': validity_expiry}

    return self.generate_pipeline_url('Recipient', return_url, params)


  def get_url_for_prepaid_instrument(self, caller_ref_sender, caller_ref_funding,
                                     amount, return_url, method=None, reason=None,
                                     validity_start=None, validity_expiry=None):
    params = {
      'callerReferenceSender': caller_ref_sender,
      'callerReferenceFunding': caller_ref_funding,
      'fundingAmount': amount,
      'paymentMethod': method,
      'paymentReason': reason,
      'validityStart': validity_start,
      'validityExpiry': validity_expiry}

    return self.generate_pipeline_url('SetupPrepaid', return_url, params)


  def get_url_for_postpaid_instrument(self, caller_ref_sender, caller_ref_settlement,
      credit_limit_amount, global_limit_amount, return_url, method=None, reason=None,
      validity_start=None, validity_expiry=None, usage_limits=[]):
    params = {
      'callerReferenceSender': caller_ref_sender,
      'callerReferenceSettlement': caller_ref_settlement,
      'creditLimit': credit_limit_amount,
      'globalAmountLimit': global_limit_amount,
      'paymentMethod': method,
      'paymentReason': reason,
      'validityStart': validity_start,
      'validityExpiry': validity_expiry}

    limit_suffix = 1
    for usage_limit in usage_limits:
      params['usageLimitType%i' % limit_suffix] = usage_limit['type']
      params['usageLimitValue%i' % limit_suffix] = usage_limit['value']
      params['usageLimitPeriod%i' % limit_suffix] = usage_limit['period']
      limit_suffix += 1

    return self.generate_pipeline_url('SetupPostpaid', return_url, params)


  def get_url_for_editing(self, caller_ref, token_id, return_url, method=None):
    params = {
      'callerReference': caller_ref,
      'tokenID': token_id,
      'paymentMethod': method}

    return self.generate_pipeline_url('EditToken', return_url, params)


  ###
  # Methods for handling result URIs from the Co-Branded UI Pipeline
  ###

  def parse_url_parameters(self, url):
    scheme, host, path, params, query, fragment = urlparse.urlparse(url)
    params = {}

    if not query: return params

    for param_name_and_value in query.split('&'):
      equals_offset = param_name_and_value.index('=')

      # Everything before the first '=' is the parameter's name,
      param_name = param_name_and_value[0:equals_offset]

      # Everything after the first '=' is the parameter's value.
      param_value = param_name_and_value[equals_offset + 1:]

      # Unescape parameter values, except for 'awsSignature'.
      if param_name == 'awsSignature':
        params[param_name] = param_value
      else:
        params[param_name] = urllib.unquote(param_value)

    return params


  def verify_pipeline_result_url(self, url):
    params = self.parse_url_parameters(url)

    # Find the AWS signature and remove it from the parameters
    sig_received = params['awsSignature']
    del params['awsSignature']

    # Sort the remaining parameters alphabetically, ignoring case.
    sorted_param_names = list(params)
    sorted_param_names.sort(lambda x,y: cmp(x.lower(), y.lower()))

    # Build our own request description string from the result URI
    request_desc = urlparse.urlparse(url)[2] + '?'
    for param_name in sorted_param_names:
      if request_desc[-1] != '?': request_desc += '&'
      request_desc += '%s=%s' % (param_name,
                                 urllib.quote(params[param_name], safe=''))

    # Calculate the signature we expect
    sig_expected = self.generate_signature(request_desc)

    return sig_received == sig_expected


  ###
  # Methods that implement the FPS service's Query API interface operations.
  ###

  def get_account_activity(self, start_date, end_date=None, max_batch_size=None,
                           sort_order=None, response_group=None, operation=None,
                           method=None, role=None, status=None):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'GetAccountActivity',
       'StartDate': start_date,
       'EndDate': end_date,
       'MaxBatchSize': max_batch_size,
       'SortOrderByDate': sort_order,
       'ResponseGroup': response_group,
       'Operation': operation,
       'PaymentMethod': method,
       'Role': role,
       'Status': status})

    xmldoc = self.do_fps_query(params)

    activity = {
      'response_batch_size': int(self.xpath_value(
        '//ResponseBatchSize', xmldoc)),
      'start_time_for_next_transaction': self.xpath_value(
        '//StartTimeForNextTransaction', xmldoc),
      'transactions': []}

    for trans_node in self.xpath_list('//Transactions', xmldoc):
      activity['transactions'].append(self.parse_transaction(trans_node))

    return activity


  def get_account_balance(self):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'GetAccountBalance'})

    xmldoc = self.do_fps_query(params)

    return {
      'total_balance': self.parse_amount(
        self.xpath_node('//TotalBalance', xmldoc)),
      'pending_in_balance': self.parse_amount(
        self.xpath_node('//PendingInBalance', xmldoc)),
      'pending_out_balance': self.parse_amount(
        self.xpath_node('//PendingOutBalance', xmldoc)),
      'disburse_balance': self.parse_amount(
        self.xpath_node('//DisburseBalance', xmldoc)),
      'refund_balance': self.parse_amount(
        self.xpath_node('//RefundBalance', xmldoc)),
    }


  def install_payment_instruction(self, payment_instructions,
                                  caller_ref, token_type, reason=None,
                                  friendly_name=None):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'InstallPaymentInstruction',
       'PaymentInstruction': payment_instructions,
       'CallerReference': caller_ref,
       'TokenType': token_type,
       'TokenFriendlyName': friendly_name,
       'PaymentReason': reason})

    xmldoc = self.do_fps_query(params)
    return self.xpath_value('//TokenId', xmldoc)


  def get_payment_instruction(self, token_id):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'GetPaymentInstruction',
       'TokenId': token_id})

    xmldoc = self.do_fps_query(params)
    return {
      'account_id': self.xpath_value('//AccountId', xmldoc),
      'instructions': self.xpath_value('//PaymentInstruction', xmldoc),
      'token': self.parse_token(self.xpath_node('//Token', xmldoc))}


  def get_tokens(self, friendly_name=None, status=None, caller_ref=None):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'GetTokens',
       'TokenFriendlyName': friendly_name,
       'TokenStatus': status,
       'CallerReference': caller_ref})

    xmldoc = self.do_fps_query(params)

    tokens = []
    for token_node in self.xpath_list('//Tokens', xmldoc):
      tokens.append(self.parse_token(token_node))
    return tokens


  def get_token_by_caller(self, token_id=None, caller_ref=None):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'GetTokenByCaller',
       'TokenId': token_id,
       'CallerReference': caller_ref})

    xmldoc = self.do_fps_query(params)

    return self.parse_token(self.xpath_node('//Token', xmldoc))


  def get_token_usage(self, token_id):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'GetTokenUsage',
       'TokenId': token_id})

    xmldoc = self.do_fps_query(params)

    limits = []
    for usage_node in self.xpath_list('//TokenUsageLimits', xmldoc):
      limits.append(self.parse_token_usage_limit(usage_node))
    return limits


  def cancel_token(self, token_id, reason=None):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'CancelToken',
       'TokenId': token_id,
       'ReasonText': reason})

    xmldoc = self.do_fps_query(params)
    return True


  def pay(self, recipient_token_id, sender_token_id, caller_token_id,
      caller_ref, amount, currency_code, charge_to, caller_date=None,
      sender_ref=None, recipient_ref=None, caller_description=None,
      sender_description=None, recipient_description=None, metadata=None):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'Pay',
       'RecipientTokenId': recipient_token_id,
       'SenderTokenId': sender_token_id,
       'CallerTokenId': caller_token_id,
       'CallerReference': caller_ref,
       'TransactionAmount.Amount': amount,
       'TransactionAmount.CurrencyCode': currency_code,
       'ChargeFeeTo': charge_to,

       # Optional parameters
       'TransactionDate': caller_date,
       'SenderReference': sender_ref,
       'RecipientReference': recipient_ref,
       'SenderDescription': sender_description,
       'RecipientDescription': recipient_description,
       'CallerDescription': caller_description,
       'MetaData': metadata})

    xmldoc = self.do_fps_query(params)
    return self.parse_transaction_response(
      self.xpath_node("//*[local-name()='TransactionResponse']", xmldoc))


  def refund(self, refund_token_id, caller_token_id, transaction_id,
      caller_ref, charge_to, refund_amount=None, currency_code=None,
      caller_date=None, sender_ref=None, recipient_ref=None,
      caller_description=None, sender_description=None,
      recipient_description=None, metadata=None):

    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'Refund',
       'CallerTokenId': caller_token_id,
       'RefundSenderTokenId': refund_token_id,
       'TransactionId': transaction_id,
       'CallerReference': caller_ref,
       'ChargeFeeTo': charge_to,

       # Optional parameters
       'RefundAmount.Amount': refund_amount,
       'RefundAmount.CurrencyCode': currency_code,
       'TransactionDate': caller_date,
       'RefundSenderReference': sender_ref,
       'RefundRecipientReference': recipient_ref,
       'RefundSenderDescription': sender_description,
       'RefundRecipientDescription': recipient_description,
       'CallerDescription': caller_description,
       'MetaData': metadata})

    xmldoc = self.do_fps_query(params)
    return self.parse_transaction_response(
      self.xpath_node("//*[local-name()='TransactionResponse']", xmldoc))


  def reserve(self, recipient_token_id, sender_token_id, caller_token_id,
      caller_ref, amount, currency_code, charge_to,
      caller_date=None, sender_ref=None, recipient_ref=None,
      caller_description=None, sender_description=None,
      recipient_description=None, metadata=None):

    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'Reserve',
       'CallerTokenId': caller_token_id,
       'RecipientTokenId': recipient_token_id,
       'SenderTokenId': sender_token_id,
       'TransactionAmount.Amount': amount,
       'TransactionAmount.CurrencyCode': currency_code,
       'CallerReference': caller_ref,
       'ChargeFeeTo': charge_to,

       # Optional parameters
       'TransactionDate': caller_date,
       'SenderReference': sender_ref,
       'RecipientReference': recipient_ref,
       'SenderDescription': sender_description,
       'RecipientDescription': recipient_description,
       'CallerDescription': caller_description,
       'MetaData': metadata})

    xmldoc = self.do_fps_query(params)
    return self.parse_transaction_response(
      self.xpath_node("//*[local-name()='TransactionResponse']", xmldoc))


  def settle(self, transaction_id, amount=None, currency_code=None,
             caller_date=None):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'Settle',
       'ReserveTransactionId': transaction_id,

       # Optional parameters
       'TransactionAmount.Amount': amount,
       'TransactionAmount.CurrencyCode': currency_code,
       'TransactionDate': caller_date})

    xmldoc = self.do_fps_query(params)
    return self.parse_transaction_response(
      self.xpath_node("//*[local-name()='TransactionResponse']", xmldoc))


  def get_transaction(self, transaction_id):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'GetTransaction',
       'TransactionId': transaction_id})

    xmldoc = self.do_fps_query(params)
    return self.parse_transaction(
      self.xpath_node("//*[local-name()='Transaction']", xmldoc))


  def retry_transaction(self, transaction_id):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'RetryTransaction',
       'OriginalTransactionId': transaction_id})

    xmldoc = self.do_fps_query(params)
    return self.parse_transaction_response(
      self.xpath_node("//*[local-name()='TransactionResponse']", xmldoc))


  def get_results(self, max_results_count=None, operation=None):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'GetResults',
       'MaxResultsCount': max_results_count,
       'Operation': operation})

    xmldoc = self.do_fps_query(params)

    results = []
    for result_node in self.xpath_list('//TransactionResults', xmldoc):
      results.append({
        'id': self.xpath_value('TransactionId', result_node),
        'operation': self.xpath_value('Operation', result_node),
        'status': self.xpath_value('Status', result_node),
        'caller_ref': self.xpath_value('CallerReference', result_node)})

    return {
      'number_pending': int(self.xpath_value('//NumberPending', xmldoc)),
      'results': results}


  def discard_results(self, transaction_ids):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'DiscardResults'},
      {'TransactionIds': transaction_ids})

    xmldoc = self.do_fps_query(params)

    errors = []
    for error_node in self.xpath_list('//DiscardErrors', xmldoc):
      errors.append(error_node.text)
    return errors


  def fund_prepaid(self, funding_sender_token_id, instrument_id, caller_token_id,
      caller_ref, amount, currency_code, charge_to, caller_date=None,
      sender_ref=None, recipient_ref=None, caller_description=None,
      sender_description=None, recipient_description=None, metadata=None):

    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'FundPrepaid',
       'SenderTokenId': funding_sender_token_id,
       'PrepaidInstrumentId': instrument_id,
       'CallerTokenId': caller_token_id,
       'CallerReference': caller_ref,
       'FundingAmount.Amount': amount,
       'FundingAmount.CurrencyCode': currency_code,
       'ChargeFeeTo': charge_to,

       # Optional parameters
       'TransactionDate': caller_date,
       'SenderReference': sender_ref,
       'RecipientReference': recipient_ref,
       'SenderDescription': sender_description,
       'RecipientDescription': recipient_description,
       'CallerDescription': caller_description,
       'MetaData': metadata})

    xmldoc = self.do_fps_query(params)
    return self.parse_transaction_response(
      self.xpath_node("//*[local-name()='TransactionResponse']", xmldoc))


  def get_prepaid_balance(self, instrument_id):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'GetPrepaidBalance',
       'PrepaidInstrumentId': instrument_id})

    xmldoc = self.do_fps_query(params)

    balance_node = self.xpath_node('//PrepaidBalance', xmldoc)

    return {
      'available_balance': self.parse_amount(
        self.xpath_node('AvailableBalance', balance_node)),
      'pending_in_balance': self.parse_amount(
        self.xpath_node('PendingInBalance', balance_node))}


  def get_all_prepaid_instruments(self, status=None):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'GetAllPrepaidInstruments',
       'InstrumentStatus': status})

    xmldoc = self.do_fps_query(params)

    instrument_ids = []
    for node in self.xpath_list('//PrepaidInstrumentIds/InstrumentId', xmldoc):
      instrument_ids.append(node.text)
    return instrument_ids


  def get_total_prepaid_liability(self):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'GetTotalPrepaidLiability'})

    xmldoc = self.do_fps_query(params)

    liability_node = self.xpath_node('//OutstandingPrepaidLiability', xmldoc)

    return {
      'outstanding_balance': self.parse_amount(
        self.xpath_node('OutstandingBalance', liability_node)),
      'pending_in_balance': self.parse_amount(
        self.xpath_node('PendingInBalance', liability_node))}


  def settle_debt(self, settlement_token_id, instrument_id, caller_token_id,
      caller_ref, amount, currency_code, caller_date=None,
      sender_ref=None, recipient_ref=None, caller_description=None,
      sender_description=None, recipient_description=None, metadata=None):

    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'SettleDebt',
       'SenderTokenId': settlement_token_id,
       'CreditInstrumentId': instrument_id,
       'CallerTokenId': caller_token_id,
       'CallerReference': caller_ref,
       'SettlementAmount.Amount': amount,
       'SettlementAmount.CurrencyCode': currency_code,

       # Fee-payer is hard-coded to 'Recipient'
       'ChargeFeeTo': 'Recipient',

       # Optional parameters
       'TransactionDate': caller_date,
       'SenderReference': sender_ref,
       'RecipientReference': recipient_ref,
       'SenderDescription': sender_description,
       'RecipientDescription': recipient_description,
       'CallerDescription': caller_description,
       'MetaData': metadata})

    xmldoc = self.do_fps_query(params)
    return self.parse_transaction_response(
      self.xpath_node("//*[local-name()='TransactionResponse']", xmldoc))


  def write_off_debt(self, instrument_id, caller_token_id,
      caller_ref, amount, currency_code, caller_date=None,
      sender_ref=None, recipient_ref=None, caller_description=None,
      sender_description=None, recipient_description=None, metadata=None):

    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'WriteOffDebt',
       'CreditInstrumentId': instrument_id,
       'CallerTokenId': caller_token_id,
       'CallerReference': caller_ref,
       'AdjustmentAmount.Amount': amount,
       'AdjustmentAmount.CurrencyCode': currency_code,

       # Optional parameters
       'TransactionDate': caller_date,
       'SenderReference': sender_ref,
       'RecipientReference': recipient_ref,
       'SenderDescription': sender_description,
       'RecipientDescription': recipient_description,
       'CallerDescription': caller_description,
       'MetaData': metadata})

    xmldoc = self.do_fps_query(params)
    return self.parse_transaction_response(
      self.xpath_node("//*[local-name()='TransactionResponse']", xmldoc))


  def get_all_credit_instruments(self, status=None):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'GetAllCreditInstruments',
       'InstrumentStatus': status})

    xmldoc = self.do_fps_query(params)

    instrument_ids = []
    for node in self.xpath_list('//CreditInstrumentIds/InstrumentId', xmldoc):
      instrument_ids.append(node.text)
    return instrument_ids


  def get_debt_balance(self, instrument_id):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'GetDebtBalance',
       'CreditInstrumentId': instrument_id})

    xmldoc = self.do_fps_query(params)

    balance_node = self.xpath_node('//DebtBalance', xmldoc)

    return {
      'available_balance': self.parse_amount(
        self.xpath_node('AvailableBalance', balance_node)),
      'pending_out_balance': self.parse_amount(
        self.xpath_node('PendingOutBalance', balance_node))}


  def get_outstanding_debt_balance(self):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'GetOutstandingDebtBalance'})

    xmldoc = self.do_fps_query(params)

    debt_node = self.xpath_node('//OutstandingDebt', xmldoc)

    return {
      'outstanding_balance': self.parse_amount(
        self.xpath_node('OutstandingBalance', debt_node)),
      'pending_out_balance': self.parse_amount(
        self.xpath_node('PendingOutBalance', debt_node))}


  def subscribe_for_caller_notification(self, operation, web_service_api_url):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'SubscribeForCallerNotification',
       'NotificationOperationName': operation,
       'WebServiceAPIURL': web_service_api_url})

    xmldoc = self.do_fps_query(params)
    return True


  def unsubscribe_for_caller_notification(self, operation):
    params = self.build_query_parameters(
      self.API_VERSION, self.SIGNATURE_VERSION,
      {'Action': 'UnSubscribeForCallerNotification',
       'NotificationOperationName': operation})

    xmldoc = self.do_fps_query(params)
    return True


  def build_payment_widget(self, payments_account_id, amount, description,
                           extra_fields={}):
    fields = {}
    for n in extra_fields:
      fields[n] = extra_fields[n]

    # Mandatory fields
    fields['amazonPaymentsAccountId'] = payments_account_id
    fields['accessKey'] = self.aws_access_key
    fields['amount'] = "USD %.2f" % amount
    fields['description'] = description

    # Generate a widget description and sign it
    field_names_sorted = list(fields)
    field_names_sorted.sort()

    widget_desc = ''
    for field_name in field_names_sorted:
      widget_desc += field_name + fields[field_name]
    fields['signature'] = self.generate_signature(widget_desc)

    # Combine all fields into a string
    fields_string = ''
    for n in fields:
      fields_string += '<input type="hidden" name="%s" value="%s">\n' % \
                       (n, fields[n])

    return """
    <form method="post" action="https://authorize.payments-sandbox.amazon.com/pba/paypipeline">
    %s
    <input type="image" border="0" src="https://authorize.payments-sandbox.amazon.com/pba/images/payNowButton.png">
    </form>
    """ % fields_string



# An exception object that captures information about an FPS service error.
class FpsServiceException(Exception):
  message = ''
  code = None
  reason = None
  type = None
  is_retriable = False

  def __init__(self, fps, xmldoc):
    error_node = fps.xpath_node('//Errors/Errors', xmldoc)

    self.code = fps.xpath_value('ErrorCode', error_node)
    self.reason = fps.xpath_value('ReasonText', error_node)
    self.type = fps.xpath_value('ErrorType', error_node)
    self.is_retriable = 'true' == fps.xpath_value('IsRetriable', error_node)

    self.message = 'FPS Service Error: %s - %s' % (self.code, self.reason)
    self.args = (self.message, etree.tostring(xmldoc))


if __name__ == '__main__':
  return_url = 'http://localhost:8888/'

  fps = FPS(debug_mode=True)

  #print fps.get_account_balance()

  #days_ago = 10
  #start_time = time.gmtime(time.time() - (days_ago * 24 * 3600))
  #print fps.get_account_activity(
  #  time.strftime(AWS.ISO8601, start_time), status='Success', role='Caller')

  #print fps.install_payment_instruction(
  #  "MyRole == 'Caller';", "CallerRef1", 'Unrestricted',
  #  reason="Reason text", friendly_name="TemporaryCallerToken")
  #token_id = '71UF8U6NGAAL5FGDQ71U4MUNDEIDNEV95VMD3I88KVPIEU3QQHVRHQ8QXJNVTUCJ'

  #print fps.get_payment_instruction(token_id)

  #print fps.get_tokens(friendly_name='TemporaryCallerToken')

  #print fps.get_token_by_caller(caller_ref='CallerRef1')
  #print fps.get_token_by_caller(token_id=token_id)

  #token_id = '77UF6UQNGUAU5FQD171B4UUNMENDNTVC5VBDVI8TKFPIZU7QQUVDHQFQ9JN1TGCU'
  #print fps.get_token_usage(token_id)

  #token_id = '71UF8U6NGAAL5FGDQ71U4MUNDEIDNEV95VMD3I88KVPIEU3QQHVRHQ8QXJNVTUCJ'
  #print fps.cancel_token(token_id, 'Python Testing')

  ## Generate SingleUse Sender token (one-off payment)
  #print fps.get_url_for_single_use_sender('SingleUsePythonTest1', 1.01, return_url)

  ## Pay
  #sender_tok_id = \
  #  "U27XL5KSQMTT3274Q8UG6CTBNYUUINLHPXLGKGMCM32P6EJ18BH84GXN9DOSXAON"
  #caller_tok_id = \
  #  "74UFLUGNG3AF5FHD67114VUNNEBDNUVL5VRDCI8HK6PIRU8QQQVVHQXQNJN3TZC2"
  #recipient_tok_id = \
  #  "76UF2UENGSAZ5F8D571T4IUNKEDDNLV25VDDQI8TKTPIQU3QQLVEHQBQKJNSTJCE"
  #print fps.pay(recipient_tok_id, sender_tok_id, caller_tok_id,
  #              'PayPythonTest1', 1.01, 'USD', 'Recipient',
  #              caller_date=time.strftime(AWS.ISO8601, time.gmtime()),
  #              caller_description='PayPythonTest1.Caller',
  #              metadata=base64.b64encode('Just some metadata'))

  ## Refund
  #transaction_id = '12QN4ZDH71OFGQ2GRD2MZ7UQHPD46DPHA2F'
  #caller_tok_id = \
  #  "74UFLUGNG3AF5FHD67114VUNNEBDNUVL5VRDCI8HK6PIRU8QQQVVHQXQNJN3TZC2"
  #refund_sender_tok_id = \
  #  "71UFVU7NGNAK5F3DC71Z47UNGE3DNCVJ5VJD6I8KKJPISU3QQ8VVHQLQEJNCTNC1"
  #print fps.refund(refund_sender_tok_id, caller_tok_id, transaction_id,
  #                 'RefundPythonTest1', 'Recipient',
  #                 refund_amount=0.51, currency_code='USD')

  ## Generate SingleUse Sender token (reserve/settle payment)
  #print fps.get_url_for_single_use_sender('SingleUsePythonTest2', 5.50,
  #                                        return_url, method='CC',
  #                                        can_reserve=True)

  ## Reserve
  #sender_tok_id = \
  #  "U67XF5CSQMTK32R448UH6BTBAYQUI5L7PX6GFGMGMS2PPEH18HHI4GFNGDOLXEON"
  #caller_tok_id = \
  #  "74UFLUGNG3AF5FHD67114VUNNEBDNUVL5VRDCI8HK6PIRU8QQQVVHQXQNJN3TZC2"
  #recipient_tok_id = \
  #  "76UF2UENGSAZ5F8D571T4IUNKEDDNLV25VDDQI8TKTPIQU3QQLVEHQBQKJNSTJCE"
  #print fps.reserve(recipient_tok_id, sender_tok_id, caller_tok_id,
  #                  'ReservePythonTest1', 5.50, 'USD', 'Recipient')

  ## Settle
  #transaction_id = '12QN59JT97UKZJ9HODR5DC17FBHFSLH94LG'
  #print fps.settle(transaction_id, amount=5.00, currency_code='USD')

  ## GetTransaction
  #transaction_id = '12QN59JT97UKZJ9HODR5DC17FBHFSLH94LG'
  #print fps.get_transaction(transaction_id)

  ## RetryTransaction
  #transaction_id = "12Q4IJRVL54RHSG4ZMJIZL5DUTI7Q7Q1EFQ"
  #print fps.retry_transaction(transaction_id)

  ## GetResults
  #print fps.get_results(max_results_count=10, operation='Pay')

  ## DiscardResults
  #print fps.discard_results(
  #  ['12QN4ZDH71OFGQ2GRD2MZ7UQHPD46DPHA2F',
  #   '12Q5B1UNN1I1MVRM3M2K8385P9C836551VL'])

  ## Generate MultiUse Sender token
  #usage_limits = [
  #  {'type': 'Count', 'value': 3, 'period': '1 Day'},
  #  {'type': 'Amount', 'value': 75.25, 'period': '1 Month'}]
  #print fps.get_url_for_multi_use_sender(
  #  'MultiUsePythonTest1', 100, return_url,
  #  method='CC', amount_limit_type='Maximum', amount_limit_value=25.15,
  #  reason='Testing', usage_limits=usage_limits)

  ## Generate Recurring Sender token
  #print fps.get_url_for_recurring_sender('RecurringPythonTest1', 100.00,
  #  '7 Days', return_url,
  #  validity_expiry=int(time.time() + 24 * 3600))

  ## Generate Recipient and Refund tokens
  #print fps.get_url_for_recipient('RecipientPythonTest1',
  #                                'RecipientRefundPythonTest1',
  #                                False, return_url)

  ## Generate URL for EditToken CBUI Pipeline
  #token_id = 'U77X455SQ5T53294Q8UV6BTBCY7UIILDPX8GEGMZMP2PIEQ18RHS4G1N5DOLXHOT'
  #print fps.get_url_for_editing('EditTokenPythonTest1', token_id, return_url)

  ## Generate URL for SetupPrepaid CBUI Pipeline
  #print fps.get_url_for_prepaid_instrument('PrepaidPythonTest1.SenderToken',
  #  'PrepaidPythonTest1.FundingToken', 15.00, return_url, method='CC')

  ## FundPrepaid
  #prepaid_funding_sender_token_id = \
  #  'U67XR5RSQCT332P4K8U56KTBUY4UICLIPXSGKGM1MR2PSEC18ZH74GJNMDOMXZOJ'
  #instrument_id = \
  #  'U57XZ5ZSQETV3214I8UX6ETB1Y5UIJLAPXCGXGMFM12PBEA18XH74GGNKDOHX7OK'
  #caller_tok_id = \
  #  "74UFLUGNG3AF5FHD67114VUNNEBDNUVL5VRDCI8HK6PIRU8QQQVVHQXQNJN3TZC2"
  #print fps.fund_prepaid(prepaid_funding_sender_token_id, instrument_id,
  #  caller_tok_id, 'FundPrepaidPythonTest1', 15.00, 'USD', 'Recipient')

  ## GetPrepaidBalance
  #instrument_id = \
  #  'U57XZ5ZSQETV3214I8UX6ETB1Y5UIJLAPXCGXGMFM12PBEA18XH74GGNKDOHX7OK'
  #print fps.get_prepaid_balance(instrument_id)

  ## GetAllPrepaidInstruments
  #print fps.get_all_prepaid_instruments(status='Active')

  ## GetTotalPrepaidLiability
  #print fps.get_total_prepaid_liability()

  ## Generate URL for SetupPostpaid CBUI Pipeline
  #usage_limits = [
  #  {'type': 'Count', 'value': 5, 'period': '7 Days'},
  #  {'type': 'Amount', 'value': 50.00, 'period': '2 Months'}]
  #print fps.get_url_for_postpaid_instrument('PostpaidPythonTest1.Sender',
  #  'PostpaidPythonTest1.Settlement', 15.00, 100.00, return_url,
  #  usage_limits=usage_limits)

  ## Make a payment from the Postpaid (Credit) instrument
  #caller_tok_id = \
  #  "74UFLUGNG3AF5FHD67114VUNNEBDNUVL5VRDCI8HK6PIRU8QQQVVHQXQNJN3TZC2"
  #recipient_tok_id = \
  #    "76UF2UENGSAZ5F8D571T4IUNKEDDNLV25VDDQI8TKTPIQU3QQLVEHQBQKJNSTJCE"
  #credit_sender_tok_id = \
  #    "US7X75QSQSTD3214F8UH6TTBFYPUIJLKPXKGKGMZM12PCE718NHP4G6NJDOKXPOR"
  #print fps.pay(recipient_tok_id, credit_sender_tok_id, caller_tok_id,
  #  'PayFromCreditPythonTest1', 10.00, 'USD', 'Recipient')

  ## SettleDebt
  #settlement_token_id = \
  #  'U47XX58SQATE32X4B8U36VTBDYIUIKL8PX4GQGMHMU2P3EL18VHG4GLNBDO1X1OD'
  #instrument_id = \
  #  'U67XX5HSQDTC32Q4F8UK6QTBNYDUIDLNPX4GNGM3MB2PRE718DHT4GQN7DO8X2ON'
  #caller_tok_id = \
  #  "74UFLUGNG3AF5FHD67114VUNNEBDNUVL5VRDCI8HK6PIRU8QQQVVHQXQNJN3TZC2"
  #print fps.settle_debt(settlement_token_id, instrument_id, caller_tok_id,
  #  'SettleDebtPythonTest1', 8.00, 'USD')

  ## WriteOffDebt
  #instrument_id = \
  #  'U67XX5HSQDTC32Q4F8UK6QTBNYDUIDLNPX4GNGM3MB2PRE718DHT4GQN7DO8X2ON'
  #caller_tok_id = \
  #  "74UFLUGNG3AF5FHD67114VUNNEBDNUVL5VRDCI8HK6PIRU8QQQVVHQXQNJN3TZC2"
  #print fps.write_off_debt(instrument_id, caller_tok_id,
  #  'SettleDebtPythonTest1', 2.00, 'USD')

  ## GetAllCreditInstruments
  #print fps.get_all_credit_instruments()

  ## GetDebtBalance
  #instrument_id = \
  #  'U67XX5HSQDTC32Q4F8UK6QTBNYDUIDLNPX4GNGM3MB2PRE718DHT4GQN7DO8X2ON'
  #print fps.get_debt_balance(instrument_id)

  ## GetOutstandingDebtBalance
  #print fps.get_outstanding_debt_balance()

  ## SubscribeForCallerNotification
  #notification_web_service = 'https://www.myhost.com/api'
  #print fps.subscribe_for_caller_notification('postTokenCancellation',
  #                                            notification_web_service)

  ## UnsubscribeForCallerNotification
  #print fps.unsubscribe_for_caller_notification('postTokenCancellation')

  ## Parse parameters in Result URL from Co-Branded UI Pipeline, and verify the URL
  #postpaid_instrument_result_url = \
  #  "http://localhost:8888/?status=SC" + \
  #  "&expiry=03%2F2013" + \
  #  "&creditSenderTokenID=US7X75QSQSTD3214F8UH6TTBFYPUIJLKPXKGKGMZM12PCE718NHP4G6NJDOKXPOR" + \
  #  "&settlementTokenID=U47XX58SQATE32X4B8U36VTBDYIUIKL8PX4GQGMHMU2P3EL18VHG4GLNBDO1X1OD" + \
  #  "&creditInstrumentID=U67XX5HSQDTC32Q4F8UK6QTBNYDUIDLNPX4GNGM3MB2PRE718DHT4GQN7DO8X2ON" + \
  #  "&awsSignature=wSCVtfFYHdx6OE+STuAlCxJ2aXE="
  #print fps.parse_url_parameters(postpaid_instrument_result_url)
  #print fps.verify_pipeline_result_url(postpaid_instrument_result_url)

  #payments_account_id = 'ABCDEFGHIJ1234567890ABCDEFGHIJ12345678'
  #options = {
  #  'referenceId' : 'ProductCode-1234',
  #  'returnUrl' : 'http://localhost/post_payment_success',
  #  'abandonUrl' : 'http://localhost/post_payment_cancel',
  #  'immediateReturn' : '1'
  #}
  #print fps.build_payment_widget(payments_account_id, 500.00, 'Moon', options)
