import StringIO, time, os, sys, traceback
from threading import Thread
from boto.services.service import Service
from boto.sqs.message import Message, MHMessage

ISO8601 = '%Y-%m-%dT%H:%M:%SZ'

class MultiCommandService(Service):

#####################################################################
#  Add the following method to hard-code service variable values, so
#  you can test this service implementation outside the EC2
#  environment.
#
#  def get_userdata(self):
#    # Service.get_userdata(self)
#    self.notify_email = 'your_email@address.com'
#    self.input_queue_name = 'serviceInput'
#    self.output_queue_name = 'serviceOutput'
#    self.log_queue_name = 'serviceLog'
#
#    self.status_queue_name = 'serviceStatus'
#    self.meta_data = {'instance-id' : 'i-00000001'}
#
#####################################################################

## Methods in this section are used in the Lifeguard Application ####

  def __init__(self, **params):
    self.status_queue_name = None
    self.last_status_update = None
    Service.__init__(self, params)

    if self.status_queue_name:
      self.status_queue = self.get_queue(self.status_queue_name)
      self.status_queue.set_message_class(Message)


  def send_status(self, state):
    print 'Sending status message: %s' % state
    if self.status_queue_name == None:
      return

    if self.meta_data.has_key('instance-id'):
      iid = self.meta_data['instance-id']
    else:
      iid = 'unknown'

    if self.last_status_update == None:
      duration = 0
    else:
      duration = int((time.time() - self.last_status_update) / 1000)
    self.last_status_update = time.time()

    timestamp = time.strftime(ISO8601, time.gmtime())

    status_msg = "<InstanceStatus " + \
      "xmlns=\"http://lifeguard.directthought.com/doc/2007-06-12/\">\n" + \
      "<InstanceId>%s</InstanceId>\n" + \
      "<State>%s</State>\n" + \
      "<LastInterval>PT%sS</LastInterval>\n" + \
      "<Timestamp>%s</Timestamp>\n" + \
      "</InstanceStatus>"
    status_msg = status_msg % (iid, state, duration, timestamp)

    m = self.status_queue.new_message(body=status_msg)
    self.status_queue.write(m)

#####################################################################

  ## Empty stub, will be implemented in the Lifeguard Use Case
  #def send_status(self, state):
  #  pass


  # Process an input message, run the commands it contains and return
  # a list of the result objects stored in S3.
  def process_message(self, input_message, log_fp):
    # Retrieve information from input message
    bucket = input_message['Bucket']
    input_keys = input_message['InputKeys'].split(',')
    input_names = input_message['InputNames'].strip().split(',')
    commands = input_message['Commands'].split(',')

    # Download all input files from S3 using InputKeys
    key_index = 0
    in_files = []
    for input_key in input_keys:
      # Use alternative name for downloaded file if InputName is set
      if len(input_names) > key_index and len(input_names[key_index]) > 0:
        input_name = input_names[key_index]
      else:
        input_name = input_key
      in_file = os.path.join(self.working_dir, input_name)
      in_files.append(in_file)
      self.get_file(bucket, input_key, in_file) # Download file
      key_index += 1

    # Perform commands
    results = []

    for command in commands:
      # Deduce name of output file from last component of command
      output_name = command.split()[-1]
      out_file = os.path.join(self.working_dir, output_name)
      results.append((out_file, output_name))

      # Invoke the command in a shell
      log_fp.write('\n= Command =\n%s\n' % command)
      fs = os.popen3(command)

      # Log the results of the command
      stdout = fs[1].read()
      stderr = fs[2].read()
      log_fp.write('= Output =\n%s%s\n' % (stdout, stderr))
      for f in fs:
        f.close()

      # Command failed if anything was written to the error stream
      if len(stderr) > 0:
        self.delete_message(input_message)
        raise Exception('Command failed: %s' % stderr)

    # Upload all output files to S3 (key name will be an MD5 hash)
    output_keys = []
    output_names = []
    for file, name in results:
      key = self.put_file(bucket, file)
      output_keys.append('%s;type=%s' % (key.name, key.content_type))
      output_names.append(name)

    # Delete all input and output files
    for file, name in results:
      os.remove(file)
    for file in in_files:
      os.remove(file)

    return (output_keys, output_names)


  # Overrides the default Service life cycle
  def run(self):
    # Override default Service settings
    self.MainLoopDelay = 5
    self.ProcessingTime = 30

    # Output from command invocations will be logged
    log_file_name = self.__class__.__name__ + '.log'
    log_file_name = os.path.join(self.working_dir, log_file_name)
    log_fp = open(log_file_name, 'a', 0)

    self.notify('Starting MultiCommandService')
    self.send_status('idle')

    vthread = None # Variable to hold the current VisibilityThread
    while True:
      try:
        input_message = self.read_message()
        if input_message:
          log_fp.write('\n---------------------------\n')
          log_fp.write('= Message =\n%s\n' % input_message.get_body())

          self.send_status('busy')

          # Start visibility extension thread in the background
          vthread = VisibilityThread(self, input_message)
          vthread.start()

          # Process message
          (output_keys, output_names) = \
            self.process_message(input_message, log_fp)

          # Build and send output message describing results
          output_message = MHMessage(None, input_message.get_body())
          output_message['OutputKeys'] = ','.join(output_keys)
          output_message['OutputNames'] = ','.join(output_names)
          self.write_message(output_message)

          # Clean up
          vthread.halt()
          self.delete_message(input_message)
          self.cleanup()
          self.send_status('idle')
        else:
          time.sleep(self.MainLoopDelay)
      except KeyboardInterrupt, ke:
        if vthread: vthread.halt()
        log_fp.close()
        self.notify('Service manually shut down')
        return
      except Exception, e:
        if vthread: vthread.halt()
        fp = StringIO.StringIO()
        traceback.print_exc(None, fp)
        s = fp.getvalue()
        self.notify('Service failed\n%s' % s)
        self.create_connections()
        self.send_status('idle')


class VisibilityThread(Thread):

  def __init__(self, service, message):
    Thread.__init__(self)
    self.service = service
    self.message = message
    self.run_flag = True

  def run(self):
    sleep_time = max(self.service.ProcessingTime - 3, 2)
    extra_time = self.service.ProcessingTime
    print 'VisibilityThread - Extensions of %ds every %ds' % \
      (extra_time, sleep_time)

    while 1:
      # Delay until sleep time is elapsed, but only ever sleep
      # for a short time so the thread can be quickly halted.
      stime = time.time()
      while self.run_flag and time.time() - stime < sleep_time:
        time.sleep(0.5)

      # Check whether thread is halted before doing work
      if not self.run_flag:
        return

      print 'Extending visibility timeout by %ss' % extra_time
      self.message.change_visibility(extra_time)

  def halt(self):
    self.run_flag = False
