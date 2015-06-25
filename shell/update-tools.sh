#!/bin/bash
# Update ec2-ami-tools to the latest version.
[ -f ec2-ami-tools.noarch.rpm ] && rm -f ec2-ami-tools.noarch.rpm
echo "Attempting ami-utils update from S3"
(wget http://s3.amazonaws.com/ec2-downloads/ec2-ami-tools.noarch.rpm \
      && echo "Retrieved ec2-ami-tools from S3" \
      || echo "Unable to retreive ec2-ami-tools from S3") \
   | logger -s -t "ec2"
(rpm -Uvh ec2-ami-tools.noarch.rpm \
      && echo "Updated ec2-ami-tools from S3" \
      || echo "ec2-ami-tools already up to date") \
   | logger -s -t "ec2"

# Update EC2 API tools to the latest version
[ -f ec2-api-tools.zip ] && rm -f ec2-api-tools.zip
wget http://s3.amazonaws.com/ec2-downloads/ec2-api-tools.zip -P /root
(cksum ec2-api-tools.zip | diff -q - .ec2-api_latest \
      && echo "API tools are up to date" \
      || (echo "Downloaded latest API tools"; unzip ec2-api-tools.zip; \
      cksum ec2-api-tools.zip > .ec2-api_latest)) \
   | logger -s -t "ec2"