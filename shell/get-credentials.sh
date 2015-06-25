#!/bin/bash
# Retrieve the keypair public key data from the Instance Data service and
# add it to the root user’s authorized public keys file.

PUB_KEY_URI=http://169.254.169.254/latest/meta-data/public-keys/0/openssh-key
PUB_KEY_FROM_HTTP=/mnt/openssh_id.pub
ROOT_AUTHORIZED_KEYS=/root/.ssh/authorized_keys

# Make sure the root user's home directory has an .ssh directory for
# the authorized_keys file.
if [ ! -d /root/.ssh ]
then
  mkdir -p /root/.ssh
  chmod 700 /root/.ssh
fi

# Fetch the public key data and save it to a file
echo "Attempting to fetch public keypair"
curl --retry 3 --retry-delay 0 --silent --fail -o $PUB_KEY_FROM_HTTP $PUB_KEY_URI

# Add the public key data to the root user's authorized_keys file if it
# is not already present in this file.
if ! grep -q -f $PUB_KEY_FROM_HTTP $ROOT_AUTHORIZED_KEYS; then
  cat $PUB_KEY_FROM_HTTP >> $ROOT_AUTHORIZED_KEYS
  echo "New keypair added to authorized_keys file" | logger -t "ec2"
fi

# Ensure the authorized_keys file has the right permissions
chmod 600 $ROOT_AUTHORIZED_KEYS

# Remove the public key data file
rm -f $PUB_KEY_FROM_HTTP