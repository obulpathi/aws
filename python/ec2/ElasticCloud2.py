import boto
regions = boto.ec2.regions()
regions
useast = regions[1]
useast
useast.endpoint
useconn = useast.connect()

useconn.get_all_security_groups()
sg=useconn.get_all_security_groups()[0]
sg.authorize('tcp', 22, 22, '0.0.0.0/0')

ubuimages=useconn.get_all_images(owners= ['099720109477', ])
ubumachines = [ x for x in ubuimages if x.type == 'machine' ]
nattymachines = [ x for x in ubuimages if (x.type == 'machine' and re.search("atty", str(x.name))) ]

natty = [ x for x in ubuimages if x.id == 'ami-7e4ab917' ][0]
reservation = natty.run(key_name='default',instance_type='t1.micro')

instance = reservation.instances[0]

instance.update()
instance.state

instance.public_dns_name

useconn.terminate_instances(instance_ids=[instance.id])
