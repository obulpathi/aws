import boto

cdn = boto.connect_cloudfront()

origin = boto.cloudfront.origin.S3Origin('mycdnbucket.s3.amazonaws.com')
distribution = cdn.create_distribution(origin=origin, enabled=True, comment='My CDN Distribution')
print distribution.domain_name
# u'd2oxf3980lnb8l.cloudfront.net'
print distribution.id
# u'ECH69MOIW7613'
print distribution.status
# u'InProgress'
print distribution.config.comment
# u'My new distribution'
print distribution.config.origin
# <S3Origin: mybucket.s3.amazonaws.com>
print distribution.config.caller_reference
# u'31b8d9cf-a623-4a28-b062-a91856fac6d0'
print distribution.config.enabled
