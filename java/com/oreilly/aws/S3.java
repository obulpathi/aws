package com.oreilly.aws;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.w3c.dom.Document;
import org.w3c.dom.Node;

/**
 * Sample Java code for the O'Reilly book "Using AWS Infrastructure Services"
 * by James Murty.
 * <p>
 * This code was written for Java version 5.0 or greater. If you are not using
 * Sun's Java implementation, this also code requires the Apache Commons Codec
 * library (see http://commons.apache.org/codec/)
 * <p>
 * The S3 class implements the REST API of the Amazon Simple Storage Service.
 */
public class S3 extends AWS {
    
    public static final String S3_ENDPOINT = "s3.amazonaws.com";
    public static final String XMLNS = "http://s3.amazonaws.com/doc/2006-03-01/";
        
    /**
     * Initialize the service and set the service-specific variables: 
     * awsAccessKey, awsSecretKey, isDebugMode, and isSecureHttp.
     * 
     * This constructor obtains your AWS access and secret key credentials from
     * the AWS_ACCESS_KEY and AWS_SECRET_KEY environment variables respectively.
     * It sets isDebugMode to false, and isSecureHttp to true.
     */
    public S3() {
        super();
    }
    
    /**
     * Initialize the service and set the service-specific variables: 
     * awsAccessKey, awsSecretKey, isDebugMode, and isSecureHttp.
     * 
     * This constructor obtains your AWS access and secret key credentials from
     * the AWS_ACCESS_KEY and AWS_SECRET_KEY environment variables respectively.
     * It sets isDebugMode and isSecureHttp according to the values you provide.
     */
    public S3(boolean isDebugMode, boolean isSecureHttp) {
        super(isDebugMode, isSecureHttp);
    }

    /**
     * Initialize the service and set the service-specific variables: 
     * awsAccessKey, awsSecretKey, isDebugMode, and isSecureHttp.
     */
    public S3(String awsAccessKey, String awsSecretKey, boolean isDebugMode,
        boolean isSecureHttp) 
    {
        super(awsAccessKey, awsSecretKey, isDebugMode, isSecureHttp);
    }
    
    /**
     * Returns true if the given bucket name can be used as part of an S3
     * sub-domain host name. 
     */
    protected boolean isValidDnsName(String bucketName) {
        // Ensure bucket name is within length constraints
        if (bucketName == null || bucketName.length() > 63 
            || bucketName.length() < 3) 
        {
            return false;
        }
        
        // Only lower-case letters, numbers, '.' or '-' characters allowed
        if (!Pattern.matches("^[a-z0-9][a-z0-9.-]+$", bucketName)) {
            return false;
        }

        // Cannot be an IP address (must contain at least one lower-case letter)
        if (!Pattern.matches(".*[a-z].*", bucketName)) {
            return false;
        }
        
        // Components of name between '.' characters cannot start or end with '-', 
        // and cannot be empty
        String[] fragments = bucketName.split("\\.");
        for (int i = 0; i < fragments.length; i++) {
            if (Pattern.matches("^-.*", fragments[i])
                || Pattern.matches(".*-$", fragments[i])
                || Pattern.matches("^$", fragments[i])) 
            {
                return false;
            }
        }
        
        return true;        
    }
    
    protected URL generateS3Url(String bucketName, String objectKey, 
        Map<String, String> parameters) throws Exception
    {
        // Decide between the default and sub-domain host name formats
        String hostname = null;
        if (isValidDnsName(bucketName)) {
          hostname = bucketName + "." + S3_ENDPOINT;
        } else {
          hostname = S3_ENDPOINT;
        }

        // Build an initial secure or non-secure URI for the end point.
        String requestUrl = (isSecureHttp 
            ? "https://" : "http://") + hostname;

        // Include the bucket name in the URI except for alternative hostnames
        if (bucketName.length() > 0 && hostname.equals(S3_ENDPOINT)) {
          requestUrl += "/" + URLEncoder.encode(bucketName, "UTF-8");
        }

        // Add object name component to URI if present
        if (objectKey.length() > 0) {
            requestUrl += "/" + URLEncoder.encode(objectKey, "UTF-8");
        }
        
        // Ensure URL includes at least a slash in the path, if nothing else
        if (objectKey.length() == 0 && !hostname.equals(S3_ENDPOINT)) {
            requestUrl += "/";
        }

        // Add request parameters to the URI.
        StringBuffer query = new StringBuffer();
        for (Map.Entry<String, String> parameter : parameters.entrySet()) {
            if (query.length() > 0) {
                query.append("&");
            }
            
            if (parameter.getValue() == null) {
                query.append(parameter.getKey());
            } else {
                query.append(parameter.getKey() + "=" 
                    + URLEncoder.encode(parameter.getValue(), "UTF-8"));                    
            }
        }
        if (query.length() > 0) {
            requestUrl += "?" + query;            
        }

        return new URL(requestUrl);
    }
    
    public BucketList listBuckets() throws Exception {
        URL url = generateS3Url("", "", EMPTY_STRING_MAP);
        HttpURLConnection conn = doRest(HttpMethod.GET, url);
        Document xmlDoc = parseToDocument(conn.getInputStream());
        
        BucketList bucketList = new BucketList();
        
        for (Node node : xpathToNodeList("//Buckets/Bucket", xmlDoc)) {
            Bucket bucket = new Bucket();
            bucket.name = xpathToContent("Name", node);
            bucket.creationDate = xpathToContent("CreationDate", node);
            bucketList.buckets.add(bucket);
        }
        
        Owner owner = new Owner();
        owner.id = xpathToContent("//Owner/ID", xmlDoc);
        owner.displayName = xpathToContent("//Owner/DisplayName", xmlDoc);
        
        bucketList.owner = owner;
        return bucketList;
    }
    
    public Bucket createBucket(String bucketName, BucketLocation location) 
        throws Exception 
    {
        URL url = generateS3Url(bucketName, "", EMPTY_STRING_MAP);

        if (location != BucketLocation.US) {
            String configDocText =
                "<CreateBucketConfiguration xmlns='" + XMLNS + "'>" +
                    "<LocationConstraint>" + 
                        location.toString() + 
                    "</LocationConstraint>" +
                "</CreateBucketConfiguration>";
            InputStream dataInputStream = 
                new ByteArrayInputStream(configDocText.getBytes("UTF-8"));
            
            Map<String, String> headers = new HashMap<String, String>();
            headers.put("Content-Type", "application/xml");
            doRest(HttpMethod.PUT, url, dataInputStream, headers);             
        } else {
            doRest(HttpMethod.PUT, url);
        }
        
        Bucket bucket = new Bucket();
        bucket.name = bucketName;
        return bucket;
    }
    
    public boolean deleteBucket(String bucketName) throws Exception {
        URL url = generateS3Url(bucketName, "", EMPTY_STRING_MAP);
        doRest(HttpMethod.DELETE, url);
        return true;
    }
    
    public String getBucketLocation(String bucketName) throws Exception {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("location", null);
        
        URL url = generateS3Url(bucketName, "", parameters);
        HttpURLConnection conn = doRest(HttpMethod.GET, url);

        Document xmlDoc = parseToDocument(conn.getInputStream());
        return xpathToContent("//LocationConstraint", xmlDoc);
    }
    
    public ObjectList listObjects(String bucketName, 
        Map<String, String> parameters) throws Exception 
    {
        ObjectList objectList = new ObjectList();
        objectList.bucketName = bucketName;
        
        boolean isTruncated = true;
        
        while (isTruncated) {
            URL url = generateS3Url(bucketName, "", parameters);
            HttpURLConnection conn = doRest(HttpMethod.GET, url);
            
            Document xmlDoc = parseToDocument(conn.getInputStream());

            for (Node node : xpathToNodeList("//Contents", xmlDoc)) {
                S3Object object = new S3Object();
                object.key = xpathToContent("Key", node);
                object.size = xpathToContent("Size", node);
                object.lastModified = xpathToContent("LastModified", node);
                object.etag = xpathToContent("ETag", node);
                Owner owner = new Owner();
                owner.id = xpathToContent("Owner/ID", node);
                owner.displayName = xpathToContent("Owner/DisplayName", node);
                object.owner = owner;
                objectList.objects.add(object);
            }
            
            for (Node node : xpathToNodeList("//CommonPrefixes", xmlDoc)) {
                objectList.prefixes.add(node.getTextContent());
            }
            
            // Determine whether listing is truncated
            isTruncated = "true".equals(xpathToContent("//IsTruncated", xmlDoc));
            
            // Set the marker parameter to the NextMarker if possible,
            // otherwise set it to the last key name in the listing            
            if (xpathToContent("//NextMarker", xmlDoc) != null) {
                parameters.put("marker", 
                    xpathToContent("//NextMarker", xmlDoc));
            } else if (xpathToContent("//Contents/Key", xmlDoc) != null) {
                // Java's XPath implementation doesn't support the 'last()'
                // function, so we must manually find the last Key node.
                List<Node> keys = xpathToNodeList("//Contents/Key", xmlDoc);
                Node lastNode = keys.get(keys.size() - 1);
                parameters.put("marker", lastNode.getTextContent());
            } else {
                parameters.put("marker", "");
            }
        } // End of while loop
        
        return objectList;
    }
    
    public boolean createObject(String bucketName, String objectKey, 
        InputStream dataInputStream, Map<String, String> headers, 
        Map<String, String> metadata) throws Exception
    {
        return createObject(bucketName, objectKey, dataInputStream, headers, 
            metadata, null);
    }
    
    public boolean createObject(String bucketName, String objectKey, 
        InputStream dataInputStream, Map<String, String> headers, 
        Map<String, String> metadata, String policy) throws Exception
    {
        // The Content-Length header must always be set when data is uploaded.
        headers.put("Content-Length", 
            String.valueOf(dataInputStream.available()));
        
        // Calculate an md5 hash of the data for upload verification,
        // provided we can reset the input stream when we're done.
        String md5Digest = "";
        if (dataInputStream.markSupported()) {
            MessageDigest messageDigest = MessageDigest.getInstance("MD5");
            byte[] buf = new byte[8192];
            int bytes = -1;
            while ((bytes = dataInputStream.read(buf, 0, buf.length)) != -1) {
                messageDigest.update(buf, 0, bytes);
            }
            dataInputStream.reset();

            md5Digest = encodeBase64(messageDigest.digest());
        }
        headers.put("Content-MD5", md5Digest);
        
        // Set the canned policy, may be: 'private', 'public-read',
        // 'public-read-write', 'authenticated-read'
        if (policy != null) {
            headers.put("x-amz-acl", policy);
        }
        
        // Set an explicit content type if none is provided
        if (!headers.containsKey("Content-Type")) {
            headers.put("Content-Type", "application/octet-stream");            
        }

        // Convert metadata items to headers using the
        // S3 metadata header name prefix.
        for (Map.Entry<String, String> metadataHeader : metadata.entrySet()) {
            headers.put("x-amz-meta-" + metadataHeader.getKey(), 
                metadataHeader.getValue());
        }

        URL url = generateS3Url(bucketName, objectKey, EMPTY_STRING_MAP);
        doRest(HttpMethod.PUT, url, dataInputStream, headers);        
        return true;
    }
    
    public boolean deleteObject(String bucketName, String objectKey) 
        throws Exception 
    {
        URL url = generateS3Url(bucketName, objectKey, EMPTY_STRING_MAP);
        doRest(HttpMethod.DELETE, url);
        return true;
    }
    
    public S3Object getObject(String bucketName, String objectKey, 
        Map<String, String> headers, OutputStream dataOutputStream) 
        throws Exception 
    {
        URL url = generateS3Url(bucketName, objectKey, EMPTY_STRING_MAP);
        HttpURLConnection conn = doRest(HttpMethod.GET, url, null, headers);
        
        Map<String, String> responseHeaders = new HashMap<String, String>();
        Map<String, String> metadata = new HashMap<String, String>();
        
        // Find metadata headers.
        for (String headerName : conn.getHeaderFields().keySet()) {
            if (headerName == null) continue;

            if (headerName.startsWith("x-amz-meta")) {
                metadata.put(headerName.substring(11), 
                    conn.getHeaderFields().get(headerName).get(0));
            } else {
                responseHeaders.put(headerName, 
                    conn.getHeaderFields().get(headerName).get(0));                
            }
        }
        
        S3Object object = new S3Object();
        object.key = objectKey;
        object.etag = responseHeaders.get("ETag");
        object.lastModified = responseHeaders.get("Last-Modified");
        object.size = responseHeaders.get("Content-Length");
        object.metadata = metadata;
        
        // Download data
        if (dataOutputStream != null) {
            InputStream inputStream = conn.getInputStream();
            byte[] buffer = new byte[8192];
            int count = -1;
            while ((count = inputStream.read(buffer)) != -1) {
                dataOutputStream.write(buffer, 0, count);
            }
            dataOutputStream.close();
            inputStream.close();                    
        } else {
            object.body = getInputStreamAsString(conn.getInputStream());
        }
        
        return object;
    }
 
    public S3Object getObjectMetadata(String bucketName, String objectKey, 
        Map<String, String> headers) throws Exception
    {
        URL url = generateS3Url(bucketName, objectKey, EMPTY_STRING_MAP);
        HttpURLConnection conn = doRest(HttpMethod.HEAD, url, null, headers);
        
        Map<String, String> responseHeaders = new HashMap<String, String>();
        Map<String, String> metadata = new HashMap<String, String>();
        
        // Find metadata headers.
        for (String headerName : conn.getHeaderFields().keySet()) {
            if (headerName == null) continue;

            if (headerName.startsWith("x-amz-meta")) {
                metadata.put(headerName.substring(11), 
                    conn.getHeaderFields().get(headerName).get(0));
            } else {
                responseHeaders.put(headerName, 
                    conn.getHeaderFields().get(headerName).get(0));                
            }
        }
        
        S3Object object = new S3Object();
        object.key = objectKey;
        object.etag = responseHeaders.get("ETag");
        object.lastModified = responseHeaders.get("Last-Modified");
        object.size = responseHeaders.get("Content-Length");
        object.metadata = metadata;        
        
        return object;
    }
    
    public BucketLoggingStatus getLogging(String bucketName) throws Exception {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("logging", null);
        
        URL url = generateS3Url(bucketName, "", parameters);
        HttpURLConnection conn = doRest(HttpMethod.GET, url);

        Document xmlDoc = parseToDocument(conn.getInputStream());
        
        BucketLoggingStatus status = new BucketLoggingStatus();

        if (xpathToNodeList("//LoggingEnabled", xmlDoc).size() > 0) {
            status.enabled = true;
            status.targetBucket = xpathToContent("//TargetBucket", xmlDoc); 
            status.targetPrefix = xpathToContent("//TargetPrefix", xmlDoc); 
        } 
        
        return status;
    }
    
    public boolean setLogging(String bucketName, BucketLoggingStatus status) 
        throws Exception 
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("logging", null);
        
        URL url = generateS3Url(bucketName, "", parameters);

        String statusText = "<BucketLoggingStatus xmlns='" + XMLNS + "'>";
        if (status.enabled) {
            statusText += 
                "<LoggingEnabled>" +
                    "<TargetBucket>" + status.targetBucket + "</TargetBucket>" +
                    "<TargetPrefix>" + status.targetPrefix + "</TargetPrefix>" +
                "</LoggingEnabled>";
        }        
        statusText += "</BucketLoggingStatus>";

        InputStream dataInputStream = 
            new ByteArrayInputStream(statusText.getBytes("UTF-8"));
            
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("Content-Type", "application/xml");
        doRest(HttpMethod.PUT, url, dataInputStream, headers);             
        
        return true;
    }
    
    public AccessControlList getAcl(String bucketName, String objectKey) 
        throws Exception 
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("acl", null);
        
        URL url = generateS3Url(bucketName, objectKey, parameters);
        HttpURLConnection conn = doRest(HttpMethod.GET, url);

        Document xmlDoc = parseToDocument(conn.getInputStream());
        AccessControlList acl = new AccessControlList();
        
        for (Node grantNode : xpathToNodeList("//Grant", xmlDoc)) {
            Grant grant = new Grant();
            grant.permission = Permission.valueOf(
                xpathToContent("Permission", grantNode));
            
            String type = xpathToContent("Grantee/@type", grantNode);
            
            if (type.equals("Group")) {
                GroupGrantee grantee = new GroupGrantee();
                grantee.type = type;
                grantee.uri = xpathToContent("Grantee/URI", grantNode);
                grant.grantee = grantee;
            } else {
                CanonicalUserGrantee grantee = new CanonicalUserGrantee();
                grantee.type = type;
                grantee.id = xpathToContent("Grantee/ID", grantNode);
                grantee.displayName = 
                    xpathToContent("Grantee/DisplayName", grantNode);
                grant.grantee = grantee;
            }
            acl.grants.add(grant);
        }
        acl.owner = new Owner();
        acl.owner.id = xpathToContent("//Owner/ID", xmlDoc);
        acl.owner.displayName = xpathToContent("//Owner/DisplayName", xmlDoc);
        
        return acl;
    }
    
    public boolean setAcl(String bucketName, String objectKey, 
        AccessControlList acl) throws Exception 
    {
        StringBuffer aclText = new StringBuffer();
        aclText.append("<AccessControlPolicy xmlns='" + XMLNS + "'>");
        aclText.append("<Owner><ID>" + acl.owner.id + "</ID></Owner>");
        aclText.append("<AccessControlList>");
        
        for (Grant grant : acl.grants) {
            Grantee grantee = grant.grantee;

            aclText.append("<Grant><Permission>" 
                + grant.permission 
                + "</Permission>");
            aclText.append("<Grantee");
            aclText.append(" ").append(
                "xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'");
            
            if (grantee instanceof GroupGrantee) {
                aclText.append(" xsi:type='Group'>");
                aclText.append("<URI>" + ((GroupGrantee)grantee).uri + "</URI>");
            } else if (grantee instanceof CanonicalUserGrantee) {
                aclText.append(" xsi:type='CanonicalUser'>");
                aclText.append("<ID>" 
                    + ((CanonicalUserGrantee)grantee).id + "</ID>");                
            } else {
                aclText.append(" xsi:type='AmazonCustomerByEmail'>");
                aclText.append("<EmailAddress>" 
                    + ((AmazonCustomerByEmail)grantee).emailAddress 
                    + "</EmailAddress>");                
            }
            aclText.append("</Grantee>");
            aclText.append("</Grant>");
        }        
        aclText.append("</AccessControlList>");
        aclText.append("</AccessControlPolicy>");
        
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("acl", null);
        
        URL url = generateS3Url(bucketName, objectKey, parameters);

        InputStream dataInputStream = 
            new ByteArrayInputStream(aclText.toString().getBytes("UTF-8"));
            
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("Content-Type", "application/xml");
        doRest(HttpMethod.PUT, url, dataInputStream, headers);             
        return true;
    }
    
    public boolean setCannedAcl(String cannedAcl, String bucketName, 
        String objectKey) throws Exception
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("acl", null);
        URL url = generateS3Url(bucketName, objectKey, parameters);

        Map<String, String> headers = new HashMap<String, String>();
        headers.put("x-amz-acl", cannedAcl);
        doRest(HttpMethod.PUT, url, null, headers);             
        return true;
    }
    
    public void getTorrent(String bucketName, String objectKey, File torrentFile)
        throws Exception
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("torrent", null);
        URL url = generateS3Url(bucketName, objectKey, parameters);
        
        HttpURLConnection conn = doRest(HttpMethod.GET, url);
        
        // Download torrent file data
        FileOutputStream dataOutputStream = new FileOutputStream(torrentFile);        
        InputStream inputStream = conn.getInputStream();
        byte[] buffer = new byte[8192];
        int count = -1;
        while ((count = inputStream.read(buffer)) != -1) {
            dataOutputStream.write(buffer, 0, count);
        }
        dataOutputStream.close();
        inputStream.close();                    
    }
    
    public URL getSignedUri(HttpMethod method, long expires, String bucketName,
        String objectKey, Map<String, String> parameters, 
        Map<String, String> headers, boolean isVirtualHost) throws Exception
    {
        headers.put("Date", String.valueOf(expires));
        URL url = generateS3Url(bucketName, objectKey, parameters);
        
        String signature = generateRestSignature(method, url, headers);
        String signedUrl = (isSecureHttp ? "https" : "http");
        signedUrl += "://";
        
        if (isVirtualHost) {
            signedUrl += bucketName;
        } else {
            signedUrl += url.getHost();
        }
        
        signedUrl += url.getPath();
            
        if (url.getQuery() != null) {
            signedUrl += "?" + url.getQuery() + "&";
        } else {
            signedUrl += "?";
        }
        
        signedUrl += "Signature=" + URLEncoder.encode(signature, "UTF-8");
        signedUrl += "&Expires=" + expires;
        signedUrl += "&AWSAccessKeyId=" + awsAccessKey;
        
        return new URL(signedUrl);
    }
        
    public String buildPostPolicy(Date expirationTime, 
        Map<String, Object> conditions) throws Exception 
    {
        if (expirationTime == null) {
            throw new Exception("Policy document must include a valid expiration");
        }
        if (conditions == null) {
            throw new Exception("Policy document must include valid conditions");
        }
         
        // Convert conditions object mappings to condition statements
        List<String> conds = new ArrayList<String>();
        for (Map.Entry<String, Object> condition : conditions.entrySet()) {
            String name = condition.getKey();
            Object test = condition.getValue();
                        
            if (test == null) {
                // A nil condition value means allow anything.
                conds.add("[\"starts-with\", \"$" + name + "\", \"\"]");
            } else if (test instanceof String) {
                conds.add("{\"" + name + "\": \"" + test + "\"}");
            } else if (test instanceof List) {
                conds.add("{\"" + name + "\": \"" + join((List) test, ",") + "\"}");
            } else if (test instanceof String[]) {                
                conds.add("{\"" + name + "\": \"" + join(Arrays.asList((String[]) test), ",") + "\"}");
            } else if (test instanceof Map) {
                String operation = (String) ((Map) test).get("op");
                String value = (String) ((Map) test).get("value");
                conds.add("[\"" + operation + "\", \"$" + name + "\", \"" + value + "\"]");
            } else if (test instanceof Range) {
                Range range = (Range) test;
                conds.add("[\"" + name + "\", " + range.begin + ", " + range.end + "]");                
            } else {
                throw new Exception("Unexpected value type for condition '" 
                    + name + "': " + test.getClass());
            }
        }
        
        return "{\"expiration\": \"" + iso8601DateFormat.format(expirationTime) 
            + "\", \"conditions\": [" + join(conds, ",") + "]}"; 
    }

    public String buildPostForm(String bucketName, String key) throws Exception 
    {
        return buildPostForm(bucketName, key, null, null, null, null);
    }

    public String buildPostForm(String bucketName, String key, Date expiration,
        Map<String, Object> conditions) throws Exception 
    {
        return buildPostForm(bucketName, key, expiration, conditions, null, null);
    }

    public String buildPostForm(String bucketName, String key, Date expiration,
        Map<String, Object> conditions, Map<String, String> fields, 
        String textInput) throws Exception 
    {
        List<String> inputFields = new ArrayList<String>();
        
        // Form is only authenticated if a policy is specified.
        if (expiration != null || conditions != null) {
            // Generate policy document
            String policy = buildPostPolicy(expiration, conditions);
            if (isDebugMode()) {
                System.out.println("POST Policy\n===========\n" + policy + "\n\n");
            }

            // Add the base64-encoded policy document as the 'policy' field
            String policyB64 = encodeBase64(policy);
            inputFields.add("<input type=\"hidden\" name=\"policy\" value=\""
                + policyB64 + "\">");

            // Add the AWS access key as the 'AWSAccessKeyId' field
            inputFields.add("<input type=\"hidden\" name=\"AWSAccessKeyId\" " +
                "value=\"" + getAwsAccessKey() + "\">");

            // Add signature for encoded policy document as the 'AWSAccessKeyId' field
            String signature = generateSignature(policyB64);
            inputFields.add("<input type=\"hidden\" name=\"signature\" " +
                "value=\"" + signature + "\">");
        }
        
        // Include any additional fields
        if (fields != null) {
            for (Map.Entry<String, String> field : fields.entrySet()) {
                if (field.getValue() == null) {
                    // Allow users to provide their own <input> fields as text.
                    inputFields.add(field.getKey());
                } else {
                    inputFields.add("<input type=\"hidden\" name=\"" + 
                        field.getKey() + "\" value=\"" + field.getValue() + "\">");                    
                }
            }
        }

        // Add the vital 'file' input item, which may be a textarea or file.
        if (textInput != null) {
            // Use the textInput option which should specify a textarea or text
            // input field. For example:
            // '<textarea name="file" cols="80" rows="5">Default Text</textarea>'
            inputFields.add(textInput);
        } else {
            inputFields.add("<input name=\"file\" type=\"file\">");            
        }

        // Construct a sub-domain URL to refer to the target bucket. The
        // HTTPS protocol will be used if the secure HTTP option is enabled.
        String url = "http" + (isSecureHttp()? "s" : "") + 
            "://" + bucketName + ".s3.amazonaws.com/";

        // Construct the entire form.
        String form = 
          "<form action=\"" + url + "\" method=\"post\" " + 
              "enctype=\"multipart/form-data\">\n" +
            "<input type=\"hidden\" name=\"key\" value=\"" + key + "\">\n" +
            join(inputFields, "\n") +
            "\n<br>\n" +
            "<input type=\"submit\" value=\"Upload to Amazon S3\">\n" +
          "</form>";
        
        if (isDebugMode()) {
            System.out.println("POST Form\n=========\n" + form + "\n");
        }

        return form;
    }    
    
    
        
    /*
     * Below this point are class and enum definitions specific to the Java
     * implementation of AWS clients. These items make it easier to pass
     * parameters into this client's methods, and to retrieve results from the
     * methods.
     */

    public static enum BucketLocation { US, EU };

    public static enum Permission { 
        READ, WRITE, READ_ACP, WRITE_ACP, FULL_CONTROL };
        
    class Owner {
        String id;
        String displayName;

        public String toString() { 
            return "{" + this.getClass().getName()
            + ": id=" + id + ", displayName=" + displayName + "}";
        }
    }
    
    class Bucket {
        String name;
        String creationDate;

        public String toString() { 
            return "{" + this.getClass().getName()
            + ": name=" + name + ", creationDate=" + creationDate + "}";
        }
    }
    
    class BucketList {
        Owner owner = null;
        List<Bucket> buckets = new ArrayList<Bucket>();
        
        public String toString() { 
            return "{" + this.getClass().getName()
            + ": owner=" + owner + ", buckets=" + buckets + "}";
        }
    }
    
    class S3Object {
        String key;
        String size;
        String lastModified;
        String etag;
        Owner owner;
        Map<String, String> metadata = new HashMap<String, String>();
        String body = null;
        
        public String toString() { 
            return "{" + this.getClass().getName()
            + ": key=" + key + ", size=" + size + ", lastModified=" 
            + lastModified + ", etag=" + etag + ", owner=" + owner + 
            ", metadata=" + metadata + ", body=" + body + "}";
        }
    }
    
    class ObjectList {
        String bucketName;
        List<S3Object> objects = new ArrayList<S3Object>();
        List<String> prefixes = new ArrayList<String>();
        
        public String toString() { 
            return "{" + this.getClass().getName()
            + ": bucketName=" + bucketName + ", prefixes=" + prefixes 
            + ", objects=" + objects + "}";
        }
    }
    
    class BucketLoggingStatus {
        boolean enabled = false;
        String targetBucket = "";
        String targetPrefix = "";
        
        public String toString() { 
            return "{" + this.getClass().getName()
            + ": enabled=" + enabled + ", targetBucket=" + targetBucket 
            + ", targetPrefix=" + targetPrefix + "}";
        }
    }
        
    class AccessControlList {
        Owner owner;
        List<Grant> grants = new ArrayList<Grant>();
        
        public String toString() { 
            return "{" + this.getClass().getName()
            + ": owner=" + owner + ", grants=" + grants + "}";
        }
    }
    
    class Grant {
        Grantee grantee;
        Permission permission;
        
        public String toString() { 
            return "{" + this.getClass().getName()
            + ": grantee=" + grantee + ", permission=" + permission + "}";
        }
    }
    
    abstract class Grantee {
        String type;
    }
    
    class CanonicalUserGrantee extends Grantee {
        String id;
        String displayName;
        
        public String toString() { 
            return "{" + this.getClass().getName()
            + ": id=" + id + ", displayName=" + displayName + "}";
        }
    }
    
    class GroupGrantee extends Grantee {
        String uri;
        
        public String toString() { 
            return "{" + this.getClass().getName()
            + ": uri=" + uri+ "}";
        }
    }
    
    class AmazonCustomerByEmail extends Grantee {
        String emailAddress;
        
        public String toString() { 
            return "{" + this.getClass().getName()
            + ": emailAddress=" + emailAddress + "}";
        }
    }
    
    class Range {
        long begin;
        long end;
        
        public Range(long begin, long end) {
            this.begin = begin;
            this.end = end;
        }
        
        public String toString() { 
            return "{" + this.getClass().getName()
            + ": begin=" + begin + ", end=" + end + "}";
        }        
    }

}
