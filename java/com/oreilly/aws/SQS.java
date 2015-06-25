package com.oreilly.aws;

import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
 * The SQS class implements the Query API of the Amazon Simple Queue
 * Service.
 */
public class SQS extends AWS {
    
    public static URL ENDPOINT_URI;
    public static final String API_VERSION = "2007-05-01";
    public static final String SIGNATURE_VERSION = "1";

    public HttpMethod HTTP_METHOD = HttpMethod.POST; // GET

    static {
        try {
            ENDPOINT_URI = new URL("https://queue.amazonaws.com/");
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
    }
    
    /**
     * Initialize the service and set the service-specific variables: 
     * awsAccessKey, awsSecretKey, isDebugMode, and isSecureHttp.
     * 
     * This constructor obtains your AWS access and secret key credentials from
     * the AWS_ACCESS_KEY and AWS_SECRET_KEY environment variables respectively.
     * It sets isDebugMode to false, and isSecureHttp to true.
     */
    public SQS() {
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
    public SQS(boolean isDebugMode, boolean isSecureHttp) {
        super(isDebugMode, isSecureHttp);
    }

    /**
     * Initialize the service and set the service-specific variables: 
     * awsAccessKey, awsSecretKey, isDebugMode, and isSecureHttp.
     */
    public SQS(String awsAccessKey, String awsSecretKey, boolean isDebugMode,
        boolean isSecureHttp) 
    {
        super(awsAccessKey, awsSecretKey, isDebugMode, isSecureHttp);
    }
    
    public List<URL> listQueues() throws Exception {
        return listQueues(null);
    }
    
    public List<URL> listQueues(String queueNamePrefix) throws Exception {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "ListQueues");
        parameters.put("QueueNamePrefix", queueNamePrefix);
        
        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION, 
            parameters, EMPTY_INDEXED_MAP);
        
        HttpURLConnection conn = doQuery(HTTP_METHOD, ENDPOINT_URI, 
            parameters);   
        Document xmlDoc = parseToDocument(conn.getInputStream());

        List<URL> queues = new ArrayList<URL>();
        for (Node node : xpathToNodeList("//QueueUrl", xmlDoc)) {
            queues.add(new URL(node.getTextContent()));
        }
        return queues;
    }
    
    public URL createQueue(String queueName) 
        throws Exception 
    {
        return createQueue(queueName, null);
    }
    
    public URL createQueue(String queueName, Integer visibilityTimeoutSecs) 
        throws Exception 
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "CreateQueue");
        parameters.put("QueueName", queueName);
        if (visibilityTimeoutSecs != null) {
            parameters.put("DefaultVisibilityTimeout", 
                visibilityTimeoutSecs.toString());
        }
        
        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION, 
            parameters, EMPTY_INDEXED_MAP);
        
        HttpURLConnection conn = doQuery(HTTP_METHOD, ENDPOINT_URI, 
            parameters);   
        Document xmlDoc = parseToDocument(conn.getInputStream());
        return new URL(xpathToContent("//QueueUrl", xmlDoc));
    }
    
    public boolean deleteQueue(URL queueUrl) 
        throws Exception 
    {
        return deleteQueue(queueUrl, false);    
    }
        
    public boolean deleteQueue(URL queueUrl, boolean forceDeletion) 
        throws Exception 
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "DeleteQueue");
        parameters.put("ForceDeletion", String.valueOf(forceDeletion));
        
        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION, 
            parameters, EMPTY_INDEXED_MAP);
        
        doQuery(HTTP_METHOD, queueUrl, parameters);   
        return true;
    }

    public Map<String, Integer> getQueueAttributes(URL queueUrl) throws Exception
    { 
        return getQueueAttributes(queueUrl, "All");
    }
    
    public Map<String, Integer> getQueueAttributes(URL queueUrl, 
        String attributeName) throws Exception 
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "GetQueueAttributes");
        parameters.put("Attribute", attributeName);
        
        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION, 
            parameters, EMPTY_INDEXED_MAP);
        
        HttpURLConnection conn = doQuery(HTTP_METHOD, queueUrl, parameters);
        Document xmlDoc = parseToDocument(conn.getInputStream());

        Map<String, Integer> attributes = new HashMap<String, Integer>();
        for (Node node : xpathToNodeList("//AttributedValue", xmlDoc)) {
            attributes.put(
                xpathToContent("Attribute", node), 
                Integer.valueOf(xpathToContent("Value", node)));
        }
        
        return attributes;
    }

    public boolean setQueueAttribute(URL queueUrl, String attributeName, 
        Integer attributeValue) throws Exception 
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "SetQueueAttributes");
        parameters.put("Attribute", attributeName);
        parameters.put("Value", attributeValue.toString());
        
        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION, 
            parameters, EMPTY_INDEXED_MAP);
        
        doQuery(HTTP_METHOD, queueUrl, parameters);
        return true;
    }

    public String sendMessage(URL queueUrl, String messageBody) 
        throws Exception 
    {
        return sendMessage(queueUrl, messageBody, true);
    }
        
    public String sendMessage(URL queueUrl, String messageBody, boolean encode) 
        throws Exception 
    {
        if (encode) {
            messageBody = encodeBase64(messageBody); 
        }
        
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "SendMessage");
        parameters.put("MessageBody", messageBody);
        
        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION, 
            parameters, EMPTY_INDEXED_MAP);
        
        HttpURLConnection conn = doQuery(HTTP_METHOD, queueUrl, parameters);
        Document xmlDoc = parseToDocument(conn.getInputStream());
        return xpathToContent("//MessageId", xmlDoc);
    }

    public Message peekMessage(URL queueUrl, String messageId) throws Exception 
    {
        return peekMessage(queueUrl, messageId, true);
    }

    public Message peekMessage(URL queueUrl, String messageId, 
        boolean decodeBody) throws Exception 
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "PeekMessage");
        parameters.put("MessageId", messageId);
        
        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION, 
            parameters, EMPTY_INDEXED_MAP);
        
        HttpURLConnection conn = doQuery(HTTP_METHOD, queueUrl, parameters);
        Document xmlDoc = parseToDocument(conn.getInputStream());
        
        Message message = new Message();
        message.id = xpathToContent("//MessageId", xmlDoc);
        if (decodeBody) {
            message.body = new String(decodeBase64(
                xpathToContent("//MessageBody", xmlDoc)), "UTF-8");
        } else {
            message.body = xpathToContent("//MessageBody", xmlDoc);                
        }
        
        return message;
    }

    public List<Message> receiveMessages(URL queueUrl, Integer maximum) 
        throws Exception 
    {
        return receiveMessages(queueUrl, maximum, null, true);
    }

    public List<Message> receiveMessages(URL queueUrl, Integer maximum, 
        Integer visibilityTimeoutSecs, boolean decodeBody) throws Exception 
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "ReceiveMessage");
        parameters.put("NumberOfMessages", maximum.toString());
        if (visibilityTimeoutSecs != null) {
            parameters.put("VisibilityTimeout", 
                visibilityTimeoutSecs.toString());
        }
        
        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION, 
            parameters, EMPTY_INDEXED_MAP);
        
        HttpURLConnection conn = doQuery(HTTP_METHOD, queueUrl, parameters);
        Document xmlDoc = parseToDocument(conn.getInputStream());
        
        List<Message> messages = new ArrayList<Message>();
        for (Node node : xpathToNodeList("//Message", xmlDoc)) {
            Message message = new Message();
            message.id = xpathToContent("MessageId", node);
            if (decodeBody) {
                message.body = new String(decodeBase64(
                    xpathToContent("MessageBody", node)), "UTF-8");
            } else {
                message.body = xpathToContent("MessageBody", node);                
            }
            messages.add(message);
        }
                
        return messages;
    }
    
    public boolean deleteMessage(URL queueUrl, String messageId) 
        throws Exception 
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "DeleteMessage");
        parameters.put("MessageId", messageId);
        
        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION, 
            parameters, EMPTY_INDEXED_MAP);
        
        doQuery(HTTP_METHOD, queueUrl, parameters);
        return true;
    }

    public boolean changeMessageVisibility(URL queueUrl, String messageId, 
        Integer visibilityTimeoutSecs) throws Exception 
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "ChangeMessageVisibility");
        parameters.put("MessageId", messageId);
        parameters.put("VisibilityTimeout", 
            visibilityTimeoutSecs.toString());
        
        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION, 
            parameters, EMPTY_INDEXED_MAP);
        
        doQuery(HTTP_METHOD, queueUrl, parameters);
        return true;
    }

    public List<Grant> listGrants(URL queueUrl) throws Exception 
    {
        return listGrants(queueUrl, null, null);
    }
    
    public List<Grant> listGrants(URL queueUrl, Permission permission,  
        Grantee grantee) throws Exception 
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "ListGrants");
        
        // Add filter parameters if filter options are non-null.
        if (permission != null) {
            parameters.put("Permission", permission.toString());
        }        
        if (grantee != null) {
            if (grantee instanceof CanonicalUserGrantee) {
                parameters.put("Grantee.ID", 
                    ((CanonicalUserGrantee)grantee).id);                
            } else {
                parameters.put("Grantee.EmailAddress", 
                    ((AmazonCustomerByEmail)grantee).emailAddress);                                
            }
        }
        
        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION, 
            parameters, EMPTY_INDEXED_MAP);
        
        HttpURLConnection conn = doQuery(HTTP_METHOD, queueUrl, parameters);
        Document xmlDoc = parseToDocument(conn.getInputStream());
        
        List<Grant> grants = new ArrayList<Grant>();
        for (Node node : xpathToNodeList("//GrantList", xmlDoc)) {
            Grant grant = new Grant();
            grant.permission = Permission.valueOf(
                xpathToContent("Permission", node));
            
            // This operation only ever returns CanonicalUser grantees
            CanonicalUserGrantee userGrantee = new CanonicalUserGrantee();
            userGrantee.type = xpathToContent("Grantee/@type", node);
            userGrantee.id = xpathToContent("Grantee/ID", node);
            userGrantee.displayName = 
                xpathToContent("Grantee/DisplayName", node);
            grant.grantee = userGrantee;
            
            grants.add(grant);
        }
                
        return grants;
    }

    public boolean addGrant(URL queueUrl, Grantee grantee, 
        Permission permission) throws Exception 
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "AddGrant");
        parameters.put("Permission", permission.toString());

        if (grantee instanceof CanonicalUserGrantee) {
            parameters.put("Grantee.ID", 
                ((CanonicalUserGrantee)grantee).id);                
        } else {
            parameters.put("Grantee.EmailAddress", 
                ((AmazonCustomerByEmail)grantee).emailAddress);                                
        }

        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION, 
            parameters, EMPTY_INDEXED_MAP);        
        doQuery(HTTP_METHOD, queueUrl, parameters);
        return true;
    }

    public boolean removeGrant(URL queueUrl, Grantee grantee, 
        Permission permission) throws Exception 
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "RemoveGrant");
        parameters.put("Permission", permission.toString());
    
        if (grantee instanceof CanonicalUserGrantee) {
            parameters.put("Grantee.ID", 
                ((CanonicalUserGrantee)grantee).id);                
        } else {
            parameters.put("Grantee.EmailAddress", 
                ((AmazonCustomerByEmail)grantee).emailAddress);                                
        }
    
        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION, 
            parameters, EMPTY_INDEXED_MAP);        
        doQuery(HTTP_METHOD, queueUrl, parameters);
        return true;
    }    
    
    /*
     * Below this point are class and enum definitions specific to the Java
     * implementation of AWS clients. These items make it easier to pass
     * parameters into this client's methods, and to retrieve results from the
     * methods.
     */

    public static enum Permission { FULLCONTROL, RECEIVEMESSAGE, SENDMESSAGE }; 

    class Message {
        String id;
        String body;
        
        public String toString() { 
            return "{" + this.getClass().getName() 
                + ": id=" + id + ", body=" + body + "}"; 
        }
    }
    
    abstract class Grantee {
        String type;
    }
    
    class CanonicalUserGrantee extends Grantee {
        String id;
        String displayName;

        public String toString() { 
            return "{" + this.getClass().getName() + ": id=" + id 
                + ", displayName=" + displayName + "}"; 
        }
    }
    
    class AmazonCustomerByEmail extends Grantee {
        String emailAddress;

        public String toString() { 
            return "{" + this.getClass().getName() 
                + ": emailAddress=" + emailAddress + "}"; 
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
    
}
