package com.oreilly.aws;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

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
 * The SimpleDB class implements the Query API of the Amazon SimpleDB service.
 */
public class SimpleDB extends AWS {

    public static URL ENDPOINT_URI;
    public static final String API_VERSION = "2007-11-07";
    public static final String SIGNATURE_VERSION = "1";

    public HttpMethod HTTP_METHOD = HttpMethod.POST; // GET

    protected double priorBoxUsage = 0.0;
    protected double totalBoxUsage = 0.0;


    static {
        try {
            ENDPOINT_URI = new URL("https://sdb.amazonaws.com/");
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
    public SimpleDB() {
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
    public SimpleDB(boolean isDebugMode, boolean isSecureHttp) {
        super(isDebugMode, isSecureHttp);
    }

    /**
     * Initialize the service and set the service-specific variables:
     * awsAccessKey, awsSecretKey, isDebugMode, and isSecureHttp.
     */
    public SimpleDB(String awsAccessKey, String awsSecretKey,
        boolean isDebugMode, boolean isSecureHttp)
    {
        super(awsAccessKey, awsSecretKey, isDebugMode, isSecureHttp);
    }

    public double getPriorBoxUsage() {
        return priorBoxUsage;
    }

    public double getTotalBoxUsage() {
        return totalBoxUsage;
    }

    /**
     * Uses the doQuery method defined in AWS to sends a GET or POST request
     * message to the SimpleDB service's Query API interface and returns the
     * response result from the service.
     *
     * This method performs retrieves box usage values from the service's
     * response.
     */
    protected Document doSdbQuery(Map<String, String> parameters)
        throws Exception
    {
        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION, 
            parameters, EMPTY_INDEXED_MAP);        
        HttpURLConnection conn = doQuery(HTTP_METHOD, ENDPOINT_URI, parameters);
        Document xmlDoc = parseToDocument(conn.getInputStream());
        
        String usageStr = xpathToContent("//BoxUsage", xmlDoc);
        if (usageStr != null) {
            priorBoxUsage = Double.parseDouble(usageStr);
            totalBoxUsage += priorBoxUsage;
        }

        return xmlDoc;
    }
    
    public List<String> listDomains() throws Exception {
        return listDomains(100);
    }
    
    public List<String> listDomains(int maxDomains) throws Exception {
        boolean moreDomains = true;
        String nextToken = null;
        List<String> domainNames = new ArrayList<String>();
                        
        while (moreDomains) {        
            Map<String, String> parameters = new HashMap<String, String>();
            parameters.put("Action", "ListDomains");
            parameters.put("MaxNumberOfDomains", String.valueOf(maxDomains));
            parameters.put("NextToken", nextToken);
                
            Document xmlDoc = doSdbQuery(parameters);

            for (Node node : xpathToNodeList("//DomainName", xmlDoc)) {
                domainNames.add(node.getTextContent());
            }
            
            // If we receive a NextToken element, perform a follow-up operation
            // to retrieve the next set of domain names.
            nextToken = xpathToContent("//NextToken/text()", xmlDoc);
            moreDomains = (nextToken != null);
        }
        return domainNames;
    }
    
    public boolean createDomain(String domainName) throws Exception {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "CreateDomain");
        parameters.put("DomainName", domainName);
    
        doSdbQuery(parameters);
        return true;
    }
    
    public boolean deleteDomain(String domainName) throws Exception {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "DeleteDomain");
        parameters.put("DomainName", domainName);
    
        doSdbQuery(parameters);
        return true;
    }
    
    protected Map<String, String> buildAttributeParams(
        Map<String, Object> attributes, boolean replace) throws Exception 
    {
        Map<String, String> attributeParams = new HashMap<String, String>();
        int index = 0;
        
        if (attributes == null) {
            return attributeParams;
        }
        
        for (Map.Entry<String, Object> param : attributes.entrySet()) {
            List valueList = null;
            if (param.getValue() instanceof List) {
                valueList = (List) param.getValue();
            }  else {
                valueList = new ArrayList();
                valueList.add(param.getValue());
            }
            
            for (Object value: valueList) {
                attributeParams.put("Attribute." + index + ".Name", param.getKey());
                if (value != null) {
                    // Automatically encode attribute values
                    String encodedValue = encodeAttributeValue(value);
                    attributeParams.put("Attribute." + index + ".Value", 
                        encodedValue);                    
                }
                // Add a Replace parameter for the attribute if the replace flag is set
                if (replace) {
                    attributeParams.put("Attribute." + index + ".Replace", "true");
                }
                index++;
            }
        }        
        return attributeParams;
    }
    
    public boolean putAttributes(String domainName, String itemName, 
        Map<String, Object> attributes) throws Exception
    {
        return putAttributes(domainName, itemName, attributes, false);
    }

    public boolean putAttributes(String domainName, String itemName, 
        Map<String, Object> attributes, boolean replace) throws Exception 
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "PutAttributes");
        parameters.put("DomainName", domainName);
        parameters.put("ItemName", itemName);
        
        parameters.putAll(buildAttributeParams(attributes, replace));
    
        doSdbQuery(parameters);
        return true;
    }

    public boolean deleteAttributes(String domainName, String itemName) 
        throws Exception 
    {
        return deleteAttributes(domainName, itemName, null);
    }
    
    public boolean deleteAttributes(String domainName, String itemName, 
        Map<String, Object> attributes) throws Exception 
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "DeleteAttributes");
        parameters.put("DomainName", domainName);
        parameters.put("ItemName", itemName);
        
        parameters.putAll(buildAttributeParams(attributes, false));
    
        doSdbQuery(parameters);
        return true;
    }
    
    public Map<String, List> getAttributes(String domainName, String itemName) 
        throws Exception
    {
        return getAttributesImpl(domainName, itemName, null);
    }

    
    public List getAttributes(String domainName, String itemName, 
        String attributeName) throws Exception
    {
        // When a specific attribute is requested, return only the values array
        // list for this attribute.
        Map<String, List> attributes = 
            getAttributesImpl(domainName, itemName, attributeName);
        List valueList = (List) attributes.get(attributeName);
        if (valueList == null) {
            return new ArrayList();
        } else {
            return valueList;
        }
    }

    protected Map<String, List> getAttributesImpl(String domainName, String itemName, 
        String attributeName) throws Exception 
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "GetAttributes");
        parameters.put("DomainName", domainName);
        parameters.put("ItemName", itemName);
        parameters.put("AttributeName", attributeName);
        
        Document xmlDoc = doSdbQuery(parameters);
        
        Map<String, List> attributes = new TreeMap<String, List>();
        for (Node node : xpathToNodeList("//Attribute", xmlDoc)) {
            String name = xpathToContent("Name", node);
            String value = xpathToContent("Value", node);
            
            // Automatically decode attribute values
            Object decodedValue = decodeAttributeValue(value);
            
            if (decodedValue == null) {
                // An empty attribute value is an empty string, not null.
                decodedValue = "";
            }
            
            List valueList = attributes.get(name);
            if (valueList != null) {
                valueList.add(decodedValue);
            } else {
                valueList = new ArrayList();
                valueList.add(decodedValue);
                attributes.put(name, valueList);
            }
        }        
        return attributes;
    }

    public List<String> query(String domainName, String queryExpression) 
        throws Exception 
    {
        return query(domainName, queryExpression, 100, true);
    }
    
    public List<String> query(String domainName, String queryExpression,
        int maxItems, boolean fetchAll) throws Exception 
    {
        boolean moreItems = true;
        String nextToken = null;
        
        List<String> itemNames = new ArrayList<String>();
        while (moreItems) {        
            Map<String, String> parameters = new HashMap<String, String>();
            parameters.put("Action", "Query");
            parameters.put("DomainName", domainName);
            parameters.put("QueryExpression", queryExpression);
            parameters.put("NextToken", nextToken);
            parameters.put("MaxNumberOfItems", String.valueOf(maxItems));
        
            Document xmlDoc = doSdbQuery(parameters);
        
            for (Node node : xpathToNodeList("//ItemName", xmlDoc)) {
                itemNames.add(node.getTextContent());
            }
            
            nextToken = xpathToContent("//NextToken", xmlDoc);
            moreItems = (nextToken != null && fetchAll);
        }        
        return itemNames;
    }

    
    public String encodeBoolean(boolean value) {
        if (value) {
            return "!b";
        } else {
            return "!B";            
        }
    }
    
    public boolean decodeBoolean(String value) throws Exception {
        if ("!B".equals(value)) {
            return false;
        } else if ("!b".equals(value)) {
            return true;
        } else {
            throw new Exception("Cannot decode boolean from string: " + value);
        }
    }
    
    public String encodeDate(Date value) {
        return "!d" + iso8601DateFormat.format(value);
    }
    
    public Date decodeDate(String value) throws Exception {
        if (value.startsWith("!d")) {
            return iso8601DateFormat.parse(value.substring(2));            
        } else {
            throw new Exception("Cannot decode date from string: " + value);
        }        
    }
    
    protected String printf(String format, Object value) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        new PrintStream(baos).printf("%0" + 2 + "d", value);
        return new String(baos.toByteArray(), "UTF-8");
    }

    public String encodeInteger(long value) throws Exception {
        return encodeInteger(value, 18);
    }
    
    public String encodeInteger(long value, int maxDigits) throws Exception {
        long upperBound = Math.round(Math.pow(10, maxDigits));
        
        if (value > upperBound || value < -upperBound) {
            throw new Exception("Integer " + value + " is outside encoding range (-"
                + upperBound + " to " + (upperBound - 1));
        }
        
        if (value < 0) {
            return "!I" + printf("%0" + maxDigits + "d", upperBound + value);    
        } else {
            return "!i" + printf("%0" + maxDigits + "d", value);    
        }
    }
    
    public long decodeInteger(String value) throws Exception {
        if (value.startsWith("!I")) {
            // Encoded value is a negative integer
            int maxDigits = value.length() - 2;
            long upperBound = Math.round(Math.pow(10, maxDigits));

            return Long.valueOf(value.substring(2)) - upperBound;
        }  else if (value.startsWith("!i")) {
            // Encoded value is a positive integer
            return Long.valueOf(value.substring(2));
        } else {
            throw new Exception("Cannot decode integer from string: " + value);
        }        
    }

    public String encodeFloat(double value) throws Exception 
    {
        return encodeFloat(value, 2, 15);
    }

    public String encodeFloat(double value, int maxExpDigits, 
        int maxPrecisionDigits) throws Exception 
    {
        long expMidpoint = Math.round(Math.pow(10, maxExpDigits) / 2);
        
        String precisionFormat = "";
        for (int i = 0; i < maxPrecisionDigits; i++) {
            precisionFormat += "0";
        }              
        DecimalFormat formatter = new DecimalFormat(
            " ." + precisionFormat + "E0;-." + precisionFormat + "E0");
               
        String formattedValue = formatter.format(value);
        String fractionStr = formattedValue.substring(2, 2 + maxPrecisionDigits);
        int exponent = Integer.valueOf(formattedValue.substring(3 + maxPrecisionDigits));
        
        if (exponent >= expMidpoint || exponent < -expMidpoint) {
          throw new Exception ("Exponent " + exponent + 
              " is outside encoding range (-" + expMidpoint + " to " +
              (expMidpoint - 1));
        }
        
        if (value == 0) {
            // The zero value is a special case, for which the exponent must be 0
            exponent = (int) -expMidpoint;
        }

        if (value >= 0) {
            return "!f" + printf("%0" + maxExpDigits + "d", expMidpoint + exponent) +
                "!" + fractionStr;
        } else {
            long fractionUpperBound = Math.round(Math.pow(10, maxPrecisionDigits));
            long diffFraction = fractionUpperBound - Long.valueOf(fractionStr);
            return "!F" + printf("%0" + maxExpDigits + "d", expMidpoint - exponent) +
                "!" + printf("%0" + maxPrecisionDigits + "d", diffFraction);
        }
    }
    
    public double decodeFloat(String value) throws Exception {
        if (!value.startsWith("!f") && !value.startsWith("!F")) {
            throw new Exception("Cannot decode float from string: " + value);
        }
        
        int expIndex = value.substring(2).indexOf("!");
        String expStr = value.substring(2, 2 + expIndex);
        String fractionStr = value.substring(expIndex + 3, value.length());
        
        int maxExpDigits = expStr.length();
        long expMidpoint = Math.round(Math.pow(10, maxExpDigits) / 2);
        int maxPrecisionDigits = fractionStr.length();
        
        long fraction = 0;
        long exp = 0;
        int sign = 1;
        if (value.startsWith("!F")) {
            sign = -1;
            exp = expMidpoint - Long.valueOf(expStr);
            long fractionUpperBound = Math.round(Math.pow(10, maxPrecisionDigits));
            fraction = fractionUpperBound - Long.valueOf(fractionStr);
        } else {
            sign = 1;
            exp = Long.valueOf(expStr) - expMidpoint;
            fraction = Long.valueOf(fractionStr);
        }

       return sign * Double.valueOf("0." + fraction) * Math.pow(10, exp);
    }

    
    public String encodeAttributeValue(Object value) throws Exception {
      if (value == null) {
          return null;
      } else if (value instanceof Boolean) {
          return encodeBoolean((Boolean) value);
      } else if (value instanceof Date) {
          return encodeDate((Date) value);
      } else if (value instanceof Long) {
          return encodeInteger((Long) value);
      } else if (value instanceof Integer) {
          return encodeInteger((Integer) value);          
      } else if (value instanceof Short) {
          return encodeInteger((Short) value);          
      } else if (value instanceof Double) {
          return encodeFloat((Double) value);
      } else if (value instanceof Float) {
          return encodeFloat((Float) value);
      } else {
          // No type-specific encoding is available, so we simply convert
          // the value to a string.
          return String.valueOf(value);
      }
    }

    public Object decodeAttributeValue(String value) throws Exception {
        if (value == null) {
            return "";
        }
                
        // Check whether the '!' flag is present to indicate an encoded value
        if (!value.startsWith("!")) {
            return value;
        }
        
        String prefix = value.substring(0, 2).toUpperCase();
        if (prefix.startsWith("!B")) {
            return decodeBoolean(value);
        } else if (prefix.startsWith("!D")) {
            return decodeDate(value);
        } else if (prefix.startsWith("!I")) {
            return decodeInteger(value);
        } else if (prefix.startsWith("!F")) {
            return decodeFloat(value);
        } else {
            return value;
        }
    }
        
}
