package com.oreilly.aws;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SimpleTimeZone;
import java.util.TreeMap;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSession;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * Sample Java code for the O'Reilly book "Using AWS Infrastructure Services"
 * by James Murty.
 * <p>
 * This code was written for Java version 5.0 or greater. If you are not using
 * Sun's Java implementation, this also code requires the Apache Commons Codec
 * library (see http://commons.apache.org/codec/)
 * <p>
 * The AWS class includes HTTP messaging and utility methods that handle
 * communication with Amazon Web Services' REST or Query APIs. Service
 * client implementations are built on top of this class. 
 */

public class AWS {

    /**
     * HTTP Methods used in the AWS implementations.
     */
    public enum HttpMethod {
        GET, HEAD, PUT, DELETE, POST
    };

    /**
     * Date formatter to parse and format dates in ISO 8601 format.
     */
    protected static final SimpleDateFormat iso8601DateFormat = 
        new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    /**
     * Date formatter to parse and format dates in RFC 822 format.
     */
    protected static final SimpleDateFormat rfc822DateFormat = 
        new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z");

    // Empty Map objects, to be used as stand-ins when Map
    // parameters are required but no values are needed.
    public static final Map<String, String> EMPTY_STRING_MAP = 
        new HashMap<String, String>();

    public static final Map<String, List<String>> EMPTY_INDEXED_MAP = 
        new HashMap<String, List<String>>();

    /**
     * Your Amazon Web Services Access Key credential.
     */
    protected String awsAccessKey = null;

    /**
     * Your Amazon Web Services Secret Key credential.
     */
    protected String awsSecretKey = null;

    /**
     * Enable debugging messages? When this value is true, debug logging
     * messages describing AWS communication messages are printed to standard
     * output.
     */
    protected boolean isDebugMode = false;

    /**
     * Use only the Secure HTTP protocol (HTTPS)? When this value is true, all
     * requests are sent using HTTPS. When this value is false, standard HTTP is
     * used.
     */
    protected boolean isSecureHttp = false;

    /**
     * The approximate difference in the current time between your computer and
     * Amazon's servers, measured in milliseconds.
     * 
     * This value is 0 by default. Use the {@link #currentTime()} to obtain the
     * current time with this offset factor included, and the 
     * {@link #adjustTime()} method to calculate an offset value for your
     * computer based on a response from an AWS server.
     */
    protected long timeOffset = 0;

    /**
     * Initialize AWS and set the service-specific variables: awsAccessKey,
     * awsSecretKey, isDebugMode, and isSecureHttp.
     * 
     * This constructor obtains your AWS access and secret key credentials from
     * the AWS_ACCESS_KEY and AWS_SECRET_KEY environment variables respectively.
     * It sets isDebugMode to false, and isSecureHttp to true.
     */
    public AWS() {
        this(System.getenv("AWS_ACCESS_KEY"), System.getenv("AWS_SECRET_KEY"),
            false, true);
    }

    /**
     * Initialize AWS and set the service-specific variables: awsAccessKey,
     * awsSecretKey, isDebugMode, and isSecureHttp.
     * 
     * This constructor obtains your AWS access and secret key credentials from
     * the AWS_ACCESS_KEY and AWS_SECRET_KEY environment variables respectively.
     * It sets isDebugMode and isSecureHttp according to the values you provide.
     */
    public AWS(boolean isDebugMode, boolean isSecureHttp) {
        this(System.getenv("AWS_ACCESS_KEY"), System.getenv("AWS_SECRET_KEY"),
            isDebugMode, isSecureHttp);
    }

    /**
     * Initialize AWS and set the service-specific variables: awsAccessKey,
     * awsSecretKey, isDebugMode, and isSecureHttp.
     */
    public AWS(String awsAccessKey, String awsSecretKey, boolean isDebugMode,
        boolean isSecureHttp) {
        if (awsAccessKey == null) {
            throw new IllegalStateException("AWS Access Key is not available");
        }
        if (awsSecretKey == null) {
            throw new IllegalStateException("AWS Secret Key is not available");
        }

        this.awsAccessKey = awsAccessKey;
        this.awsSecretKey = awsSecretKey;
        this.isDebugMode = isDebugMode;
        this.isSecureHttp = isSecureHttp;

        // Initialize date formats to use GMT timezone
        rfc822DateFormat.setTimeZone(new SimpleTimeZone(0, "GMT"));
        iso8601DateFormat.setTimeZone(new SimpleTimeZone(0, "GMT"));

        // Configure HTTPS hostname verifier to ignore certificate
        // mismatches (this is necessary to use S3 alternative host names).
        HttpsURLConnection.setDefaultHostnameVerifier(new HostnameVerifier() {
            public boolean verify(String urlHostName, SSLSession session) {
                return true;
            }
        });
    }
    
    /**
     * A exception object that captures information about an AWS service error.
     */
    class ServiceException extends Exception {
        private static final long serialVersionUID = -2035272002939136219L;
        
        private String errorMessage = null;
        private Document awsErrorXml = null;
        private String awsErrorText = null;

        public ServiceException(HttpURLConnection conn, Throwable t) {
            super(t);
            readErrorDetails(conn);
        }

        public ServiceException(HttpURLConnection conn) {
            super();
            readErrorDetails(conn);
        }

        private void readErrorDetails(HttpURLConnection conn) {
            try {
                // Add the HTTP status code and message to a descriptive message
                errorMessage = "HTTP Error: " + conn.getResponseCode() + " - "
                    + conn.getResponseMessage();

                awsErrorText = getInputStreamAsString(conn.getErrorStream());
                
                // If an AWS error message is available, add its code and 
                // message to the overall descriptive message.
                if (awsErrorText.startsWith("<?xml")) {
                    awsErrorXml = parseToDocument(awsErrorText);

                    errorMessage += ", AWS Error: "
                        + xpathToContent("//Code", awsErrorXml) + " - "
                        + xpathToContent("//Message", awsErrorXml);
                }

            } catch (Exception ioe) {
                // Nothing we can do here, print the stack trace and move on...
                ioe.printStackTrace();
            }
        }

        public Document getAwsErrorXml() {
            return awsErrorXml;
        }

        public String getAwsErrorText() {
            return awsErrorText;
        }

        public String getMessage() {
            if (errorMessage != null) {
                return errorMessage;
            } else {
                return super.getMessage();
            }
        }
    }

    public String getAwsAccessKey() {
        return awsAccessKey;
    }

    public void setAwsAccessKey(String awsAccessKey) {
        this.awsAccessKey = awsAccessKey;
    }

    public String getAwsSecretKey() {
        return awsSecretKey;
    }

    public void setAwsSecretKey(String awsSecretKey) {
        this.awsSecretKey = awsSecretKey;
    }

    public boolean isDebugMode() {
        return isDebugMode;
    }

    public void setDebugMode(boolean isDebugMode) {
        this.isDebugMode = isDebugMode;
    }

    public boolean isSecureHttp() {
        return isSecureHttp;
    }

    public void setSecureHttp(boolean isSecureHttp) {
        this.isSecureHttp = isSecureHttp;
    }

    /**
     * Generates an AWS signature value for the given request description.
     * The result value is a HMAC signature that is cryptographically signed 
     * with the SHA1 algorithm using your AWS Secret Key credential. The
     * signature value is Base64 encoded before being returned. 
     * 
     * This method can be used to sign requests destined for the REST or
     * Query AWS API interfaces.
     */
    public String generateSignature(String requestDescription) throws Exception
    {
        // Create an HMAC signing object
        Mac hmac = Mac.getInstance("HmacSHA1");

        // Use your AWS Secret Key as the crypto secret key
        SecretKeySpec secretKey = new SecretKeySpec(awsSecretKey
            .getBytes("UTF-8"), "HmacSHA1");
        hmac.init(secretKey);

        // Compute the signature using the HMAC algorithm
        byte[] signature = hmac.doFinal(requestDescription.getBytes("UTF-8"));

        // Encode the signature bytes into a Base64 string
        return encodeBase64(signature);
    }

    /**
     * Converts a minimal set of parameters destined for an AWS Query API
     * interface into a complete set necessary for invoking an AWS operation.
     * 
     * Normal parameters are included in the resultant complete set as-is.
     * 
     * Indexed parameters are converted into multiple parameter name/value
     * pairs, where the name starts with the given parameter name but has a 
     * suffix value appended to it. For example, the input mapping
     * "Name" => ['x','y'] will be converted to two parameters, 
     * "Name.1" => 'x' and "Name.2" => 'y'.
     */
    protected Map<String, String> buildQueryParameters(String apiVersion,
        String signatureVersion, Map<String, String> parameters,
        Map<String, List<String>> indexedParameters) throws Exception
    {
        Map<String, String> builtParameters = new HashMap<String, String>();

        // Set mandatory query parameters
        builtParameters.put("Version", apiVersion);
        builtParameters.put("SignatureVersion", signatureVersion);
        builtParameters.put("AWSAccessKeyId", awsAccessKey);

        // Use current time as timestamp if no date/time value is already set
        if (!parameters.containsKey("Timestamp")
            && !parameters.containsKey("Expires")) {
            parameters
                .put("Timestamp", iso8601DateFormat.format(currentTime()));
        }

        // Merge parameters provided with defaults after removing
        // any parameters without a value.
        for (Map.Entry<String, String> param : parameters.entrySet()) {
            if (param.getValue() != null) {
                builtParameters.put(param.getKey(), param.getValue());
            }
        }

        // Add any indexed parameters as ParamName.1, ParamName.2, etc
        for (Map.Entry<String, List<String>> indexedParam : indexedParameters
            .entrySet()) {
            int indexSuffix = 1;
            if (indexedParam.getValue() == null)
                continue;

            for (String value : indexedParam.getValue()) {
                builtParameters.put(indexedParam.getKey() + "." + indexSuffix,
                    value);
                indexSuffix += 1;
            }
        }

        return builtParameters;
    }

    /**
     * Sends a GET or POST request message to an AWS service's Query API
     * interface and returns the response result from the service. This method
     * signs the request message with your AWS credentials.
     * 
     * If the AWS service returns an error message, this method will throw a
     * ServiceException describing the error.
     */
    public HttpURLConnection doQuery(HttpMethod method, URL url,
        Map<String, String> parameters) throws Exception
    {
        // Ensure the URL is using Secure HTTP protocol if the flag is set
        if (isSecureHttp && !url.getProtocol().equals("https")) {
            url = new URL("https", url.getHost(), url.getFile());
        } else if (!isSecureHttp && url.getProtocol().equals("https")) {
            url = new URL("http", url.getHost(), url.getFile());
        }

        // Generate request description and signature by:
        // - sorting parameters into alphabtical order ignoring case
        Map<String, String> sortedParameters = new TreeMap<String, String>(
            new Comparator<String>() {
                public int compare(String o1, String o2) {
                    return o1.toLowerCase().compareTo(o2.toLowerCase());
                }
            });
        sortedParameters.putAll(parameters);

        // - merging the original parameter names and values in a string
        //   in order, and without any extra separator characters
        StringBuffer requestDescription = new StringBuffer();
        for (Map.Entry<String, String> param : sortedParameters.entrySet()) {
            requestDescription.append(param.getKey() + param.getValue());
        }
        // - signing the resultant request description
        String signature = generateSignature(requestDescription.toString());

        // - adding the signature to the URL as the parameter 'Signature'
        parameters.put("Signature", signature);

        HttpURLConnection conn = null;

        switch (method) {
        case GET:
            // Create GET request with parameters in URI
            StringBuffer urlString = new StringBuffer(url.toString() + "?");
            for (Map.Entry<String, String> param : parameters.entrySet()) {
                urlString.append(param.getKey() + "="
                    + URLEncoder.encode(param.getValue(), "UTF-8") + "&");
            }
            url = new URL(urlString.toString());
            conn = (HttpURLConnection) url.openConnection();
            break;

        case POST:
            // Create POST request with parameters in form data
            conn = (HttpURLConnection) url.openConnection();
            conn.setDoOutput(true);
            conn.setRequestProperty("Content-Type",
                "application/x-www-form-urlencoded; charset=utf-8");
            break;

        default:
            throw new IllegalArgumentException("Invalid HTTP Query method: "
                + method.toString());
        }

        // Set the HTTP method
        conn.setRequestMethod(method.toString());

        if (isDebugMode) {
            debugRequest(conn, parameters, null);
        }

        // Perform the request
        conn.connect();

        if (method == HttpMethod.POST) {
            // Upload POST form data
            OutputStream outputStream = conn.getOutputStream();
            for (Map.Entry<String, String> param : parameters.entrySet()) {
                String paramString = param.getKey() + "="
                    + URLEncoder.encode(param.getValue(), "UTF-8") + "&";
                outputStream.write(paramString.getBytes("UTF-8"));
            }
            outputStream.close();
        }

        if (isDebugMode) {
            debugResponse(conn);
        }

        try {
            int responseCode = conn.getResponseCode();
            if (responseCode >= 200 && responseCode < 300) {
                return conn;
            } else {
                throw new ServiceException(conn);
            }
        } catch (IOException e) {
            throw new ServiceException(conn, e);
        }
    }

    /**
     * Generates a request description string for a request destined for a REST
     * AWS API interface, and returns a signature value for the request. 
     * 
     * This method will work for any REST AWS request, though it is intended
     * mainly for the S3 service's API and handles special cases required for
     * this service. 
     */
    protected String generateRestSignature(HttpMethod method, URL url,
        Map<String, String> headers) throws Exception
    {
        // Set mandatory Date header if it is missing
        if (!headers.containsKey("Date")) {
            headers.put("Date", rfc822DateFormat.format(currentTime()));
        }

        // Describe main components of REST request. If Content-MD5
        // or Content-Type headers are missing, use an empty string
        StringBuffer requestDescription = new StringBuffer();
        requestDescription.append(method.toString()
            + "\n"
            + (headers.containsKey("Content-MD5") ? headers.get("Content-MD5")
                : "")
            + "\n"
            + (headers.containsKey("Content-Type") ? headers
                .get("Content-Type") : "") + "\n" + headers.get("Date") + "\n");

        // Find any x-amz-* headers and store them in a sorted (Tree) map
        Map<String, String> amzHeaders = new TreeMap<String, String>();
        for (Map.Entry<String, String> header : headers.entrySet()) {
            if (header.getKey().startsWith("x-amz-")) {
                amzHeaders
                    .put(header.getKey().toLowerCase(), header.getValue());
            }
        }
        // Append x-maz-* headers to the description string
        for (Map.Entry<String, String> amzHeader : amzHeaders.entrySet()) {
            requestDescription.append(amzHeader.getKey() + ":"
                + amzHeader.getValue() + "\n");
        }

        String path = "";

        // Handle special case of S3 alternative hostname URLs. The bucket
        // portion of alternative hostnames must be included in the request
        // description's URL path.
        if (!url.getHost().equals("s3.amazonaws.com")
            && !url.getHost().equals("queue.amazonaws.com")) 
        {
            if (url.getHost().endsWith(".s3.amazonaws.com")) {
                path = "/" + url.getHost().replace(".s3.amazonaws.com", "");
            } else {
                path = "/" + url.getHost();
            }

            // For alternative hosts, the path must end with a slash
            // if there is no object in the path.
            if (url.getPath().equals("")) {
                path += "/";
            }
        }

        // Append the request's URL path to the description
        path += url.getPath();

        // Ensure the request description's URL path includes at least a slash.
        if (path.length() == 0) {
            requestDescription.append("/");
        } else {
            requestDescription.append(path);
        }

        // Append special S3 parameters to request description
        if (url.getQuery() != null) {
            for (String param : url.getQuery().split("&")) {
                if (param.equals("acl") || param.equals("torrent")
                    || param.equals("logging") || param.equals("location")) {
                    requestDescription.append("?" + param);
                }
            }
        }

        if (isDebugMode) {
            System.out.println("REQUEST DESCRIPTION\n=======");
            System.out.println(requestDescription.toString().replaceAll("\n",
                "\\\\n\n"));
            System.out.println();
        }

        // Generate signature
        return generateSignature(requestDescription.toString());
    }

    public HttpURLConnection doRest(HttpMethod method, URL url)
        throws Exception
    {
        return doRest(method, url, null, EMPTY_STRING_MAP);
    }

    /**
     * Sends a GET, HEAD, DELETE or PUT request message to an AWS service's 
     * REST API interface and returns the response result from the service. This 
     * method signs the request message with your AWS credentials.
     * 
     * If the AWS service returns an error message, this method will throw a
     * ServiceException describing the error. This method also includes support
     * for following Temporary Redirect responses (with HTTP response 
     * codes 307).
     */
    public HttpURLConnection doRest(HttpMethod method, URL url,
        InputStream dataInputStream, Map<String, String> headers)
        throws Exception
    {
        // Ensure the URL is using Secure HTTP protocol if the flag is set
        if (isSecureHttp && !url.getProtocol().equals("https")) {
            url = new URL("https", url.getHost(), url.getFile());
        } else if (!isSecureHttp && url.getProtocol().equals("https")) {
            url = new URL("http", url.getHost(), url.getFile());
        }

        // Generate request description and signature, and add to the request
        // as the header 'Authorization'
        String signature = generateRestSignature(method, url, headers);
        headers.put("Authorization", "AWS " + awsAccessKey + ":" + signature);

        // Ensure the Host header is always set
        headers.put("Host", url.getHost());

        int redirectCount = 0;
        while (redirectCount < 5) // Repeat requests after a Temporary Redirect
        {
            // Open a new HTTP connection
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();

            // Add headers to request
            for (Map.Entry<String, String> header : headers.entrySet()) {
                conn.setRequestProperty(header.getKey(), header.getValue());
            }

            // Set the HTTP method
            conn.setRequestMethod(method.toString());

            // Uploads via the PUT method get special treatment
            if (method == HttpMethod.PUT) {
                // Tell service to confirm the request message is valid before
                // it accepts data. Confirmation is indicated by a 100 
                // (Continue) message
                conn.setRequestProperty("Expect", "100-continue");

                // Ensure HTTP content-length header is set to the correct value
                if (!conn.getRequestProperties().containsKey("Content-Length")) {
                    if (dataInputStream != null) {
                        conn.setRequestProperty("Content-Length", String
                            .valueOf(dataInputStream.available()));
                    } else {
                        conn.setRequestProperty("Content-Length", "0");
                    }
                }

                // Enable streaming of uploads
                conn.setFixedLengthStreamingMode(Integer.parseInt(
                    conn.getRequestProperty("Content-Length")));

                if (isDebugMode) {
                    debugRequest(conn, EMPTY_STRING_MAP, dataInputStream);
                }

                // Perform the request
                conn.setDoOutput(true);
                conn.connect();

                // Upload data
                if (dataInputStream != null) {
                    OutputStream outputStream = conn.getOutputStream();
                    byte[] buffer = new byte[8192];
                    int count = -1;
                    while ((count = dataInputStream.read(buffer)) != -1) {
                        outputStream.write(buffer, 0, count);
                    }
                    outputStream.close();
                }
            } else {
                // Set an explicit content type if none is provided, otherwise
                // the Java HTTP library will use its own default type
                // 'application/x-www-form-urlencoded'
                conn.setRequestProperty("Content-Type", "");

                if (isDebugMode) {
                    debugRequest(conn, EMPTY_STRING_MAP, dataInputStream);
                }

                // Perform the request
                conn.setDoInput(true);
                conn.connect();
            }

            if (isDebugMode) {
                debugResponse(conn);
            }

            try {
                int responseCode = conn.getResponseCode();
                
                // Automatically follow Temporary Redirects
                if (responseCode == 307) {                                                                   
                    String location = conn.getHeaderField("Location");
                    conn.disconnect();
                    url = new URL(location);
                    redirectCount += 1; // Count to prevent infinite redirects

                    if (dataInputStream != null) {
                        dataInputStream.reset();
                    }

                } else if (responseCode >= 200 && responseCode < 300) {
                    if (dataInputStream != null) {
                        dataInputStream.close();
                    }
                    return conn;
                } else {
                    throw new ServiceException(conn);
                }
            } catch (IOException e) {
                throw new ServiceException(conn, e);
            } finally {
                if (dataInputStream != null) {
                    dataInputStream.close();
                }
            }
        } // End of while loop

        // We shouldn't ever reach this point.
        throw new IllegalStateException(
            "HTTP request did not result in a redirect, success or error");
    }

    /**
     * Prints detailed information about an HTTP request message to standard 
     * output. 
     */
    protected void debugRequest(HttpURLConnection conn,
        Map<String, String> queryParameters, InputStream dataInputStream)
        throws Exception
    {
        System.out.println("REQUEST\n=======");
        System.out.println("Method : " + conn.getRequestMethod());

        // Print URI
        String[] portions = conn.getURL().toString().split("&");
        System.out.println("URI    : " + portions[0]);
        for (int i = 1; i < portions.length; i++) {
            System.out.println("\t &" + portions[i]);
        }

        // Print Headers
        if (conn.getRequestProperties().size() > 0) {
            System.out.println("Headers:");
            for (Map.Entry<String, List<String>> header : conn
                .getRequestProperties().entrySet()) {
                System.out.println("  " + header.getKey() + "="
                    + header.getValue().get(0));
            }
        }

        // Print Query Parameters
        if (queryParameters.size() > 0) {
            System.out.println("Query Parameters:");
            for (Map.Entry<String, String> param : queryParameters.entrySet()) {
                System.out.println("  " + param.getKey() + "="
                    + param.getValue());
            }
        }

        // Print Request Data
        if (dataInputStream != null && dataInputStream.markSupported()) {
            System.out.println("Request Body Data:");

            if (conn.getRequestProperties().get("Content-Type").get(0).equals(
                "application/xml")) {
                // Pretty-print XML data
                System.out
                    .println(serializeDocument(parseToDocument(dataInputStream)));
            } else {
                System.out.println(getInputStreamAsString(dataInputStream));
            }
            dataInputStream.reset();
            System.out.println();
        }
    }

    /**
     * Prints detailed information about an HTTP response message to standard 
     * output. 
     */
    public void debugResponse(HttpURLConnection conn) throws Exception {
        System.out.println("\nRESPONSE\n========");
        System.out.println("Status : " + conn.getResponseCode() + " "
            + conn.getResponseMessage());

        // Print Headers
        if (conn.getHeaderFields().size() > 0) {
            System.out.println("Headers:");
            for (Map.Entry<String, List<String>> header : conn
                .getHeaderFields().entrySet()) {
                System.out.println("  " + header.getKey() + "="
                    + header.getValue().get(0));
            }
        }

        System.out.println();

        /*
         * We cannot print the response body here in the Java implementation as
         * the HTTP response's input stream cannot be reset, and therefore 
         * cannot be read multiple times. Instead, the response body will be 
         * printed by the methods below that read the response input stream  
         * into a String or Document.
         */
    }

    /**
     * Returns the current date and time, adjusted according to the time offset
     * between your computer and an AWS server (as set by the 
     * {@link #adjustTime()} method. 
     */
    public Date currentTime() {
        return new Date(System.currentTimeMillis() + timeOffset);
    }

    /**
     * Sets a time offset value to reflect the time difference between your
     * computer's clock and the current time according to an AWS server. This
     * method returns the calculated time difference and also sets the 
     * timeOffset variable in AWS.
     * 
     * Ideally you should not rely on this method to overcome clock-related
     * disagreements between your computer and AWS. If you computer is set
     * to update its clock periodically and has the correct timezone setting 
     * you should never have to resort to this work-around.
     */
    public long adjustTime() throws Exception {
        // Connect to an AWS server to obtain response headers.
        URL url = new URL("http://aws.amazon.com/");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.connect();

        // Retrieve the time according to AWS, based on the Date header
        Date awsTime = rfc822DateFormat.parse(conn.getHeaderField("Date"));

        // Calculate the difference between the current time according to AWS,
        // and the current time according to your computer's clock.
        Date localTime = new Date();
        timeOffset = awsTime.getTime() - localTime.getTime();

        if (isDebugMode) {
            System.out.println("Time offset for AWS requests: " + timeOffset
                + " milliseconds");
        }

        return timeOffset;
    }
    
    /*
     * The methods defined below this point are specific to the Java 
     * implementation of the AWS clients. They provide convenient short-cuts
     * for performing tasks like Base64 encoding and XPath queries.
     */

    /**
     * Returns a Base64 encoded version of the data provided. 
     * 
     * As Base64 encoding support is not built-in to all Java platforms, this 
     * method will try to find and use either Sun's encoder or the Apache 
     * Commons Codec encoder.  
     */
    public String encodeBase64(String string) throws Exception {
        return encodeBase64(string.getBytes("UTF-8")).replaceAll("\n", "");
    }

    /**
     * Returns a Base64 encoded version of the data provided. 
     * 
     * As Base64 encoding support is not built-in to all Java platforms, this 
     * method will try to find and use either Sun's encoder or the Apache 
     * Commons Codec encoder.  
     */
    public String encodeBase64(byte[] data) throws Exception {
        // Try loading Sun's Base64 encoder implementation
        try {
            Class b64Class = this.getClass().getClassLoader().loadClass(
                "sun.misc.BASE64Encoder");
            if (b64Class != null) {
                Method encodeMethod = b64Class.getMethod("encode",
                    new Class[] { byte[].class });
                return (String) encodeMethod.invoke(b64Class.newInstance(),
                    new Object[] { data });
            }
        } catch (ClassNotFoundException cnfe) {
        }

        // Try loading the Apache Commons Base64 encoder implementation
        try {
            Class b64Class = this.getClass().getClassLoader().loadClass(
                "org.apache.commons.codec.binary.Base64");
            if (b64Class != null) {
                Method encodeMethod = b64Class.getMethod("encodeBase64",
                    new Class[] { byte[].class });
                byte[] encodedData = (byte[]) encodeMethod.invoke(b64Class,
                    new Object[] { data });
                return new String(encodedData, "UTF-8");
            }
        } catch (ClassNotFoundException cnfe) {
        }

        throw new ClassNotFoundException(
            "Cannot find a recognized Base64 encoder implementation. "
                + "If you are not using Sun's JDK, please include the "
                + "Apache Commons Codec library in your classpath.");
    }

    /**
     * Returns data decoded from a Base64 encoded string. 
     * 
     * As Base64 encoding support is not built-in to all Java platforms, this 
     * method will try to find and use either Sun's decoder or the Apache 
     * Commons Codec decoder.  
     */
    public byte[] decodeBase64(String data) throws Exception {
        // Try loading Sun's Base64 decoder implementation
        try {
            Class b64Class = this.getClass().getClassLoader().loadClass(
                "sun.misc.BASE64Decoder");
            if (b64Class != null) {
                Method decodeMethod = b64Class.getMethod("decodeBuffer",
                    new Class[] { String.class });
                return (byte[]) decodeMethod.invoke(b64Class.newInstance(),
                    new Object[] { data });
            }
        } catch (ClassNotFoundException cnfe) {
        }

        // Try loading the Apache Commons Base64 decoder implementation
        try {
            Class b64Class = this.getClass().getClassLoader().loadClass(
                "org.apache.commons.codec.binary.Base64");
            if (b64Class != null) {
                Method decodeMethod = b64Class.getMethod("decodeBase64",
                    new Class[] { byte[].class });
                return (byte[]) decodeMethod.invoke(b64Class,
                    new Object[] { data.getBytes("UTF-8") });
            }
        } catch (ClassNotFoundException cnfe) {
        }

        throw new ClassNotFoundException(
            "Cannot find a recognized Base64 decoder implementation. "
                + "If you are not using Sun's JDK, please include the "
                + "Apache Commons Codec library in your classpath.");
    }

    /**
     * Returns all the data from an input stream as a string.
     */
    protected String getInputStreamAsString(InputStream is) throws IOException {
        StringBuffer responseBody = new StringBuffer();
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        String line = null;
        while ((line = reader.readLine()) != null) {
            responseBody.append(line + "\n");
        }
        reader.close();

        if (isDebugMode) {
            System.out.println("Body:\n" + responseBody + "\n");
        }

        return responseBody.toString();
    }

    /**
     * Returns all the data from an input stream as an XML document.
     */
    protected Document parseToDocument(InputStream is) throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document document = builder.parse(is);

        if (isDebugMode) {
            // Pretty-print XML data
            System.out.println("Body:\n" + serializeDocument(document) + "\n");
        }

        return document;
    }

    /**
     * Returns the given text data as an XML document.
     */
    protected Document parseToDocument(String text) throws Exception {
        return parseToDocument(new ByteArrayInputStream(text.getBytes("UTF-8")));
    }

    /**
     * Performs an XPath query on the given XML object and returns the resultant 
     * set of Nodes. The nodes are returned in a List object to make it easier
     * to iterate over the nodeset.
     * 
     * The domObject parameter can be an XML Document, Element, or Node.
     */
    protected List<Node> xpathToNodeList(String xpathQuery, Object domObject)
        throws XPathExpressionException
    {
        XPathFactory xpathFactory = XPathFactory.newInstance();
        XPath xpath = xpathFactory.newXPath();
        NodeList nodeList = (NodeList) xpath.evaluate(xpathQuery, domObject,
            XPathConstants.NODESET);

        List<Node> nodeArray = new ArrayList<Node>();
        for (int i = 0; i < nodeList.getLength(); i++) {
            nodeArray.add(nodeList.item(i));
        }
        return nodeArray;
    }

    /**
     * Performs an XPath query on the given XML object and returns the resultant 
     * node. 
     * 
     * The domObject parameter can be an XML Document, Element, or Node.
     */
    protected Node xpathToNode(String xpathQuery, Object domObject)
        throws XPathExpressionException
    {
        XPathFactory xpathFactory = XPathFactory.newInstance();
        XPath xpath = xpathFactory.newXPath();
        return (Node) xpath
            .evaluate(xpathQuery, domObject, XPathConstants.NODE);
    }

    /**
     * Performs an XPath query on the given XML object and returns the text 
     * content of the resultant node. If the XPath query does not find a Node,
     * this method returns null.
     * 
     * The domObject parameter can be an XML Document, Element, or Node.
     */
    protected String xpathToContent(String xpathQuery, Object domObject)
        throws XPathExpressionException
    {
        Node node = xpathToNode(xpathQuery, domObject); 
        if (node != null) {
            return node.getTextContent();
        } else {
            return null;
        }
    }

    /**
     * Converts an XML Document object to a string with whitespace added to 
     * make it suitable for printing.
     */
    protected String serializeDocument(Document document) throws Exception {
        // Serialize XML document to String.
        StringWriter writer = new StringWriter();
        StreamResult streamResult = new StreamResult(writer);

        DOMSource domSource = new DOMSource(document);
        TransformerFactory tf = TransformerFactory.newInstance();
        Transformer serializer = tf.newTransformer();
        serializer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
        serializer.setOutputProperty(OutputKeys.INDENT, "yes");
        serializer.transform(domSource, streamResult);
        return writer.toString();
    }

    /**
     * Joins a list of items into a delimiter-separated string.
     * 
     * @param items
     * @param delimiter
     * @return
     */
    protected String join(List items, String delimiter) {
        StringBuffer sb = new StringBuffer();        
        for (int i = 0; i < items.size(); i++) {
            sb.append(items.get(i));
            if (i < items.size() - 1) {
                sb.append(delimiter);
            }
        }
        return sb.toString();
    }

}
