package com.oreilly.aws;

import java.io.File;
import java.io.FileOutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
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
 * The EC2 class implements the Query API of the Amazon Elastic Compute Cloud
 * service.
 */
public class EC2_2008_02_01 extends AWS {
    
    public static URL ENDPOINT_URI;
    public static final String API_VERSION = "2008-02-01";
    public static final String SIGNATURE_VERSION = "1";

    public HttpMethod HTTP_METHOD = HttpMethod.POST; // GET
    
    static {
        try {
            ENDPOINT_URI = new URL("https://ec2.amazonaws.com/");
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
    public EC2_2008_02_01() {
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
    public EC2_2008_02_01(boolean isDebugMode, boolean isSecureHttp) {
        super(isDebugMode, isSecureHttp);
    }

    /**
     * Initialize the service and set the service-specific variables: 
     * awsAccessKey, awsSecretKey, isDebugMode, and isSecureHttp.
     */
    public EC2_2008_02_01(String awsAccessKey, String awsSecretKey, boolean isDebugMode,
        boolean isSecureHttp) 
    {
        super(awsAccessKey, awsSecretKey, isDebugMode, isSecureHttp);
    }

    protected Reservation parseReservation(Node resNode) throws Exception {
        Reservation reservation = new Reservation();
        reservation.id = xpathToContent("reservationId", resNode);
        reservation.ownerId = xpathToContent("ownerId", resNode);

        for (Node node : xpathToNodeList("groupSet/item/groupId", resNode)) {
            reservation.groups.add(node.getTextContent());
        }
        
        for (Node node : xpathToNodeList("instancesSet/item", resNode)) {
            Instance instance = new Instance();
            instance.id = xpathToContent("instanceId", node);
            instance.imageId = xpathToContent("imageId", node);
            instance.state = xpathToContent("instanceState/name", node);
            instance.privateDns = xpathToContent("privateDnsName", node);
            instance.publicDns = xpathToContent("dnsName", node);
            instance.type = xpathToContent("instanceType", node);
            instance.launchTime = iso8601DateFormat.parse(
                xpathToContent("launchTime", node));
            instance.reason = xpathToContent("reason", node);
            instance.keyName = xpathToContent("keyName", node);
            instance.amiLaunchIndex = Integer.parseInt( 
                xpathToContent("amiLaunchIndex", node));            
            instance.availabilityZone = 
                xpathToContent("placement/availabilityZone", node);
            instance.kernelId = xpathToContent("kernelId", node);
            instance.ramdiskId = xpathToContent("ramdiskId", node);            
            
            for (Node codeNode : 
                xpathToNodeList("productCodes/item/productCode", node)) 
            {
                instance.productCodes.add(codeNode.getTextContent());
            }

            reservation.instances.add(instance);
        }
        return reservation;
    }
    
    public List<Reservation> describeInstances() 
        throws Exception 
    {
        return describeInstances(null);
    }
    
    public List<Reservation> describeInstances(List<String> instanceIds) 
        throws Exception 
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "DescribeInstances");
        
        Map<String, List<String>> indexedParams = 
            new HashMap<String, List<String>>();
        if (instanceIds != null) {
            indexedParams.put("InstanceId", instanceIds);
        }
        
        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION, 
            parameters, indexedParams);
        
        HttpURLConnection conn = doQuery(HTTP_METHOD, ENDPOINT_URI, parameters);   
        Document xmlDoc = parseToDocument(conn.getInputStream());

        List<Reservation> reservations = new ArrayList<Reservation>();
        for (Node node : xpathToNodeList("//reservationSet/item", xmlDoc)) {
            reservations.add(parseReservation(node));
        }
        return reservations;        
    }

    public List<AvailabilityZone> describeAvailabilityZones(List<String> names) 
        throws Exception 
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "DescribeAvailabilityZones");
        
        Map<String, List<String>> indexedParams = 
            new HashMap<String, List<String>>();
        if (names != null) {
            indexedParams.put("ZoneName", names);
        }
        
        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION, 
            parameters, indexedParams);
        
        HttpURLConnection conn = doQuery(HTTP_METHOD, ENDPOINT_URI, parameters);   
        Document xmlDoc = parseToDocument(conn.getInputStream());

        List<AvailabilityZone> zones = new ArrayList<AvailabilityZone>();
        for (Node node : xpathToNodeList("//availabilityZoneInfo/item", xmlDoc)) {
            AvailabilityZone zone = new AvailabilityZone();
            zone.name = xpathToContent("zoneName", node);
            zone.state = xpathToContent("zoneState", node);
            zones.add(zone);
        }
        return zones;        
    }
    
    public List<KeyPair> describeKeypairs(List<String> keypairNames) 
        throws Exception 
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "DescribeKeyPairs");
        
        Map<String, List<String>> indexedParams = 
            new HashMap<String, List<String>>();
        indexedParams.put("KeyName", keypairNames);
        
        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION, 
            parameters, indexedParams);
        
        HttpURLConnection conn = doQuery(HTTP_METHOD, ENDPOINT_URI, parameters);   
        Document xmlDoc = parseToDocument(conn.getInputStream());
    
        List<KeyPair> keypairs = new ArrayList<KeyPair>();
        for (Node node : xpathToNodeList("//keySet/item", xmlDoc)) {
            KeyPair keyPair = new KeyPair();
            keyPair.name = xpathToContent("keyName", node);
            keyPair.fingerprint = xpathToContent("keyFingerprint", node);
            keypairs.add(keyPair);
        }
        return keypairs;        
    }
    
    public KeyPair createKeypair(String keypairName, boolean autosave) 
        throws Exception 
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "CreateKeyPair");
        parameters.put("KeyName", keypairName);
        
        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION, 
            parameters, EMPTY_INDEXED_MAP);
        
        HttpURLConnection conn = doQuery(HTTP_METHOD, ENDPOINT_URI, parameters);   
        Document xmlDoc = parseToDocument(conn.getInputStream());
    
        KeyPair keyPair = new KeyPair();
        keyPair.name = xpathToContent("//keyName", xmlDoc);
        keyPair.fingerprint = xpathToContent("//keyFingerprint", xmlDoc);
        keyPair.material = xpathToContent("//keyMaterial", xmlDoc);
        
        if (autosave) {
            // Locate key material and save to a file named after the keyName
            File keypairFile = new File(keyPair.name + ".pem");
            FileOutputStream outputStream = new FileOutputStream(keypairFile);
            String pemMaterial = keyPair.material + "\n";
            outputStream.write(pemMaterial.getBytes("UTF-8"));
            outputStream.close();
            keyPair.fileName = keypairFile.getAbsolutePath();
        }
        
        return keyPair;        
    }
    
    public boolean deleteKeypair(String keypairName) 
        throws Exception 
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "DeleteKeyPair");
        parameters.put("KeyName", keypairName);
        
        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION, 
            parameters, EMPTY_INDEXED_MAP);
        
        doQuery(HTTP_METHOD, ENDPOINT_URI, parameters);
        return true;        
    }
    
    public List<Image> describeImages(List<String> imageIds, 
        List<String> ownerIds, String executableBy) throws Exception 
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "DescribeImages");
        parameters.put("ExecutableBy", executableBy);
        // Despite API documentation, the ImageType parameter is *not* supported, 
        // see: http://developer.amazonwebservices.com/connect/thread.jspa?threadID=20655&tstart=25
        // parameters.put("ImageType", imageType);
        
        Map<String, List<String>> indexedParams = 
            new HashMap<String, List<String>>();
        indexedParams.put("ImageId", imageIds);
        indexedParams.put("Owner", ownerIds);
        
        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION, 
            parameters, indexedParams);
        
        HttpURLConnection conn = doQuery(HTTP_METHOD, ENDPOINT_URI, parameters);   
        Document xmlDoc = parseToDocument(conn.getInputStream());
    
        List<Image> images = new ArrayList<Image>();
        for (Node node : xpathToNodeList("//imagesSet/item", xmlDoc)) {
            Image image = new Image();
            image.id = xpathToContent("imageId", node);
            image.location = xpathToContent("imageLocation", node);
            image.state = xpathToContent("imageState", node);
            image.isPublic = "true".equals(xpathToContent("isPublic", node));
            image.ownerId = xpathToContent("imageOwnerId", node);
            image.architecture = xpathToContent("architecture", node);
            image.type = xpathToContent("imageType", node);
            
            // Items only available when listing 'machine' image types
            // that have associated kernel and ramdisk metadata
            image.kernelId = xpathToContent("kernelId", node);
            image.ramdiskId = xpathToContent("ramdiskId", node);
            
            for (Node codeNode : 
                xpathToNodeList("productCodes/item/productCode", node)) 
            {
                image.productCodes.add(codeNode.getTextContent());
            }
            images.add(image);
        }
        return images;        
    }
    
    public Reservation runInstances(String imageId, String keypairName, 
        List<String> securityGroups, byte[] userData) throws Exception 
    {
        return runInstances(imageId, keypairName, securityGroups, userData,
            1, 1, "m1.small", null, null, null);
    }

    public Reservation runInstances(String imageId, String keypairName, 
        List<String> securityGroups, byte[] userData, int minimum, int maximum, 
        String instanceType, String availabilityZone, String kernelId, 
        String ramdiskId) throws Exception 
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "RunInstances");
        parameters.put("ImageId", imageId);
        parameters.put("MinCount", String.valueOf(minimum));
        parameters.put("MaxCount", String.valueOf(maximum));
        parameters.put("KeyName", keypairName);
        parameters.put("InstanceType", instanceType);
        if (userData != null) {
            parameters.put("UserData", encodeBase64(userData));
        }
        parameters.put("Placement.AvailabilityZone", availabilityZone);
        parameters.put("KernelId", kernelId);
        parameters.put("RamdiskId", ramdiskId);
                
        Map<String, List<String>> indexedParams = 
            new HashMap<String, List<String>>();
        indexedParams.put("SecurityGroup", securityGroups);
        
        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION, 
            parameters, indexedParams);
        
        HttpURLConnection conn = doQuery(HTTP_METHOD, ENDPOINT_URI, parameters);   
        Document xmlDoc = parseToDocument(conn.getInputStream());
        return parseReservation(xmlDoc.getDocumentElement());
    }
    
    public ConsoleOutput getConsoleOutput(String instanceId) throws Exception 
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "GetConsoleOutput");
        parameters.put("InstanceId", instanceId);
        
        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION, 
            parameters, EMPTY_INDEXED_MAP);
        
        HttpURLConnection conn = doQuery(HTTP_METHOD, ENDPOINT_URI, parameters);   
        Document xmlDoc = parseToDocument(conn.getInputStream());
        
        ConsoleOutput output = new ConsoleOutput();
        output.instanceId = xpathToContent("//instanceId", xmlDoc);
        output.timestamp = iso8601DateFormat.parse(
            xpathToContent("//timestamp", xmlDoc));
        output.output = new String(decodeBase64(
            xpathToContent("//output", xmlDoc)), "UTF-8");
        
        return output;
    }
    
    public boolean rebootInstances(List<String> instanceIds) throws Exception 
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "RebootInstances");
        
        Map<String, List<String>> indexedParams = 
            new HashMap<String, List<String>>();
        indexedParams.put("InstanceId", instanceIds);

        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION, 
            parameters, indexedParams);
        
        doQuery(HTTP_METHOD, ENDPOINT_URI, parameters);
        return true;
    }

    public List<Instance> terminateInstances(List<String> instanceIds) 
        throws Exception 
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "TerminateInstances");
        
        Map<String, List<String>> indexedParams = 
            new HashMap<String, List<String>>();
        indexedParams.put("InstanceId", instanceIds);

        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION, 
            parameters, indexedParams);
        
        HttpURLConnection conn = doQuery(HTTP_METHOD, ENDPOINT_URI, parameters);   
        Document xmlDoc = parseToDocument(conn.getInputStream());
        
        List <Instance> instances = new ArrayList<Instance>();
        for (Node node : xpathToNodeList("//instancesSet/item", xmlDoc)) {
            Instance instance = new Instance();
            instance.id = xpathToContent("instanceId", node);
            instance.state = xpathToContent("shutdownState/name", node);
            instance.previousState = xpathToContent("previousState/name", node);
            instances.add(instance);
        }
        return instances;
    }
    
    public List<SecurityGroup> describeSecurityGroups(List<String> groupNames) 
        throws Exception 
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "DescribeSecurityGroups");
        
        Map<String, List<String>> indexedParams = 
            new HashMap<String, List<String>>();
        indexedParams.put("GroupName", groupNames);
        
        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION, 
            parameters, indexedParams);
        
        HttpURLConnection conn = doQuery(HTTP_METHOD, ENDPOINT_URI, parameters);   
        Document xmlDoc = parseToDocument(conn.getInputStream());
    
        List<SecurityGroup> groups = new ArrayList<SecurityGroup>();
        for (Node node : xpathToNodeList("//securityGroupInfo/item", xmlDoc)) {
            SecurityGroup group = new SecurityGroup();
            group.name = xpathToContent("groupName", node);
            group.description = xpathToContent("groupDescription", node);
            group.ownerId = xpathToContent("ownerId", node);            
            
            for (Node ipNode : xpathToNodeList("ipPermissions/item", node)) {
                IpPermission grant = new IpPermission();
                grant.ipProtocol = IpProtocol.valueOf( 
                    xpathToContent("ipProtocol", ipNode));
                grant.fromPort = Integer.valueOf(
                    xpathToContent("fromPort", ipNode));
                grant.toPort = Integer.valueOf(
                    xpathToContent("toPort", ipNode));
                grant.cidrRange = xpathToContent("ipRanges/item/cidrIp", ipNode);
                
                for (Node groupNode : xpathToNodeList("groups/item", ipNode)) {
                    GroupPermission groupPerm = new GroupPermission();
                    groupPerm.name = xpathToContent("groupName", groupNode);
                    groupPerm.userId = xpathToContent("userId", groupNode);
                    grant.groups.add(groupPerm);
                }
                group.grants.add(grant);                
            }
            groups.add(group);
        }
        return groups;        
    }

    public boolean authorizeIngressByCidr(String groupName, 
        IpProtocol ipProtocol, Integer fromPort, Integer toPort, 
        String cidrRange) throws Exception 
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "AuthorizeSecurityGroupIngress");
        parameters.put("GroupName", groupName);
        parameters.put("IpProtocol", ipProtocol.toString());
        parameters.put("FromPort", fromPort.toString());
        parameters.put("ToPort", toPort.toString());
        parameters.put("CidrIp", cidrRange);
        
        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION, 
            parameters, EMPTY_INDEXED_MAP);
        
        doQuery(HTTP_METHOD, ENDPOINT_URI, parameters);
        return true;
    }

    public boolean revokeIngressByCidr(String groupName, 
        IpProtocol ipProtocol, Integer fromPort, Integer toPort, 
        String cidrRange) throws Exception 
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "RevokeSecurityGroupIngress");
        parameters.put("GroupName", groupName);
        parameters.put("IpProtocol", ipProtocol.toString());
        parameters.put("FromPort", fromPort.toString());
        parameters.put("ToPort", toPort.toString());
        parameters.put("CidrIp", cidrRange);
        
        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION, 
            parameters, EMPTY_INDEXED_MAP);
        
        doQuery(HTTP_METHOD, ENDPOINT_URI, parameters);
        return true;
    }

    public boolean authorizeIngressByGroup(String groupName, 
        String sourceGroupName, String sourceGroupOwnerId) throws Exception 
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "AuthorizeSecurityGroupIngress");
        parameters.put("GroupName", groupName);
        parameters.put("SourceSecurityGroupName", sourceGroupName);
        parameters.put("SourceSecurityGroupOwnerId", sourceGroupOwnerId);
        
        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION, 
            parameters, EMPTY_INDEXED_MAP);
        
        doQuery(HTTP_METHOD, ENDPOINT_URI, parameters);
        return true;
    }
    
    public boolean revokeIngressByGroup(String groupName, 
        String sourceGroupName, String sourceGroupOwnerId) throws Exception 
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "RevokeSecurityGroupIngress");
        parameters.put("GroupName", groupName);
        parameters.put("SourceSecurityGroupName", sourceGroupName);
        parameters.put("SourceSecurityGroupOwnerId", sourceGroupOwnerId);
        
        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION, 
            parameters, EMPTY_INDEXED_MAP);
        
        doQuery(HTTP_METHOD, ENDPOINT_URI, parameters);
        return true;
    }
    
    public boolean createSecurityGroup(String groupName, String groupDescription) 
        throws Exception 
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "CreateSecurityGroup");
        parameters.put("GroupName", groupName);
        parameters.put("GroupDescription", groupDescription);
        
        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION, 
            parameters, EMPTY_INDEXED_MAP);
        
        doQuery(HTTP_METHOD, ENDPOINT_URI, parameters);
        return true;
    }
    
    public boolean deleteSecurityGroup(String groupName) 
        throws Exception 
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "DeleteSecurityGroup");
        parameters.put("GroupName", groupName);
        
        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION, 
            parameters, EMPTY_INDEXED_MAP);
        
        doQuery(HTTP_METHOD, ENDPOINT_URI, parameters);
        return true;
    }

    public String registerImage(String imageLocation) 
        throws Exception 
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "RegisterImage");
        parameters.put("ImageLocation", imageLocation);
        
        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION, 
            parameters, EMPTY_INDEXED_MAP);
        
        HttpURLConnection conn = doQuery(HTTP_METHOD, ENDPOINT_URI, parameters);
        Document xmlDoc = parseToDocument(conn.getInputStream());
        return xpathToContent("//imageId", xmlDoc);
    }

    public boolean deregisterImage(String imageId) 
        throws Exception 
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "DeregisterImage");
        parameters.put("ImageId", imageId);
        
        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION, 
            parameters, EMPTY_INDEXED_MAP);
        
        doQuery(HTTP_METHOD, ENDPOINT_URI, parameters);
        return true;
    }
    
    public ImageAttribute describeImageAttribute(String imageId, 
        AttributeName attributeName) throws Exception 
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "DescribeImageAttribute");
        parameters.put("ImageId", imageId);
        parameters.put("Attribute", attributeName.toString());
        
        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION, 
            parameters, EMPTY_INDEXED_MAP);
        
        HttpURLConnection conn = doQuery(HTTP_METHOD, ENDPOINT_URI, parameters);   
        Document xmlDoc = parseToDocument(conn.getInputStream());
    
        ImageAttribute attribute = null;
        if (xpathToNodeList("//launchPermission", xmlDoc).size() > 0) {
            attribute = new LaunchPermissionAttribute();
            for (Node node : 
                xpathToNodeList("//launchPermission/item/group", xmlDoc)) 
            {
                ((LaunchPermissionAttribute)attribute).groups.add(
                    node.getTextContent());
            }
            for (Node node : 
                xpathToNodeList("//launchPermission/item/userId", xmlDoc)) 
            {
                ((LaunchPermissionAttribute)attribute).userIds.add(
                    node.getTextContent());
            }
        } else if (xpathToNodeList("//productCodes", xmlDoc).size() > 0) {
            attribute = new ProductCodesAttribute();
            for (Node node : xpathToNodeList("//productCodes/item", xmlDoc)) {
                ((ProductCodesAttribute)attribute).codes.add(
                    node.getTextContent());
            }            
        }
        attribute.imageId = xpathToContent("//imageId", xmlDoc);
        
        return attribute;
    }
    
    public boolean modifyImageAttribute(String imageId, 
        AttributeName attributeName, AttributeOperation operation, Map<String, 
        List<String>> values) throws Exception 
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "ModifyImageAttribute");
        parameters.put("ImageId", imageId);
        parameters.put("Attribute", attributeName.toString());
        parameters.put("OperationType", operation.toString());
        
        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION, 
            parameters, values);
        
        doQuery(HTTP_METHOD, ENDPOINT_URI, parameters);
        return true;
    }
    
    public boolean resetImageAttribute(String imageId, 
        AttributeName attributeName) throws Exception 
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "ResetImageAttribute");
        parameters.put("ImageId", imageId);
        parameters.put("Attribute", attributeName.toString());
        
        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION, 
            parameters, EMPTY_INDEXED_MAP);
        
        doQuery(HTTP_METHOD, ENDPOINT_URI, parameters);
        return true;
    }

    public String confirmProductInstance(String productCode, 
        String instanceId) throws Exception 
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "ConfirmProductInstance");
        parameters.put("ProductCode", productCode);
        parameters.put("InstanceId", instanceId);
        
        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION, 
            parameters, EMPTY_INDEXED_MAP);
        
        HttpURLConnection conn = doQuery(HTTP_METHOD, ENDPOINT_URI, parameters);   
        Document xmlDoc = parseToDocument(conn.getInputStream());
        
        return xpathToContent("//ownerId", xmlDoc);
    }

    public List<AddressAllocation> describeAddresses() throws Exception
    {
        return describeAddresses(null);
    }

    public List<AddressAllocation> describeAddresses(List<String> ipAddresses) 
        throws Exception 
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "DescribeAddresses");
        
        Map<String, List<String>> indexedParams = 
            new HashMap<String, List<String>>();
        indexedParams.put("PublicIp", ipAddresses);
        
        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION, 
            parameters, indexedParams);
                
        HttpURLConnection conn = doQuery(HTTP_METHOD, ENDPOINT_URI, parameters);   
        Document xmlDoc = parseToDocument(conn.getInputStream());

        List<AddressAllocation> addresses = new ArrayList<AddressAllocation>();
        for (Node node : xpathToNodeList("//addressesSet/item", xmlDoc)) {
            AddressAllocation address = new AddressAllocation();
            address.publicIp = xpathToContent("publicIp", node);
            address.instanceId = xpathToContent("instanceId", node);
            addresses.add(address);
        }

        return addresses;
    }

    public String allocateAddress() throws Exception 
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "AllocateAddress");
        
        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION, 
            parameters, EMPTY_INDEXED_MAP);
        
        HttpURLConnection conn = doQuery(HTTP_METHOD, ENDPOINT_URI, parameters);   
        Document xmlDoc = parseToDocument(conn.getInputStream());
        
        return xpathToContent("//publicIp", xmlDoc);
    }

    public boolean releaseAddress(String publicIp) throws Exception 
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "ReleaseAddress");
        parameters.put("PublicIp", publicIp);
        
        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION, 
            parameters, EMPTY_INDEXED_MAP);
        
        HttpURLConnection conn = doQuery(HTTP_METHOD, ENDPOINT_URI, parameters);
        Document xmlDoc = parseToDocument(conn.getInputStream());

        return "true".equals(xpathToContent("//return", xmlDoc));
    }

    public boolean associateAddress(String instanceId, String publicIp) 
        throws Exception 
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "AssociateAddress");
        parameters.put("InstanceId", instanceId);
        parameters.put("PublicIp", publicIp);
        
        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION, 
            parameters, EMPTY_INDEXED_MAP);
        
        HttpURLConnection conn = doQuery(HTTP_METHOD, ENDPOINT_URI, parameters);
        Document xmlDoc = parseToDocument(conn.getInputStream());

        return "true".equals(xpathToContent("//return", xmlDoc));
    }
    
    public boolean disassociateAddress(String publicIp) 
        throws Exception 
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "DisassociateAddress");
        parameters.put("PublicIp", publicIp);
        
        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION, 
            parameters, EMPTY_INDEXED_MAP);
        
        HttpURLConnection conn = doQuery(HTTP_METHOD, ENDPOINT_URI, parameters);
        Document xmlDoc = parseToDocument(conn.getInputStream());
    
        return "true".equals(xpathToContent("//return", xmlDoc));
    }
    
    /*
     * Below this point are class and enum definitions specific to the Java
     * implementation of AWS clients. These items make it easier to pass
     * parameters into this client's methods, and to retrieve results from the
     * methods.
     */

    public static enum IpProtocol { tcp, udp, icmp };

    public static enum AttributeOperation { add, remove };

    public static enum AttributeName { launchPermission, productCodes };
        
    class Reservation {
        String id;
        String ownerId;
        List<String> groups = new ArrayList<String>();
        List<Instance> instances = new ArrayList<Instance>();

        public String toString() { 
            return "{" + this.getClass().getName()
            + ": id=" + id + ", ownerId=" + ownerId 
            + ", groups=" + groups 
            + ", instances=" + instances + "}";
        }
    }    
   
    class Instance {
        String id;
        String imageId;
        String state;
        String previousState;
        String privateDns;
        String publicDns;
        String type;
        Date launchTime;
        String reason;
        String keyName;
        Integer amiLaunchIndex;
        String availabilityZone;
        String kernelId;
        String ramdiskId;
        List<String> productCodes = new ArrayList<String>();
        
        public String toString() { 
            return "{" + this.getClass().getName() 
            + ": id=" + id + ", imageId=" + imageId 
            + ", state=" + state + ", previousState=" + previousState
            + ", privateDns=" + privateDns + ", publicDns=" + publicDns
            + ", type=" + type + ", launchTime=" + launchTime
            + ", reason=" + reason + ", keyName=" + keyName 
            + ", amiLaunchIndex=" + amiLaunchIndex 
            + ", productCodes=" + productCodes 
            + ", availabilityZone=" + availabilityZone 
            + ", kernelId=" + kernelId + ", ramdiskId=" + ramdiskId + "}"; 
        }
    }
    
    class KeyPair {
        String name;
        String fingerprint;
        String material;
        String fileName; 

        public String toString() { 
            return "{" + this.getClass().getName()
            + ": name=" + name + ", fingerprint=" + fingerprint
            + ", fileName=" + fileName 
            + ", material=" + material + "}";
        }
    }

    class Image {
        String id;
        String location;
        String state;
        String ownerId;
        boolean isPublic = false;
        String architecture;
        String type;
        String kernelId;
        String ramdiskId;
        List<String> productCodes = new ArrayList<String>();

        public String toString() { 
            return "{" + this.getClass().getName()
            + ": id=" + id + ", location=" + location
            + ", state=" + state + ", isPublic=" + isPublic
            + ", productCodes=" + productCodes
            + ", ownerId=" + ownerId + ", architecture=" + architecture
            + ", type=" + type + ", kernelId=" + kernelId 
            + ", ramdiskId=" + ramdiskId + "}";
        }
    }

    class ConsoleOutput {
        String instanceId;
        Date timestamp;
        String output;

        public String toString() { 
            return "{" + this.getClass().getName()
            + ": instanceId=" + instanceId 
            + ", timestamp=" + timestamp
            + ", output=" + output + "}";
        }
    }
    
    class GroupPermission {
        String userId;
        String name;

        public String toString() { 
            return "{" + this.getClass().getName()
            + ": userId=" + userId  + ", groupName=" + name +"}";
        }
    }
    
    class IpPermission {
        IpProtocol ipProtocol;
        Integer fromPort;
        Integer toPort;
        String cidrRange;
        List<GroupPermission> groups = new ArrayList<GroupPermission>();

        public String toString() { 
            return "{" + this.getClass().getName()
            + ": ipProtocol=" + ipProtocol  + ", fromPort=" + fromPort 
            + ", toPort=" + toPort + ", cidrRange=" + cidrRange 
            + ", groups=" + groups + "}";
        }
    }
    
    class SecurityGroup {
        String name;
        String description;
        String ownerId;
        List<IpPermission> grants = new ArrayList<IpPermission>();

        public String toString() { 
            return "{" + this.getClass().getName()
            + ": groupName=" + name + ", ownerId=" + ownerId
            + ", groupDescription=" + description 
            + ", grants=" + grants + "}";
        }
    }
    
    abstract class ImageAttribute {
        String imageId;

        public String toString() { 
            return "{" + this.getClass().getName()
            + ": imageId=" + imageId + "}";
        }
    }

    class LaunchPermissionAttribute extends ImageAttribute {
        List<String> userIds = new ArrayList<String>();
        List<String> groups = new ArrayList<String>();

        public String toString() { 
            return "{" + this.getClass().getName()
            + ": imageId=" + imageId + ", userIds=" 
            + userIds + ", groups=" + groups + "}";
        }
    }

    class ProductCodesAttribute extends ImageAttribute {
        List<String> codes = new ArrayList<String>();

        public String toString() { 
            return "{" + this.getClass().getName()
            + ": imageId=" + imageId + ", codes=" + codes + "}";
        }
    }

    class AvailabilityZone {
        String name = null;
        String state = null;

        public String toString() { 
            return "{" + this.getClass().getName()
            + ": name=" + name + ", state=" + state + "}";
        }
    }
    
    class AddressAllocation {
        String publicIp;
        String instanceId;

        public String toString() { 
            return "{" + this.getClass().getName()
            + ": publicIp=" + publicIp + ", instanceId=" + instanceId + "}";
        }
    }

}
