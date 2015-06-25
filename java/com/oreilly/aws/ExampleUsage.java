package com.oreilly.aws;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.oreilly.aws.AWS.HttpMethod;
import com.oreilly.aws.EC2.AttributeName;
import com.oreilly.aws.EC2.AttributeOperation;
import com.oreilly.aws.EC2.IpProtocol;
import com.oreilly.aws.FPS.Amount;
import com.oreilly.aws.FPS.AmountLimitType;
import com.oreilly.aws.FPS.ChargeToRole;
import com.oreilly.aws.FPS.InstrumentStatus;
import com.oreilly.aws.FPS.NotificationOperation;
import com.oreilly.aws.FPS.PaymentMethod;
import com.oreilly.aws.FPS.TokenStatus;
import com.oreilly.aws.FPS.TokenType;
import com.oreilly.aws.FPS.TokenUsageLimit;
import com.oreilly.aws.FPS.TokenUsageLimitAmount;
import com.oreilly.aws.FPS.TokenUsageLimitCount;
import com.oreilly.aws.FPS.TransactionOperation;
import com.oreilly.aws.S3.AccessControlList;
import com.oreilly.aws.S3.BucketLocation;
import com.oreilly.aws.S3.BucketLoggingStatus;
import com.oreilly.aws.SQS.AmazonCustomerByEmail;

/**
 * Code fragments that give a very basic demonstration of how to use the S3, 
 * EC2, SQS and FPS client implementations. Do not run these fragments as they
 * are, they are unlikely to work unless you adapt them to your own situation.
 */
public class ExampleUsage {
    private boolean isDebugEnabled = true;
    private boolean isSecureHttpEnabled = true;

    
    public static void main(String[] args) throws Exception {
        ExampleUsage usage = new ExampleUsage();
        usage.s3ServiceOperations();
        usage.ec2ServiceOperations();
        usage.ec2ServiceVersion_2008_02_01_Operations();
        usage.sqsServiceOperations();
        usage.sqsServiceVersion_2008_01_01_Operations();
        usage.fpsServiceOperations();
        usage.sdbServiceOperations();
    }

    public void s3ServiceOperations() throws Exception {
        S3 s3 = new S3(isDebugEnabled, isSecureHttpEnabled);

        System.out.println(s3.listBuckets());                

        /*
        System.out.println(
            s3.createBucket("test.location3", BucketLocation.EU));

        System.out.println(s3.deleteBucket("test.location3"));
        
        System.out.println(s3.getBucketLocation("test.location1"));
        
        Map<String, String> params = new HashMap<String, String>();
        params.put("max-keys", "3");
        System.out.println(s3.listObjects("using-aws", params));
        
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("Content-Type", "text/html");
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("Description", "this is just a description");
        String webpage = "<html><body>This is a <b>Web Page</b></body></html>";
        InputStream dataInputStream =
            new ByteArrayInputStream(webpage.getBytes("UTF-8"));
            // new FileInputStream("/path/to/some/file");
        System.out.println(
            s3.createObject("using-aws", "WebPage.html", 
                dataInputStream, headers, metadata));

        // getObject to string
        headers = new HashMap<String, String>();
        System.out.println(
            s3.getObject("using-aws", "WebPage.html", headers, null));

        // getObject to File
        headers = new HashMap<String, String>();
        FileOutputStream fos = 
            new FileOutputStream("/path/to/output/file");
        System.out.println(
            s3.getObject("using-aws", "WebPage.html", headers, fos));
        
        System.out.println(
            s3.deleteObject("using-aws", "WebPage.html"));

        System.out.println(s3.getLogging("using-aws"));

        BucketLoggingStatus status = s3.new BucketLoggingStatus();
        status.enabled = true;
        status.targetBucket = "using-aws";
        status.targetPrefix = "using-aws.";
        System.out.println(s3.setLogging("using-aws", status));

        AccessControlList acl = s3.getAcl("using-aws", "WebPage.html");
        System.out.println(acl);

        System.out.println(s3.setAcl("using-aws", "", acl));
        
        System.out.println(s3.setCannedAcl("private", "using-aws", ""));
        
        s3.getTorrent("using-aws", "WebPage.html", 
            new File("/path/to/output/WebPage.html.torrent"));

        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("acl", null);
        long timeInThirtySecs = (System.currentTimeMillis() / 1000) + 30;
        System.out.println(
            s3.getSignedUri(HttpMethod.GET, timeInThirtySecs, "using-aws", 
                "WebPage.html", parameters, AWS.EMPTY_STRING_MAP, false));
                
        // Build a policy document
        Map<String, Object> conditions = new HashMap<String, Object>();
        conditions.put("key", null); // Empty starts-with 
        conditions.put("bucket", "my-bucket"); // Equality condition
        conditions.put("x-amz-meta-mytag", new String[] {"Work", "TODO"});
        Map<String, String> operationParams = new HashMap<String, String>();
        operationParams.put("op", "starts-with");
        operationParams.put("value", "text/");
        conditions.put("Content-Type", operationParams);
        S3.Range range = s3.new Range(1, 50);
        conditions.put("content-length-range", range);
        System.out.println(s3.buildPostPolicy(new Date(), conditions));

        System.out.println(s3.buildPostForm("my-bucket", "${filename}"));
        
        Map<String, String> fields = new HashMap<String, String>();
        fields.put("acl", "public-read");
        fields.put("Content-Type", "image/jpeg");
        fields.put("success_action_redirect", "http://localhost/post_upload");
        conditions = new HashMap<String, Object>();
        conditions.put("key", "uploads/images/pic.jpg"); 
        conditions.put("bucket", "using-aws");        
        conditions.put("content-length-range", s3.new Range(10240, 204800));        
        conditions.putAll(fields);
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.MINUTE, 5);
        System.out.println(s3.buildPostForm("using-aws", 
            "uploads/images/pic.jpg", cal.getTime(), conditions, fields, null));                
        */
    }

    public void ec2ServiceOperations() throws Exception {
        EC2 ec2 = new EC2(isDebugEnabled, isSecureHttpEnabled);

        System.out.println(ec2.describeInstances());

        /*
        String instanceId = "i-44ff0b2d";

        List<String> instanceIds = new ArrayList<String>();
        instanceIds.add(instanceId);
        System.out.println(ec2.describeInstances(instanceIds));

        List<String> keypairNames = new ArrayList<String>();
        keypairNames.add("my-private-key");
        System.out.println(ec2.describeKeypairs(keypairNames));

        System.out.println(ec2.createKeypair("testing", true));

        ec2.deleteKeypair("testing");

        List<String> ids = new ArrayList<String>();
        ids.add("self");
        System.out.println(ec2.describeImages(null, ids, null));

        List<String> securityGroups = new ArrayList<String>();
        securityGroups.add("OpenVPN");
        String userData = "Just testing...";
        System.out.println(ec2.runInstances("ami-62cc290b", "my-private-key",
            securityGroups, userData.getBytes("UTF-8")));

        System.out.println(ec2.getConsoleOutput(instanceId));

        instanceIds = new ArrayList<String>();
        instanceIds.add(instanceId);
        ec2.rebootInstances(instanceIds);

        instanceIds = new ArrayList<String>();
        instanceIds.add(instanceId);
        System.out.println(ec2.terminateInstances(instanceIds));

        ec2.authorizeIngressByCidr("web", IpProtocol.tcp, 1234, 1234,
            "0.0.0.0/0");

        ec2.revokeIngressByCidr("web", IpProtocol.tcp, 1234, 1234, "0.0.0.0/0");

        ec2.authorizeIngressByGroup("web", "OpenVPN", "123456789012");

        ec2.revokeIngressByGroup("web", "OpenVPN", "123456789012");

        System.out.println(ec2.describeSecurityGroups(null));

        ec2.createSecurityGroup("Test", "Just testing");

        ec2.deleteSecurityGroup("Test");

        System.out.println(ec2.describeImageAttribute("ami-1eca2f77",
            AttributeName.launchPermission));

        Map<String, List<String>> userIds = new HashMap<String, List<String>>();
        ids = new ArrayList<String>();
        ids.add("123456789012");
        ids.add("210987654321");
        userIds.put("UserId", ids);
        ec2.modifyImageAttribute("ami-1eca2f77",
            AttributeName.launchPermission, AttributeOperation.add, userIds);

        ec2.resetImageAttribute("ami-1eca2f77", AttributeName.launchPermission);

        ec2.confirmProductInstance("1234", "i-1234");
        */
    }
    
    public void ec2ServiceVersion_2008_02_01_Operations() throws Exception {
        EC2_2008_02_01 ec2 = new EC2_2008_02_01(isDebugEnabled, isSecureHttpEnabled);

        System.out.println(ec2.describeAvailabilityZones(null));

        /*
        List<String> ids = new ArrayList<String>();
        ids.add("amazon");        
        System.out.println(ec2.describeImages(null, ids, null));
        
        com.oreilly.aws.EC2_2008_02_01.Reservation reservation = 
            ec2.runInstances("ami-2bb65342", "ec2-private-key", null, null, 
                1, 1, "m1.small", "us-east-1a", "aki-9b00e5f2", null); 
        System.out.println(reservation);
        
        System.out.println(ec2.describeInstances());

        String publicIp = ec2.allocateAddress();
        System.out.println(publicIp);

        com.oreilly.aws.EC2_2008_02_01.Instance instance = 
            reservation.instances.get(0);
        System.out.println(ec2.associateAddress(instance.id, publicIp));

        System.out.println(ec2.disassociateAddress(publicIp));

        System.out.println(ec2.releaseAddress(publicIp));

        System.out.println(ec2.describeAddresses());
        */
    }

    public void sqsServiceOperations() throws Exception {
        SQS sqs = new SQS(isDebugEnabled, isSecureHttpEnabled);

        System.out.println(sqs.listQueues());

        /*
        URL queueUrl = new URL(
            "http://queue.amazonaws.com/A1MU5FWLQSN7CU/test123");
        String msgId = 
            "0B19PMA8VQTAAGRH6VDF|260BDAS2K7RNYSSR6GY1|5Z5381DXG1YPT7CM1FQ1";

        System.out.println(sqs.createQueue("test123", 45));

        sqs.deleteQueue(queueUrl, false);

        sqs.setQueueAttribute(queueUrl, "VisibilityTimeout", 15);

        System.out.println(sqs.getQueueAttributes(queueUrl));

        System.out.println(sqs.sendMessage(queueUrl,
            "This is jüst a tést message"));

        System.out.println(sqs.peekMessage(queueUrl, msgId));

        System.out.println(sqs.receiveMessages(queueUrl, 10, 30, true));

        sqs.deleteMessage(queueUrl, msgId);

        sqs.changeMessageVisibility(queueUrl, msgId, 0);

        AmazonCustomerByEmail emailGrantee = sqs.new AmazonCustomerByEmail();
        emailGrantee.emailAddress = "your_email@address.com";
        System.out.println(sqs.listGrants(queueUrl, null, emailGrantee));

        emailGrantee = sqs.new AmazonCustomerByEmail();
        emailGrantee.emailAddress = "your_email@address.com";
        sqs.addGrant(queueUrl, emailGrantee, SQS.Permission.RECEIVEMESSAGE);

        emailGrantee = sqs.new AmazonCustomerByEmail();
        emailGrantee.emailAddress = "your_email@address.com";
        sqs.removeGrant(queueUrl, emailGrantee, SQS.Permission.RECEIVEMESSAGE);
        */
    }
    
    public void sqsServiceVersion_2008_01_01_Operations() throws Exception {
        SQS_2008_01_01 sqs = new SQS_2008_01_01(isDebugEnabled, isSecureHttpEnabled);

        System.out.println(sqs.listQueues());

        /*
        System.out.println(sqs.createQueue("test-queue", 45));
        URL queueUrl = new URL("http://queue.amazonaws.com/test-queue");
        

        sqs.setQueueAttribute(queueUrl, "VisibilityTimeout", 15);

        System.out.println(sqs.getQueueAttributes(queueUrl));

        System.out.println(sqs.sendMessage(queueUrl,
            "This is jüst a tést message"));

        System.out.println(sqs.receiveMessages(queueUrl, 10, 30, true));
        String receiptHandle = 
            "Euvo62/1nlL6HgIxaODhKX66jRoqc+ccyqWMFny07GYhfQqw6XXkNchiKYIpELQyUn"
            + "Rg8vKiDGOdclXA49oHhCxokbKAZ85m3naoqIwVPnuH0oJfwoTCqkSahYJ/61q4bX"
            + "MhkrCvj5xOPgA65Fyc3NDNqW3nEyUsIfmBpzhq0Lbii5FpLcCuggx0EgkKcw/8iU"
            + "atCbtBkL7FZLKMsvbT4nVFJOsF/Q2c0QxfWIyR8OpbQoVQYS78nw=="; 

        sqs.deleteMessage(queueUrl, receiptHandle);

        sqs.deleteQueue(queueUrl);
        */            
    }
    
    
    public void fpsServiceOperations() throws Exception {
        FPS fps = new FPS(isDebugEnabled, isSecureHttpEnabled);
        
        System.out.println(fps.getAccountBalance());
                
        /*
        int daysAgo = 100;
        Date startDate = new Date(
            (System.currentTimeMillis() - (3600l * 24 * daysAgo * 1000)));
        System.out.println(
            fps.getAccountActivity(startDate));
                
        System.out.println(
            fps.installPaymentInstruction("MyRole == 'Caller';", 
                "CallerRef1", TokenType.Unrestricted, "Reason text", 
                "TemporaryCallerToken"));
        
        String tokenId = 
            "71UF8U6NGAAL5FGDQ71U4MUNDEIDNEV95VMD3I88KVPIEU3QQHVRHQ8QXJNVTUCJ";
        System.out.println(
            fps.getPaymentInstruction(tokenId));
        
        System.out.println(
            fps.getTokens("TemporaryCallerToken", TokenStatus.Active, 
                "CallerRef1"));
        
        tokenId = 
            "71UF8U6NGAAL5FGDQ71U4MUNDEIDNEV95VMD3I88KVPIEU3QQHVRHQ8QXJNVTUCJ";
        String callerRef = "CallerRef1";
        System.out.println(fps.getTokenByCaller(tokenId, null));
        System.out.println(fps.getTokenByCaller(null, callerRef));
        System.out.println(fps.getTokenByCaller(tokenId, callerRef));
        
        tokenId = 
            "77UF6UQNGUAU5FQD171B4UUNMENDNTVC5VBDVI8TKFPIZU7QQUVDHQFQ9JN1TGCU";
        System.out.println(
            fps.getTokenUsage(tokenId));
        
        tokenId = 
            "71UF8U6NGAAL5FGDQ71U4MUNDEIDNEV95VMD3I88KVPIEU3QQHVRHQ8QXJNVTUCJ";
        System.out.println(
            fps.cancelToken(tokenId, "Java testing"));
        
        // Generate SingleUse Sender token (one-off payment)
        URL returnURL = new URL("http://localhost:8888/");
        System.out.println(
            fps.getUrlForSingleUseSender("SingleUseJavaTest6", 1.01d, returnURL));

        // Pay
        String senderTokId = 
            "U37X35ZSQ1TG328468U96ITB9YHUI8LSPX7GQGMTMI2PNE118DH84GINFDOMXEOE";
        
        String callerTokId = 
            "74UFLUGNG3AF5FHD67114VUNNEBDNUVL5VRDCI8HK6PIRU8QQQVVHQXQNJN3TZC2";
        String recipientTokId = 
            "76UF2UENGSAZ5F8D571T4IUNKEDDNLV25VDDQI8TKTPIQU3QQLVEHQBQKJNSTJCE";
        Amount amount = fps.new Amount();
        amount.amount = 1.01d;
        amount.currencyCode = Amount.US_DOLLARS;
        Date date = new Date(1196232172000l);
        System.out.println(
            fps.pay(recipientTokId, senderTokId, callerTokId, "PayJavaTest6", 
                amount, ChargeToRole.Recipient, 
                date, "PayJavaTest6.Sender", "PayJavaTest6.Recipient", 
                "Caller Description", "Sender Description", "Recipient Description",
                fps.encodeBase64("This is just some métadata")));

        // Refund
        callerTokId = 
            "74UFLUGNG3AF5FHD67114VUNNEBDNUVL5VRDCI8HK6PIRU8QQQVVHQXQNJN3TZC2";
        String refundSenderTokId = 
            "71UFVU7NGNAK5F3DC71Z47UNGE3DNCVJ5VJD6I8KKJPISU3QQ8VVHQLQEJNCTNC1";
        String transactionId = "12Q2G1ABV7Q6UJ4BAJSEIBHJ6B4S4OAE8AS";
        Amount refundAmount = fps.new Amount();
        refundAmount.amount = 0.51d;
        refundAmount.currencyCode = Amount.US_DOLLARS;
        date = new Date(1196232173000l);
        System.out.println(
            fps.refund(refundSenderTokId, callerTokId, transactionId, 
                "RefundJavaTest1", ChargeToRole.Recipient,   
                refundAmount, date, "RefundJavaTest1.Sender", "RefundJavaTest1.Recipient", 
                "Caller Description", "Refund Sender Description", "Refund Recipient Description", 
                fps.encodeBase64("This is just some métadata")));

        // Generate SingleUse Sender token (reserve/settle payment)
        returnURL = new URL("http://localhost:8888/");
        System.out.println(
            fps.getUrlForSingleUseSender("SingleUseJavaTest7", 1.01d, returnURL,
                PaymentMethod.CC, null, null, true));
                        
        // Reserve
        senderTokId = 
            "U37XT55SQRT1322418UR6ZTBPYIUIDL9PXRGDGM5ME2PHEZ188HM4GCNNDOXX1OP";
        callerTokId = 
            "74UFLUGNG3AF5FHD67114VUNNEBDNUVL5VRDCI8HK6PIRU8QQQVVHQXQNJN3TZC2";
        recipientTokId = 
            "76UF2UENGSAZ5F8D571T4IUNKEDDNLV25VDDQI8TKTPIQU3QQLVEHQBQKJNSTJCE";
        amount = fps.new Amount();
        amount.amount = 1.01d;
        amount.currencyCode = Amount.US_DOLLARS;
        date = new Date(1196232172000l);
        System.out.println(
            fps.reserve(recipientTokId, senderTokId, callerTokId, 
                "ReserveJavaTest1", amount, ChargeToRole.Recipient, date,
                "ReserveJavaTest1.Sender", "ReserveJavaTest1.Recipient", 
                "Caller Description", "Sender Description", 
                "Recipient Description", fps.encodeBase64(
                "This is just some métadata")));                
        
        // Settle
        transactionId = "12Q4IJRVL54RHSG4ZMJIZL5DUTI7Q7Q1EFQ";
        amount = fps.new Amount();
        amount.amount = 1.01d;
        amount.currencyCode = Amount.US_DOLLARS;
        date = new Date(1196232172000l);
        System.out.println(
            fps.settle(transactionId, amount, date));
        
        // GetTransaction
        transactionId = "12Q4IJRVL54RHSG4ZMJIZL5DUTI7Q7Q1EFQ";
        System.out.println(
            fps.getTransaction(transactionId));

        // RetryTransaction
        transactionId = "12Q4IJRVL54RHSG4ZMJIZL5DUTI7Q7Q1EFQ";
        System.out.println(
            fps.retryTransaction(transactionId));
        
        // GetResults
        System.out.println(
            fps.getResults(20, TransactionOperation.Pay));
        
        // DiscardResults
        List<String> transactionIds = new ArrayList<String>();
        transactionIds.add("12Q2FOE2R7ME4G89FP61G2M8J26DPQ369HD");
        transactionIds.add("12Q2G1ABV7Q6UJ4BAJSEIBHJ6B4S4OAE8AS");
        transactionIds.add("12Q2GSK2B2PMSEV9BVQO1GSF5ZB6SJNHM42");
        transactionIds.add("12Q4IJRVL54RHSG4ZMJIZL5DUTI7Q7Q1EFQ");
        System.out.println(
            fps.discardResults(transactionIds));
        
        // Generate MultiUse Sender token
        returnURL = new URL("http://localhost:8888/");
        Date validityExpiryDate = new Date(1212242400000l);
        List<TokenUsageLimit> usageLimits = new ArrayList<TokenUsageLimit>();
        TokenUsageLimitCount limit1 = fps.new TokenUsageLimitCount();
        limit1.count = 3;
        limit1.period = "1 Day";
        usageLimits.add(limit1);
        TokenUsageLimitAmount limit2 = fps.new TokenUsageLimitAmount();
        limit2.amount = 50.00;
        limit2.period = "1 Month";
        usageLimits.add(limit2);
        System.out.println(
            fps.getUrlForMultiUseSender("MultiUseJavaTest1", 100.00, returnURL,
                PaymentMethod.CC, "Reason: Testing", null, 
                AmountLimitType.Maximum, 10.00, null, validityExpiryDate, 
                usageLimits)); 

        // Generate Recurring Sender token
        returnURL = new URL("http://localhost:8888/");
        validityExpiryDate = new Date(1230728400000l);
        System.out.println(
            fps.getUrlForRecurringSender("RecurringJavaTest1", 100.00,
                "7 Days", returnURL, PaymentMethod.CC, "Reason: Testing", null, 
                null, validityExpiryDate)); 

        // Generate Recipient and Refund tokens
        returnURL = new URL("http://localhost:8888/");
        System.out.println(
            fps.getUrlForRecipient("RecipientJavaTest1", 
                "RecipientRefundJavaTest1", false, returnURL));

        // Generate URL for EditToken CBUI Pipeline
        tokenId = 
            "U27XQ55SQUTG32I4Z8UB6KTBFYBUIBLKPXAGVGM3M92PSEK186HA4G3N1DO4XGOV";
        returnURL = new URL("http://localhost:8888/");
        System.out.println(
            fps.getUrlForEditingToken("EditTokenJavaTest1", tokenId, returnURL));

        // Generate URL for SetupPrepaid CBUI Pipeline
        returnURL = new URL("http://localhost:8888/");
        validityExpiryDate = new Date(1212242400000l);
        System.out.println(
            fps.getUrlForPrepaidInstrument("PrepaidJavaTest1.SenderToken", 
                "PrepaidJavaTest1.FundingToken", 15.00, returnURL,
                PaymentMethod.CC, "Reason: Testing", 
                new Date(), validityExpiryDate));        
        
        // FundPrepaid
        String fundingSenderTokenId = 
            "U57X85NSQFTL32P4F8UM6MTBKYPUIHLPPXHG5GM8MX2PTE518TH84GSN3DONXFOS";
        String instrumentId = 
            "UY7XV5CSQCTD32V4H8UA6HTBSYTUIDLBPXHGFGMXMC2PKED189HX4GKNADO6XVO5";
        String callerTokenId = 
            "74UFLUGNG3AF5FHD67114VUNNEBDNUVL5VRDCI8HK6PIRU8QQQVVHQXQNJN3TZC2";
        date = new Date(1196232172000l);
        amount = fps.new Amount();
        amount.amount = 15.00;
        amount.currencyCode = Amount.US_DOLLARS;
        System.out.println(
            fps.fundPrepaid(fundingSenderTokenId, instrumentId, callerTokenId, 
                "FundPrepaidJavaTest1", amount, ChargeToRole.Recipient, 
                date, "SenderRef", "RecipientRef", "Caller Description", 
                "Sender Description", "Recipient Description", 
                fps.encodeBase64("This is just some métadata")));
        
        // GetPrepaidBalance
        instrumentId = 
            "UY7XV5CSQCTD32V4H8UA6HTBSYTUIDLBPXHGFGMXMC2PKED189HX4GKNADO6XVO5";
        System.out.println(
            fps.getPrepaidBalance(instrumentId));
        
        // GetAllPrepaidInstruments
        System.out.println(
            fps.getAllPrepaidInstruments(InstrumentStatus.Active));
        
        // GetTotalPrepaidLiability
        System.out.println(
            fps.getTotalPrepaidLiability());
        
        // Generate URL for SetupPostpaid CBUI Pipeline
        returnURL = new URL("http://localhost:8888/");
        validityExpiryDate = new Date(1212242400000l);
        usageLimits = new ArrayList<TokenUsageLimit>();
        limit1 = fps.new TokenUsageLimitCount();
        limit1.count = 3;
        limit1.period = "1 Day";
        usageLimits.add(limit1);
        limit2 = fps.new TokenUsageLimitAmount();
        limit2.amount = 50.00;
        limit2.period = "1 Month";
        usageLimits.add(limit2);        
        System.out.println(
            fps.getUrlForPostpaidInstrument("PostpaidJavaTest1.SenderToken", 
                "PostpaidJavaTest1.SettlementToken", 15.00, 100.00, returnURL,
                PaymentMethod.CC, "Reason: Testing", 
                new Date(), validityExpiryDate, usageLimits));        

        // Make a payment from the Postpaid (Credit) instrument
        callerTokId = 
            "74UFLUGNG3AF5FHD67114VUNNEBDNUVL5VRDCI8HK6PIRU8QQQVVHQXQNJN3TZC2";
        recipientTokId = 
            "76UF2UENGSAZ5F8D571T4IUNKEDDNLV25VDDQI8TKTPIQU3QQLVEHQBQKJNSTJCE";
        senderTokId = 
            "U67X459SQZTK32A4Z8UT6TTBEYUUIQLQPXUGUGMZMK2PIE318THL4G6NKDOTX7OL";
        amount = fps.new Amount();
        amount.amount = 10.50d;
        amount.currencyCode = Amount.US_DOLLARS;
        System.out.println(
            fps.pay(recipientTokId, senderTokId, callerTokId, 
                "PayFromCreditJavaTest1", amount, ChargeToRole.Recipient)); 

        // SettleDebt
        String settlementTokenId = 
            "U57XU5JSQZTX32D4R8UT6FTB1YIUISLMPXCGXGMEM82P2EI18CHX4G5NNDOEX3OJ";
        instrumentId = 
            "U37XM5BSQGTC3264G8UL6ATBLY3UI3LVPX1GTGM7M82PFEU18LHS4G3NHDOPXSO1";
        callerTokenId = 
            "74UFLUGNG3AF5FHD67114VUNNEBDNUVL5VRDCI8HK6PIRU8QQQVVHQXQNJN3TZC2";
        amount = fps.new Amount();
        amount.amount = 5.50;
        amount.currencyCode = Amount.US_DOLLARS;
        System.out.println(
            fps.settleDebt(settlementTokenId, instrumentId, callerTokenId, 
                "SettleDebtJavaTest1", amount));
        
        // WriteOffDebt
        instrumentId = 
            "U37XM5BSQGTC3264G8UL6ATBLY3UI3LVPX1GTGM7M82PFEU18LHS4G3NHDOPXSO1";
        callerTokenId = 
            "74UFLUGNG3AF5FHD67114VUNNEBDNUVL5VRDCI8HK6PIRU8QQQVVHQXQNJN3TZC2";
        amount = fps.new Amount();
        amount.amount = 5.0;
        amount.currencyCode = Amount.US_DOLLARS;
        System.out.println(
            fps.writeOffDebt(instrumentId, callerTokenId, "SettleDebtJavaTest2", 
                amount));

        // GetAllCreditInstruments
        System.out.println(
            fps.getAllCreditInstruments());

        // GetDebtBalance
        instrumentId = 
            "U37XM5BSQGTC3264G8UL6ATBLY3UI3LVPX1GTGM7M82PFEU18LHS4G3NHDOPXSO1";
        System.out.println(
            fps.getDebtBalance(instrumentId));

        // GetOutstandingDebtBalance
        System.out.println(
            fps.getOutstandingDebtBalance());

        // SubscribeForCallerNotification
        URL notificationWebService = new URL("https:www.myhost.com/api");
        System.out.println(
            fps.subscribeForCallerNotification(
                NotificationOperation.postTokenCancellation, 
                notificationWebService));

        // UnsubscribeForCallerNotification
        System.out.println(
            fps.unsubscribeForCallerNotification(
                NotificationOperation.postTokenCancellation));
        
        // Parse parameters in Result URL from Co-Branded UI Pipeline, and 
        // verify the URL
        URL postpaidInstrumentURL = new URL(
            "http://localhost:8888/?status=SC&expiry=03%2F2013"
            + "&creditSenderTokenID="
            + "U67X459SQZTK32A4Z8UT6TTBEYUUIQLQPXUGUGMZMK2PIE318THL4G6NKDOTX7OL"
            + "&settlementTokenID="
            + "U57XU5JSQZTX32D4R8UT6FTB1YIUISLMPXCGXGMEM82P2EI18CHX4G5NNDOEX3OJ"
            + "&creditInstrumentID="
            + "U37XM5BSQGTC3264G8UL6ATBLY3UI3LVPX1GTGM7M82PFEU18LHS4G3NHDOPXSO1"
            + "&awsSignature=v9p8CkrmEvqvJnqoZsNlpG8cLj0=");
        System.out.println(
            fps.parseURLParameters(postpaidInstrumentURL));
        System.out.println("Is a valid URI? " +
            fps.verifyPipelineResultURL(postpaidInstrumentURL));

        // Generate an FPS Pay Now widget form
        Map<String, String> options = new HashMap<String, String>();
        options.put("referenceId", "ProductCode-1234");
        options.put("returnUrl", "http://my.website.com/post_payment_success");
        options.put("abandonUrl", "http://my.website.com/post_payment_cancel");
        options.put("immediateReturn", "1");
        String form = 
            fps.buildPaymentWidget("ABCDEFGHIJ1234567890ABCDEFGHIJ12345678", 
            500, "Moon", options);
        System.out.println(form);
            
        */
    }

    public void sdbServiceOperations() throws Exception {
        SimpleDB sdb = new SimpleDB(isDebugEnabled, isSecureHttpEnabled);
        
        System.out.println(sdb.listDomains());
                
        /*
        String testDomainName = "test-domain";
        
        System.out.println(sdb.createDomain(testDomainName));
        
        // Test construction of attribute parameters
        Map<String, Object> attributes = new HashMap<String, Object>();
        attributes.put("Model", "MacBook");
        List<String> colors = new ArrayList<String>();
        colors.add("White");
        colors.add("Black");
        attributes.put("Color", colors);
        System.out.println(sdb.buildAttributeParams(attributes, false));

        attributes = new HashMap<String, Object>();
        attributes.put("Name", "Tomorrow Never Knows");
        attributes.put("Artist", "The Beatles");
        attributes.put("Time", 177);
        List<String> albums = new ArrayList<String>();
        albums.add("Revolver");
        albums.add("The Beatles Box Set");
        attributes.put("Album", albums);
        System.out.println(sdb.putAttributes(testDomainName, "TestItem", attributes));

        Map<String, List> receivedAttributes = sdb.getAttributes(testDomainName, "TestItem");
        System.out.println(receivedAttributes);

        List attributeValues = sdb.getAttributes(testDomainName, "TestItem", "Time");
        System.out.println(attributeValues);

        Map<String, Object> deletes = new HashMap<String, Object>();
        deletes.put("Album", "Revolver");
        sdb.deleteAttributes(testDomainName, "TestItem", deletes);
        
        deletes = new HashMap<String, Object>();
        deletes.put("Album", null);
        sdb.deleteAttributes(testDomainName, "TestItem", deletes);
        
        sdb.deleteAttributes(testDomainName, "TestItem");
        
        System.out.println(sdb.deleteDomain(testDomainName));
        
        System.out.println("Encoded true: " + sdb.encodeBoolean(true));
        System.out.println("Encoded false: " + sdb.encodeBoolean(false));
        
        System.out.println("Encoded date: " + sdb.encodeDate(new Date()));        
        
        System.out.println("Encoded 7: " + sdb.encodeInteger(7, 2));
        System.out.println("Encoded 25: " + sdb.encodeInteger(25, 2));
        System.out.println("Encoded -3: " + sdb.encodeInteger(-3, 2));
        System.out.println("Encoded -100: " + sdb.encodeInteger(-100, 2));
        
        System.out.println("Decoded !i07: " + sdb.decodeInteger("!i07"));
        System.out.println("Decoded !i25: " + sdb.decodeInteger("!i25"));
        System.out.println("Decoded !I97: " + sdb.decodeInteger("!I97"));
        System.out.println("Decoded !I00: " + sdb.decodeInteger("!I00"));

        System.out.println("Encoded 0.0: " + sdb.encodeFloat(0.0));
        System.out.println("Encoded 12345678901234567890: " 
            + sdb.encodeFloat(12345678901234567890d));
        System.out.println("Encoded 0.12345678901234567890: " 
            + sdb.encodeFloat(0.12345678901234567890));
        System.out.println("Encoded -12345678901234567890: " 
            + sdb.encodeFloat(-12345678901234567890d));
        System.out.println("Encoded -0.12345678901234567890: " 
            + sdb.encodeFloat(-0.12345678901234567890));
        System.out.println("Encoded 0.00000000000000000000123: " 
            + sdb.encodeFloat(0.00000000000000000000123));
        System.out.println("Encoded -0.00000000000000000000123: " 
            + sdb.encodeFloat(-0.00000000000000000000123));

        System.out.println("Decoded !f50!000000000000000: " 
            + sdb.decodeFloat("!f50!000000000000000"));
        System.out.println("Decoded !f70!123456789012346: " 
            + sdb.decodeFloat("!f70!123456789012346"));
        System.out.println("Decoded !f50!123456789012346: " 
            + sdb.decodeFloat("!f50!123456789012346"));
        System.out.println("Decoded !F30!876543210987654: " 
            + sdb.decodeFloat("!F30!876543210987654"));
        System.out.println("Decoded !F50!876543210987654: " 
            + sdb.decodeFloat("!F50!876543210987654"));
        System.out.println("Decoded !f30!123000000000000: " 
            + sdb.decodeFloat("!f30!123000000000000"));
        System.out.println("Decoded !F70!877000000000000: " 
            + sdb.decodeFloat("!F70!877000000000000"));

        String queryExpression = "['Code' != 'AAPL']";
        List results = sdb.query("stocks", queryExpression, 250, true);
        System.out.println(results);
        System.out.println(results.size());

        queryExpression = "['Date' > '2007-06-05T00:00:00Z']";
        results = sdb.query("stocks", queryExpression);
        System.out.println(results);
        System.out.println(results.size());

        System.out.println("Prior usage: " + sdb.getPriorBoxUsage() * 3600 + "s");
        System.out.println("Total usage: " + sdb.getTotalBoxUsage() * 3600 + "s");
        */
    }
    
}
