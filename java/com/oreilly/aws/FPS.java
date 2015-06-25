package com.oreilly.aws;

import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SimpleTimeZone;
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
 * The FPS class implements the Query API of the Amazon Flexible Payments
 * Service.
 */
public class FPS extends AWS {

    public static URL ENDPOINT_URI;

    public static URL PIPELINE_URI;

    public static final String API_VERSION = "2007-01-08";

    public static final String SIGNATURE_VERSION = "1";

    public HttpMethod HTTP_METHOD = HttpMethod.POST; // GET

    // We must allow ISO 8601 dates in responses from FPS to have a timezone.
    protected static final SimpleDateFormat pseudoIso8601DateFormat =
        new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

    static {
        try {
            ENDPOINT_URI = new URL("https://fps.sandbox.amazonaws.com/");
            PIPELINE_URI = new URL(
                "https://authorize.payments-sandbox.amazon.com"
                    + "/cobranded-ui/actions/start");
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
        pseudoIso8601DateFormat.setTimeZone(new SimpleTimeZone(0, "GMT"));
    }

    /**
     * Initialize the service and set the service-specific variables:
     * awsAccessKey, awsSecretKey, isDebugMode, and isSecureHttp.
     *
     * This constructor obtains your AWS access and secret key credentials from
     * the AWS_ACCESS_KEY and AWS_SECRET_KEY environment variables respectively.
     * It sets isDebugMode to false, and isSecureHttp to true.
     */
    public FPS() {
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
    public FPS(boolean isDebugMode, boolean isSecureHttp) {
        super(isDebugMode, isSecureHttp);
    }

    /**
     * Initialize the service and set the service-specific variables:
     * awsAccessKey, awsSecretKey, isDebugMode, and isSecureHttp.
     */
    public FPS(String awsAccessKey, String awsSecretKey, boolean isDebugMode,
        boolean isSecureHttp) {
        super(awsAccessKey, awsSecretKey, isDebugMode, isSecureHttp);
    }

    /**
     * An exception object that captures information about an FPS service error.
     */
    class FpsServiceException extends Exception {
        private static final long serialVersionUID = 5879963413714582655L;

        private String errorMessage = null;
        private Document awsErrorXml = null;
        private String code = null;
        private String reason = null;
        private String type = null;
        private boolean isRetriable = false;

        public FpsServiceException(Document xmlDoc) {
            super();
            readErrorDetails(xmlDoc);
        }

        private void readErrorDetails(Document xmlDoc) {
            try {
                Node errorNode = xpathToNodeList("//Errors/Errors", xmlDoc)
                    .get(0);
                this.code = xpathToContent("ErrorCode", errorNode);
                this.reason = xpathToContent("ReasonText", errorNode);
                this.type = xpathToContent("ErrorType", errorNode);
                this.isRetriable = "true".equals(xpathToContent("IsRetriable",
                    errorNode));

                awsErrorXml = xmlDoc;
                errorMessage = "FPS Service Error: " + code + " - " + reason;
            } catch (Exception e) {
                // Nothing we can do here, print the stack trace and move on...
                e.printStackTrace();
            }
        }

        public Document getAwsErrorXml() {
            return awsErrorXml;
        }

        public String getMessage() {
            if (errorMessage != null) {
                return errorMessage;
            } else {
                return super.getMessage();
            }
        }

        public String getCode() {
            return code;
        }

        public boolean isRetriable() {
            return isRetriable;
        }

        public String getReason() {
            return reason;
        }

        public String getType() {
            return type;
        }
    }

    /**
     * Uses the doQuery method defined in AWS to sends a GET or POST request
     * message to the FPS service's Query API interface and returns the response
     * result from the service.
     *
     * This method performs additional checking of the response from the FPS
     * service to check for error results, which may not be detected by the more
     * generic doQuery method in AWS.
     */
    protected Document doFpsQuery(Map<String, String> parameters)
        throws Exception
    {
        HttpURLConnection conn = doQuery(HTTP_METHOD, ENDPOINT_URI, parameters);
        Document xmlDoc = parseToDocument(conn.getInputStream());

        if (!"Success".equals(xpathToContent("*/Status", xmlDoc))) {
            throw new FpsServiceException(xmlDoc);
        }

        return xmlDoc;
    }

    /*
     * Methods to parse XML document elements into the class structures used by
     * this Java implementation.
     */
    protected Amount parseAmount(Node amtNode) throws Exception {
        if (amtNode == null) {
            return null;
        }
        Amount amount = new Amount();
        amount.amount = Double.valueOf(xpathToContent("Amount", amtNode));
        amount.currencyCode = xpathToContent("CurrencyCode", amtNode);
        return amount;
    }

    protected Transaction parseTransactionResponse(Node transNode)
        throws Exception
    {
        Transaction transaction = new Transaction();
        transaction.id = xpathToContent("TransactionId", transNode);
        transaction.status = TransactionStatus.valueOf(xpathToContent("Status",
            transNode));
        transaction.statusDetail = xpathToContent("StatusDetail", transNode);

        for (Node usageNode : xpathToNodeList("NewSenderTokenUsage", transNode)) {
            transaction.tokenUsage.add(parseTokenUsageLimit(usageNode));
        }

        return transaction;
    }

    protected TokenUsage parseTokenUsageLimit(Node usageNode) throws Exception {
        TokenUsage usage = null;
        if (xpathToNodeList("Amount", usageNode).size() > 0) {
            AmountTokenUsage amountUsage = new AmountTokenUsage();
            amountUsage.amount = parseAmount(xpathToNode("Amount", usageNode));
            amountUsage.lastResetAmount = parseAmount(xpathToNode(
                "LastResetAmount", usageNode));
            usage = amountUsage;
        } else {
            CountTokenUsage countUsage = new CountTokenUsage();
            countUsage.count = Integer.parseInt(xpathToContent("Count",
                usageNode));
            countUsage.lastResetCount = Integer.parseInt(xpathToContent(
                "LastResetCount", usageNode));
            usage = countUsage;
        }
        String lastResetTimestampStr = xpathToContent("LastResetTimeStamp",
            usageNode);
        usage.lastResetTimestamp = parseIso8601Date(lastResetTimestampStr);
        return usage;
    }

    protected Transaction parseTransaction(Node transNode) throws Exception {
        Transaction transaction = new Transaction();
        transaction.id = xpathToContent("TransactionId", transNode);
        transaction.callerTransactionDate = parseIso8601Date(xpathToContent(
            "CallerTransactionDate", transNode));
        transaction.dateReceived = parseIso8601Date(xpathToContent(
            "DateReceived", transNode));
        transaction.transactionAmount = parseAmount(xpathToNode(
            "TransactionAmount", transNode));
        transaction.feesAmount = parseAmount(xpathToNode("Fees", transNode));
        transaction.operation = xpathToContent("Operation", transNode);
        transaction.paymentMethod = PaymentMethod.valueOf(xpathToContent(
            "PaymentMethod", transNode));
        transaction.status = TransactionStatus.valueOf(xpathToContent("Status",
            transNode));
        transaction.statusDetail = xpathToContent("StatusDetail", transNode);
        transaction.callerName = xpathToContent("CallerName", transNode);
        transaction.senderName = xpathToContent("SenderName", transNode);
        transaction.recipientName = xpathToContent("RecipientName", transNode);
        transaction.callerTokenId = xpathToContent("CallerTokenId", transNode);
        transaction.senderTokenId = xpathToContent("SenderTokenId", transNode);
        transaction.recipientTokenId = xpathToContent("RecipientTokenId",
            transNode);
        transaction.errorCode = xpathToContent("ErrorCode", transNode);
        transaction.errorMessage = xpathToContent("ErrorMessage", transNode);
        transaction.metadata = xpathToContent("Metadata", transNode);
        transaction.originalTransactionId = xpathToContent(
            "OriginalTransactionId", transNode);
        transaction.dateCompleted = parseIso8601Date(xpathToContent(
            "DateCompleted", transNode));
        if (xpathToNodeList("Balance", transNode).size() > 0) {
            transaction.balance = parseAmount(xpathToNode("Balance", transNode));
        }

        for (Node partNode : xpathToNodeList("TransactionParts", transNode)) {
            TransactionPart part = new TransactionPart();
            part.accountId = xpathToContent("AccountId", partNode);
            part.role = TransactionRole
                .valueOf(xpathToContent("Role", partNode));
            part.name = xpathToContent("Name", partNode);
            part.instrumentId = xpathToContent("InstrumentId", partNode);
            part.description = xpathToContent("Description", partNode);
            part.reference = xpathToContent("Reference", partNode);
            if (xpathToNodeList("FeePaid", partNode).size() > 0) {
                part.feePaid = parseAmount(xpathToNode("FeePaid", partNode));
            }
            transaction.transactionParts.add(part);
        }

        for (Node relatedNode : xpathToNodeList("RelatedTransactions",
            transNode)) {
            transaction.relatedTransactionIds.add(xpathToContent(
                "TransactionId", relatedNode));
        }

        for (Node statusNode : xpathToNodeList("StatusHistory", transNode)) {
            StatusChange status = new StatusChange();
            status.status = TransactionStatus.valueOf(xpathToContent("Status",
                statusNode));
            status.date = parseIso8601Date(xpathToContent("Date", statusNode));
            if (xpathToNodeList("Amount", statusNode).size() > 0) {
                status.amount = parseAmount(xpathToNode("Amount", statusNode));
            }
            transaction.statusHistory.add(status);
        }

        for (Node usageNode : xpathToNodeList("NewSenderTokenUsage", transNode)) {
            transaction.tokenUsage.add(parseTokenUsageLimit(usageNode));
        }

        return transaction;
    }

    protected Token parseToken(Node tokenNode) throws Exception {
        Token token = new Token();
        token.id = xpathToContent("TokenId", tokenNode);
        token.oldId = xpathToContent("OldTokenId", tokenNode);
        token.status = TokenStatus.valueOf(xpathToContent("Status", tokenNode));
        token.callerInstalled = xpathToContent("CallerInstalled", tokenNode);
        token.dateInstalled = parseIso8601Date(xpathToContent("DateInstalled",
            tokenNode));
        token.callerRef = xpathToContent("CallerReference", tokenNode);
        token.type = TokenType.valueOf(xpathToContent("TokenType", tokenNode));
        token.friendlyName = xpathToContent("FriendlyName", tokenNode);
        token.reason = xpathToContent("PaymentReason", tokenNode);
        return token;
    }

    /*
     * Methods for generating Co-Branded UI Pipeline request URIs.
     */

    /**
     * Generates generic Co-Branded UI Pipeline request URIs.
     */
    protected URL generatePipelineURL(Pipeline pipeline, URL returnURL,
        Map<String, String> parameters) throws Exception
    {
        // Set mandatory parameters
        Map<String, String> myParams = new TreeMap<String, String>();
        myParams.put("callerKey", awsAccessKey);
        myParams.put("pipelineName", pipeline.toString());
        myParams.put("returnURL", returnURL.toString());

        // Add any extra parameters, ignoring those with a null value
        for (Map.Entry<String, String> param : parameters.entrySet()) {
            if (param.getValue() != null) {
                myParams.put(param.getKey(), param.getValue());
            }
        }

        // Build CBUI pipeline URI with sorted parameters. Our
        // parameters are already sorted in the TreeMap.
        StringBuffer queryString = new StringBuffer();
        for (Map.Entry<String, String> param : myParams.entrySet()) {
            queryString.append(queryString.length() == 0 ? "" : "&");
            queryString.append(param.getKey() + "="
                + URLEncoder.encode(param.getValue(), "UTF-8"));
        }

        // Sign Pipeline URI
        String requestDesc = PIPELINE_URI.getFile() + "?" + queryString;
        String signature = generateSignature(requestDesc);
        queryString.append("&awsSignature="
            + URLEncoder.encode(signature, "UTF-8"));

        return new URL(PIPELINE_URI.toString() + "?" + queryString);
    }

    public URL getUrlForSingleUseSender(String callerRef, Double amount,
        URL returnURL) throws Exception
    {
        return getUrlForSingleUseSender(callerRef, amount, returnURL, null,
            null, null, false);
    }

    public URL getUrlForSingleUseSender(String callerRef, Double amount,
        URL returnURL, PaymentMethod method, String reason,
        String recipientToken, boolean canReserve) throws Exception
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("callerReference", callerRef);
        parameters.put("transactionAmount", amount.toString());

        // Optional parameters
        parameters.put("paymentReason", reason);
        if (method != null) {
            parameters.put("paymentMethod", method.toString());
        }
        parameters.put("recipientToken", recipientToken);
        parameters.put("reserve", canReserve ? "True" : "False");

        return generatePipelineURL(Pipeline.SingleUse, returnURL, parameters);
    }

    public URL getUrlForMultiUseSender(String callerRef,
        Double globalAmountLimit, URL returnURL) throws Exception
    {
        return getUrlForMultiUseSender(callerRef, globalAmountLimit, returnURL,
            null, null, null, null, null, null, null, null);
    }

    public URL getUrlForMultiUseSender(String callerRef,
        Double globalAmountLimit, URL returnURL, PaymentMethod method,
        String reason, String recipientTokens, AmountLimitType amountLimitType,
        Double amountLimitValue, Date validityStart, Date validityExpiry,
        List<TokenUsageLimit> usageLimits) throws Exception
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("callerReference", callerRef);
        parameters.put("globalAmountLimit", globalAmountLimit.toString());

        // Optional parameters
        parameters.put("paymentReason", reason);
        if (method != null) {
            parameters.put("paymentMethod", method.toString());
        }
        parameters.put("recipientTokenList", recipientTokens);
        if (amountLimitType != null) {
            parameters.put("amountType", amountLimitType.toString());
        }
        if (amountLimitValue != null) {
            parameters.put("transactionAmount", amountLimitValue.toString());
        }
        if (validityStart != null) {
            parameters.put("validityStart", String.valueOf((validityStart
                .getTime() / 1000)));
        }
        if (validityExpiry != null) {
            parameters.put("validityExpiry", String.valueOf((validityExpiry
                .getTime() / 1000)));
        }
        if (usageLimits != null) {
            int limitSuffix = 1;
            for (TokenUsageLimit usageLimit : usageLimits) {
                if (usageLimit instanceof TokenUsageLimitCount) {
                    parameters.put("usageLimitType" + limitSuffix, "Count");
                    parameters.put("usageLimitValue" + limitSuffix,
                        ((TokenUsageLimitCount) usageLimit).count.toString());
                } else {
                    parameters.put("usageLimitType" + limitSuffix, "Amount");
                    parameters.put("usageLimitValue" + limitSuffix,
                        ((TokenUsageLimitAmount) usageLimit).amount.toString());
                }
                if (usageLimit.period != null) {
                    parameters.put("usageLimitPeriod" + limitSuffix,
                        usageLimit.period);
                }
                limitSuffix += 1;
            }
        }

        return generatePipelineURL(Pipeline.MultiUse, returnURL, parameters);
    }

    public URL getUrlForRecurringSender(String callerRef, Double amount,
        String recurringPeriod, URL returnURL) throws Exception
    {
        return getUrlForRecurringSender(callerRef, amount, recurringPeriod,
            returnURL, null, null, null, null, null);
    }

    public URL getUrlForRecurringSender(String callerRef, Double amount,
        String recurringPeriod, URL returnURL, PaymentMethod method,
        String reason, String recipientToken, Date validityStart,
        Date validityExpiry) throws Exception
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("callerReference", callerRef);
        parameters.put("transactionAmount", amount.toString());
        parameters.put("recurringPeriod", recurringPeriod);

        // Optional parameters
        parameters.put("paymentReason", reason);
        if (method != null) {
            parameters.put("paymentMethod", method.toString());
        }
        parameters.put("recipientToken", recipientToken);
        if (validityStart != null) {
            parameters.put("validityStart", String.valueOf((validityStart
                .getTime() / 1000)));
        }
        if (validityExpiry != null) {
            parameters.put("validityExpiry", String.valueOf((validityExpiry
                .getTime() / 1000)));
        }

        return generatePipelineURL(Pipeline.Recurring, returnURL, parameters);
    }

    public URL getUrlForRecipient(String callerRef,
        String callerRefRefund, boolean recipientPaysFees,
        URL returnURL) throws Exception
    {
        return getUrlForRecipient(callerRef, callerRefRefund,
            recipientPaysFees, returnURL, null, null, null);
    }

    public URL getUrlForRecipient(String callerRef,
        String callerRefRefund, boolean recipientPaysFees, URL returnURL,
        PaymentMethod method, Date validityStart, Date validityExpiry)
        throws Exception
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("callerReference", callerRef);
        parameters.put("callerReferenceRefund", callerRefRefund);
        parameters
            .put("recipientPaysFee", recipientPaysFees ? "True" : "False");

        // Optional parameters
        if (method != null) {
            parameters.put("paymentMethod", method.toString());
        }
        if (validityStart != null) {
            parameters.put("validityStart", String.valueOf((validityStart
                .getTime() / 1000)));
        }
        if (validityExpiry != null) {
            parameters.put("validityExpiry", String.valueOf((validityExpiry
                .getTime() / 1000)));
        }

        return generatePipelineURL(Pipeline.Recipient, returnURL, parameters);
    }

    public URL getUrlForPrepaidInstrument(String callerRefSender,
        String callerRefFunding, Double amount, URL returnURL) throws Exception
    {
        return getUrlForPrepaidInstrument(callerRefSender, callerRefFunding,
            amount, returnURL, null, null, null, null);
    }

    public URL getUrlForPrepaidInstrument(String callerRefSender,
        String callerRefFunding, Double amount, URL returnURL,
        PaymentMethod method, String reason, Date validityStart,
        Date validityExpiry) throws Exception
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("callerReferenceSender", callerRefSender);
        parameters.put("callerReferenceFunding", callerRefFunding);
        parameters.put("fundingAmount", amount.toString());

        // Optional parameters
        if (method != null) {
            parameters.put("paymentMethod", method.toString());
        }
        parameters.put("paymentReason", reason);
        if (validityStart != null) {
            parameters.put("validityStart", String.valueOf((validityStart
                .getTime() / 1000)));
        }
        if (validityExpiry != null) {
            parameters.put("validityExpiry", String.valueOf((validityExpiry
                .getTime() / 1000)));
        }

        return generatePipelineURL(Pipeline.SetupPrepaid, returnURL, parameters);
    }

    public URL getUrlForPostpaidInstrument(String callerRefSender,
        String callerRefSettlement, Double creditLimitAmount,
        Double globalLimitAmount, URL returnURL) throws Exception
    {
        return getUrlForPostpaidInstrument(callerRefSender,
            callerRefSettlement, creditLimitAmount, globalLimitAmount,
            returnURL, null, null, null, null, null);
    }

    public URL getUrlForPostpaidInstrument(String callerRefSender,
        String callerRefSettlement, Double creditLimitAmount,
        Double globalLimitAmount, URL returnURL, PaymentMethod method,
        String reason, Date validityStart, Date validityExpiry,
        List<TokenUsageLimit> usageLimits) throws Exception
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("callerReferenceSender", callerRefSender);
        parameters.put("callerReferenceSettlement", callerRefSettlement);
        parameters.put("creditLimit", creditLimitAmount.toString());
        parameters.put("globalAmountLimit", globalLimitAmount.toString());

        // Optional parameters
        if (method != null) {
            parameters.put("paymentMethod", method.toString());
        }
        parameters.put("paymentReason", reason);
        if (validityStart != null) {
            parameters.put("validityStart", String.valueOf((validityStart
                .getTime() / 1000)));
        }
        if (validityExpiry != null) {
            parameters.put("validityExpiry", String.valueOf((validityExpiry
                .getTime() / 1000)));
        }
        if (usageLimits != null) {
            int limitSuffix = 1;
            for (TokenUsageLimit usageLimit : usageLimits) {
                if (usageLimit instanceof TokenUsageLimitCount) {
                    parameters.put("usageLimitType" + limitSuffix, "Count");
                    parameters.put("usageLimitValue" + limitSuffix,
                        ((TokenUsageLimitCount) usageLimit).count.toString());
                } else {
                    parameters.put("usageLimitType" + limitSuffix, "Amount");
                    parameters.put("usageLimitValue" + limitSuffix,
                        ((TokenUsageLimitAmount) usageLimit).amount.toString());
                }
                if (usageLimit.period != null) {
                    parameters.put("usageLimitPeriod" + limitSuffix,
                        usageLimit.period);
                }
                limitSuffix += 1;
            }
        }

        return generatePipelineURL(Pipeline.SetupPostpaid, returnURL,
            parameters);
    }

    public URL getUrlForEditingToken(String callerRef, String tokenId,
        URL returnURL) throws Exception
    {
        return getUrlForEditingToken(callerRef, tokenId, returnURL, null);
    }

    public URL getUrlForEditingToken(String callerRef, String tokenId,
        URL returnURL, PaymentMethod method) throws Exception
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("callerReference", callerRef);
        parameters.put("tokenID", tokenId);

        // Optional parameters
        if (method != null) {
            parameters.put("paymentMethod", method.toString());
        }

        return generatePipelineURL(Pipeline.EditToken, returnURL, parameters);
    }

    /*
     * Methods for handling result URIs from the Co-Branded UI Pipeline
     */

    public Map<String, String> parseURLParameters(URL url) throws Exception {
        Map<String, String> params = new HashMap<String, String>();
        if (url.getQuery() == null) {
            return params;
        }

        for (String paramNameAndValue : url.getQuery().split("&")) {
            int equalsOffset = paramNameAndValue.indexOf('=');

            // Everything before the first '=' is the parameter's name,
            String paramName = paramNameAndValue.substring(0, equalsOffset);

            // Everything after the first '=' is the parameter's value.
            String paramValue = paramNameAndValue.substring(equalsOffset + 1);

            // Unescape parameter values, except for 'awsSignature'.
            if ("awsSignature".equals(paramName)) {
                params.put(paramName, paramValue);
            } else {
                params.put(paramName, URLDecoder.decode(paramValue, "UTF-8"));
            }
        }
        return params;
    }

    public boolean verifyPipelineResultURL(URL url) throws Exception {
        Map<String, String> params = parseURLParameters(url);

        // Find the AWS signature and remove it from the parameters
        String signatureReceived = params.get("awsSignature");
        params.remove("awsSignature");

        // Sort the remaining parameters alphabetically, ignoring case.
        Map<String, String> sortedParameters = new TreeMap<String, String>(
            new Comparator<String>() {
                public int compare(String o1, String o2) {
                    return o1.toLowerCase().compareTo(o2.toLowerCase());
                }
            });
        sortedParameters.putAll(params);

        // Build our own request description string from the result URI
        StringBuffer requestDescription = new StringBuffer();
        requestDescription.append(url.getPath() + "?");
        for (Map.Entry<String, String> param : sortedParameters.entrySet()) {
            if (!requestDescription.toString().endsWith("?")) {
                requestDescription.append("&");
            }
            requestDescription.append(param.getKey() + "="
                + URLEncoder.encode(param.getValue(), "UTF-8"));
        }

        // Calculate the signature we expect
        String signatureExpected = generateSignature(requestDescription
            .toString());

        // Check whether the result URI's signature matches the expected one
        return signatureExpected.equals(signatureReceived);
    }

    /*
     * Methods that implement the FPS service's Query API interface operations.
     */

    public AccountActivity getAccountActivity(Date startDate) throws Exception {
        return getAccountActivity(startDate, null, null, null, null, null,
            null, null, null);
    }

    public AccountActivity getAccountActivity(Date startDate, Date endDate,
        Integer maxBatchSize, SortOrderByDate sortOrder, ResponseGroup detail,
        TransactionOperation operation, PaymentMethod method,
        TransactionRole role, TransactionStatus status) throws Exception
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "GetAccountActivity");
        parameters.put("StartDate", iso8601DateFormat.format(startDate));

        // Optional parameters
        if (endDate != null) {
            parameters.put("EndDate", iso8601DateFormat.format(endDate));
        }
        if (maxBatchSize != null) {
            parameters.put("MaxBatchSize", maxBatchSize.toString());
        }
        if (sortOrder != null) {
            parameters.put("SortOrderByDate", sortOrder.toString());
        }
        if (detail != null) {
            parameters.put("ResponseGroup", detail.toString());
        }
        if (operation != null) {
            parameters.put("Operation", operation.toString());
        }
        if (method != null) {
            parameters.put("PaymentMethod", method.toString());
        }
        if (role != null) {
            parameters.put("Role", role.toString());
        }
        if (status != null) {
            parameters.put("Status", status.toString());
        }

        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION,
            parameters, EMPTY_INDEXED_MAP);
        Document xmlDoc = doFpsQuery(parameters);

        AccountActivity activity = new AccountActivity();
        activity.responseBatchSize = Integer.parseInt(xpathToContent(
            "//ResponseBatchSize", xmlDoc));
        for (Node transNode : xpathToNodeList("//Transactions", xmlDoc)) {
            activity.transactions.add(parseTransaction(transNode));
        }
        activity.startTimeForNextTransaction = parseIso8601Date(xpathToContent(
            "//StartTimeForNextTransaction", xmlDoc));
        return activity;
    }

    public AccountBalance getAccountBalance() throws Exception {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "GetAccountBalance");

        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION,
            parameters, EMPTY_INDEXED_MAP);
        Document xmlDoc = doFpsQuery(parameters);

        AccountBalance accountBalance = new AccountBalance();
        accountBalance.totalBalance = parseAmount(xpathToNode("//TotalBalance",
            xmlDoc));
        accountBalance.pendingInBalance = parseAmount(xpathToNode(
            "//PendingInBalance", xmlDoc));
        accountBalance.pendingOutBalance = parseAmount(xpathToNode(
            "//PendingOutBalance", xmlDoc));
        accountBalance.disburseBalance = parseAmount(xpathToNode(
            "//DisburseBalance", xmlDoc));
        accountBalance.refundBalance = parseAmount(xpathToNode(
            "//RefundBalance", xmlDoc));

        return accountBalance;
    }

    public String installPaymentInstruction(String paymentInstructions,
        String callerRef, TokenType type) throws Exception
    {
        return installPaymentInstruction(paymentInstructions, callerRef, type);
    }

    public String installPaymentInstruction(String paymentInstructions,
        String callerRef, TokenType type, String reason, String friendlyName)
        throws Exception
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "InstallPaymentInstruction");
        parameters.put("PaymentInstruction", paymentInstructions);
        parameters.put("CallerReference", callerRef);
        parameters.put("TokenType", type.toString());

        // Optional parameters
        parameters.put("TokenFriendlyName", friendlyName);
        parameters.put("PaymentReason", reason);

        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION,
            parameters, EMPTY_INDEXED_MAP);
        Document xmlDoc = doFpsQuery(parameters);
        return xpathToContent("//TokenId", xmlDoc);
    }

    public PaymentInstruction getPaymentInstruction(String tokenId)
        throws Exception
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "GetPaymentInstruction");
        parameters.put("TokenId", tokenId);

        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION,
            parameters, EMPTY_INDEXED_MAP);
        Document xmlDoc = doFpsQuery(parameters);

        PaymentInstruction paymentInstruction = new PaymentInstruction();
        paymentInstruction.token = parseToken(xpathToNode("//Token", xmlDoc));
        paymentInstruction.accountId = xpathToContent("//AccountId", xmlDoc);
        paymentInstruction.instructions = xpathToContent(
            "//PaymentInstruction", xmlDoc);

        return paymentInstruction;
    }

    public List<Token> getTokens() throws Exception {
        return getTokens(null, null, null);
    }

    public List<Token> getTokens(String friendlyName, TokenStatus status,
        String callerRef) throws Exception
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "GetTokens");

        // Optional parameters
        parameters.put("TokenFriendlyName", friendlyName);
        if (status != null) {
            parameters.put("TokenStatus", status.toString());
        }
        parameters.put("CallerReference", callerRef);

        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION,
            parameters, EMPTY_INDEXED_MAP);
        Document xmlDoc = doFpsQuery(parameters);

        List<Token> tokens = new ArrayList<Token>();
        for (Node tokenNode : xpathToNodeList("//Tokens", xmlDoc)) {
            tokens.add(parseToken(tokenNode));
        }
        return tokens;
    }

    public Token getTokenByCaller(String tokenId, String callerRef)
        throws Exception
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "GetTokenByCaller");
        parameters.put("TokenId", tokenId);
        parameters.put("CallerReference", callerRef);

        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION,
            parameters, EMPTY_INDEXED_MAP);
        Document xmlDoc = doFpsQuery(parameters);

        return parseToken(xpathToNode("//Token", xmlDoc));
    }

    public List<TokenUsage> getTokenUsage(String tokenId) throws Exception {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "GetTokenUsage");
        parameters.put("TokenId", tokenId);

        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION,
            parameters, EMPTY_INDEXED_MAP);
        Document xmlDoc = doFpsQuery(parameters);

        List<TokenUsage> limits = new ArrayList<TokenUsage>();
        for (Node usageNode : xpathToNodeList("//TokenUsageLimits", xmlDoc)) {
            limits.add(parseTokenUsageLimit(usageNode));
        }
        return limits;
    }

    public boolean cancelToken(String tokenId) throws Exception {
        return cancelToken(tokenId, null);
    }

    public boolean cancelToken(String tokenId, String reason) throws Exception {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "CancelToken");
        parameters.put("TokenId", tokenId);

        // Optional parameters
        parameters.put("ReasonText", reason);

        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION,
            parameters, EMPTY_INDEXED_MAP);
        doFpsQuery(parameters);
        return true;
    }

    public Transaction pay(String recipientTokenId, String senderTokenId,
        String callerTokenId, String callerRef, Amount amount,
        ChargeToRole chargeTo) throws Exception
    {
        return pay(recipientTokenId, senderTokenId, callerTokenId, callerRef,
            amount, chargeTo, null, null, null, null, null, null, null);
    }

    public Transaction pay(String recipientTokenId, String senderTokenId,
        String callerTokenId, String callerRef, Amount amount,
        ChargeToRole chargeTo, Date callerDate, String senderRef,
        String recipientRef, String callerDescription,
        String senderDescription, String recipientDescription, String metadata)
        throws Exception
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "Pay");
        parameters.put("RecipientTokenId", recipientTokenId);
        parameters.put("SenderTokenId", senderTokenId);
        parameters.put("CallerTokenId", callerTokenId);
        parameters.put("CallerReference", callerRef);
        parameters.put("TransactionAmount.Amount", amount.amount.toString());
        parameters.put("TransactionAmount.CurrencyCode", amount.currencyCode);
        parameters.put("ChargeFeeTo", chargeTo.toString());

        // Optional parameters
        if (callerDate != null) {
            parameters.put("TransactionDate", iso8601DateFormat
                .format(callerDate));
        }
        parameters.put("SenderReference", senderRef);
        parameters.put("RecipientReference", recipientRef);
        parameters.put("SenderDescription", senderDescription);
        parameters.put("RecipientDescription", recipientDescription);
        parameters.put("CallerDescription", callerDescription);
        parameters.put("MetaData", metadata);

        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION,
            parameters, EMPTY_INDEXED_MAP);
        Document xmlDoc = doFpsQuery(parameters);

        return parseTransactionResponse(xpathToNode(
            "//*[local-name()='TransactionResponse']", xmlDoc));
    }

    public Transaction refund(String refundTokenId, String callerTokenId,
        String transactionId, String callerRef, ChargeToRole chargeTo)
        throws Exception
    {
        return refund(refundTokenId, callerTokenId, transactionId, callerRef,
            chargeTo, null, null, null, null, null, null, null, null);
    }

    public Transaction refund(String refundTokenId, String callerTokenId,
        String transactionId, String callerRef, ChargeToRole chargeTo,
        Amount refundAmount, Date callerDate, String refundSenderRef,
        String refundRecipientRef, String callerDescription,
        String refundSenderDescription, String refundRecipientDescription,
        String metadata) throws Exception
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "Refund");
        parameters.put("CallerTokenId", callerTokenId);
        parameters.put("RefundSenderTokenId", refundTokenId);
        parameters.put("TransactionId", transactionId);
        parameters.put("CallerReference", callerRef);
        parameters.put("ChargeFeeTo", chargeTo.toString());

        // Optional parameters
        if (refundAmount != null) {
            parameters.put("RefundAmount.Amount", refundAmount.amount
                .toString());
            parameters.put("RefundAmount.CurrencyCode",
                refundAmount.currencyCode);
        }
        if (callerDate != null) {
            parameters.put("TransactionDate", iso8601DateFormat
                .format(callerDate));
        }
        parameters.put("RefundSenderReference", refundSenderRef);
        parameters.put("RefundRecipientReference", refundRecipientRef);
        parameters.put("RefundSenderDescription", refundSenderDescription);
        parameters
            .put("RefundRecipientDescription", refundRecipientDescription);
        parameters.put("CallerDescription", callerDescription);
        parameters.put("MetaData", metadata);

        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION,
            parameters, EMPTY_INDEXED_MAP);
        Document xmlDoc = doFpsQuery(parameters);

        return parseTransactionResponse(xpathToNode(
            "//*[local-name()='TransactionResponse']", xmlDoc));
    }

    public Transaction reserve(String recipientTokenId, String senderTokenId,
        String callerTokenId, String callerRef, Amount amount,
        ChargeToRole chargeTo) throws Exception
    {
        return reserve(recipientTokenId, senderTokenId, callerTokenId,
            callerRef, amount, chargeTo, null, null, null, null, null, null,
            null);
    }

    public Transaction reserve(String recipientTokenId, String senderTokenId,
        String callerTokenId, String callerRef, Amount amount,
        ChargeToRole chargeTo, Date callerDate, String senderRef,
        String recipientRef, String callerDescription,
        String senderDescription, String recipientDescription, String metadata)
        throws Exception
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "Reserve");
        parameters.put("CallerTokenId", callerTokenId);
        parameters.put("RecipientTokenId", recipientTokenId);
        parameters.put("SenderTokenId", senderTokenId);
        parameters.put("TransactionAmount.Amount", amount.amount.toString());
        parameters.put("TransactionAmount.CurrencyCode", amount.currencyCode);
        parameters.put("ChargeFeeTo", chargeTo.toString());
        parameters.put("CallerReference", callerRef);

        // Optional parameters
        if (callerDate != null) {
            parameters.put("TransactionDate", iso8601DateFormat
                .format(callerDate));
        }
        parameters.put("SenderReference", senderRef);
        parameters.put("RecipientReference", recipientRef);
        parameters.put("SenderDescription", senderDescription);
        parameters.put("RecipientDescription", recipientDescription);
        parameters.put("CallerDescription", callerDescription);
        parameters.put("MetaData", metadata);

        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION,
            parameters, EMPTY_INDEXED_MAP);
        Document xmlDoc = doFpsQuery(parameters);

        return parseTransactionResponse(xpathToNode(
            "//*[local-name()='TransactionResponse']", xmlDoc));
    }

    public Transaction settle(String transactionId) throws Exception {
        return settle(transactionId, null, null);
    }

    public Transaction settle(String transactionId, Amount amount,
        Date callerDate) throws Exception
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "Settle");
        parameters.put("ReserveTransactionId", transactionId);

        // Optional parameters
        if (amount != null) {
            parameters
                .put("TransactionAmount.Amount", amount.amount.toString());
            parameters.put("TransactionAmount.CurrencyCode",
                amount.currencyCode);
        }
        if (callerDate != null) {
            parameters.put("TransactionDate", iso8601DateFormat
                .format(callerDate));
        }

        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION,
            parameters, EMPTY_INDEXED_MAP);
        Document xmlDoc = doFpsQuery(parameters);

        return parseTransactionResponse(xpathToNode(
            "//*[local-name()='TransactionResponse']", xmlDoc));
    }

    public Transaction getTransaction(String transactionId) throws Exception {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "GetTransaction");
        parameters.put("TransactionId", transactionId);

        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION,
            parameters, EMPTY_INDEXED_MAP);
        Document xmlDoc = doFpsQuery(parameters);

        return parseTransaction(xpathToNode("//*[local-name()='Transaction']",
            xmlDoc));
    }

    public Transaction retryTransaction(String transactionId) throws Exception {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "RetryTransaction");
        parameters.put("OriginalTransactionId", transactionId);

        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION,
            parameters, EMPTY_INDEXED_MAP);
        Document xmlDoc = doFpsQuery(parameters);

        return parseTransactionResponse(xpathToNode(
            "//*[local-name()='TransactionResponse']", xmlDoc));
    }

    public TransactionResults getResults() throws Exception {
        return getResults(null, null);
    }

    public TransactionResults getResults(Integer maxResultsCount,
        TransactionOperation operation) throws Exception
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "GetResults");

        // Optional parameters
        if (maxResultsCount != null) {
            parameters.put("MaxResultsCount", maxResultsCount.toString());
        }
        if (operation != null) {
            parameters.put("Operation", operation.toString());
        }

        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION,
            parameters, EMPTY_INDEXED_MAP);
        Document xmlDoc = doFpsQuery(parameters);

        TransactionResults results = new TransactionResults();
        results.numberPending = Integer.parseInt(xpathToContent(
            "//NumberPending", xmlDoc));
        for (Node resultNode : xpathToNodeList("//TransactionResults", xmlDoc)) {
            TransactionResult result = new TransactionResult();
            result.id = xpathToContent("TransactionId", resultNode);
            result.operation = TransactionOperation.valueOf(xpathToContent(
                "Operation", resultNode));
            result.status = TransactionStatus.valueOf(xpathToContent("Status",
                resultNode));
            result.callerRef = xpathToContent("CallerReference", resultNode);
            results.results.add(result);
        }
        return results;
    }

    public List<String> discardResults(List<String> transactionIds)
        throws Exception
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "DiscardResults");

        Map<String, List<String>> indexedParams =
            new HashMap<String, List<String>>();
        indexedParams.put("TransactionIds", transactionIds);

        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION,
            parameters, indexedParams);
        Document xmlDoc = doFpsQuery(parameters);

        List<String> discardErrors = new ArrayList<String>();
        for (Node errorNode : xpathToNodeList("//DiscardErrors", xmlDoc)) {
            discardErrors.add(errorNode.getTextContent());
        }
        return discardErrors;
    }

    public Transaction fundPrepaid(String fundingSenderTokenId,
        String instrumentId, String callerTokenId, String callerRef,
        Amount amount, ChargeToRole chargeTo) throws Exception
    {
        return fundPrepaid(fundingSenderTokenId, instrumentId, callerTokenId,
            callerRef, amount, chargeTo, null, null, null, null, null, null,
            null);
    }

    public Transaction fundPrepaid(String fundingSenderTokenId,
        String instrumentId, String callerTokenId, String callerRef,
        Amount amount, ChargeToRole chargeTo, Date callerDate,
        String senderRef, String recipientRef, String callerDescription,
        String senderDescription, String recipientDescription, String metadata)
        throws Exception
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "FundPrepaid");
        parameters.put("SenderTokenId", fundingSenderTokenId);
        parameters.put("PrepaidInstrumentId", instrumentId);
        parameters.put("CallerTokenId", callerTokenId);
        parameters.put("FundingAmount.Amount", amount.amount.toString());
        parameters.put("FundingAmount.CurrencyCode", amount.currencyCode);
        parameters.put("ChargeFeeTo", chargeTo.toString());
        parameters.put("CallerReference", callerRef);

        // Optional parameters
        if (callerDate != null) {
            parameters.put("TransactionDate", iso8601DateFormat
                .format(callerDate));
        }
        parameters.put("SenderReference", senderRef);
        parameters.put("RecipientReference", recipientRef);
        parameters.put("SenderDescription", senderDescription);
        parameters.put("RecipientDescription", recipientDescription);
        parameters.put("CallerDescription", callerDescription);
        parameters.put("MetaData", metadata);

        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION,
            parameters, EMPTY_INDEXED_MAP);
        Document xmlDoc = doFpsQuery(parameters);

        return parseTransactionResponse(xpathToNode(
            "//*[local-name()='TransactionResponse']", xmlDoc));
    }

    public PrepaidBalance getPrepaidBalance(String instrumentId)
        throws Exception
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "GetPrepaidBalance");
        parameters.put("PrepaidInstrumentId", instrumentId);

        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION,
            parameters, EMPTY_INDEXED_MAP);
        Document xmlDoc = doFpsQuery(parameters);

        Node balanceNode = xpathToNode("//PrepaidBalance", xmlDoc);
        PrepaidBalance prepaidBalance = new PrepaidBalance();
        prepaidBalance.availableBalance.amount = Double
            .parseDouble(xpathToContent("AvailableBalance/Amount", balanceNode));
        prepaidBalance.availableBalance.currencyCode = xpathToContent(
            "AvailableBalance/CurrencyCode", balanceNode);
        prepaidBalance.pendingInBalance.amount = Double
            .parseDouble(xpathToContent("PendingInBalance/Amount", balanceNode));
        prepaidBalance.pendingInBalance.currencyCode = xpathToContent(
            "PendingInBalance/CurrencyCode", balanceNode);

        return prepaidBalance;
    }

    public List<String> getAllPrepaidInstruments() throws Exception {
        return getAllPrepaidInstruments(null);
    }

    public List<String> getAllPrepaidInstruments(InstrumentStatus status)
        throws Exception
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "GetAllPrepaidInstruments");

        // Optional parameters
        if (status != null) {
            parameters.put("InstrumentStatus", status.toString());
        }

        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION,
            parameters, EMPTY_INDEXED_MAP);
        Document xmlDoc = doFpsQuery(parameters);

        List<String> instrumentIds = new ArrayList<String>();
        for (Node idNode : xpathToNodeList(
            "//PrepaidInstrumentIds/InstrumentId", xmlDoc)) {
            instrumentIds.add(idNode.getTextContent());
        }
        return instrumentIds;
    }

    public PrepaidLiability getTotalPrepaidLiability() throws Exception {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "GetTotalPrepaidLiability");

        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION,
            parameters, EMPTY_INDEXED_MAP);
        Document xmlDoc = doFpsQuery(parameters);

        Node node = xpathToNode("//OutstandingPrepaidLiability", xmlDoc);
        PrepaidLiability prepaidLiability = new PrepaidLiability();
        prepaidLiability.outstandingBalance.amount = Double
            .parseDouble(xpathToContent("OutstandingBalance/Amount", node));
        prepaidLiability.outstandingBalance.currencyCode = xpathToContent(
            "OutstandingBalance/CurrencyCode", node);
        prepaidLiability.pendingInBalance.amount = Double
            .parseDouble(xpathToContent("PendingInBalance/Amount", node));
        prepaidLiability.pendingInBalance.currencyCode = xpathToContent(
            "PendingInBalance/CurrencyCode", node);

        return prepaidLiability;
    }

    public Transaction settleDebt(String settlementTokenId,
        String instrumentId, String callerTokenId, String callerRef,
        Amount amount) throws Exception
    {
        return settleDebt(settlementTokenId, instrumentId, callerTokenId,
            callerRef, amount, null, null, null, null, null, null, null);
    }

    public Transaction settleDebt(String settlementTokenId,
        String instrumentId, String callerTokenId, String callerRef,
        Amount amount, Date callerDate, String senderRef, String recipientRef,
        String callerDescription, String senderDescription,
        String recipientDescription, String metadata) throws Exception
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "SettleDebt");
        parameters.put("SenderTokenId", settlementTokenId);
        parameters.put("CreditInstrumentId", instrumentId);
        parameters.put("CallerTokenId", callerTokenId);
        parameters.put("SettlementAmount.Amount", amount.amount.toString());
        parameters.put("SettlementAmount.CurrencyCode", amount.currencyCode);
        parameters.put("CallerReference", callerRef);

        // Fee-payer is hard-coded to 'Recipient'
        parameters.put("ChargeFeeTo", ChargeToRole.Recipient.toString());

        // Optional parameters
        if (callerDate != null) {
            parameters.put("TransactionDate", iso8601DateFormat
                .format(callerDate));
        }
        parameters.put("SenderReference", senderRef);
        parameters.put("RecipientReference", recipientRef);
        parameters.put("SenderDescription", senderDescription);
        parameters.put("RecipientDescription", recipientDescription);
        parameters.put("CallerDescription", callerDescription);
        parameters.put("MetaData", metadata);

        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION,
            parameters, EMPTY_INDEXED_MAP);
        Document xmlDoc = doFpsQuery(parameters);

        return parseTransactionResponse(xpathToNode(
            "//*[local-name()='TransactionResponse']", xmlDoc));
    }

    public Transaction writeOffDebt(String instrumentId, String callerTokenId,
        String callerRef, Amount amount) throws Exception
    {
        return writeOffDebt(instrumentId, callerTokenId, callerRef, amount,
            null, null, null, null, null, null, null);
    }

    public Transaction writeOffDebt(String instrumentId, String callerTokenId,
        String callerRef, Amount amount, Date callerDate, String senderRef,
        String recipientRef, String callerDescription,
        String senderDescription, String recipientDescription, String metadata)
        throws Exception
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "WriteOffDebt");
        parameters.put("CreditInstrumentId", instrumentId);
        parameters.put("CallerTokenId", callerTokenId);
        parameters.put("AdjustmentAmount.Amount", amount.amount.toString());
        parameters.put("AdjustmentAmount.CurrencyCode", amount.currencyCode);
        parameters.put("CallerReference", callerRef);

        // Optional parameters
        if (callerDate != null) {
            parameters.put("TransactionDate", iso8601DateFormat
                .format(callerDate));
        }
        parameters.put("SenderReference", senderRef);
        parameters.put("RecipientReference", recipientRef);
        parameters.put("SenderDescription", senderDescription);
        parameters.put("RecipientDescription", recipientDescription);
        parameters.put("CallerDescription", callerDescription);
        parameters.put("MetaData", metadata);

        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION,
            parameters, EMPTY_INDEXED_MAP);
        Document xmlDoc = doFpsQuery(parameters);

        return parseTransactionResponse(xpathToNode(
            "//*[local-name()='TransactionResponse']", xmlDoc));
    }

    public List<String> getAllCreditInstruments() throws Exception {
        return getAllCreditInstruments(null);
    }

    public List<String> getAllCreditInstruments(InstrumentStatus status)
        throws Exception
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "GetAllCreditInstruments");

        // Optional parameters
        if (status != null) {
            parameters.put("InstrumentStatus", status.toString());
        }

        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION,
            parameters, EMPTY_INDEXED_MAP);
        Document xmlDoc = doFpsQuery(parameters);

        List<String> instrumentIds = new ArrayList<String>();
        for (Node idNode : xpathToNodeList(
            "//CreditInstrumentIds/InstrumentId", xmlDoc)) {
            instrumentIds.add(idNode.getTextContent());
        }
        return instrumentIds;
    }

    public DebtBalance getDebtBalance(String instrumentId) throws Exception {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "GetDebtBalance");
        parameters.put("CreditInstrumentId", instrumentId);

        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION,
            parameters, EMPTY_INDEXED_MAP);
        Document xmlDoc = doFpsQuery(parameters);

        Node node = xpathToNode("//DebtBalance", xmlDoc);
        DebtBalance debtBalance = new DebtBalance();
        debtBalance.availableBalance.amount = Double
            .parseDouble(xpathToContent("AvailableBalance/Amount", node));
        debtBalance.availableBalance.currencyCode = xpathToContent(
            "AvailableBalance/CurrencyCode", node);
        debtBalance.pendingOutBalance.amount = Double
            .parseDouble(xpathToContent("PendingOutBalance/Amount", node));
        debtBalance.pendingOutBalance.currencyCode = xpathToContent(
            "PendingOutBalance/CurrencyCode", node);

        return debtBalance;
    }

    public OutstandingDebtBalance getOutstandingDebtBalance() throws Exception {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "GetOutstandingDebtBalance");

        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION,
            parameters, EMPTY_INDEXED_MAP);
        Document xmlDoc = doFpsQuery(parameters);

        Node node = xpathToNode("//OutstandingDebt", xmlDoc);
        OutstandingDebtBalance outstandingBalance = new OutstandingDebtBalance();
        outstandingBalance.outstandingBalance.amount = Double
            .parseDouble(xpathToContent("OutstandingBalance/Amount", node));
        outstandingBalance.outstandingBalance.currencyCode = xpathToContent(
            "OutstandingBalance/CurrencyCode", node);
        outstandingBalance.pendingOutBalance.amount = Double
            .parseDouble(xpathToContent("PendingOutBalance/Amount", node));
        outstandingBalance.pendingOutBalance.currencyCode = xpathToContent(
            "PendingOutBalance/CurrencyCode", node);

        return outstandingBalance;
    }

    public boolean subscribeForCallerNotification(
        NotificationOperation operation, URL webServiceApiURL) throws Exception
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "SubscribeForCallerNotification");
        parameters.put("NotificationOperationName", operation.toString());
        parameters.put("WebServiceAPIURL", webServiceApiURL.toString());

        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION,
            parameters, EMPTY_INDEXED_MAP);
        doFpsQuery(parameters);
        return true;
    }

    public boolean unsubscribeForCallerNotification(
        NotificationOperation operation) throws Exception
    {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("Action", "UnSubscribeForCallerNotification");
        parameters.put("NotificationOperationName", operation.toString());

        parameters = buildQueryParameters(API_VERSION, SIGNATURE_VERSION,
            parameters, EMPTY_INDEXED_MAP);
        doFpsQuery(parameters);
        return true;
    }

    public String buildPaymentWidget(String paymentsAccountId, double amount,
        String description, Map<String, String> extraFields) throws Exception
    {
        Map<String, String> fields = new TreeMap<String, String>();
        fields.putAll(extraFields);

        // Mandatory fields
        fields.put("amazonPaymentsAccountId", paymentsAccountId);
        fields.put("accessKey", getAwsAccessKey());
        fields.put("amount", "USD " + String.valueOf(amount));
        fields.put("description", description);

        // Generate a widget description and sign it. The
        // fields are already sorted in the TreeMap.
        StringBuffer widgetDescBuffer = new StringBuffer();
        for (Map.Entry<String, String> field : fields.entrySet()) {
            widgetDescBuffer.append(field.getKey() + field.getValue());
        }
        fields.put("signature", generateSignature(widgetDescBuffer.toString()));

        // Combine all fields into a string
        StringBuffer fieldsBuffer = new StringBuffer();
        for (Map.Entry<String, String> field : fields.entrySet()) {
            fieldsBuffer.append(
                "<input type=\"hidden\" name=\"" + field.getKey() + "\" " +
                "value=\"" + field.getValue() + "\">\n");
        }

        return "<form method=\"post\" action=\"" +
            "https://authorize.payments-sandbox.amazon.com/pba/paypipeline\">\n" +
            fieldsBuffer.toString() +
            "<input type=\"image\" border=\"0\" src=\"" +
            "https://authorize.payments-sandbox.amazon.com/pba/images/payNowButton.png\">\n" +
            "</form>";
    }



    /**
     * The SimpleDateFormat class cannot parse ISO 8601 format dates with the
     * ':' character in the timezone portion. To parse these dates, we will
     * strip out the ':' character before applying the
     * {@link #pseudoIso8601DateFormat} formatter.
     *
     * @throws ParseException
     */
    protected Date parseIso8601Date(String dateString) throws ParseException {
        if (dateString == null) {
            return null;
        }
        String kludgedDate = dateString;
        if (kludgedDate.charAt(kludgedDate.length() - 3) == ':') {
            // If third-last character is ':', remove it.
            kludgedDate = kludgedDate.substring(0, kludgedDate.length() - 3)
                + kludgedDate.substring(kludgedDate.length() - 2);
        }
        return pseudoIso8601DateFormat.parse(kludgedDate);
    }

    /*
     * Below this point are class and enum definitions specific to the Java
     * implementation of AWS clients. These items make it easier to pass
     * parameters into this client's methods, and to retrieve results from the
     * methods.
     */

    public static enum TokenType {
        SingleUse, MultiUse, Recurring, Unrestricted
    };

    public static enum TokenStatus {
        Active, Inactive
    };

    public static enum InstrumentStatus {
        Active, Inactive, Cancelled
    };

    public static enum ResponseGroup {
        Summary, Detail
    };

    public static enum SortOrderByDate {
        Ascending, Descending
    };

    public static enum TransactionOperation {
        Pay, Refund, Settle, SettleDebt, WriteOffDebt,
        FundPrepaid, DepositFunds, WithdrawFunds
    };

    public static enum TransactionRole {
        Caller, Sender, Recipient
    };

    public static enum ChargeToRole {
        Caller, Recipient
    };

    public static enum TransactionStatus {
        Success, Initiated, Failure, TemporaryDecline, Reinitiated, Reserved
    };

    public static enum PaymentMethod {
        CC, ABT, ACH, Debt, Prepaid
    };

    public static enum Pipeline {
        SingleUse, Recipient, MultiUse, Recurring,
        SetupPrepaid, SetupPostpaid, EditToken
    };

    public static enum AmountLimitType {
        Minimum, Maximum, Exact
    };

    public static enum NotificationOperation {
        postTransactionResult, postTokenCancellation
    };

    class PaymentInstruction {
        Token token;
        String instructions;
        String accountId;

        public String toString() {
            return "{" + this.getClass().getName() + ": accountId=" + accountId
                + ", token=" + token + ", instructions=" + instructions + "}";
        }
    }

    class Token {
        String id;
        TokenStatus status;
        TokenType type;
        Date dateInstalled;
        String callerInstalled;
        String callerRef;
        String oldId;
        String friendlyName;
        String reason;

        public String toString() {
            return "{" + this.getClass().getName() + ": id=" + id + ", status="
                + status + ", type=" + type + ", dateInstalled="
                + dateInstalled + ", callerInstalled=" + callerInstalled
                + ", callerRef=" + callerRef + ", oldId=" + oldId
                + ", friendlyName=" + friendlyName + ", reason=" + reason + "}";
        }
    }

    class AccountBalance {
        Amount totalBalance = new Amount();
        Amount pendingInBalance = new Amount();
        Amount pendingOutBalance = new Amount();
        Amount disburseBalance = new Amount();
        Amount refundBalance = new Amount();

        public String toString() {
            return "{" + this.getClass().getName() + ": totalBalance="
                + totalBalance + ", pendingInBalance=" + pendingInBalance
                + ", pendingOutBalance=" + pendingOutBalance
                + ", disburseBalance=" + disburseBalance + ", refundBalance="
                + refundBalance + "}";
        }
    }

    class Amount {
        public static final String US_DOLLARS = "USD";
        Double amount;
        String currencyCode;

        public String toString() {
            return "{" + this.getClass().getName() + ": amount=" + amount
                + ", currencyCode=" + currencyCode + "}";
        }
    }

    class Transaction {
        String id;
        String originalTransactionId;
        Date callerTransactionDate;
        Date dateReceived;
        Date dateCompleted;
        Amount transactionAmount;
        Amount feesAmount;
        Amount balance;
        String operation;
        PaymentMethod paymentMethod;
        TransactionStatus status;
        String statusDetail;
        String callerTokenId;
        String senderTokenId;
        String recipientTokenId;
        String callerName;
        String senderName;
        String recipientName;
        String errorCode;
        String errorMessage;
        String metadata;
        List<String> relatedTransactionIds = new ArrayList<String>();
        List<TokenUsage> tokenUsage = new ArrayList<TokenUsage>();
        List<TransactionPart> transactionParts = new ArrayList<TransactionPart>();
        List<StatusChange> statusHistory = new ArrayList<StatusChange>();

        public String toString() {
            return "{" + this.getClass().getName() + ": id=" + id + ", status="
                + status + ", originalTransactionId=" + originalTransactionId
                + ", callerTransactionDate=" + callerTransactionDate
                + ", dateReceived=" + dateReceived + ", dateCompleted="
                + dateCompleted + ", transactionAmount=" + transactionAmount
                + ", feesAmount=" + feesAmount + ", balance=" + balance
                + ", operation=" + operation + ", paymentMethod="
                + paymentMethod + ", statusDetail=" + statusDetail
                + ", callerTokenId=" + callerTokenId + ", senderTokenId="
                + senderTokenId + ", recipientTokenId=" + recipientTokenId
                + ", callerName=" + callerName + ", senderName=" + senderName
                + ", recipientName=" + recipientName + ", errorCode="
                + errorCode + ", errorMessage=" + errorMessage + ", metadata="
                + metadata + ", tokenUsage=" + tokenUsage
                + ", transactionParts=" + transactionParts + "}";
        }
    }

    class TransactionPart {
        String accountId;
        TransactionRole role;
        String reference;
        String name;
        String instrumentId;
        String description;
        Amount feePaid;

        public String toString() {
            return "{" + this.getClass().getName() + ": role=" + role
                + ", accountId=" + accountId + ", reference=" + reference
                + ", name=" + name + ", instrumentId=" + instrumentId
                + ", description=" + description + ", feePaid=" + feePaid + "}";
        }
    }

    class TransactionResult {
        String id;
        TransactionStatus status;
        TransactionOperation operation;
        String callerRef;

        public String toString() {
            return "{" + this.getClass().getName() + ": id=" + id + ", status="
                + status + ", operation=" + operation + ", callerRef="
                + callerRef + "}";
        }
    }

    class TransactionResults {
        Integer numberPending = 0;
        List<TransactionResult> results = new ArrayList<TransactionResult>();

        public String toString() {
            return "{" + this.getClass().getName() + ": numberPending="
                + numberPending + ", results=" + results + "}";
        }
    }

    class StatusChange {
        Date date;
        TransactionStatus status;
        Amount amount;

        public String toString() {
            return "{" + this.getClass().getName() + ": status=" + status
                + ", date=" + date + ", amount=" + amount + "}";
        }
    }

    abstract class TokenUsage {
        Date lastResetTimestamp;
    }

    class AmountTokenUsage extends TokenUsage {
        Amount amount;
        Amount lastResetAmount;

        public String toString() {
            return "{" + this.getClass().getName() + ": amount=" + amount
                + ", lastResetAmount=" + lastResetAmount
                + ", lastResetTimestamp=" + lastResetTimestamp + "}";
        }
    }

    class CountTokenUsage extends TokenUsage {
        Integer count;
        Integer lastResetCount;

        public String toString() {
            return "{" + this.getClass().getName() + ": count=" + count
                + ", lastResetCount=" + lastResetCount
                + ", lastResetTimestamp=" + lastResetTimestamp + "}";
        }
    }

    abstract class TokenUsageLimit {
        String period;
    }

    class TokenUsageLimitCount extends TokenUsageLimit {
        Integer count;

        public String toString() {
            return "{" + this.getClass().getName() + ": count=" + count
                + ", period=" + period + "}";
        }
    }

    class TokenUsageLimitAmount extends TokenUsageLimit {
        Double amount;

        public String toString() {
            return "{" + this.getClass().getName() + ": amount=" + amount
                + ", period=" + period + "}";
        }
    }

    class AccountActivity {
        List<Transaction> transactions = new ArrayList<Transaction>();
        Integer responseBatchSize;
        Date startTimeForNextTransaction;

        public String toString() {
            return "{" + this.getClass().getName() + ": responseBatchSize="
                + responseBatchSize + ", startTimeForNextTransaction="
                + startTimeForNextTransaction + ", transactions="
                + transactions + "}";
        }
    }

    class PrepaidBalance {
        Amount availableBalance = new Amount();
        Amount pendingInBalance = new Amount();

        public String toString() {
            return "{" + this.getClass().getName() + ": availableBalance="
                + availableBalance + ", pendingInBalance=" + pendingInBalance
                + "}";
        }
    }

    class PrepaidLiability {
        Amount outstandingBalance = new Amount();
        Amount pendingInBalance = new Amount();

        public String toString() {
            return "{" + this.getClass().getName() + ": outstandingBalance="
                + outstandingBalance + ", pendingInBalance=" + pendingInBalance
                + "}";
        }
    }

    class DebtBalance {
        Amount availableBalance = new Amount();
        Amount pendingOutBalance = new Amount();

        public String toString() {
            return "{" + this.getClass().getName() + ": availableBalance="
                + availableBalance + ", pendingOutBalance=" + pendingOutBalance
                + "}";
        }
    }

    class OutstandingDebtBalance {
        Amount outstandingBalance = new Amount();
        Amount pendingOutBalance = new Amount();

        public String toString() {
            return "{" + this.getClass().getName() + ": outstandingBalance="
                + outstandingBalance + ", pendingOutBalance="
                + pendingOutBalance + "}";
        }
    }

}
