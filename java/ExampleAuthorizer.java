package org.jets3t.servlets.gatekeeper.impl;

import org.jets3t.servlets.gatekeeper.Authorizer;
import org.jets3t.service.utils.gatekeeper.GatekeeperMessage;
import org.jets3t.service.utils.gatekeeper.SignatureRequest;
import org.jets3t.servlets.gatekeeper.ClientInformation;

/**
 * Authorizer implementation to disallow DELETE requests from 
 * users not in the 'gatekeeper-admin' role. 
 */
public class ExampleAuthorizer extends Authorizer {

  /**
   * Default constructor - no configuration parameters are required. 
   */
  public ExampleAuthorizer(javax.servlet.ServletConfig servletConfig) 
      throws javax.servlet.ServletException 
  {
    super(servletConfig);
  }

  /**
   * Control which users can perform DELETE requests.
   */
  public boolean allowSignatureRequest(GatekeeperMessage requestMessage,
    ClientInformation clientInformation, SignatureRequest signatureRequest)
  {
    // Apply custom rules if this is a DELETE request.
    if (SignatureRequest.SIGNATURE_TYPE_DELETE.equals(
        signatureRequest.getSignatureType()))
    {            
      // Return true if the user is a member of the "gatekeeper-admin"
      // access role, false otherwise.
      return clientInformation.getHttpServletRequest()
          .isUserInRole("gatekeeper-admin");
    } else {
      // Requests for operations other than DELETE are always allowed.
      return true;            
    }
  }

  /**
   * Allow any user to obtain a listing of a bucket's contents.
   */
  public boolean allowBucketListingRequest(
    GatekeeperMessage requestMessage, ClientInformation clientInformation)
  {
    return true;
  }
  
}

