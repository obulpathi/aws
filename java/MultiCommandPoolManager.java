import com.directthought.lifeguard.PoolManager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class MultiCommandPoolManager extends PoolManager {
  private static Log logger = LogFactory
    .getLog(MultiCommandPoolManager.class);

  protected String moduleName = "";
  protected String notifyEmail = "";
  protected String logQueueName = "";
  protected String outputQueueName = "";
  protected String statusQueueName = "";

  public void setLogQueueName(String value) {
    logQueueName = value;
  }

  public void setOutputQueueName(String value) {
    outputQueueName = value;
  }

  public void setStatusQueueName(String value) {
    statusQueueName = value;
  }

  public void setModuleName(String value) {
    moduleName = value;
  }

  public void setNotifyEmail(String value) {
    notifyEmail = value;
  }

  protected String getUserData() {
    String userData =
      // User Data parameters available in standard Lifeguard configuration
      "ami=" + config.getServiceAMI() +
      "|input_queue_name=" + config.getServiceWorkQueue() +
      "|class_name=" + config.getServiceName() +
      "|aws_access_key_id=" + awsAccessId +
      "|aws_secret_access_key=" + awsSecretKey +

      // Parameters requiring additional configuration options
      "|module_name=" + moduleName +
      "|log_queue_name=" + logQueueName +
      "|output_queue_name=" + outputQueueName +
      "|status_queue_name=" + statusQueueName +
      "|notify_email=" + notifyEmail;

    logger.debug("User Data: " + userData);
    return userData;
  }

}
