package kafka.ops.topology;

import static kafka.ops.topology.TopologyBuilderConfig.*;

import java.io.IOException;
import java.lang.reflect.Constructor;
import kafka.ops.topology.api.adminclient.TopologyBuilderAdminClient;
import kafka.ops.topology.api.ccloud.CCloudCli;
import kafka.ops.topology.api.mds.MdsApiClient;
import kafka.ops.topology.api.mds.MdsApiClientBuilder;
import kafka.ops.topology.roles.CCloudAclsProvider;
import kafka.ops.topology.roles.RbacProvider;
import kafka.ops.topology.roles.SimpleAclsProvider;
import kafka.ops.topology.roles.acls.AclsBindingsBuilder;
import kafka.ops.topology.roles.rbac.RBACBindingsBuilder;
import kafka.ops.topology.utils.CCloudUtils;

public class AccessControlProviderFactory {

  private final TopologyBuilderConfig config;
  private final TopologyBuilderAdminClient builderAdminClient;
  private final MdsApiClientBuilder mdsApiClientBuilder;

  public AccessControlProviderFactory(
      TopologyBuilderConfig config,
      TopologyBuilderAdminClient builderAdminClient,
      MdsApiClientBuilder mdsApiClientBuilder) {
    this.config = config;
    this.builderAdminClient = builderAdminClient;
    this.mdsApiClientBuilder = mdsApiClientBuilder;
  }

  public AccessControlProvider get() throws IOException {

    String accessControlClassName = config.getAccessControlClassName();

    try {
      Class<?> clazz = Class.forName(accessControlClassName);
      switch (accessControlClassName) {
        case ACCESS_CONTROL_DEFAULT_CLASS:
          Constructor<?> aclsProviderConstructor =
              clazz.getConstructor(TopologyBuilderAdminClient.class);
          return (SimpleAclsProvider) aclsProviderConstructor.newInstance(builderAdminClient);
        case CONFLUENT_CLOUD_CONTROL_CLASS:
          Constructor<?> ccloudProviderConstructor =
              clazz.getConstructor(TopologyBuilderAdminClient.class, TopologyBuilderConfig.class);
          return (CCloudAclsProvider)
              ccloudProviderConstructor.newInstance(builderAdminClient, config);
        case RBAC_ACCESS_CONTROL_CLASS:
          Constructor<?> rbacProviderContructor = clazz.getConstructor(MdsApiClient.class);
          MdsApiClient apiClient = apiClientLogIn();
          apiClient.authenticate();
          return (RbacProvider) rbacProviderContructor.newInstance(apiClient);
        default:
          throw new IOException("Unknown access control provided. " + accessControlClassName);
      }
    } catch (Exception ex) {
      throw new IOException(ex);
    }
  }

  public BindingsBuilderProvider builder() throws IOException {
    String accessControlClass = config.getAccessControlClassName();

    CCloudCli cCloudApi = new CCloudCli();
    CCloudUtils cCloudUtils = new CCloudUtils(cCloudApi, config);

    try {
      if (accessControlClass.equalsIgnoreCase(ACCESS_CONTROL_DEFAULT_CLASS)) {
        return new AclsBindingsBuilder(config, cCloudUtils);
      } else if (accessControlClass.equalsIgnoreCase(CONFLUENT_CLOUD_CONTROL_CLASS)) {
        return new AclsBindingsBuilder(config, cCloudUtils);
      } else if (accessControlClass.equalsIgnoreCase(RBAC_ACCESS_CONTROL_CLASS)) {
        MdsApiClient apiClient = apiClientLogIn();
        apiClient.authenticate();
        return new RBACBindingsBuilder(apiClient);
      } else {
        throw new IOException(accessControlClass + " Unknown access control provided.");
      }
    } catch (Exception ex) {
      throw new IOException(ex);
    }
  }

  private MdsApiClient apiClientLogIn() {
    MdsApiClient apiClient = mdsApiClientBuilder.build();
    String mdsUser = config.getProperty(MDS_USER_CONFIG);
    String mdsPassword = config.getProperty(MDS_PASSWORD_CONFIG);
    apiClient.login(mdsUser, mdsPassword);
    return apiClient;
  }
}