package com.purbon.kafka.topology.serviceaccounts;

import com.purbon.kafka.topology.PrincipalProvider;
import com.purbon.kafka.topology.TopologyBuilderConfig;
import com.purbon.kafka.topology.api.ccloud.CCloudCli;
import com.purbon.kafka.topology.model.cluster.ServiceAccount;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CCloudPrincipalProvider implements PrincipalProvider {

  private CCloudCli cCloudCLI;
  private String env;

  public CCloudPrincipalProvider(TopologyBuilderConfig config) {
    this.cCloudCLI = new CCloudCli();
    this.env = config.getConfluentCloudEnv();
  }

  @Override
  public void configure() throws IOException {
    cCloudCLI.setEnvironment(env);
  }

  @Override
  public Set<ServiceAccount> listServiceAccounts() throws IOException {
    return new HashSet<>(cCloudCLI.serviceAccounts().values());
  }

  @Override
  public ServiceAccount createServiceAccount(String principal, String description)
      throws IOException {
    return cCloudCLI.newServiceAccount(principal, description);
  }

  @Override
  public void deleteServiceAccount(String principal) throws IOException {
    Map<String, ServiceAccount> accounts = cCloudCLI.serviceAccounts();
    ServiceAccount sa = accounts.get(principal);
    cCloudCLI.deleteServiceAccount(sa.getId());
  }
}
