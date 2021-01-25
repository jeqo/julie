package com.purbon.kafka.topology;

import static com.purbon.kafka.topology.TopologyBuilderConfig.CCLOUD_ENV_CONFIG;

import com.purbon.kafka.topology.serviceaccounts.CCloudPrincipalProvider;
import com.purbon.kafka.topology.serviceaccounts.VoidPrincipalProvider;

public class PrincipalProviderFactory {

  private TopologyBuilderConfig config;

  public PrincipalProviderFactory(TopologyBuilderConfig config) {
    this.config = config;
  }

  public PrincipalProvider get() {
    if (config.hasProperty(CCLOUD_ENV_CONFIG)) {
      return new CCloudPrincipalProvider(config);
    } else {
      return new VoidPrincipalProvider();
    }
  }
}
