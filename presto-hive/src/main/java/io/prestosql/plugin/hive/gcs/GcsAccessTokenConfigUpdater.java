package io.prestosql.plugin.hive.gcs;

import io.prestosql.plugin.hive.HdfsEnvironment;
import org.apache.hadoop.conf.Configuration;

public class GcsAccessTokenConfigUpdater
        implements GcsDynamicConfigurationUpdater
{
    @Override
    public void updateConfiguration(Configuration configuration, HdfsEnvironment.HdfsContext context)
    {
        context.getIdentity().getConnectorTokens().ifPresent(x -> {
            if (x.containsKey(GCS_ACCESS_TOKEN_CONF_KEY)) {
                configuration.set(GCS_ACCESS_TOKEN_CONF_KEY, x.get(GCS_ACCESS_TOKEN_CONF_KEY));
            }
        });
    }
}
