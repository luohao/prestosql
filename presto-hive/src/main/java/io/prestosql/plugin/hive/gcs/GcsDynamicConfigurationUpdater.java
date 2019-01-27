package io.prestosql.plugin.hive.gcs;

import org.apache.hadoop.conf.Configuration;

import static io.prestosql.plugin.hive.HdfsEnvironment.HdfsContext;

public interface GcsDynamicConfigurationUpdater
{
    String GCS_ACCESS_TOKEN_CONF_KEY = "fs.gs.auth.access.token";

    void updateConfiguration(Configuration config, HdfsContext context);
}
