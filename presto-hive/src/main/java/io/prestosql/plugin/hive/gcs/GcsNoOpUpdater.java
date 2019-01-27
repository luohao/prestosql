package io.prestosql.plugin.hive.gcs;

import io.prestosql.plugin.hive.HdfsEnvironment;
import org.apache.hadoop.conf.Configuration;

public class GcsNoOpUpdater
        implements GcsDynamicConfigurationUpdater
{
    @Override
    public void updateConfiguration(Configuration config, HdfsEnvironment.HdfsContext context)
    { }
}
