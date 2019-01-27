package io.prestosql.plugin.hive;

import io.prestosql.plugin.hive.gcs.GcsDynamicConfigurationUpdater;
import org.apache.hadoop.conf.Configuration;

import javax.inject.Inject;

import java.net.URI;

import static java.util.Objects.requireNonNull;

public class HdfsDynamicConfigurationUpdater
{
    private final GcsDynamicConfigurationUpdater gcsUpdater;

    @Inject
    public HdfsDynamicConfigurationUpdater(GcsDynamicConfigurationUpdater gcsUpdater)
    {
        this.gcsUpdater = requireNonNull(gcsUpdater, "gcsUpdater is null");
    }

    public void updateConfiguration(
            Configuration configuration,
            HdfsEnvironment.HdfsContext context,
            URI uri)
    {
        gcsUpdater.updateConfiguration(configuration, context);
    }
}
