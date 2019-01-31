package io.prestosql.plugin.hive;

import org.apache.hadoop.conf.Configuration;

import java.net.URI;

public interface DynamicConfigurationUpdater
{
    Configuration updateConfiguration(Configuration configuration, HdfsEnvironment.HdfsContext context, URI uri);
}
