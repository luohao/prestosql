/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.plugin.hive;

import io.prestosql.plugin.hive.HdfsEnvironment.HdfsContext;
import org.apache.hadoop.conf.Configuration;

import javax.inject.Inject;

import java.net.URI;
import java.util.Optional;

import static io.prestosql.plugin.hive.util.ConfigurationUtils.copy;
import static io.prestosql.plugin.hive.util.ConfigurationUtils.getInitialConfiguration;
import static java.util.Objects.requireNonNull;

public class HiveHdfsConfiguration
        implements HdfsConfiguration
{
    private static final Configuration INITIAL_CONFIGURATION = getInitialConfiguration();

    @SuppressWarnings("ThreadLocalNotStaticFinal")
    private final ThreadLocal<Configuration> hadoopConfiguration = new ThreadLocal<Configuration>()
    {
        @Override
        protected Configuration initialValue()
        {
            Configuration configuration = new Configuration(false);
            copy(INITIAL_CONFIGURATION, configuration);
            updater.updateConfiguration(configuration);
            return configuration;
        }
    };

    private final HdfsConfigurationUpdater updater;
    private final Optional<HdfsDynamicConfigurationUpdater> dynamicUpdater;

    @Inject
    public HiveHdfsConfiguration(
            HdfsConfigurationUpdater updater,
            Optional<HdfsDynamicConfigurationUpdater> dynamicUpdater)
    {
        this.updater = requireNonNull(updater, "updater is null");
        this.dynamicUpdater = requireNonNull(dynamicUpdater, "dynamicUpdater is null");
    }

    @Override
    public Configuration getConfiguration(HdfsContext context, URI uri)
    {
        // use the same configuration for everything
        Configuration configuration = copy(hadoopConfiguration.get());
        dynamicUpdater.ifPresent(x -> x.updateConfiguration(configuration, context, uri);
        return configuration;
    }
}
