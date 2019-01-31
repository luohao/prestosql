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
import java.util.List;

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
            staticUpdater.updateConfiguration(configuration);
            return configuration;
        }
    };

    private final HdfsConfigurationStaticUpdater staticUpdater;
    private final List<DynamicConfigurationUpdater> dynamicConfigurationUpdaters;

    @Inject
    public HiveHdfsConfiguration(HdfsConfigurationStaticUpdater staticUpdater, List<DynamicConfigurationUpdater> dynamicUpdater)
    {
        this.staticUpdater = requireNonNull(staticUpdater, "staticUpdater is null");
        this.dynamicConfigurationUpdaters = requireNonNull(dynamicUpdater, "dynamicUpdater is null");
    }

    @Override
    public Configuration getConfiguration(HdfsContext context, URI uri)
    {
        // TODO(hluo): if the copy introduce performance regression, I can make the dynamic updater configurable
        Configuration config = copy(hadoopConfiguration.get());
        dynamicConfigurationUpdaters.stream().forEach(updater -> updater.updateConfiguration(config, context, uri));
        return config;
    }
}
