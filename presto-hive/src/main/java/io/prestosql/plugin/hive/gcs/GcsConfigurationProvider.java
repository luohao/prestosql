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
package io.prestosql.plugin.hive.gcs;

import io.prestosql.plugin.hive.DynamicConfigurationProvider;
import io.prestosql.plugin.hive.HdfsEnvironment;
import org.apache.hadoop.conf.Configuration;

import java.net.URI;

import static io.prestosql.plugin.hive.gcs.GcsAccessTokenProvider.GCS_ACCESS_KEY_CONF;

public class GcsConfigurationProvider
        implements DynamicConfigurationProvider
{
    @Override
    public Configuration updateConfiguration(Configuration configuration, HdfsEnvironment.HdfsContext context, URI uri)
    {
        if (!uri.getScheme().equals("gs")) {
            return configuration;
        }

        String accessToken = context.getIdentity().getCredentials().get(GCS_ACCESS_KEY_CONF);
        if (accessToken != null) {
            configuration.set(GCS_ACCESS_KEY_CONF, accessToken);
        }
        return configuration;
    }
}
