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

import com.google.cloud.hadoop.util.AccessTokenProvider;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

import static io.prestosql.plugin.hive.gcs.GcsDynamicConfigurationUpdater.GCS_ACCESS_TOKEN_CONF_KEY;

// The implementation of the AccessTokenProvider interface used for GCS Connector.
// The token provider will parse the token from configuration object.
public class GcsAccessTokenProvider
        implements AccessTokenProvider
{
    public static final Long EXPIRATION_TIME_MILLISECONDS = 3600L;
    private Configuration config;

    @Override
    public AccessToken getAccessToken()
    {
        return new AccessToken(config.get(GCS_ACCESS_TOKEN_CONF_KEY), EXPIRATION_TIME_MILLISECONDS);
    }

    @Override
    public void refresh()
            throws IOException
    { }

    @Override
    public void setConf(Configuration configuration)
    {
        this.config = configuration;
    }

    @Override
    public Configuration getConf()
    {
        return config;
    }
}
