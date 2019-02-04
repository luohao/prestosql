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

import org.apache.hadoop.conf.Configuration;

import javax.inject.Inject;

public class PrestoGasConfigInitializer
        implements GasConfigurationInitializer
{
    private final boolean useGcsAccessToken;
    private final String jsonKeyFilePath;

    @Inject
    public PrestoGasConfigInitializer(HiveGcsConfig config)
    {
        this.useGcsAccessToken = config.isUseGcsAccessToken();
        this.jsonKeyFilePath = config.getJsonKeyFilePath();
    }

    public void updateConfiguration(Configuration config)
    {
        config.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
        config.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");

        if (useGcsAccessToken) {
            // use oauth token to authenticate with google cloud storage
            config.set("google.cloud.auth.service.account.enable", "false");
            config.set("fs.gs.auth.access.token.provider.impl", GcsAccessTokenProvider.class.getName());
        }
        else if (jsonKeyFilePath != null) {
            // use service account key file
            config.set("fs.gs.auth.service.account.enable", "true");
            config.set("fs.gs.auth.service.account.json.keyfile", jsonKeyFilePath);
        }
    }
}
