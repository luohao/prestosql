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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

public class HiveGcsConfig
{
    private boolean useGcsAccessToken;
    private String jsonKeyFilePath;

    public String getJsonKeyFilePath()
    {
        return jsonKeyFilePath;
    }

    @Config("hive.gcs.json-key-file-path")
    @ConfigDescription("Use JSON key file to access Google Cloud Storage")
    public HiveGcsConfig setJsonKeyFilePath(String jsonKeyFilePath)
    {
        this.jsonKeyFilePath = jsonKeyFilePath;
        return this;
    }

    public boolean isUseGcsAccessToken()
    {
        return useGcsAccessToken;
    }

    @Config("hive.gcs.use-access-token")
    @ConfigDescription("Use OAuth token to access Google Cloud Storage")
    public HiveGcsConfig setUseGcsAccessToken(boolean useGcsAccessToken)
    {
        this.useGcsAccessToken = useGcsAccessToken;
        return this;
    }
}
