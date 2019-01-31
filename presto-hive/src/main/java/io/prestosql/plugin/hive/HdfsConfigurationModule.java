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

import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.prestosql.plugin.hive.gcs.GcsAccessTokenUpdater;

import static com.google.inject.multibindings.Multibinder.newSetBinder;

public class HdfsConfigurationModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        HiveClientConfig hiveClientConfig = buildConfigObject(HiveClientConfig.class);
        if (hiveClientConfig.isDynamicHdfsConfiguration()) {
            // bind dynamic configuration provider
            binder.bind(HdfsConfigurationProvider.class).to(DynamicConfigurationProvider.class).in(Scopes.SINGLETON);
            // bind updater
            Multibinder<HdfsDynamicConfigurationUpdater> updaterBinder = newSetBinder(binder, HdfsDynamicConfigurationUpdater.class);
            if (hiveClientConfig.isUseGcsAccessToken()) {
                updaterBinder.addBinding().to(GcsAccessTokenUpdater.class).in(Scopes.SINGLETON);
            }
        }
        else {
            // bind no-op configuration provider
            binder.bind(HdfsConfigurationProvider.class).to(DefaultConfigurationProvider.class).in(Scopes.SINGLETON);
        }
    }
}
