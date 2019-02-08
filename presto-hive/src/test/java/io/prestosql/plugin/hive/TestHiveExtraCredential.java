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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Module;
import io.prestosql.tests.AbstractTestDistributedQueries;
import org.apache.hadoop.conf.Configuration;

import java.net.URI;
import java.util.Map;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.tpch.TpchTable.getTables;
import static io.prestosql.plugin.hive.HiveQueryRunner.createQueryRunner;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;

public class TestHiveExtraCredential
        extends AbstractTestDistributedQueries
{
    private static final Map<String, String> EXTRA_CREDENTIAL = ImmutableMap.of("foo", "bar", "abc", "xyz");

    public TestHiveExtraCredential()
    {
        super(() -> createQueryRunner(getTables(), ImmutableMap.of(), "sql-standard", ImmutableMap.of(), EXTRA_CREDENTIAL, ImmutableList.of(new ExtraCredentialVerifierModule())));
    }

    @Override
    public void testDelete() {}

    private static class ExtraCredentialVerifierModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            newSetBinder(binder, DynamicConfigurationProvider.class).addBinding().toInstance(new ExtraCredentialVerifier(EXTRA_CREDENTIAL));
        }
    }

    private static class ExtraCredentialVerifier
            implements DynamicConfigurationProvider
    {
        private final Map<String, String> extraCredentials;

        public ExtraCredentialVerifier(Map<String, String> extraCredentials)
        {
            this.extraCredentials = requireNonNull(extraCredentials, "extraCredentials is null");
        }

        @Override
        public void updateConfiguration(Configuration configuration, HdfsEnvironment.HdfsContext context, URI uri)
        {
            assertEquals(context.getIdentity().getExtraCredentials(), extraCredentials);
        }
    }
}
