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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.inject.Module;
import io.prestosql.plugin.hive.metastore.ExtendedHiveMetastore;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.connector.ConnectorFactory;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

public class HivePlugin
        implements Plugin
{
    private final String name;
    private final Optional<ExtendedHiveMetastore> metastore;
    private final List<Module> extraModules;

    public HivePlugin(String name)
    {
        this(name, Optional.empty(), ImmutableList.of());
    }

    public HivePlugin(String name, Optional<ExtendedHiveMetastore> metastore)
    {
        this(name, metastore, ImmutableList.of());
    }

    @VisibleForTesting
    public HivePlugin(String name, Optional<ExtendedHiveMetastore> metastore, List<Module> extraModules)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or empty");
        this.name = name;
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.extraModules = requireNonNull(extraModules, "extraModules is null");
    }

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new HiveConnectorFactory(name, getClassLoader(), metastore, extraModules));
    }

    private static ClassLoader getClassLoader()
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = HivePlugin.class.getClassLoader();
        }
        return classLoader;
    }
}
