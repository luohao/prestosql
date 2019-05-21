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
package com.twitter.presto.gateway;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestGatewayConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(GatewayConfig.class)
                .setVersion(null)
                .setClusterManagerType(null)
                .setClusters(null)
                .setZookeeperPath(null)
                .setZookeeperUri(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("gateway.version", "testversion")
                .put("gateway.cluster-manager.type", "STATIC")
                .put("gateway.cluster-manager.static.cluster-list", "http://example.net/,http://twitter.com/")
                .put("gateway.cluster-manager.zookeeper.path", "/foo/bar")
                .put("gateway.cluster-manager.zookeeper.uri", "abc:123")
                .build();

        GatewayConfig expected = new GatewayConfig()
                .setVersion("testversion")
                .setClusterManagerType("STATIC")
                .setClusters("http://example.net/,http://twitter.com/")
                .setZookeeperPath("/foo/bar")
                .setZookeeperUri("abc:123");

        assertFullMapping(properties, expected);
    }
}
