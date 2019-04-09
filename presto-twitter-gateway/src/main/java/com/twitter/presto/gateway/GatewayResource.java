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

import com.google.inject.Inject;
import io.airlift.log.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import java.net.URI;

import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

@Path("/")
public class GatewayResource
{
    private static final Logger log = Logger.get(GatewayResource.class);

    private final ClusterManager clusterManager;

    @Inject
    public GatewayResource(ClusterManager clusterManager)
    {
        this.clusterManager = requireNonNull(clusterManager, "clusterManager is null");
    }

    @GET
    @Path("/v1/gateway")
    public String getGateInfo()
    {
        return "hello world";
    }

    @POST
    @Path("/v1/statement")
    @Produces(APPLICATION_JSON)
    public Response routeQuery(String statement, @Context HttpServletRequest servletRequest)
    {
        RequestInfo requestInfo = new RequestInfo(servletRequest, statement);
        URI destination = uriBuilderFrom(clusterManager.getPrestoCluster(requestInfo)).replacePath("/v1/statement").build();
        log.info("route query to %s", destination);
        return Response.temporaryRedirect(destination).build();
    }
}
