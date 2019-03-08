/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.deploy.rest.k8s

import java.io.InputStream
import javax.ws.rs.{Consumes, GET, HeaderParam, Path, PathParam, POST, Produces}
import javax.ws.rs.core.{MediaType, StreamingOutput}

import org.glassfish.jersey.media.multipart.FormDataParam

import org.apache.spark.deploy.k8s.KubernetesCredentials
import org.apache.spark.deploy.k8s.submit.SubmittedResourceIdAndSecret

/**
 * Service that receives application data that can be retrieved later on. This is primarily used
 * in the context of Spark, but the concept is generic enough to be used for arbitrary applications.
 * The use case is to have a place for Kubernetes application submitters to bootstrap dynamic,
 * heavyweight application data for pods. Application submitters may have data stored on their
 * local disks that they want to provide to the pods they create through the API server. ConfigMaps
 * are one way to provide this data, but the data in ConfigMaps are stored in etcd which cannot
 * maintain data in the hundreds of megabytes in size.
 * <p>
 * The general use case is for an application submitter to ship the dependencies to the server via
 * {@link uploadResources}; the application submitter will then receive a unique secure token.
 * The application submitter then ought to convert the token into a secret, and use this secret in
 * a pod that fetches the uploaded dependencies via {@link downloadResources}. An application can
 * provide multiple resource bundles simply by hitting the upload endpoint multiple times and
 * downloading each bundle with the appropriate secret.
 */
@Path("/v0")
private[spark] trait ResourceStagingService {

  /**
   * Register a resource with the dependency service, so that pods with the given labels can
   * retrieve them when they run.
   *
   * @param resources Application resources to upload, compacted together in tar + gzip format.
   *                  The tarball should contain the files laid out in a flat hierarchy, without
   *                  any directories. We take a stream here to avoid holding these entirely in
   *                  memory.
   * @param resourcesOwner A description of the "owner" of a resource. A resource owner is a
   *                       Kubernetes API object in a given namespace, with a specific set of
   *                       labels. When there are no resources of the owner's type in the given
   *                       namespace with the given labels, the resources are cleaned up. The owner
   *                       bundle also includes any Kubernetes credentials that are required for
   *                       resource staging server to watch the object's state over time.
   * @return A unique token that should be provided when retrieving these dependencies later.
   */
  @POST
  @Consumes(Array(MediaType.MULTIPART_FORM_DATA, MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN))
  @Produces(Array(MediaType.APPLICATION_JSON))
  @Path("/resources")
  def uploadResources(
        @FormDataParam("resources") resources: InputStream,
        @FormDataParam("resourcesOwner") resourcesOwner: StagedResourcesOwner)
      : SubmittedResourceIdAndSecret

  /**
   * Download an application's resources. The resources are provided as a stream, where the stream's
   * underlying data matches the stream that was uploaded in uploadResources.
   */
  @GET
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Produces(Array(MediaType.APPLICATION_OCTET_STREAM))
  @Path("/resources/{resourceId}")
  def downloadResources(
    @PathParam("resourceId") resourceId: String,
    @HeaderParam("Authorization") resourceSecret: String): StreamingOutput

  /**
   * Health check.
   */
  @GET
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Produces(Array(MediaType.TEXT_PLAIN))
  @Path("/ping")
  def ping(): String
}
