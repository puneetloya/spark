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
package org.apache.spark.deploy.k8s.features

import org.apache.spark.deploy.k8s.{KubernetesConf, KubernetesDriverSpecificConf, KubernetesSpec}

private[spark] class KubernetesDriverBuilder {

  def buildFromFeatures(
    kubernetesConf: KubernetesConf[KubernetesDriverSpecificConf]): KubernetesSpec = {
    val baseFeatures = Seq(
      new BasicDriverFeatureStep(kubernetesConf),
      new DriverKubernetesCredentialsFeatureStep(kubernetesConf),
      new DriverServiceFeatureStep(kubernetesConf))
    val allFeatures = if (kubernetesConf.roleSecretNamesToMountPaths.nonEmpty) {
      baseFeatures ++ Seq(new MountSecretsFeatureStep(kubernetesConf))
    } else baseFeatures
    var spec = KubernetesSpec.initialSpec(kubernetesConf.sparkConf.getAll.toMap)
    for (feature <- allFeatures) {
      val configuredPod = feature.configurePod(spec.pod)
      val addedSystemProperties = feature.getAdditionalPodSystemProperties()
      val addedResources = feature.getAdditionalKubernetesResources()
      spec = KubernetesSpec(
        configuredPod,
        spec.additionalDriverKubernetesResources ++ addedResources,
        spec.podJavaSystemProperties ++ addedSystemProperties)
    }
    spec
  }
}
