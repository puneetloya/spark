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
package org.apache.spark.deploy.k8s.submit.submitsteps

import io.fabric8.kubernetes.api.model.ContainerBuilder

import org.apache.spark.deploy.k8s.constants._
import org.apache.spark.deploy.k8s.submit.KubernetesFileUtils

private[spark] class PythonStep(
    primaryPyFile: String,
    otherPyFiles: Seq[String],
    filesDownloadPath: String) extends DriverConfigurationStep {

  override def configureDriver(driverSpec: KubernetesDriverSpec): KubernetesDriverSpec = {
    val resolvedOtherPyFilesString = if (otherPyFiles.isEmpty) {
      "null"
    } else {
      KubernetesFileUtils.resolveFilePaths(otherPyFiles, filesDownloadPath).mkString(",")
    }
    val withPythonPrimaryFileContainer = new ContainerBuilder(driverSpec.driverContainer)
      .addNewEnv()
        .withName(ENV_PYSPARK_PRIMARY)
        .withValue(KubernetesFileUtils.resolveFilePath(primaryPyFile, filesDownloadPath))
        .endEnv()
      .addNewEnv()
        .withName(ENV_PYSPARK_FILES)
        .withValue(resolvedOtherPyFilesString)
        .endEnv()
    driverSpec.copy(driverContainer = withPythonPrimaryFileContainer.build())
  }
}
