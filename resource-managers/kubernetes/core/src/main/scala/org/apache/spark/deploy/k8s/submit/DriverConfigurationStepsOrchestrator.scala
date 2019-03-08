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
package org.apache.spark.deploy.k8s.submit

import org.apache.spark.SparkConf
import org.apache.spark.deploy.k8s.ConfigurationUtils
import org.apache.spark.deploy.k8s.config._
import org.apache.spark.deploy.k8s.constants._
import org.apache.spark.deploy.k8s.submit.submitsteps.{BaseDriverConfigurationStep, DependencyResolutionStep, DriverConfigurationStep, DriverKubernetesCredentialsStep, DriverServiceBootstrapStep, InitContainerBootstrapStep, MountSecretsStep, MountSmallLocalFilesStep, PythonStep, RStep}
import org.apache.spark.deploy.k8s.submit.submitsteps.initcontainer.InitContainerConfigurationStepsOrchestrator
import org.apache.spark.deploy.k8s.submit.submitsteps.LocalDirectoryMountConfigurationStep
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.util.{SystemClock, Utils}

/**
 * Constructs the complete list of driver configuration steps to run to deploy the Spark driver.
 */
private[spark] class DriverConfigurationStepsOrchestrator(
    namespace: String,
    kubernetesAppId: String,
    launchTime: Long,
    mainAppResource: MainAppResource,
    appName: String,
    mainClass: String,
    appArgs: Array[String],
    additionalPythonFiles: Seq[String],
    submissionSparkConf: SparkConf) {

  // The resource name prefix is derived from the application name, making it easy to connect the
  // names of the Kubernetes resources from e.g. kubectl or the Kubernetes dashboard to the
  // application the user submitted. However, we can't use the application name in the label, as
  // label values are considerably restrictive, e.g. must be no longer than 63 characters in
  // length. So we generate a separate identifier for the app ID itself, and bookkeeping that
  // requires finding "all pods for this application" should use the kubernetesAppId.
  private val kubernetesResourceNamePrefix =
      s"$appName-$launchTime".toLowerCase.replaceAll("\\.", "-")
  private val jarsDownloadPath = submissionSparkConf.get(INIT_CONTAINER_JARS_DOWNLOAD_LOCATION)
  private val filesDownloadPath = submissionSparkConf.get(INIT_CONTAINER_FILES_DOWNLOAD_LOCATION)
  private val dockerImagePullPolicy = submissionSparkConf.get(DOCKER_IMAGE_PULL_POLICY)
  private val initContainerConfigMapName = s"$kubernetesResourceNamePrefix-init-config"

  def getAllConfigurationSteps(): Seq[DriverConfigurationStep] = {
    val additionalMainAppJar = mainAppResource match {
      case JavaMainAppResource(resource) if resource != SparkLauncher.NO_RESOURCE =>
        Option(resource)
      case _ => Option.empty
    }
    val additionalMainAppPythonFile = mainAppResource match {
      case PythonMainAppResource(resource) if resource != SparkLauncher.NO_RESOURCE =>
        Option(resource)
      case _ => Option.empty
    }
    val additionalMainAppRFile = mainAppResource match {
      case RMainAppResource(resource) if resource != SparkLauncher.NO_RESOURCE =>
        Option(resource)
      case _ => Option.empty
    }
    val sparkJars = submissionSparkConf.getOption("spark.jars")
        .map(_.split(","))
        .getOrElse(Array.empty[String]) ++
        additionalMainAppJar.toSeq
    val sparkFiles = submissionSparkConf.getOption("spark.files")
        .map(_.split(","))
        .getOrElse(Array.empty[String]) ++
        additionalMainAppPythonFile.toSeq ++
        additionalMainAppRFile.toSeq ++
        additionalPythonFiles
    val driverCustomLabels = ConfigurationUtils.parsePrefixedKeyValuePairs(
        submissionSparkConf,
        KUBERNETES_DRIVER_LABEL_PREFIX,
        "label")
    require(!driverCustomLabels.contains(SPARK_APP_ID_LABEL), s"Label with key " +
        s" $SPARK_APP_ID_LABEL is not allowed as it is reserved for Spark bookkeeping" +
        s" operations.")
    val allDriverLabels = driverCustomLabels ++ Map(
        SPARK_APP_ID_LABEL -> kubernetesAppId,
        SPARK_ROLE_LABEL -> SPARK_POD_DRIVER_ROLE)
    val driverSecretNamesToMountPaths = ConfigurationUtils.parsePrefixedKeyValuePairs(
      submissionSparkConf,
      KUBERNETES_DRIVER_SECRETS_PREFIX,
      "driver secrets")

    val initialSubmissionStep = new BaseDriverConfigurationStep(
        kubernetesAppId,
        kubernetesResourceNamePrefix,
        allDriverLabels,
        dockerImagePullPolicy,
        appName,
        mainClass,
        appArgs,
        submissionSparkConf)
    val driverAddressStep = new DriverServiceBootstrapStep(
        kubernetesResourceNamePrefix,
        allDriverLabels,
        submissionSparkConf,
        new SystemClock)
    val kubernetesCredentialsStep = new DriverKubernetesCredentialsStep(
        submissionSparkConf, kubernetesResourceNamePrefix)

    val localDirectoryMountConfigurationStep = new LocalDirectoryMountConfigurationStep(
        submissionSparkConf)

    val resourceStep = mainAppResource match {
      case PythonMainAppResource(mainPyResource) =>
        Option(new PythonStep(mainPyResource, additionalPythonFiles, filesDownloadPath))
      case RMainAppResource(mainRFile) =>
        Option(new RStep(mainRFile, filesDownloadPath))
      case _ => Option.empty[DriverConfigurationStep]
    }

    val (localFilesDownloadPath, submittedDependenciesBootstrapSteps) =
      if (areAnyFilesNonContainerLocal(sparkJars ++ sparkFiles)) {
        val (submittedLocalFilesDownloadPath,
            sparkFilesResolvedFromInitContainer,
            mountSmallFilesWithoutInitContainerStep) =
          // If the resource staging server is specified, submit all local files through that.
          submissionSparkConf.get(RESOURCE_STAGING_SERVER_URI).map { _ =>
            (filesDownloadPath, sparkFiles, Option.empty[DriverConfigurationStep])
          }.getOrElse {
            // Else - use a small files bootstrap that submits the local files via a secret.
            // Then, indicate to the outer block that the init-container should not handle
            // those local files simply by filtering them out.
            val sparkFilesWithoutLocal = KubernetesFileUtils.getNonSubmitterLocalFiles(sparkFiles)
            val smallFilesSecretName = s"$kubernetesAppId-submitted-files"
            val mountSmallFilesBootstrap = new MountSmallFilesBootstrapImpl(
                smallFilesSecretName, MOUNTED_SMALL_FILES_SECRET_MOUNT_PATH)
            val mountSmallLocalFilesStep = new MountSmallLocalFilesStep(
              sparkFiles,
              smallFilesSecretName,
              MOUNTED_SMALL_FILES_SECRET_MOUNT_PATH,
              mountSmallFilesBootstrap)
            (MOUNTED_SMALL_FILES_SECRET_MOUNT_PATH,
              sparkFilesWithoutLocal.toArray,
              Some(mountSmallLocalFilesStep))
          }

        val initContainerBootstrapStep =
          if (areAnyFilesNonContainerLocal(sparkJars ++ sparkFilesResolvedFromInitContainer)) {
            val initContainerConfigurationStepsOrchestrator =
              new InitContainerConfigurationStepsOrchestrator(
                namespace,
                kubernetesResourceNamePrefix,
                sparkJars,
                sparkFilesResolvedFromInitContainer,
                jarsDownloadPath,
                filesDownloadPath,
                dockerImagePullPolicy,
                allDriverLabels,
                initContainerConfigMapName,
                INIT_CONTAINER_CONFIG_MAP_KEY,
                submissionSparkConf)
            val initContainerConfigurationSteps =
              initContainerConfigurationStepsOrchestrator.getAllConfigurationSteps()
            Some(new InitContainerBootstrapStep(initContainerConfigurationSteps,
              initContainerConfigMapName,
              INIT_CONTAINER_CONFIG_MAP_KEY))
        } else Option.empty[DriverConfigurationStep]
        (submittedLocalFilesDownloadPath,
          mountSmallFilesWithoutInitContainerStep.toSeq ++
            initContainerBootstrapStep.toSeq)
    } else {
      (filesDownloadPath, Seq.empty[DriverConfigurationStep])
    }

    val dependencyResolutionStep = new DependencyResolutionStep(
      sparkJars,
      sparkFiles,
      jarsDownloadPath,
      localFilesDownloadPath)

    val mountSecretsStep = if (driverSecretNamesToMountPaths.nonEmpty) {
      val mountSecretsBootstrap = new MountSecretsBootstrapImpl(driverSecretNamesToMountPaths)
      Some(new MountSecretsStep(mountSecretsBootstrap))
    } else {
      None
    }

    Seq(
      initialSubmissionStep,
      driverAddressStep,
      kubernetesCredentialsStep,
      dependencyResolutionStep,
      localDirectoryMountConfigurationStep) ++
      submittedDependenciesBootstrapSteps ++
      resourceStep.toSeq ++
      mountSecretsStep.toSeq
  }

  private def areAnyFilesNonContainerLocal(files: Seq[String]): Boolean = {
    files.exists { uri =>
      Option(Utils.resolveURI(uri).getScheme).getOrElse("file") != "local"
    }
  }

}
