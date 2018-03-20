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
package org.apache.spark.deploy.k8s

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.submit.{JavaMainAppResource, MainAppResource}
import org.apache.spark.internal.config.ConfigEntry

private[k8s] sealed trait KubernetesRoleSpecificConf {

  def roleLabels(): Map[String, String]

  def roleAnnotations(): Map[String, String]

  def roleSecretNamesToMountPaths(): Map[String, String]
}

private[k8s] case class KubernetesDriverSpecificConf(
  private val sparkConf: SparkConf,
  mainAppResource: Option[MainAppResource],
  mainClass: String,
  appName: String,
  appArgs: Seq[String],
  appId: String) extends KubernetesRoleSpecificConf {

  override def roleLabels(): Map[String, String] = {
    val driverCustomLabels = KubernetesUtils.parsePrefixedKeyValuePairs(
      sparkConf,
      KUBERNETES_DRIVER_LABEL_PREFIX)
    require(!driverCustomLabels.contains(SPARK_APP_ID_LABEL), "Label with key " +
      s"$SPARK_APP_ID_LABEL is not allowed as it is reserved for Spark bookkeeping " +
      "operations.")
    require(!driverCustomLabels.contains(SPARK_ROLE_LABEL), "Label with key " +
      s"$SPARK_ROLE_LABEL is not allowed as it is reserved for Spark bookkeeping " +
      "operations.")
    driverCustomLabels ++ Map(
      SPARK_APP_ID_LABEL -> appId,
      SPARK_ROLE_LABEL -> SPARK_POD_DRIVER_ROLE)
  }

  override def roleAnnotations(): Map[String, String] = {
    val driverCustomAnnotations = KubernetesUtils.parsePrefixedKeyValuePairs(
      sparkConf, KUBERNETES_DRIVER_ANNOTATION_PREFIX)
    require(!driverCustomAnnotations.contains(SPARK_APP_NAME_ANNOTATION),
      s"Annotation with key $SPARK_APP_NAME_ANNOTATION is not allowed as it is reserved for" +
        " Spark bookkeeping operations.")
    driverCustomAnnotations ++ Map(SPARK_APP_NAME_ANNOTATION -> appName)
  }

  override def roleSecretNamesToMountPaths(): Map[String, String] = {
    KubernetesUtils.parsePrefixedKeyValuePairs(sparkConf, KUBERNETES_DRIVER_SECRETS_PREFIX)
  }
}

private[k8s] case class KubernetesExecutorSpecificConf(
  private val sparkConf: SparkConf,
  private val appId: String,
  private val executorId: String) extends KubernetesRoleSpecificConf {

  override def roleLabels(): Map[String, String] = {
    val executorLabels = KubernetesUtils.parsePrefixedKeyValuePairs(
      sparkConf,
      KUBERNETES_EXECUTOR_LABEL_PREFIX)
    require(
      !executorLabels.contains(SPARK_APP_ID_LABEL),
      s"Custom executor labels cannot contain $SPARK_APP_ID_LABEL as it is reserved for Spark.")
    require(
      !executorLabels.contains(SPARK_EXECUTOR_ID_LABEL),
      s"Custom executor labels cannot contain $SPARK_EXECUTOR_ID_LABEL as it is reserved for" +
        " Spark.")
    require(
      !executorLabels.contains(SPARK_ROLE_LABEL),
      s"Custom executor labels cannot contain $SPARK_ROLE_LABEL as it is reserved for Spark.")
    Map(
      SPARK_EXECUTOR_ID_LABEL -> executorId,
      SPARK_APP_ID_LABEL -> appId,
      SPARK_ROLE_LABEL -> SPARK_POD_EXECUTOR_ROLE) ++
      executorLabels
  }

  override def roleAnnotations(): Map[String, String] = {
    KubernetesUtils.parsePrefixedKeyValuePairs(
      sparkConf, KUBERNETES_EXECUTOR_ANNOTATION_PREFIX)
  }

  override def roleSecretNamesToMountPaths(): Map[String, String] = {
    KubernetesUtils.parsePrefixedKeyValuePairs(sparkConf, KUBERNETES_EXECUTOR_SECRETS_PREFIX)
  }
}

private[k8s] class KubernetesConf[T <: KubernetesRoleSpecificConf](
    private val sparkConf: SparkConf,
    val roleSpecificConf: T,
    val appResourceNamePrefix: String,
    val appId: String) {

  def namespace(): String = sparkConf.get(KUBERNETES_NAMESPACE)

  def sparkJars(): Seq[String] = sparkConf
    .getOption("spark.jars")
    .map(str => str.split(",").toSeq)
    .getOrElse(Seq.empty[String])

  def sparkFiles(): Seq[String] = sparkConf
    .getOption("spark.files")
    .map(str => str.split(",").toSeq)
    .getOrElse(Seq.empty[String])

  def driverCustomEnvs(): Seq[(String, String)] =
    sparkConf.getAllWithPrefix(KUBERNETES_DRIVER_ENV_KEY).toSeq

  def imagePullPolicy(): String = sparkConf.get(CONTAINER_IMAGE_PULL_POLICY)

  def nodeSelector(): Map[String, String] =
    KubernetesUtils.parsePrefixedKeyValuePairs(sparkConf, KUBERNETES_NODE_SELECTOR_PREFIX)

  def getSparkConf(): SparkConf = sparkConf.clone()

  def get[T](config: ConfigEntry[T]): T = sparkConf.get(config)

  def getOption(key: String): Option[String] = sparkConf.getOption(key)

}

private[k8s] object KubernetesConf {
  def createDriverConf(
    sparkConf: SparkConf,
    appName: String,
    appResourceNamePrefix: String,
    appId: String,
    mainAppResource: Option[MainAppResource],
    mainClass: String,
    appArgs: Array[String]): KubernetesConf[KubernetesDriverSpecificConf] = {
    val sparkConfWithMainAppJar = sparkConf.clone()
    mainAppResource.foreach {
      case JavaMainAppResource(res) =>
        val previousJars = sparkConf
          .getOption("spark.jars")
          .map(_.split(","))
          .getOrElse(Array.empty)
        if (!previousJars.contains(res)) {
          sparkConfWithMainAppJar.setJars(previousJars ++ Seq(res))
        }
    }
    new KubernetesConf(
      sparkConfWithMainAppJar,
      KubernetesDriverSpecificConf(
        sparkConfWithMainAppJar,
        mainAppResource,
        appName,
        mainClass,
        appArgs,
        appId),
      appResourceNamePrefix,
      appId)
  }

  def createExecutorConf(
    sparkConf: SparkConf,
    executorId: String,
    appResourceNamePrefix: String,
    appId: String): KubernetesConf[KubernetesExecutorSpecificConf] = {
    new KubernetesConf(
      sparkConf,
      KubernetesExecutorSpecificConf(sparkConf, executorId, appId),
      appResourceNamePrefix,
      appId)
  }
}
