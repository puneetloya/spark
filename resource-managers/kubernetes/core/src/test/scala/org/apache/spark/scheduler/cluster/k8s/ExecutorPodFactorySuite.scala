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
package org.apache.spark.scheduler.cluster.k8s

import scala.collection.JavaConverters._

import io.fabric8.kubernetes.api.model.{Pod, VolumeBuilder, VolumeMountBuilder, _}
import io.fabric8.kubernetes.client.KubernetesClient
import org.apache.commons.io.FilenameUtils
import org.mockito.{AdditionalAnswers, MockitoAnnotations}
import org.mockito.Matchers.{any, eq => mockitoEq}
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.{BeforeAndAfter, BeforeAndAfterEach}

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.{constants, PodWithDetachedInitContainer, SparkPodInitContainerBootstrap}
import org.apache.spark.deploy.k8s.config._
import org.apache.spark.deploy.k8s.submit.{MountSecretsBootstrapImpl, MountSmallFilesBootstrap, MountSmallFilesBootstrapImpl}

class ExecutorPodFactorySuite extends SparkFunSuite with BeforeAndAfter with BeforeAndAfterEach {
  private val driverPodName: String = "driver-pod"
  private val driverPodUid: String = "driver-uid"
  private val driverUrl: String = "driver-url"
  private val executorPrefix: String = "base"
  private val executorImage: String = "executor-image"
  private val driverPod = new PodBuilder()
    .withNewMetadata()
      .withName(driverPodName)
      .withUid(driverPodUid)
      .endMetadata()
    .withNewSpec()
      .withNodeName("some-node")
      .endSpec()
    .withNewStatus()
      .withHostIP("192.168.99.100")
      .endStatus()
    .build()
  private var baseConf: SparkConf = _
  private val nodeAffinityExecutorPodModifier = mock(classOf[NodeAffinityExecutorPodModifier])

  before {
    MockitoAnnotations.initMocks(this)
    baseConf = new SparkConf()
      .set(KUBERNETES_DRIVER_POD_NAME, driverPodName)
      .set(KUBERNETES_EXECUTOR_POD_NAME_PREFIX, executorPrefix)
      .set(EXECUTOR_DOCKER_IMAGE, executorImage)
  }
  private var kubernetesClient: KubernetesClient = _

  override def beforeEach(cmap: org.scalatest.ConfigMap) {
    reset(nodeAffinityExecutorPodModifier)
    when(nodeAffinityExecutorPodModifier.addNodeAffinityAnnotationIfUseful(
      any(classOf[Pod]),
      any(classOf[Map[String, Int]]))).thenAnswer(AdditionalAnswers.returnsFirstArg())
  }

  test("basic executor pod has reasonable defaults") {
    val factory = new ExecutorPodFactoryImpl(
      baseConf,
      nodeAffinityExecutorPodModifier,
      None,
      None,
      None,
      None,
      None)
    val executor = factory.createExecutorPod(
      "1", "dummy", "dummy", Seq[(String, String)](), driverPod, Map[String, Int]())

    verify(nodeAffinityExecutorPodModifier, times(1))
      .addNodeAffinityAnnotationIfUseful(any(classOf[Pod]), any(classOf[Map[String, Int]]))

    // The executor pod name and default labels.
    assert(executor.getMetadata.getName === s"$executorPrefix-exec-1")
    assert(executor.getMetadata.getLabels.size() === 3)

    // There is exactly 1 container with no volume mounts and default memory limits.
    // Default memory limit is 1024M + 384M (minimum overhead constant).
    assert(executor.getSpec.getContainers.size() === 1)
    assert(executor.getSpec.getContainers.get(0).getImage === executorImage)
    assert(executor.getSpec.getContainers.get(0).getVolumeMounts.isEmpty)
    assert(executor.getSpec.getContainers.get(0).getResources.getLimits.size() === 1)
    assert(executor.getSpec.getContainers.get(0).getResources
      .getLimits.get("memory").getAmount === "1408Mi")

    // The pod has no node selector, volumes.
    assert(executor.getSpec.getNodeSelector.isEmpty)
    assert(executor.getSpec.getVolumes.isEmpty)

    checkEnv(executor, Map())
    checkOwnerReferences(executor, driverPodUid)
  }

  test("executor pod hostnames get truncated to 63 characters") {
    val conf = baseConf.clone()
    conf.set(KUBERNETES_EXECUTOR_POD_NAME_PREFIX,
      "loremipsumdolorsitametvimatelitrefficiendisuscipianturvixlegeresple")

    val factory = new ExecutorPodFactoryImpl(
      conf, nodeAffinityExecutorPodModifier, None, None, None, None, None)
    val executor = factory.createExecutorPod(
      "1", "dummy", "dummy", Seq[(String, String)](), driverPod, Map[String, Int]())

    verify(nodeAffinityExecutorPodModifier, times(1))
      .addNodeAffinityAnnotationIfUseful(any(classOf[Pod]), any(classOf[Map[String, Int]]))

    assert(executor.getSpec.getHostname.length === 63)
  }

  test("secrets get mounted") {
    val conf = baseConf.clone()

    val secretsBootstrap = new MountSecretsBootstrapImpl(Map("secret1" -> "/var/secret1"))
    val factory = new ExecutorPodFactoryImpl(
      conf,
      nodeAffinityExecutorPodModifier,
      Some(secretsBootstrap),
      None,
      None,
      None,
      None)
    val executor = factory.createExecutorPod(
      "1", "dummy", "dummy", Seq[(String, String)](), driverPod, Map[String, Int]())

    verify(nodeAffinityExecutorPodModifier, times(1))
      .addNodeAffinityAnnotationIfUseful(any(classOf[Pod]), any(classOf[Map[String, Int]]))

    assert(executor.getSpec.getContainers.size() === 1)
    assert(executor.getSpec.getContainers.get(0).getVolumeMounts.size() === 1)
    assert(executor.getSpec.getContainers.get(0).getVolumeMounts.get(0).getName
      === "secret1-volume")
    assert(executor.getSpec.getContainers.get(0).getVolumeMounts.get(0)
      .getMountPath === "/var/secret1")

    // check volume mounted.
    assert(executor.getSpec.getVolumes.size() === 1)
    assert(executor.getSpec.getVolumes.get(0).getSecret.getSecretName === "secret1")

    checkOwnerReferences(executor, driverPodUid)
  }

  test("init-container bootstrap step adds an init container") {
    val conf = baseConf.clone()
    val initContainerBootstrap = mock(classOf[SparkPodInitContainerBootstrap])
    when(initContainerBootstrap.bootstrapInitContainerAndVolumes(
      any(classOf[PodWithDetachedInitContainer]))).thenAnswer(AdditionalAnswers.returnsFirstArg())

    val factory = new ExecutorPodFactoryImpl(
      conf,
      nodeAffinityExecutorPodModifier,
      None,
      None,
      Some(initContainerBootstrap),
      None,
      None)
    val executor = factory.createExecutorPod(
      "1", "dummy", "dummy", Seq[(String, String)](), driverPod, Map[String, Int]())

    verify(nodeAffinityExecutorPodModifier, times(1))
      .addNodeAffinityAnnotationIfUseful(any(classOf[Pod]), any(classOf[Map[String, Int]]))

    assert(executor.getMetadata.getAnnotations.size() === 1)
    assert(executor.getMetadata.getAnnotations.containsKey(constants.INIT_CONTAINER_ANNOTATION))
    checkOwnerReferences(executor, driverPodUid)
  }

  test("the shuffle-service adds a volume mount") {
    val conf = baseConf.clone()
    conf.set(KUBERNETES_SHUFFLE_LABELS, "label=value")
    conf.set(KUBERNETES_SHUFFLE_NAMESPACE, "default")
    conf.set(KUBERNETES_SHUFFLE_DIR, "/tmp")

    val shuffleManager = mock(classOf[KubernetesExternalShuffleManager])
    when(shuffleManager.getExecutorShuffleDirVolumesWithMounts).thenReturn({
      val shuffleDirs = Seq("/tmp")
      shuffleDirs.zipWithIndex.map { case (shuffleDir, shuffleDirIndex) =>
        val volumeName = s"$shuffleDirIndex-${FilenameUtils.getBaseName(shuffleDir)}"
        val volume = new VolumeBuilder()
          .withName(volumeName)
          .withNewHostPath(shuffleDir)
          .build()
        val volumeMount = new VolumeMountBuilder()
          .withName(volumeName)
          .withMountPath(shuffleDir)
          .build()
        (volume, volumeMount)
      }
    })

    val factory = new ExecutorPodFactoryImpl(
      conf,
      nodeAffinityExecutorPodModifier,
      None,
      None,
      None,
      None,
      Some(shuffleManager))
    val executor = factory.createExecutorPod(
      "1", "dummy", "dummy", Seq[(String, String)](), driverPod, Map[String, Int]())

    verify(nodeAffinityExecutorPodModifier, times(1))
      .addNodeAffinityAnnotationIfUseful(any(classOf[Pod]), any(classOf[Map[String, Int]]))

    assert(executor.getSpec.getContainers.size() === 1)
    assert(executor.getSpec.getContainers.get(0).getVolumeMounts.size() === 1)
    assert(executor.getSpec.getContainers.get(0).getVolumeMounts.get(0).getName === "0-tmp")
    assert(executor.getSpec.getContainers.get(0).getVolumeMounts.get(0)
      .getMountPath === "/tmp")
    checkOwnerReferences(executor, driverPodUid)
  }

  test("Small-files add a secret & secret volume mount to the container") {
    val conf = baseConf.clone()

    val smallFiles = new MountSmallFilesBootstrapImpl("secret1", "/var/secret1")
    val factory = new ExecutorPodFactoryImpl(
      conf,
      nodeAffinityExecutorPodModifier,
      None,
      Some(smallFiles),
      None,
      None,
      None)
    val executor = factory.createExecutorPod(
      "1", "dummy", "dummy", Seq[(String, String)](), driverPod, Map[String, Int]())

    verify(nodeAffinityExecutorPodModifier, times(1))
      .addNodeAffinityAnnotationIfUseful(any(classOf[Pod]), any(classOf[Map[String, Int]]))

    assert(executor.getSpec.getContainers.size() === 1)
    assert(executor.getSpec.getContainers.get(0).getVolumeMounts.size() === 1)
    assert(executor.getSpec.getContainers.get(0).getVolumeMounts.get(0)
      .getName === "submitted-files")
    assert(executor.getSpec.getContainers.get(0).getVolumeMounts.get(0)
      .getMountPath === "/var/secret1")

    assert(executor.getSpec.getVolumes.size() === 1)
    assert(executor.getSpec.getVolumes.get(0).getSecret.getSecretName === "secret1")

    checkOwnerReferences(executor, driverPodUid)
    checkEnv(executor, Map("SPARK_MOUNTED_FILES_FROM_SECRET_DIR" -> "/var/secret1"))
  }

  test("classpath and extra java options get translated into environment variables") {
    val conf = baseConf.clone()
    conf.set(org.apache.spark.internal.config.EXECUTOR_JAVA_OPTIONS, "foo=bar")
    conf.set(org.apache.spark.internal.config.EXECUTOR_CLASS_PATH, "bar=baz")

    val factory = new ExecutorPodFactoryImpl(
      conf, nodeAffinityExecutorPodModifier, None, None, None, None, None)
    val executor = factory.createExecutorPod(
      "1", "dummy", "dummy", Seq[(String, String)]("qux" -> "quux"), driverPod, Map[String, Int]())

    verify(nodeAffinityExecutorPodModifier, times(1))
      .addNodeAffinityAnnotationIfUseful(any(classOf[Pod]), any(classOf[Map[String, Int]]))

    checkEnv(executor,
      Map("SPARK_JAVA_OPT_0" -> "foo=bar",
          "SPARK_EXECUTOR_EXTRA_CLASSPATH" -> "bar=baz",
          "qux" -> "quux"))
    checkOwnerReferences(executor, driverPodUid)
  }

  // There is always exactly one controller reference, and it points to the driver pod.
  private def checkOwnerReferences(executor: Pod, driverPodUid: String): Unit = {
    assert(executor.getMetadata.getOwnerReferences.size() === 1)
    assert(executor.getMetadata.getOwnerReferences.get(0).getUid === driverPodUid)
    assert(executor.getMetadata.getOwnerReferences.get(0).getController === true)
  }

  // Check that the expected environment variables are present.
  private def checkEnv(executor: Pod, additionalEnvVars: Map[String, String]): Unit = {
    val defaultEnvs = Map(
      constants.ENV_EXECUTOR_ID -> "1",
      constants.ENV_DRIVER_URL -> "dummy",
      constants.ENV_EXECUTOR_CORES -> "1",
      constants.ENV_EXECUTOR_MEMORY -> "1g",
      constants.ENV_APPLICATION_ID -> "dummy",
      constants.ENV_MOUNTED_CLASSPATH -> "/var/spark-data/spark-jars/*",
      constants.ENV_EXECUTOR_POD_IP -> null,
      constants.ENV_EXECUTOR_PORT -> "10000") ++ additionalEnvVars

    assert(executor.getSpec.getContainers.size() === 1)
    assert(executor.getSpec.getContainers.get(0).getEnv().size() === defaultEnvs.size)
    val mapEnvs = executor.getSpec.getContainers.get(0).getEnv.asScala.map {
      x => (x.getName, x.getValue)
    }.toMap
    assert(defaultEnvs === mapEnvs)
  }
}
