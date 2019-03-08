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
package org.apache.spark.deploy.kubernetes.submit

import org.apache.spark.{SecurityManager, SparkConf, SparkFunSuite}
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.kubernetes.KubernetesExternalShuffleService
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.shuffle.kubernetes.KubernetesExternalShuffleClientImpl

private[spark] class KubernetesExternalShuffleServiceSuite extends SparkFunSuite {

  private val SPARK_CONF = new SparkConf()
      .set(KUBERNETES_SHUFFLE_USE_SERVICE_ACCOUNT_CREDENTIALS, false)

  test("Run kubernetes shuffle service.") {
    val shuffleService = new KubernetesExternalShuffleService(
      SPARK_CONF,
      new SecurityManager(SPARK_CONF))

    val shuffleClient = new KubernetesExternalShuffleClientImpl(
      SparkTransportConf.fromSparkConf(SPARK_CONF, "shuffle"),
      new SecurityManager(SPARK_CONF),
      false)

    shuffleService.start()
    shuffleClient.init("newapp")

    // verifies that we can connect to the shuffle service and send
    // it a message.
    shuffleClient.registerDriverWithShuffleService("localhost", 7337)
    shuffleService.stop()
  }
}
