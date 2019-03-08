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
package org.apache.spark.deploy.k8s.integrationtest.jobs

import java.io.File
import java.nio.file.Paths

import com.google.common.base.Charsets
import com.google.common.io.Files

import org.apache.spark.SparkException
import org.apache.spark.sql.SparkSession

private[spark] object FileExistenceTest {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      throw new IllegalArgumentException(
          s"Invalid args: ${args.mkString}, " +
            "Usage: FileExistenceTest <source-file> <expected contents>")
    }
    // Can't use SparkContext.textFile since the file is local to the driver
    val file = Paths.get(args(0)).toFile
    if (!file.exists()) {
      throw new SparkException(s"Failed to find file at ${file.getAbsolutePath}")
    } else {
      // scalastyle:off println
      val contents = Files.toString(file, Charsets.UTF_8)
      if (args(1) != contents) {
        throw new SparkException(s"Contents do not match. Expected: ${args(1)}," +
          s" actual: $contents")
      } else {
        println(s"File found at ${file.getAbsolutePath} with correct contents.")
      }
      val spark = SparkSession.builder().getOrCreate().sparkContext
      val fileNamesRdd = spark.parallelize(Seq(args(0)))
      if (fileNamesRdd.filter(fileName => new File(fileName).isFile()).count() != 1) {
        throw new SparkException(s"Executors do not have the file ${args(0)}.")
      }
      val matchingContents = fileNamesRdd.map { fileName =>
        Files.toString(new File(fileName), Charsets.UTF_8)
      }.filter(_.equals(args(1)))
      if (matchingContents.count() != 1) {
        throw new SparkException(s"The file on the executors at ${args(0)} did not have" +
          s" the correct contents.")
      }
      println(s"File found on the executors at the relative path ${args(0)} with the" +
        s" correct contents.")
      // scalastyle:on println
    }
    while (true) {
      Thread.sleep(600000)
    }
  }

}
