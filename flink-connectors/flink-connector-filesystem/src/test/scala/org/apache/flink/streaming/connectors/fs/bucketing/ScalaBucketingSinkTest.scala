/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.fs.bucketing

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.streaming.api.operators.StreamSink
import org.apache.flink.streaming.connectors.fs.SequenceFileWriter
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSinkTestUtils.PART_PREFIX
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness
import org.apache.flink.util.NetUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path}
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hadoop.io.{IntWritable, Text}
import org.junit.rules.TemporaryFolder
import org.junit.{Assert, Rule, Test}

class ScalaBucketingSinkTest extends BucketingSinkTest {
  val _tempFolder = new TemporaryFolder

  @Rule
  def tempFolder: TemporaryFolder = _tempFolder

  @Test
  def test(): Unit = {

    var hdfsCluster: MiniDFSCluster = null
    var dfs: org.apache.hadoop.fs.FileSystem = null
    var hdfsURI: String = null
    val conf = new Configuration
    val dataDir = tempFolder.newFolder

    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, dataDir.getAbsolutePath)
    val builder = new MiniDFSCluster.Builder(conf)
    hdfsCluster = builder.build

    dfs = hdfsCluster.getFileSystem
    hdfsURI = "hdfs://" + NetUtils.hostAndPortToUrlString(hdfsCluster.getURI.getHost, hdfsCluster.getNameNodePort) + "/"
    val outPath = hdfsURI + "/seq-no-comp-non-rolling-out"

    val numElements = 20

    val sink = new BucketingSink[Tuple2[IntWritable, Text]](outPath)
      .setWriter(new SequenceFileWriter[IntWritable, Text])
      .setBucketer(new BasePathBucketer[Tuple2[IntWritable, Text]])
      .setPartPrefix(PART_PREFIX).setPendingPrefix("")
      .setPendingSuffix("")


    sink.setInputType(TypeInformation.of(new TypeHint[Tuple2[IntWritable, Text]]() {}), new ExecutionConfig)
    val testHarness = createTestSink(sink, 1, 0)

    testHarness.setProcessingTime(0L)
    testHarness.setup()
    testHarness.open()

    var i = 0
    while (i < numElements) {
      testHarness.processElement(new StreamRecord[Tuple2[IntWritable, Text]](Tuple2.of(new IntWritable(i), new Text("message #" + Integer.toString(i)))))
      i += 1
    }

    testHarness.close()
    val inStream = dfs.open(new Path(outPath + "/" + PART_PREFIX + "-0-0"))
    val reader = new org.apache.hadoop.io.SequenceFile.Reader(inStream, 1000, 0, 100000, new Configuration)
    val intWritable = new IntWritable
    val txt = new Text

    i = 0
    while (i < numElements) {
      reader.next(intWritable, txt)
      Assert.assertEquals(i, intWritable.get)
      Assert.assertEquals("message #" + i, txt.toString)
      i += 1
    }

    reader.close()
    inStream.close()
  }

  @throws[Exception]
  private def createTestSink[T](sink: BucketingSink[T], totalParallelism: Int, taskIdx: Int) = new OneInputStreamOperatorTestHarness[T, AnyRef](new StreamSink[T](sink), 10, totalParallelism, taskIdx)
}
