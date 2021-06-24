/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com._4paradigm.openmldb.batch.end2end

import com._4paradigm.openmldb.batch.api.OpenmldbSession
import com._4paradigm.openmldb.batch.utils.SparkUtil
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.FunSuite


class TestWindow extends FunSuite {

  test("Test end2end window aggregation") {

    val spark = SparkSession.builder().master("local[*]")
      .getOrCreate()
    val sess = new OpenmldbSession(spark)

    val data = Seq(
      Row(1, "tom", 100, 1),
      Row(2, "amy", 200, 2),
      Row(3, "tom", 300, 3),
      Row(4, "amy", 400, 4),
      Row(5, "tom", 500, 5),
      Row(6, "amy", 600, 6),
      Row(7, "tom", 700, 7),
      Row(8, "amy", 800, 8),
      Row(9, "tom", 900, 9),
      Row(10, "amy", 1000, 10))
    val schema = StructType(List(
      StructField("id", IntegerType),
      StructField("user", StringType),
      StructField("trans_amount", IntegerType),
      StructField("trans_time", IntegerType)))
    val df = spark.createDataFrame(spark.sparkContext.makeRDD(data), schema)

    sess.registerTable("t1", df)
    df.createOrReplaceTempView("t1")

    val sqlText = "SELECT sum(trans_amount) OVER w AS w_sum_amount FROM t1 WINDOW w AS (PARTITION BY user ORDER BY trans_time ROWS BETWEEN 10 PRECEDING AND CURRENT ROW)"
    val outputDf = sess.sql(sqlText)

    val sparksqlOutputDf = sess.sparksql(sqlText)
    // Notice that the sum column type is different for SparkSQL
    assert(SparkUtil.approximateDfEqual(outputDf.getSparkDf(), sparksqlOutputDf, false))
  }

}
