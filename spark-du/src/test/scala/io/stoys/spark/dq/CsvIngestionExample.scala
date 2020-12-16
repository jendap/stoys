package io.stoys.spark.dq

import java.nio.file.Files
import java.sql.Date

import io.stoys.scala.{Arbitrary, IO}
import io.stoys.spark.{Reshape, ReshapeConfig, SparkIO, SparkIOConfig, TableName}
import io.stoys.spark.test.SparkTestBase

class CsvIngestionExample extends SparkTestBase {
  import CsvIngestionExample._
  import io.stoys.spark.test.implicits._

  test("ingestion") {
    val recordsCsvPath = tmpDir.resolve("records.csv")
    val recordsCsvContent =
      """
        |id,date,foo_bar_baz,value
        |1,02/20/2020,foo,42
        |1,02/20/2020,meh,1234
        |""".stripMargin.trim
    Files.writeString(recordsCsvPath, recordsCsvContent)
    val inputPath = s"$recordsCsvPath?sos-table_name=raw_record&sos-format=csv&header=true"

    // dq
    // TODO: get primaryKeyFieldNames from annotations
    val primaryKeyFieldNames = Seq("id")
    val fields = DqReflection.getDqFields[RawRecord]
    val expectedViolatedRuleStatistics = Seq(
      DqRuleStatistics("_primary_key__unique", 2),
      DqRuleStatistics("foo_bar_baz__enum_values", 1)
    )

    val dq = Dq.fromFileInputPath(sparkSession, inputPath).fields(fields).primaryKeyFieldNames(primaryKeyFieldNames)
    val dqResult = dq.computeDqResult().collect().head
    assert(dqResult.statistics.rule.filter(_.violations > 0) === expectedViolatedRuleStatistics)

    // ingestion
    val sparkIOConfig = Arbitrary.empty[SparkIOConfig].copy(
      inputPaths = Seq(inputPath),
      inputReshapeConfig = ReshapeConfig.default.copy(fillMissingNulls = true)
    )
    val expectedRawRecords = Seq(
      RawRecord(1, null, "foo", 42),
      RawRecord(1, null, "meh", 1234)
    )
    // TODO: can we automatically generate this sql from the annotations?
    val ingestRecordsSql =
      """
        |SELECT
        |  id,
        |  value,
        |  TO_DATE(date, 'MM/dd/yyyy') AS date,
        |  IF(UPPER(foo_bar_baz) IN ('FOO', 'BAR', 'BAZ'), UPPER(foo_bar_baz), null) AS fbr_enum
        |FROM raw_record
        |""".stripMargin.trim
    val expectedRecords = Seq(
      Record(1, 42, "2020-02-20", "FOO"),
      Record(1, 1234, "2020-02-20", null)
    )

    IO.using(new SparkIO(sparkSession, sparkIOConfig)) { sparkIO =>
      sparkIO.init()
      assert(sparkIO.ds(TableName[RawRecord]).collect() === expectedRawRecords)
      assert(Reshape.reshape[Record](sparkSession.sql(ingestRecordsSql)).collect() === expectedRecords)
    }
  }
}

object CsvIngestionExample {
  import annotation.DqField

  case class RawRecord(
      id: Int,
      @DqField(nullable = true, format = "MM/dd/yyyy")
      date: Date,
      @DqField(nullable = false, enumValues = Array("foo", "bar", "baz"))
      foo_bar_baz: String,
      value: Int
  )

  case class Record(
      id: Int,
      value: Int,
      date: Date,
      fbr_enum: String
  )
}
