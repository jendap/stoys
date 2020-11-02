package io.stoys.spark.datasources

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkException
import org.apache.spark.sql.types._

case class BinaryFilePerRow(path: String, content: Array[Byte])

case class TextFilePerRow(path: String, content: String)

object FilePerRow {
  private[datasources] case class FieldIndexes(path: Int, content: Int)

  private[datasources] def findFieldIndexesOption(schema: StructType): Option[FieldIndexes] = {
    val pathFieldIndexes = collectFiledIndexes(schema, "path", StringType)
    val binaryContentFieldIndexes = collectFiledIndexes(schema, "content", BinaryType)
    val stringContentFieldIndexes = collectFiledIndexes(schema, "content", StringType)
    val contentFieldIndexes = binaryContentFieldIndexes ++ stringContentFieldIndexes
    (pathFieldIndexes, contentFieldIndexes) match {
      case (Seq(pathFieldIndex), Seq(contentFieldIndex)) => Some(FieldIndexes(pathFieldIndex, contentFieldIndex))
      case _ => None
    }
  }

  private[datasources] def findFieldIndexes(schema: StructType): FieldIndexes = {
    findFieldIndexesOption(schema) match {
      case None =>
        val msg = "The table has to have column 'path' (of StringType) and 'content' (of StringType or BinaryType)."
        throw new SparkException(s"Unsupported schema! $msg Not '$schema'.'")
      case Some(fieldIndexes) => fieldIndexes
    }
  }

  private def collectFiledIndexes(schema: StructType, fieldName: String, dataType: DataType): Seq[Int] = {
    schema.fields.zipWithIndex.collect {
      case (StructField(name, dt, _, _), index) if name.equalsIgnoreCase(fieldName) && dt == dataType => index
    }
  }

  private[datasources] def getCustomFilePath(basePath: String, relativePath: String): Path = {
    val parentDfsPath = new Path(basePath).getParent
    val dfsPath = new Path(parentDfsPath, relativePath)
    if (!dfsPath.toUri.normalize().toString.startsWith(parentDfsPath.toUri.normalize().toString)) {
      throw new SparkException(s"File path '$dfsPath' has to stay in output directory '$parentDfsPath'.")
    }
    dfsPath
  }
}