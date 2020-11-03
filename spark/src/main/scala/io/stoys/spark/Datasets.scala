package io.stoys.spark

import io.stoys.scala.Strings
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions.{Cast, CreateArray, CreateMap, Expression, Literal}
import org.apache.spark.sql.functions.{array, coalesce, col, struct}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Dataset}

import scala.collection.mutable
import scala.reflect.runtime.universe.TypeTag

object Datasets {
  case class ReshapeConfig(
      /**
       * Should primitive types be coerced?
       *
       * If enabled it will automatically do the usual upcasting like [[Int]] to [[Long]] but not the other way around
       * (as it would loose precision).
       *
       * BEWARE: Everything can be casted implicitly to [[String]]! But often it is not desirable.
       */
      coerceTypes: Boolean,
      /**
       * NOT IMPLEMENTED YET!!! It throws [[ReshapeConfig.ConflictResolution.ERROR]] regardless of the config (for now).
       *
       * What should happen when two conflicting column definitions are encountered?
       *
       * In particular the value [[ReshapeConfig.ConflictResolution.LAST]] is useful. It solves the common pattern
       * of conflicting columns occurring in queries like "SELECT *, "some_value" AS value FROM table" where "table"
       * already has "value".
       */
      conflictResolution: ReshapeConfig.ConflictResolution.ConflictingNameResolution,
      /**
       * Should we drop the extra columns (not present in target schema)?
       *
       * Use false to keep the extra columns (just like [[Dataset.as]] does).
       */
      dropExtraColumns: Boolean,
      /**
       * Should we fail on presence of extra column (not present in target schema)?
       */
      failOnExtraColumn: Boolean,
      /**
       * Should we fail on nullable source field being assigned to non nullable target field?
       *
       * Note: It is useful to ignore nullability by default (just like [[Dataset.as]] does). Spark inferred type
       * for every json and csv field as nullable. This is true even if there is not a single null in the dataset.
       * All parquet files written by spark also have every field nullable for compatibility.
       */
      failOnIgnoringNullability: Boolean,
      /**
       * Should we fill in default values instead of nulls for non nullable columns?
       *
       * Default values are 0 for numbers, false for [[Boolean]], "" for [[String]], empty arrays and nested struct
       * are present filled with the previous rules.
       *
       * Note: It probably does not make sense to use this without also setting fillMissingNulls = true.
       *
       * BEWARE: This is not intended for production code! Filling blindly all columns with default values defeats
       * the purpose of why they are not defined nullable in the first place. It would be error prone to use this.
       * But it may come handy in some notebook exploration or unit tests.
       */
      fillDefaultValues: Boolean,
      /**
       * Should we fill nulls instead of missing nullable columns?
       */
      fillMissingNulls: Boolean,
      /**
       * Should names be normalized before matching?
       *
       * Number of normalizations happen - trim, lowercase, replace non-word characters with underscores, etc.
       * For details see [[Strings.toSnakeCase]].
       *
       * Note: Trim (drop leading and trailing spaces) and lower casing happens even when this is disabled!
       */
      normalizedNameMatching: Boolean,
      /**
       * How should the output columns be sorted?
       *
       * Use [[ReshapeConfig.SortOrder.TARGET]] to get the order of target schema.
       */
      sortOrder: ReshapeConfig.SortOrder.SortOrder,
  )

  object ReshapeConfig {
    object ConflictResolution extends Enumeration {
      type ConflictingNameResolution = Value
      val UNDEFINED, ERROR, FIRST, LAST = Value
    }

    object SortOrder extends Enumeration {
      type SortOrder = Value
      val UNDEFINED, ALPHABETICAL, SOURCE, TARGET = Value
    }

    /**
     * [[ReshapeConfig.as]] behaves the same way as Spark's own [[Dataset.as]].
     */
    val as = ReshapeConfig(
      coerceTypes = false,
      conflictResolution = ReshapeConfig.ConflictResolution.ERROR,
      dropExtraColumns = false,
      failOnExtraColumn = false,
      failOnIgnoringNullability = false,
      fillDefaultValues = false,
      fillMissingNulls = false,
      normalizedNameMatching = false,
      sortOrder = ReshapeConfig.SortOrder.SOURCE,
    )
    val safe = as.copy(
      failOnExtraColumn = true,
      failOnIgnoringNullability = true,
    )
    val default = as.copy(
      coerceTypes = true,
      dropExtraColumns = true,
      sortOrder = ReshapeConfig.SortOrder.TARGET,
    )
    val dangerous = default.copy(
      conflictResolution = ReshapeConfig.ConflictResolution.LAST,
      fillDefaultValues = true,
      fillMissingNulls = true,
      normalizedNameMatching = true,
    )
  }

  case class StructValidationError(
      path: String,
      msg: String,
  )

  class StructValidationException(val errors: Seq[StructValidationError]) extends Exception {
    override def getMessage: String = {
      val errorStrings = errors.map(e => s"Column ${e.path} ${e.msg}")
      s"Errors: [${errorStrings.mkString("\n    - ", "\n    - ", "\n")}]"
    }
  }

  private def defaultValueExpression(dataType: DataType): Expression = {
    dataType match {
      case ArrayType(internalType: DataType, _) =>
        CreateArray(Seq(Literal.default(internalType)))
      case MapType(keyDataType: DataType, valueDataType: DataType, _) =>
        CreateMap(Seq(Literal.default(keyDataType), Literal.default(valueDataType)))
      case _ => Literal.default(dataType)
    }
  }

  private def nullValueExpression(dataType: DataType): Expression = {
    dataType match {
      case ArrayType(internalType: DataType, _) =>
        CreateArray(Seq(Literal.create(null, internalType)))
      case MapType(keyDataType: DataType, valueDataType: DataType, _) =>
        CreateMap(Seq(Literal.create(null, keyDataType), Literal.create(null, valueDataType)))
      case _ => Literal.create(null, dataType)
    }
  }

  private def makeFieldPath(path: String, fieldName: String): String = {
    Seq(path, fieldName).filter(_ != null).mkString(".")
  }

  private def normalizeFieldName(field: StructField, config: ReshapeConfig): String = {
    if (config.normalizedNameMatching) {
      Strings.toSnakeCase(field.name.trim)
    } else {
      field.name.trim.toLowerCase
    }
  }

  private def reshapeStructField(source: StructField, target: StructField, config: ReshapeConfig,
      sourcePath: String, normalizedFieldPath: String): Either[List[StructValidationError], List[Column]] = {
    val sourceFieldPath = makeFieldPath(sourcePath, source.name)

    val errors = mutable.Buffer.empty[StructValidationError]
    var column = col(sourceFieldPath)
    if (source.nullable && !target.nullable) {
      if (config.failOnIgnoringNullability) {
        errors += StructValidationError(normalizedFieldPath, "is nullable but target column is not")
      } else if (config.fillDefaultValues) {
        column = coalesce(column, new Column(defaultValueExpression(target.dataType)))
      }
    }
    (source.dataType, target.dataType) match {
      case (sourceDataType, targetDataType) if sourceDataType == targetDataType => // pass
      case (sourceStructType: StructType, targetStructType: StructType) =>
        reshapeStructType(sourceStructType, targetStructType, config, sourceFieldPath, normalizedFieldPath) match {
          case Right(nestedColumns) => column = struct(nestedColumns: _*)
          case Left(nestedErrors) => errors ++= nestedErrors
        }
      case (_: StructType, targetDataType) =>
        errors += StructValidationError(normalizedFieldPath, s"struct cannot be casted to target type $targetDataType")
      case (sourceDataType, _: StructType) if !sourceDataType.isInstanceOf[NullType] =>
        errors += StructValidationError(normalizedFieldPath, s"source type $sourceDataType cannot be casted to struct")
      case (sourceArrayType: ArrayType, targetArrayType: ArrayType) =>
        val sourceFieldType = StructField(null, sourceArrayType.elementType)
        val targetFieldType = StructField(null, targetArrayType.elementType)
        reshapeStructField(sourceFieldType, targetFieldType, config, sourceFieldPath, normalizedFieldPath) match {
          case Right(nestedColumns) =>
            assert(nestedColumns.size == 1)
            column = array(nestedColumns: _*)
          case Left(nestedErrors) =>
            errors ++= nestedErrors
        }
      case (_: ArrayType, targetDataType) =>
        errors += StructValidationError(normalizedFieldPath, s"array cannot be casted to target type $targetDataType")
      case (sourceDataType, _: ArrayType) if !sourceDataType.isInstanceOf[NullType] =>
        errors += StructValidationError(normalizedFieldPath, s"source type $sourceDataType cannot be casted to array")
      case (sourceDataType, targetDataType) =>
        if (Cast.canCast(sourceDataType, targetDataType) && config.coerceTypes) {
          column = column.cast(targetDataType)
        } else {
          errors += StructValidationError(normalizedFieldPath,
            s"type $sourceDataType cannot be casted to target type $targetDataType")
        }
    }

    if (errors.isEmpty) {
      Right(List(Option(target.name).map(tn => column.as(tn)).getOrElse(column)))
    } else {
      Left(errors.toList)
    }
  }

  private def reshapeStructType(sourceStruct: StructType, targetStruct: StructType, config: ReshapeConfig,
      sourcePath: String, normalizedPath: String): Either[List[StructValidationError], List[Column]] = {
    val sourceFieldsByName = sourceStruct.fields.toList.groupBy(f => normalizeFieldName(f, config))
    val targetFieldsByName = targetStruct.fields.toList.groupBy(f => normalizeFieldName(f, config))

    val normalizedFieldNames = config.sortOrder match {
      case ReshapeConfig.SortOrder.ALPHABETICAL =>
        (sourceStruct.fields ++ targetStruct.fields).map(f => normalizeFieldName(f, config)).toSeq.distinct.sorted
      case ReshapeConfig.SortOrder.SOURCE =>
        (sourceStruct.fields ++ targetStruct.fields).map(f => normalizeFieldName(f, config)).toSeq.distinct
      case ReshapeConfig.SortOrder.TARGET | ReshapeConfig.SortOrder.UNDEFINED =>
        (targetStruct.fields ++ sourceStruct.fields).map(f => normalizeFieldName(f, config)).toSeq.distinct
    }

    val resultColumns = normalizedFieldNames.map { normalizedFieldName =>
      val normalizedFieldPath = makeFieldPath(normalizedPath, normalizedFieldName)
      val sourceFields = sourceFieldsByName.getOrElse(normalizedFieldName, List.empty)
      val targetFields = targetFieldsByName.getOrElse(normalizedFieldName, List.empty)
      (sourceFields, targetFields) match {
        case (Nil, target :: Nil) =>
          if (target.nullable && config.fillMissingNulls) {
            Right(List(new Column(nullValueExpression(target.dataType)).as(target.name)))
          } else if (config.fillDefaultValues) {
            Right(List(new Column(defaultValueExpression(target.dataType)).as(target.name)))
          } else {
            Left(List(StructValidationError(normalizedFieldPath, "is missing")))
          }
        case (source :: Nil, target :: Nil) =>
          reshapeStructField(source, target, config, sourcePath, normalizedFieldPath)
        case (sources, target :: Nil) =>
          // TODO: ConflictResolution is more complicated than this
//          config.conflictResolution match {
//            case ReshapeConfig.ConflictResolution.ERROR | ReshapeConfig.ConflictResolution.UNDEFINED =>
//              Left(List(StructValidationError(normalizedFieldPath, s"has ${sources.size} conflicting occurrences")))
//            case ReshapeConfig.ConflictResolution.FIRST =>
//              reshapeStructField(sources.head, target, config, sourcePath, normalizedFieldPath)
//            case ReshapeConfig.ConflictResolution.LAST =>
//              reshapeStructField(sources.last, target, config, sourcePath, normalizedFieldPath)
//          }
          Left(List(StructValidationError(normalizedFieldPath, s"has ${sources.size} conflicting occurrences")))
        case (sources, Nil) =>
          if (config.failOnExtraColumn) {
            Left(List(StructValidationError(normalizedFieldPath, s"unexpectedly present (${sources.size}x)")))
          } else {
            if (config.dropExtraColumns) {
              Right(List.empty)
            } else {
              // TODO: Do we need ConflictResolution here?
              Right(sources.map(f => col(makeFieldPath(sourcePath, f.name))))
            }
          }
        case (_, targets) if targets.size > 1 =>
          Left(List(StructValidationError(normalizedFieldPath, s"has ${targets.size} occurrences in target struct")))
      }
    }

    resultColumns.toList.partition(_.isLeft) match {
      case (Nil, columns) => Right(columns.flatMap(_.right.toOption).flatten)
      case (errors, _) => Left(errors.flatMap(_.left.toOption).flatten)
    }
  }

  /**
   * [[reshape]][T] is similar to spark's own [[Dataset.as]][T] but way more powerful and configurable.
   *
   * See [[ReshapeConfig]] for supported configuration and/or unit tests for examples.
   *
   * BEWARE: The same config is applied on all the columns (even nested).
   *
   * @param ds input [[Dataset]] or [[DataFrame]]
   * @param config configuration - see [[ReshapeConfig]] for details
   * @tparam T case class to which we are casting the df
   * @return [[Dataset]][T] with only the columns present in case class and in that order
   */
  def reshape[T <: Product : TypeTag](ds: Dataset[_], config: ReshapeConfig = ReshapeConfig.default): Dataset[T] = {
    import ds.sparkSession.implicits._

    val actualSchema = ds.schema
    val expectedSchema = ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType]

    if (actualSchema == expectedSchema) {
      ds.as[T]
    } else {
      reshapeStructType(actualSchema, expectedSchema, config, sourcePath = null, normalizedPath = null) match {
        case Left(errors) => throw new StructValidationException(errors)
        case Right(columns) => ds.select(columns: _*).as[T]
      }
    }
  }

  class RichDataset(val ds: Dataset[_]) extends AnyVal {
    def reshape[T <: Product : TypeTag]: Dataset[T] = {
      Datasets.reshape[T](ds)
    }

    def reshape[T <: Product : TypeTag](config: ReshapeConfig): Dataset[T] = {
      Datasets.reshape[T](ds, config)
    }
  }
}
