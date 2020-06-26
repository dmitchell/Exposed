package org.jetbrains.exposed.sql.vendors

import org.jetbrains.exposed.exceptions.UnsupportedByDialectException
import org.jetbrains.exposed.exceptions.throwUnsupportedException
import org.jetbrains.exposed.sql.*
import java.util.*

/**
 * Snowflake
 *   try to avoid `insert` and `update`. @see https://docs.snowflake.com/en/user-guide-data-load.html
 *      tl;dr; snowflake records are immutable. adding or removing records requires writing new s3 buckets;
 *      so, you want to make as many changes in one transaction as possible preferably via csv or other
 *      mass ingest.
 *   for numerics, number of bytes doesn't matter.
 *     "For each micro-partition, Snowflake determines the minimum and maximum values for a given column and uses that
 *     range to store all values for that column in the partition"
 *   for string types, 16mb is the limit but DataTypeProvider says sb _unlimited_
 */
internal object SnowflakeDataTypeProvider : DataTypeProvider() {
    /** TODO serializer for
     * Snowflake supports the following special values for FLOAT/DOUBLE:
     * 'inf' (infinity).
     * '-inf' (negative infinity).
     * Will need to change DoubleColumnType and FloatColumnType like BooleanColumnType.valueFromDB
     */

    /** limit is 8mb */
    override fun binaryType(): String = "BINARY"

    private const val NO_BLOB = "Snowflake does not support BLOB"
    override fun blobType(): String {
        exposedLogger.error(NO_BLOB)
        error(NO_BLOB)
    }

    override fun uuidType(): String = "VARCHAR"
    override fun uuidToDB(value: UUID): Any {
        return value.toString()
    }

    override fun dateTimeType(): String = "TIMESTAMP_LTZ"

    override fun integerAutoincType(): String = "INT AUTOINCREMENT"
    override fun longAutoincType(): String = "INT AUTOINCREMENT"

    override fun processForDefaultValue(e: Expression<*>): String = when {
        e is LiteralOp<*> && (e.columnType is IDateColumnType) -> "to_date(${super.processForDefaultValue(e)})"
        e is LiteralOp<*> && (e.columnType is IDateTimeColumnType) -> "to_timestamp(${super.processForDefaultValue(e)})"
        else -> super.processForDefaultValue(e)
    }
}

/**
 * Snowflake supports MERGE _but_ has no concept of unique keys. The MERGE query must
 * provide a subquery for detecting collision/matches.
 * {FunctionProvider.replace} does not provide a means to supply such a parameter. If needed,
 * we could add it to that method and ensure all impls handle it correctly or throw if provided.
 */
internal object SnowflakeFunctionProvider : FunctionProvider() {

    /**
     * Not quite the same as sql group concat in that this returns a real array
     */
    override fun <T : String?> groupConcat(expr: GroupConcat<T>, queryBuilder: QueryBuilder): Unit = queryBuilder {
        append("GROUP_CONCAT(")
        if (expr.distinct) {
            append("DISTINCT ")
        }
        append(expr.expr)
        append(")")
        if (expr.orderBy.isNotEmpty()) {
            append(" within group (")
            expr.orderBy.toList().appendTo(prefix = " ORDER BY ") {
                append(it.first, " ", it.second.name)
            }
            append(")")
        }
        expr.separator?.let {
            exposedLogger.warn("Snowflake groupConcat does not support separator")
        }
    }

    override fun update(target: Table, columnsAndValues: List<Pair<Column<*>, Any?>>, limit: Int?, where: Op<Boolean>?, transaction: Transaction): String {
        if (limit != null) {
            transaction.throwUnsupportedException("Snowflake does not support `limit` on `update`")
        }
        return super.update(target, columnsAndValues, limit, where, transaction)
    }

    override fun delete(ignore: Boolean, table: Table, where: String?, limit: Int?, transaction: Transaction): String {
        if (limit != null) {
            transaction.throwUnsupportedException("Snowflake does not support `limit` on `update`")
        }
        return super.delete(ignore, table, where, limit, transaction)
    }
}


open class SnowflakeDialect : VendorDialect(dialectName, SnowflakeDataTypeProvider, SnowflakeFunctionProvider) {
    override val defaultReferenceOption = ReferenceOption.NO_ACTION

    /* TODO temporary disable schema creation */
    override val supportsCreateSchema: Boolean
        get() = false

    companion object {
        const val dialectName: String = "snowflake"
    }

    override fun setSchema(schema: Schema): String {
        return "USE SCHEMA ${schema.identifier}"
    }

    override fun createIndex(index: Index): String {
        throw UnsupportedByDialectException("Snowflake does not support indexes", this)
    }

    override fun dropIndex(tableName: String, indexName: String): String {
        throw UnsupportedByDialectException("Snowflake does not support indexes", this)
    }

    // timestamp must have parens
    private val illegalDefaults = Regex("(NOW|TIMESTAMP[^(])", RegexOption.IGNORE_CASE)
    override fun isAllowedAsColumnDefault(e: Expression<*>): Boolean {
        if (super.isAllowedAsColumnDefault(e)) return true
        // snowflake allows any expression which evaluates to a scalar which can
        // obv be rather complex (e.g., abs(day(current_timestamp())) * 99)
        // Let the db reject the statement rather than trying to proactively reject it
        return !illegalDefaults.matches(e.toString())
    }
}
