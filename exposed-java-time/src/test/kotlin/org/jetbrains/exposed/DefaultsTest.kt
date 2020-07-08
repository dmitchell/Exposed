package org.jetbrains.exposed

import org.jetbrains.exposed.dao.IntEntity
import org.jetbrains.exposed.dao.IntEntityClass
import org.jetbrains.exposed.dao.flushCache
import org.jetbrains.exposed.dao.id.EntityID
import org.jetbrains.exposed.dao.id.IntIdTable
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.SqlExpressionBuilder.eq
import org.jetbrains.exposed.sql.`java-time`.*
import org.jetbrains.exposed.sql.statements.BatchDataInconsistentException
import org.jetbrains.exposed.sql.statements.BatchInsertStatement
import org.jetbrains.exposed.sql.tests.DatabaseTestsBase
import org.jetbrains.exposed.sql.tests.TestDB
import org.jetbrains.exposed.sql.tests.currentDialectTest
import org.jetbrains.exposed.sql.tests.inProperCase
import org.jetbrains.exposed.sql.tests.shared.assertEqualCollections
import org.jetbrains.exposed.sql.tests.shared.assertEqualLists
import org.jetbrains.exposed.sql.tests.shared.assertEquals
import org.jetbrains.exposed.sql.tests.shared.expectException
import org.jetbrains.exposed.sql.vendors.*
import org.junit.Assume
import org.junit.Test
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneOffset

class DefaultsTest : DatabaseTestsBase() {
    object TableWithDBDefault : IntIdTable() {
        var cIndex = 0
        val field = varchar("field", 100)
        val t1 = datetime("t1").defaultExpression(CurrentDateTime())
        val clientDefault = integer("clientDefault").clientDefault { cIndex++ }
    }

    class DBDefault(id: EntityID<Int>): IntEntity(id) {
        var field by TableWithDBDefault.field
        var t1 by TableWithDBDefault.t1
        val clientDefault by TableWithDBDefault.clientDefault

        override fun equals(other: Any?): Boolean {
            return (other as? DBDefault)?.let { id == it.id && field == it.field && equalDateTime(t1, it.t1) } ?: false
        }

        override fun hashCode(): Int = id.value.hashCode()

        companion object : IntEntityClass<DBDefault>(TableWithDBDefault)
    }

    @Test
    fun testDefaultsWithExplicit01() {
        withTables(TableWithDBDefault) {
            val created = listOf(
                    DBDefault.new { field = "1" },
                    DBDefault.new {
                        field = "2"
                        t1 = LocalDateTime.now().minusDays(5)
                    })
            commit()
            created.forEach {
                DBDefault.removeFromCache(it)
            }

            val entities = DBDefault.all().toList()
            assertEqualCollections(created.map { it.id }, entities.map { it.id })
        }
    }

    @Test
    fun testDefaultsWithExplicit02() {
        withTables(TableWithDBDefault) {
            val created = listOf(
                    DBDefault.new{
                        field = "2"
                        t1 = LocalDateTime.now().minusDays(5)
                    }, DBDefault.new{ field = "1" })

            flushCache()
            created.forEach {
                DBDefault.removeFromCache(it)
            }
            val entities = DBDefault.all().toList()
            assertEqualCollections(created, entities)
        }
    }

    @Test
    fun testDefaultsInvokedOnlyOncePerEntity() {
        withTables(TableWithDBDefault) {
            TableWithDBDefault.cIndex = 0
            val db1 = DBDefault.new{ field = "1" }
            val db2 = DBDefault.new{ field = "2" }
            flushCache()
            assertEquals(0, db1.clientDefault)
            assertEquals(1, db2.clientDefault)
            assertEquals(2, TableWithDBDefault.cIndex)
        }
    }

    private val initBatch = listOf<(BatchInsertStatement) -> Unit>({
        it[TableWithDBDefault.field] = "1"
    }, {
        it[TableWithDBDefault.field] = "2"
        it[TableWithDBDefault.t1] = LocalDateTime.now()
    })

    @Test
    fun testRawBatchInsertFails01() {
        withTables(TableWithDBDefault) {
            expectException<BatchDataInconsistentException> {
                BatchInsertStatement(TableWithDBDefault).run {
                    initBatch.forEach {
                        addBatch()
                        it(this)
                    }
                }
            }
        }
    }

    @Test
    fun testBatchInsertNotFails01() {
        withTables(TableWithDBDefault) {
            TableWithDBDefault.batchInsert(initBatch) { foo ->
                foo(this)
            }
        }
    }

    @Test
    fun testBatchInsertFails01() {
        withTables(TableWithDBDefault) {
            expectException<BatchDataInconsistentException> {
                TableWithDBDefault.batchInsert(listOf(1)) {
                    this[TableWithDBDefault.t1] = LocalDateTime.now()
                }
            }
        }
    }

    @Test
    fun testDefaults01() {
        val currentDT = CurrentDateTime()
        val nowExpression = object : Expression<LocalDateTime>() {
            override fun toQueryBuilder(queryBuilder: QueryBuilder) = queryBuilder {
                +when (val dialect = currentDialectTest) {
                    is OracleDialect -> "SYSDATE"
                    is SQLServerDialect -> "GETDATE()"
                    is MysqlDialect -> if (dialect.isFractionDateTimeSupported()) "NOW(6)" else "NOW()"
                    is SnowflakeDialect -> "CURRENT_TIMESTAMP()"
                    else -> "NOW()"
                }
            }
        }
        val dtConstValue = LocalDate.of(2010, 1, 1)
        val dLiteral = dateLiteral(dtConstValue)
        val dtLiteral = dateTimeLiteral(dtConstValue.atStartOfDay())
        val tsConstValue = dtConstValue.atStartOfDay(ZoneOffset.UTC).plusSeconds(42).toInstant()
        val tsLiteral = timestampLiteral(tsConstValue)
        val TestTable = object : IntIdTable("t") {
            val s = varchar("s", 100).default("test")
            val sn = varchar("sn", 100).default("testNullable").nullable()
            val l = long("l").default(42)
            val c = char("c").default('X')
            val t1 = datetime("t1").defaultExpression(currentDT)
            val t2 = datetime("t2").defaultExpression(nowExpression)
            val t3 = datetime("t3").defaultExpression(dtLiteral)
            val t4 = date("t4").default(dtConstValue)
            val t5 = timestamp("t5").default(tsConstValue)
            val t6 = timestamp("t6").defaultExpression(tsLiteral)
        }

        fun Expression<*>.itOrNull() = when {
            currentDialectTest.isAllowedAsColumnDefault(this)  ->
                "DEFAULT ${currentDialectTest.dataTypeProvider.processForDefaultValue(this)} NOT NULL"
            else -> "NULL"
        }

        withTables(listOf(TestDB.SQLITE), TestTable) {
            val dtType = currentDialectTest.dataTypeProvider.dateTimeType()
            val q = db.identifierManager.quoteString
            // snowflake wraps the table name in double quotes
            val tableName = if (currentDialect is SnowflakeDialect) "\"${"t".inProperCase()}\"" else "${"t".inProperCase()}"

            val baseExpression = "CREATE TABLE " + addIfNotExistsIfSupported() +
                    "${tableName} " +
                    "(${"id".inProperCase()} ${currentDialectTest.dataTypeProvider.integerAutoincType()} PRIMARY KEY, " +
                    "${"s".inProperCase()} VARCHAR(100) DEFAULT 'test' NOT NULL, " +
                    "${"sn".inProperCase()} VARCHAR(100) DEFAULT 'testNullable' NULL, " +
                    "${"l".inProperCase()} ${currentDialectTest.dataTypeProvider.longType()} DEFAULT 42 NOT NULL, " +
                    "$q${"c".inProperCase()}$q CHAR DEFAULT 'X' NOT NULL, " +
                    "${"t1".inProperCase()} $dtType ${currentDT.itOrNull()}, " +
                    "${"t2".inProperCase()} $dtType ${nowExpression.itOrNull()}, " +
                    "${"t3".inProperCase()} $dtType ${dtLiteral.itOrNull()}, " +
                    "${"t4".inProperCase()} DATE ${dLiteral.itOrNull()}, " +
                    "${"t5".inProperCase()} $dtType ${tsLiteral.itOrNull()}, " +
                    "${"t6".inProperCase()} $dtType ${tsLiteral.itOrNull()}" +
                    ")"

            val expected = if (currentDialectTest is OracleDialect)
                arrayListOf("CREATE SEQUENCE t_id_seq", baseExpression)
            else
                arrayListOf(baseExpression)

            assertEqualLists(expected, TestTable.ddl)

            val row1 = if (currentDialect is SnowflakeDialect) {
                TestTable.insert {  }
                TestTable.selectAll().limit(1).orderBy( TestTable.id, SortOrder.DESC ).single()
            } else {
                val id1 = TestTable.insertAndGetId { }
                TestTable.select { TestTable.id eq id1 }.single()
            }
            assertEquals("test", row1[TestTable.s])
            assertEquals("testNullable", row1[TestTable.sn])
            assertEquals(42, row1[TestTable.l])
            assertEquals('X', row1[TestTable.c])
            assertEqualDateTime(dtConstValue.atStartOfDay(), row1[TestTable.t3])
            assertEqualDateTime(dtConstValue, row1[TestTable.t4])
            assertEqualDateTime(tsConstValue, row1[TestTable.t5])
            assertEqualDateTime(tsConstValue, row1[TestTable.t6])
        }
    }

    @Test
    fun testDefaultExpressions01() {
        Assume.assumeFalse(currentDialect is SnowflakeDialect)

        fun abs(value: Int) = object : ExpressionWithColumnType<Int>() {
            override fun toQueryBuilder(queryBuilder: QueryBuilder) = queryBuilder { append("ABS($value)") }

            override val columnType: IColumnType = IntegerColumnType()
        }

        val foo = object : IntIdTable("foo") {
            val name = text("name")
            val defaultDateTime = datetime("defaultDateTime").defaultExpression(CurrentDateTime())
            val defaultInt = integer("defaultInteger").defaultExpression(abs(-100))
        }

        withTables(foo) {
            val result = if (currentDialect is SnowflakeDialect) {
                foo.insert {  it[foo.name] = "bar" }
                foo.selectAll().limit(1).orderBy( foo.id, SortOrder.DESC ).single()
            } else {
                val id = foo.insertAndGetId {
                    it[foo.name] = "bar"
                }
                foo.select { foo.id eq id }.single()
            }

            assertEquals(today, result[foo.defaultDateTime].toLocalDate())
            assertEquals(100, result[foo.defaultInt])
        }
    }

    @Test
    fun testDefaultExpressions02() {
        Assume.assumeFalse(currentDialect is SnowflakeDialect)

        val foo = object : IntIdTable("foo") {
            val name = text("name")
            val defaultDateTime = datetime("defaultDateTime").defaultExpression(CurrentTimestamp())
        }

        val nonDefaultDate = LocalDate.of(2000, 1, 1).atStartOfDay()

        withTables(foo) {
            val result = if (currentDialect is SnowflakeDialect) {
                foo.insert {
                    it[foo.name] = "bar"
                    it[foo.defaultDateTime] = nonDefaultDate
                }
                foo.selectAll().limit(1).orderBy( foo.id, SortOrder.DESC ).single()
            } else {
                val id = foo.insertAndGetId {
                    it[foo.name] = "bar"
                    it[foo.defaultDateTime] = nonDefaultDate
                }
                foo.select { foo.id eq id }.single()
            }

            assertEquals("bar", result[foo.name])
            assertEqualDateTime(nonDefaultDate, result[foo.defaultDateTime])

            foo.update({foo.id eq foo.id}) {
                it[foo.name] = "baz"
            }

            val result2 = foo.select { foo.id eq foo.id }.single()
            assertEquals("baz", result2[foo.name])
            assertEqualDateTime(nonDefaultDate, result2[foo.defaultDateTime])
        }
    }
}
