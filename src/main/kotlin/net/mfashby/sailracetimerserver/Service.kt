package net.mfashby.sailracetimerserver

import com.mysql.cj.jdbc.MysqlConnectionPoolDataSource
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.sql.*
import java.time.Duration
import javax.naming.InitialContext
import javax.naming.spi.InitialContextFactoryBuilder
import javax.sql.DataSource

const val GENERATED_KEY = "GENERATED_KEY"
const val DATA_SOURCE = "jdbc/sailracedb"

class RaceApiService(url: String, user: String, password: String) {
    private val logger = LoggerFactory.getLogger(RaceApiService::class.java)
    private val source: DataSource

    init {
        source = MysqlConnectionPoolDataSource()
        source.setURL(url)
        source.user = user
        source.setPassword(password)
    }

    private inline fun <T> withConnection(wrap: (Connection) -> T): T {
//        val dataSource = InitialContext().lookup(DATA_SOURCE) as DataSource
        return source.connection.use { wrap(it) }
    }

    /**
     * AUTH
     */
    private fun ResultSet.readUser(): User =
            User(
                    id = getInt("id"),
                    name = getString("name"),
                    password = getString("password"),
                    level = getInt("level")
            )

    fun getAndValidateUser(name: String, password: String): User? = withConnection { connection ->
        val stmt = connection.prepareStatement("SELECT id,name,password,level FROM user WHERE name = ?")
        stmt.setString(1, name)
        val user = stmt.executeQuery().readOne { it.readUser() }
        return if (user?.password == password) user else null
    }

    fun getUserById(id: Int): User? = withConnection { connection ->
        val stmt = connection.prepareStatement("SELECT id,name,password,level FROM user WHERE id = ?")
        stmt.setInt(1, id)
        return stmt.executeQuery().readOne { it.readUser() }
    }

    /**
     * SERIES
     */
    private fun ResultSet.readSeries(): Series =
            Series(id = getInt("id"),
                    name = getString("name"),
                    ntocount = getInt("ntocount"),
                    weight = getInt("weight"))

    fun getSeries(id: Int) : Series? {
        val params = idParam(id)
        return getSeries(params).firstOrNull()
    }

    fun getSeries(params: Map<String, List<String>>): List<Series> {
        val select = "SELECT id, name, ntocount, weight FROM series"
        val wheres = mutableListOf<String>()
        val sqlParams = mutableListOf<Any>()

        params["id"]?.let {
            wheres.add("id IN (${repeatJoin("?", ",", it.size)})")
            sqlParams.addAll(it.map { it.toInt() })
        }

        return addLimitAndSortAndExecute(params, select, wheres, sqlParams) {
            it.readObjects{ it.readSeries() }
        }

    }

    fun addSeries(s: Series): Series = withConnection { connection ->
        val stmt = connection.prepareStatement("INSERT INTO series(name, ntocount, weight) VALUES  (?, ?, ?)",
                Statement.RETURN_GENERATED_KEYS)
        stmt.setString(1, s.name)
        stmt.setInt(2, s.ntocount)
        stmt.setInt(3, s.weight)
        stmt.executeUpdate()
        return s.copy(id = stmt.oneGeneratedKey())
    }

    fun updateSeries(s: Series): Series = withConnection { connection ->
        val stmt = connection.prepareStatement("UPDATE series SET name = ?, ntocount = ?, weight = ? WHERE id = ?")
        stmt.setString(1, s.name)
        stmt.setInt(2, s.ntocount)
        stmt.setInt(3, s.weight)
        stmt.setInt(4, s.id!!)
        stmt.executeUpdate()
        return s
    }

    fun deleteSeries(id: Int): Boolean = withConnection { connection ->
        val stmt = connection.prepareStatement("DELETE FROM series WHERE id = ?")
        stmt.setInt(1, id)
        return stmt.executeUpdate() == 1
    }

    /**
     * RACE
     */
    private fun ResultSet.readRace(): Race =
            Race(id = getInt("id"),
                seriesID = getInt("seriesID"),
                rdate = getDate("rdate").toLocalDate(),
                name = getString("name"),
                wholelegs = getInt("wholelegs"),
                partlegs = getInt("partlegs"),
                ood = getString("ood"),
                aood = getString("aood"),
                winddir = getString("winddir"),
                windstr = getString("windstr"),
                comments = getString("comments"),
                finished = getBoolean("flg"))

    fun getAllRaces(): List<Race> = withConnection { connection ->
        return connection.prepareStatement(
                "SELECT id,seriesID,rdate,name,wholelegs,partlegs,ood,aood,winddir,windstr,comments,flg FROM race")
                .executeQuery()
                .readObjects { it.readRace() }
    }

    fun getRace(id: Int): Race? {
        val params = idParam(id)
        return getRaces(params).firstOrNull()
    }

    private fun repeatJoin(string: String, separator: String, times: Int) =
            Array(times) { _-> string}.joinToString(separator = separator)

    fun getRaces(params: Map<String, List<String>>): List<Race> {
        val select = "SELECT id,seriesID,rdate,name,wholelegs,partlegs,ood,aood,winddir,windstr,comments,flg FROM race"
        val wheres = mutableListOf<String>()
        val sqlParams = mutableListOf<Any>()

        params["finished"]?.let {
            wheres.add("flg = ?")
            sqlParams.add(it[0].toBoolean())
        }
        addIdInClause(params, "id", wheres, sqlParams)
        addIdInClause(params, "seriesID", wheres, sqlParams)
        return addLimitAndSortAndExecute(params, select, wheres, sqlParams) {
            it.readObjects{ it.readRace() }
        }

    }

    fun addRace(r: Race): Race = withConnection { connection ->
        val stmt = connection.prepareStatement("INSERT INTO race (seriesID,rdate,name,wholelegs,partlegs,ood,aood,winddir,windstr,comments,flg) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                Statement.RETURN_GENERATED_KEYS)
        stmt.setInt(1, r.seriesID)
        stmt.setDate(2, Date.valueOf(r.rdate))
        stmt.setString(3, r.name)
        stmt.setIntOpt(4, r.wholelegs)
        stmt.setIntOpt(5, r.partlegs)
        stmt.setString(6, r.ood)
        stmt.setString(7, r.aood)
        stmt.setString(8, r.winddir)
        stmt.setString(9, r.windstr)
        stmt.setString(10, r.comments)
        stmt.setBoolean(11, r.finished)
        stmt.executeUpdate()
        return r.copy(id = stmt.oneGeneratedKey())
    }

    fun updateRace(r: Race): Race = withConnection { connection ->
        val stmt = connection.prepareStatement("UPDATE race SET " +
                "seriesID = ?," +
                "rdate = ?," +
                "name = ?," +
                "wholelegs = ?," +
                "partlegs = ?," +
                "ood = ?," +
                "aood = ?," +
                "winddir = ?," +
                "windstr = ?," +
                "comments = ?," +
                "flg = ? " +
                "WHERE id = ? ")

        stmt.setInt(1, r.seriesID)
        stmt.setDate(2, Date.valueOf(r.rdate))
        stmt.setString(3, r.name)
        stmt.setIntOpt(4, r.wholelegs)
        stmt.setIntOpt(5, r.partlegs)
        stmt.setString(6, r.ood)
        stmt.setString(7, r.aood)
        stmt.setString(8, r.winddir)
        stmt.setString(9, r.windstr)
        stmt.setString(10, r.comments)
        stmt.setBoolean(11, r.finished)
        stmt.setInt(12, r.id!!)
        stmt.executeUpdate()
        return r
    }

    fun deleteRace(id: Int): Boolean = withConnection { connection ->
        val stmt = connection.prepareStatement("DELETE FROM race WHERE id = ?")
        stmt.setInt(1, id)
        return stmt.executeUpdate() == 1
    }

    /**
     * RESULT
     */
    private fun ResultSet.readResult(): Result =
            Result(id = getInt("id"),
                   individualID = getInt("individualID"),
                   nlaps = getInt("nlaps"),
                   rtime = getString("rtime").toDuration(),
                   adjtime = (getString("adjtime") ?: "00:00:00").toDuration(),
                   posn = getInt("posn"),
                   raceID = getInt("raceID"),
                   fleet = getString("fleet"),
                   crew = getString("crew"))

    fun getAllResults(): List<Result> = getResults(emptyMap())

    fun getResult(id: Int): Result? {
        val params = idParam(id)
        return getResults(params).firstOrNull()
    }

    fun getResults(params: Map<String, List<String>>): List<Result> {
        val select = "SELECT id,individualID,nlaps,rtime,adjtime,posn,raceID,fleet,crew FROM result"
        val wheres = mutableListOf<String>()
        val sqlParams = mutableListOf<Any>()
        addIdInClause(params, "id", wheres, sqlParams)
        addIdInClause(params, "raceID", wheres, sqlParams)
        return addLimitAndSortAndExecute(params, select, wheres, sqlParams) {
            it.readObjects{ it.readResult() }
        }

    }

    fun addResult(r: Result): Result = withConnection { connection ->
        val stmt = connection.prepareStatement("INSERT INTO result (individualID,nlaps,rtime,adjtime,posn,raceID,fleet,crew) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",Statement.RETURN_GENERATED_KEYS)
        stmt.setIntOpt(1, r.individualID)
        stmt.setIntOpt(2, r.nlaps)
        stmt.setString(3, r.rtime?.toMySqlString())
        stmt.setString(4, r.adjtime?.toMySqlString())
        stmt.setIntOpt(5, r.posn)
        stmt.setIntOpt(6, r.raceID)
        stmt.setString(7, r.fleet)
        stmt.setString(8, r.crew)
        stmt.executeUpdate()
        return r.copy(id = stmt.oneGeneratedKey())
    }

    fun updateResult(r: Result): Result = withConnection { connection ->
        val stmt = connection.prepareStatement("UPDATE result SET individualID = ?,nlaps = ?,rtime = ?,adjtime = ?,posn = ?,raceID = ?,fleet = ?,crew = ? WHERE id = ?")
        stmt.setIntOpt(1, r.individualID)
        stmt.setIntOpt(2, r.nlaps)
        stmt.setString(3, r.rtime?.toMySqlString())
        stmt.setString(4, r.adjtime?.toMySqlString())
        stmt.setIntOpt(5, r.posn)
        stmt.setIntOpt(6, r.raceID)
        stmt.setString(7, r.fleet)
        stmt.setString(8, r.crew)
        stmt.setInt(9, r.id!!)
        stmt.executeUpdate()
        return r
    }

    fun deleteResult(id: Int): Boolean = withConnection { connection ->
        val stmt = connection.prepareStatement("DELETE FROM result WHERE id = ?")
        stmt.setInt(1, id)
        return stmt.executeUpdate() == 1
    }

    /**
     * INDIVIDUAL
     */
    private fun ResultSet.readIndividual(): Individual =
            Individual(id = getInt("id"),
                       boattypeID = getInt("boattypeID"),
                       name = getString("name"),
                       boatnum = getString("boatnum"),
                       ph = getInt("ph"),
                       btype = getString("btype"))

    fun getAllIndividuals(): List<Individual> =
            getIndividuals(emptyMap())

    fun getIndividual(id: Int): Individual? =
            getIndividuals(idParam(id)).firstOrNull()

    fun getIndividuals(params: Map<String, List<String>>): List<Individual> {
        val select = "SELECT id,boattypeID,name,boatnum,ph,btype FROM individual"
        val wheres = mutableListOf<String>()
        val sqlParams = mutableListOf<Any>()
        addIdInClause(params, "id", wheres, sqlParams)
        return addLimitAndSortAndExecute(params, select, wheres, sqlParams) {
            it.readObjects{ it.readIndividual() }
        }

    }

    fun addIndividual(r: Individual): Individual = withConnection { connection ->
        val stmt = connection.prepareStatement("INSERT INTO individual (boattypeID,name,boatnum,ph,btype) VALUES (?,?,?,?,?)",
                                               Statement.RETURN_GENERATED_KEYS)
        stmt.setIntOpt(1, r.boattypeID)
        stmt.setString(2, r.name)
        stmt.setString(3, r.boatnum)
        stmt.setIntOpt(4, r.ph)
        stmt.setString(5, r.btype)
        stmt.executeUpdate()
        return r.copy(id = stmt.oneGeneratedKey())
    }

    fun updateIndividual(r: Individual): Individual = withConnection { connection ->
        val stmt = connection.prepareStatement("UPDATE individual SET boattypeID = ?,name = ?,boatnum = ?,ph = ?,btype = ? WHERE id = ?")
        stmt.setIntOpt(1, r.boattypeID)
        stmt.setString(2, r.name)
        stmt.setString(3, r.boatnum)
        stmt.setIntOpt(4, r.ph)
        stmt.setString(5, r.btype)
        stmt.setInt(6, r.id!!)
        stmt.executeUpdate()
        return r
    }

    fun deleteIndividual(id: Int): Boolean = withConnection { connection ->
        val stmt = connection.prepareStatement("DELETE FROM individual WHERE id = ?")
        stmt.setInt(1, id)
        return stmt.executeUpdate() == 1
    }

    /**
     * BOATTYPE
     */
    private fun ResultSet.readBoatType(): BoatType =
            BoatType(id = getInt("id"),
                    btype =  getString("btype"),
                    fleet = getString("fleet"),
                    pyn = getInt("pyn"))

    fun getAllBoatTypes(): List<BoatType> =
            getBoatTypes(emptyMap())

    fun getBoatType(id: Int): BoatType? =
            getBoatTypes(idParam(id)).firstOrNull()

    fun getBoatTypes(params: Map<String, List<String>>): List<BoatType> {
        val select = "SELECT id,btype,fleet,pyn FROM boattype"
        val wheres = mutableListOf<String>()
        val sqlParams = mutableListOf<Any>()
        addIdInClause(params, "id", wheres, sqlParams)
        return addLimitAndSortAndExecute(params, select, wheres, sqlParams) {
            it.readObjects{ it.readBoatType() }
        }

    }

    fun addBoatType(r: BoatType): BoatType = withConnection { connection ->
        val stmt = connection.prepareStatement("INSERT INTO boattype (btype,fleet,pyn) VALUES (?, ?, ?)",
                Statement.RETURN_GENERATED_KEYS)
        stmt.setString(1, r.btype)
        stmt.setString(2, r.fleet)
        stmt.setInt(3, r.pyn)
        stmt.executeUpdate()
        return r.copy(id = stmt.oneGeneratedKey())
    }

    fun updateBoatType(r: BoatType): BoatType = withConnection { connection ->
        val stmt = connection.prepareStatement("UPDATE boattype SET btype = ?,fleet = ?,pyn = ? WHERE id = ?")
        stmt.setString(1, r.btype)
        stmt.setString(2, r.fleet)
        stmt.setInt(3, r.pyn)
        stmt.setInt(4, r.id!!)
        stmt.executeUpdate()
        return r
    }

    fun deleteBoatType(id: Int): Boolean = withConnection { connection ->
        val stmt = connection.prepareStatement("DELETE FROM boattype WHERE id = ?")
        stmt.setInt(1, id)
        return stmt.executeUpdate() == 1
    }

    /**
     * Add a clause of the form `somethingID IN (1,2,3)`
     */
    private fun addIdInClause(params: Map<String, List<String>>, col: String, wheres: MutableList<String>, sqlParams: MutableList<Any>) {
        params[col]?.let {
            wheres.add("$col IN (${repeatJoin("?", ",", it.size)})")
            sqlParams.addAll(it.map { it.toInt() })
        }
    }

    /**
     * Add common parts to each query (limit= & sort= should be supported for all get queries)
     */
    private fun <T> addLimitAndSortAndExecute(urlParams: Map<String, List<String>>,
                                          select: String,
                                          wheres: List<String>,
                                          sqlParams: MutableList<Any>,
                                          transform: (ResultSet) -> T): T = withConnection { connection ->
        val where = if (wheres.isNotEmpty()) {
            "WHERE ${wheres.joinToString(" AND ")}"
        } else { "" }

        val limit = urlParams["limit"]?.let {
            "LIMIT ${it[0].toInt()}"
        } ?: ""

        val orderBy = urlParams["sort"]?.let { sortParams ->
            "ORDER BY " + sortParams.joinToString(",") {
                Sorting.valueOf(it).orderBy
            }
        } ?: ""

        val sql = listOf(select, where, orderBy, limit).joinToString(" ")
        val stmt = connection.prepareStatement(sql)
        // Dumb index from 1
        sqlParams.forEachIndexed { index, param -> stmt.setObject(index + 1, param) }
        return transform(stmt.executeQuery())
    }

    // Got to do sorting this way to prevent SQL injection
    @Suppress("EnumEntryName")
    private enum class Sorting(val orderBy: String) {
        weight_desc("weight DESC"),
        rdate("rdate"),
        rdate_desc("rdate DESC"),
        posn("posn ASC")
    }
}

/**
 * General DRY stuff
 */
private fun <T> ResultSet.readObjects(block: (ResultSet) -> T): List<T> {
    val lst = mutableListOf<T>()
    while (next()) lst.add(block(this))
    return lst
}

private fun <T> ResultSet.readOne(block: (ResultSet) -> T): T? =
        if (next()) block(this) else null

private fun PreparedStatement.oneGeneratedKey(): Int =
        generatedKeys.readOne { it.getInt(GENERATED_KEY) }
                ?: throw IllegalStateException("No generated key")

private fun PreparedStatement.setIntOpt(parameterIndex: Int, value: Int?) =
        value?.let { setInt(parameterIndex, it) }
             ?: setNull(parameterIndex, Types.INTEGER)


private fun String.toDuration(): Duration {
    val tokens = split(":")
    return Duration.ZERO
            .plusHours(tokens[0].toLong())
            .plusMinutes(tokens[1].toLong())
            .plusSeconds(tokens[2].toLong())
}

fun Duration.toMySqlString(): String {
    val hours = toHours()
    val minutes = minusHours(hours)
            .toMinutes()
    val seconds = minusHours(hours)
            .minusMinutes(minutes)
            .seconds
    return "$hours:$minutes:$seconds"
}

fun idParam(id: Int) = mapOf(Pair("id", listOf(id.toString())))