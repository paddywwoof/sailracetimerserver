package net.mfashby.sailracetimerserver

import java.io.Closeable
import java.sql.*
import java.time.Duration

const val GENERATED_KEY = "GENERATED_KEY"

class RaceApiService(url: String, user: String, password: String): Closeable {
    private val connection = DriverManager.getConnection(url, user, password)

    override fun close() {
        connection.close()
    }

    private fun <T> ResultSet.readObjects(block: (ResultSet) -> T): List<T> {
        val lst = mutableListOf<T>()
        while (next()) lst.add(block(this))
        return lst
    }

    private fun <T> ResultSet.readOne(block: (ResultSet) -> T): T? =
        if (next()) block(this) else null

    private fun ResultSet.readSeries(): Series =
        Series(id = getInt("id"),
               name = getString("name"),
               ntocount = getInt("ntocount"),
               weight = getInt("weight"))

    private fun PreparedStatement.oneKey(): Int =
            generatedKeys.readOne { it.getInt(GENERATED_KEY) }
                    ?: throw IllegalStateException("No generated key")

    fun getAllSeries(): List<Series> =
            connection.prepareStatement("SELECT id, name, ntocount, weight FROM series")
                .executeQuery()
                .readObjects { it.readSeries() }

    fun getSeries(id: Int): Series? {
        val stmt = connection.prepareStatement("SELECT id, name, ntocount, weight FROM series WHERE id = ?")
        stmt.setInt(1, id)
        return stmt.executeQuery().readOne { it.readSeries() }
    }

    fun addSeries(s: Series): Series {
        val stmt = connection.prepareStatement("INSERT INTO series(name, ntocount, weight) VALUES  (?, ?, ?)",
                Statement.RETURN_GENERATED_KEYS)
        stmt.setString(1, s.name)
        stmt.setInt(2, s.ntocount)
        stmt.setInt(3, s.weight)
        stmt.executeUpdate()
        return s.copy(id = stmt.oneKey())
    }

    fun updateSeries(s: Series): Series {
        val stmt = connection.prepareStatement("UPDATE series SET name = ?, ntocount = ?, weight = ? WHERE id = ?")
        stmt.setString(1, s.name)
        stmt.setInt(2, s.ntocount)
        stmt.setInt(3, s.weight)
        stmt.setInt(4, s.id!!)
        stmt.executeUpdate()
        return s
    }

    fun deleteSeries(id: Int): Boolean {
        val stmt = connection.prepareStatement("DELETE FROM series WHERE id = ?")
        stmt.setInt(1, id)
        return stmt.executeUpdate() == 1
    }

    fun validateUser(name: String, password: String): Boolean {
        val stmt = connection.prepareStatement("SELECT password FROM user WHERE name = ?")
        stmt.setString(1, name)
        val actualPw = stmt.executeQuery().readOne { it.getString("password") }
        return password == actualPw
    }

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
                flg = getBoolean("flg"))

    fun getAllRaces(): List<Race> =
            connection.prepareStatement(
            "SELECT id,seriesID,rdate,name,wholelegs,partlegs,ood,aood,winddir,windstr,comments,flg FROM race"
            ).executeQuery()
             .readObjects { it.readRace() }

    fun getRace(id: Int): Race? {
        val stmt = connection.prepareStatement(
                "SELECT id,seriesID,rdate,name,wholelegs,partlegs,ood,aood,winddir,windstr,comments,flg FROM race WHERE id = ?"
        )
        stmt.setInt(1, id)
        return stmt.executeQuery().readOne{ it.readRace() }
    }

    fun PreparedStatement.setIntOpt(parameterIndex: Int, value: Int?) =
            value?.let { setInt(parameterIndex, it) }
                 ?:let { setNull(parameterIndex, Types.INTEGER)}

    fun addRace(r: Race): Race {
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
        stmt.setBoolean(11, r.flg)
        stmt.executeUpdate()
        return r.copy(id = stmt.oneKey())
    }

    fun updateRace(r: Race): Race {
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
        stmt.setBoolean(11, r.flg)
        stmt.setInt(12, r.id!!)
        stmt.executeUpdate()
        return r
    }

    fun deleteRace(id: Int): Boolean {
        val stmt = connection.prepareStatement("DELETE FROM race WHERE id = ?")
        stmt.setInt(1, id)
        return stmt.executeUpdate() == 1
    }

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

    fun getAllResults(): List<Result> =
            connection.prepareStatement("SELECT id,individualID,nlaps,rtime,adjtime,posn,raceID,fleet,crew FROM result")
                    .executeQuery().readObjects { it.readResult() }

    fun getResult(id: Int): Result? {
        val stmt = connection.prepareStatement("SELECT id,individualID,nlaps,rtime,adjtime,posn,raceID,fleet,crew FROM result WHERE id = ?")
        stmt.setInt(1, id)
        return stmt.executeQuery().readOne { it.readResult() }
    }

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

    fun addResult(r: Result): Result {
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
        return r.copy(id = stmt.oneKey())
    }

    fun updateResult(r: Result): Result {
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

    fun deleteResult(id: Int): Boolean {
        val stmt = connection.prepareStatement("DELETE FROM result WHERE id = ?")
        stmt.setInt(1, id)
        return stmt.executeUpdate() == 1
    }

    private fun ResultSet.readIndividual(): Individual =
            Individual(id = getInt("id"),
                       boattypeID = getInt("boattypeID"),
                       name = getString("name"),
                       boatnum = getString("boatnum"),
                       ph = getInt("ph"),
                       btype = getString("btype"))

    fun getAllIndividuals(): List<Individual> =
            connection.prepareStatement("SELECT id,boattypeID,name,boatnum,ph,btype FROM individual")
                    .executeQuery()
                    .readObjects { it.readIndividual() }

    fun getIndividual(id: Int): Individual? {
        val stmt = connection.prepareStatement("SELECT id,boattypeID,name,boatnum,ph,btype FROM individual WHERE id = ?")
        stmt.setInt(1, id)
        return stmt.executeQuery().readOne { it.readIndividual() }
    }

    fun addIndividual(r: Individual): Individual {
        val stmt = connection.prepareStatement("INSERT INTO individual (boattypeID,name,boatnum,ph,btype) VALUES (?,?,?,?,?)",
                                               Statement.RETURN_GENERATED_KEYS)
        stmt.setIntOpt(1, r.boattypeID)
        stmt.setString(2, r.name)
        stmt.setString(3, r.boatnum)
        stmt.setIntOpt(4, r.ph)
        stmt.setString(5, r.btype)
        stmt.executeUpdate()
        return r.copy(id = stmt.oneKey())
    }

    fun updateIndividual(r: Individual): Individual {
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

    fun deleteIndividual(id: Int): Boolean {
        val stmt = connection.prepareStatement("DELETE FROM individual WHERE id = ?")
        stmt.setInt(1, id)
        return stmt.executeUpdate() == 1
    }

    private fun ResultSet.readBoatType(): BoatType =
            BoatType(id = getInt("id"),
                    btype =  getString("btype"),
                    fleet = getString("fleet"),
                    pyn = getInt("pyn"))

    fun getAllBoatTypes(): List<BoatType> =
            connection.prepareStatement("SELECT id,btype,fleet,pyn FROM boattype")
                .executeQuery().readObjects { it.readBoatType() }

    fun getBoatType(id: Int): BoatType? {
        val stmt = connection.prepareStatement("SELECT id,btype,fleet,pyn FROM boattype WHERE id = ?")
        stmt.setInt(1, id)
        return stmt.executeQuery().readOne { it.readBoatType() }
    }

    fun addBoatType(r: BoatType): BoatType {
        val stmt = connection.prepareStatement("INSERT INTO boattype (btype,fleet,pyn) VALUES (?, ?, ?)",
                Statement.RETURN_GENERATED_KEYS)
        stmt.setString(1, r.btype)
        stmt.setString(2, r.fleet)
        stmt.setInt(3, r.pyn)
        stmt.executeUpdate()
        return r.copy(id = stmt.oneKey())
    }

    fun updateBoatType(r: BoatType): BoatType {
        val stmt = connection.prepareStatement("UPDATE boattype SET btype = ?,fleet = ?,pyn = ? WHERE id = ?")
        stmt.setString(1, r.btype)
        stmt.setString(2, r.fleet)
        stmt.setInt(3, r.pyn)
        stmt.setInt(4, r.id!!)
        stmt.executeUpdate()
        return r
    }

    fun deleteBoatType(id: Int): Boolean {
        val stmt = connection.prepareStatement("DELETE FROM boattype WHERE id = ?")
        stmt.setInt(1, id)
        return stmt.executeUpdate() == 1
    }
}
