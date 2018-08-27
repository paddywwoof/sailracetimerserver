package net.mfashby.sailracetimerserver
import io.ktor.auth.Principal
import java.time.Duration
import java.time.LocalDate
//test where commits go to after forking repo to paddywwoof
data class User(val id: Int? = null,
                val name: String,
                val password: String,
                val level: Int): Principal

data class Series(val id: Int? = null,
                  val name: String = "",
                  val ntocount: Int = 0,
                  val weight: Int = 0)

data class Race(val id: Int? = null,
                val seriesID: Int,
                val rdate: LocalDate,
                val name: String?,
                val wholelegs: Int?,
                val partlegs: Int?,
                val ood: String?,
                val aood: String?,
                val winddir: String?,
                val windstr: String?,
                val comments: String?,
                val finished: Boolean)

data class Result(val id: Int? = null,
                  val individualID: Int?,
                  val nlaps: Int?,
                  val rtime: Duration?,
                  val adjtime: Duration?,
                  val posn: Int?,
                  val raceID: Int?,
                  val fleet: String?,
                  val crew: String?)

data class BoatType(val id: Int? = null,
                    val btype: String,
                    val fleet: String,
                    val pyn: Int)

data class Individual(val id: Int? = null,
                      val boattypeID: Int,
                      val name: String,
                      val boatnum: String?,
                      val ph: Int?,
                      val btype: String?)