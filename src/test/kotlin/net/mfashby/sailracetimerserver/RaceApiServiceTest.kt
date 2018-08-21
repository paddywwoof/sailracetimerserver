package net.mfashby.sailracetimerserver

import com.github.karsaig.approvalcrest.matcher.Matchers.sameBeanAs
import com.github.karsaig.approvalcrest.matcher.Matchers.sameJsonAsApproved
import org.junit.Assert.*
import org.junit.Ignore
import org.junit.Test
import java.time.Duration
import java.time.LocalDate

@Ignore
class RaceApiServiceTest {
    private val underTest: RaceApiService = RaceApiService("jdbc:mysql://localhost:3306/yxsecnyo_ysc", "root", "example")

    @Test
    fun getSeries() {
        assertThat(underTest.getSeries(mapOf(Pair("id", listOf(18, 19).map { it.toString() }))), sameJsonAsApproved())
    }

    @Test
    fun getSeriesWithLimit() {
        val params = mapOf(Pair("limit", listOf(10.toString())))
        assertThat(underTest.getSeries(params), sameJsonAsApproved())
    }

    @Test
    fun getSeriesWithSort() {
        val params = mapOf(
                Pair("limit", listOf(20.toString())),
                Pair("sort", listOf("weight_desc"))
        )
        assertThat(underTest.getSeries(params), sameJsonAsApproved())
    }

    @Test
    fun addDeleteSeries() {
        val create = Series(name = "Test", ntocount = 5, weight = 100)
        val created = underTest.addSeries(create)

        // Add
        assertThat(created, sameBeanAs<Series>(create).ignoring("id"))
        assertThat(underTest.getSeries(created.id!!), sameBeanAs(created))

        // Update
        val update = created.copy(name = "Updated")
        val updatedSeries = underTest.updateSeries(update)
        assertThat(updatedSeries, sameBeanAs<Series>(update))
        assertThat(underTest.getSeries(created.id!!), sameBeanAs(updatedSeries))

        // Delete
        assertTrue(underTest.deleteSeries(created.id!!))
        assertNull(underTest.getSeries(created.id!!))
    }

    @Test
    fun validateUser() {
        assertNotNull(underTest.getAndValidateUser("ood", "720"))
        assertNull(underTest.getAndValidateUser("ood", "blahblah"))
        assertNull(underTest.getAndValidateUser("blahblah", "000"))
        assertNull(underTest.getAndValidateUser("", ""))
    }

    @Test
    fun getRaces() {
        val query = mapOf(
                Pair("seriesID", listOf(18.toString()))
        )
        assertThat(underTest.getRaces(query), sameJsonAsApproved())
    }

    @Test
    fun queryRacesByMultipleSeriesId() {
        val query = mapOf(
                Pair("seriesID", listOf(18, 11).map { it.toString() })
        )
        assertThat(underTest.getRaces(query), sameJsonAsApproved())
    }

    @Test
    fun queryRacesWithMax() {
        val query = mapOf(
                Pair("seriesID", listOf(18, 11).map { it.toString() }),
                Pair("limit", listOf(3.toString()))
        )
        assertThat(underTest.getRaces(query), sameJsonAsApproved())
    }

    @Test
    fun getFinishedRaces() {
        val query = mapOf(
                Pair("finished", listOf(true.toString()))
        )
        assertThat(underTest.getRaces(query), sameJsonAsApproved())
    }

    @Test
    fun getRace() {
        assertThat(underTest.getRace(535), sameJsonAsApproved())
    }

    @Test
    fun addUpdateDeleteRace() {
        val create = Race(seriesID = 123,
                          rdate = LocalDate.of(2018, 1, 1),
                          name = "Saturday",
                          wholelegs = 3,
                          partlegs = 4,
                          ood = "Martin Ashby",
                          aood = "Georgia Keogh",
                          winddir = "W",
                          windstr = "strong",
                          comments = "test",
                          finished = false)
        val created = underTest.addRace(create)

        // Add
        assertThat(created, sameBeanAs<Race>(create).ignoring("id"))
        assertThat(underTest.getRace(created.id!!), sameBeanAs(created))

        // Update
        val update = created.copy(name = "Updated")
        val updatedRace = underTest.updateRace(update)
        assertThat(updatedRace, sameBeanAs<Race>(update))
        assertThat(underTest.getRace(created.id!!), sameBeanAs(updatedRace))

        // Delete
        assertTrue(underTest.deleteRace(created.id!!))
        assertNull(underTest.getRace(created.id!!))
    }

    @Test
    fun getAllResults() {
        assertThat(underTest.getAllResults(), sameJsonAsApproved())
    }

    @Test
    fun getResult() {
        assertThat(underTest.getResult(3605), sameJsonAsApproved())
    }

    @Test
    fun queryResult() {
        assertThat(underTest.getResults(mapOf(Pair("raceID", listOf(535, 537).map { it.toString() }))), sameJsonAsApproved())
    }

    @Test
    fun addUpdateDeleteResult() {
        val create = Result(individualID = 123,
                            nlaps = 2,
                            rtime = Duration.ofSeconds(3265),
                            adjtime = Duration.ofSeconds(2253),
                            posn = 3,
                            raceID = 123,
                            fleet = "slow",
                            crew = "test")
        val created = underTest.addResult(create)

        // Add
        assertThat(created, sameBeanAs<Result>(create).ignoring("id"))
        assertThat(underTest.getResult(created.id!!), sameBeanAs(created))

        // Update
        val update = created.copy(rtime = Duration.ofSeconds(3568))
        val updated = underTest.updateResult(update)
        assertThat(updated, sameBeanAs<Result>(update))
        assertThat(underTest.getResult(created.id!!), sameBeanAs(updated))

        // Delete
        assertTrue(underTest.deleteResult(created.id!!))
        assertNull(underTest.getResult(created.id!!))
    }

    @Test
    fun getAllIndividuals() {
        assertThat(underTest.getAllIndividuals(), sameJsonAsApproved())
    }

    @Test
    fun getIndividual() {
        assertThat(underTest.getIndividual(612), sameJsonAsApproved())
    }

    @Test
    fun queryIndividual() {
        val query = mapOf(Pair("id", listOf(611, 612).map { it.toString() }))
        assertThat(underTest.getIndividuals(query), sameJsonAsApproved())
    }

    @Test
    fun addUpdateDeleteIndividual() {
        val create = Individual(boattypeID = 123,
                                name = "Testing",
                                boatnum = "65421L",
                                ph = 1234,
                                btype = "Laser")
        val created = underTest.addIndividual(create)

        // Add
        assertThat(created, sameBeanAs<Individual>(create).ignoring("id"))
        assertThat(underTest.getIndividual(created.id!!), sameBeanAs(created))

        // Update
        val update = created.copy(name = "Updated")
        val updated = underTest.updateIndividual(update)
        assertThat(updated, sameBeanAs<Individual>(update))
        assertThat(underTest.getIndividual(created.id!!), sameBeanAs(updated))

        // Delete
        assertTrue(underTest.deleteIndividual(created.id!!))
        assertNull(underTest.getIndividual(created.id!!))
    }

    @Test
    fun getAllBoatTypes() {
        assertThat(underTest
                .getAllBoatTypes(), sameJsonAsApproved())
    }

    @Test
    fun getBoatType() {
        assertThat(underTest.getBoatType(102), sameJsonAsApproved())
    }

    @Test
    fun addUpdateDeleteBoatType() {
        val create = BoatType(btype = "Laser TEST",
                fleet = "Something",
                pyn = 1001)
        val created = underTest.addBoatType(create)

        // Add
        assertThat(created, sameBeanAs<BoatType>(create).ignoring("id"))
        assertThat(underTest.getBoatType(created.id!!), sameBeanAs(created))

        // Update
        val update = created.copy(btype = "Updated")
        val updated = underTest.updateBoatType(update)
        assertThat(updated, sameBeanAs<BoatType>(update))
        assertThat(underTest.getBoatType(created.id!!), sameBeanAs(updated))

        // Delete
        assertTrue(underTest.deleteBoatType(created.id!!))
        assertNull(underTest.getBoatType(created.id!!))
    }
}