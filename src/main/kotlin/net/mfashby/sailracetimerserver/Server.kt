package net.mfashby.sailracetimerserver

import com.fasterxml.jackson.databind.SerializationFeature
import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.int
import io.ktor.application.ApplicationCall
import io.ktor.application.call
import io.ktor.application.install
import io.ktor.auth.Authentication
import io.ktor.auth.UserIdPrincipal
import io.ktor.auth.authenticate
import io.ktor.auth.basic
import io.ktor.features.CallLogging
import io.ktor.features.ContentNegotiation
import io.ktor.features.DefaultHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.jackson.jackson
import io.ktor.pipeline.PipelineContext
import io.ktor.request.receive
import io.ktor.response.respond
import io.ktor.routing.*
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty

suspend fun PipelineContext<Unit, ApplicationCall>.checkId(block: suspend (Int) -> Unit) {
    call.parameters["id"]?.toIntOrNull() ?.let { id ->
        block(id)
    } ?: run {
        call.respond(HttpStatusCode.BadRequest, "Invalid ID")
    }
}

suspend fun ApplicationCall.respond(obj: Any?) {
    if (obj == null) {
        respond(HttpStatusCode.NotFound, "Not found")
    } else {
        respond(obj)
    }
}

fun Route.raceApi(raceApiService: RaceApiService) {
    route("/series") {
        get("/") {
            call.respond(raceApiService.getAllSeries())
        }
        get("/{id}") {
            checkId { id ->
                call.respond(raceApiService.getSeries(id))
            }
        }
        authenticate {
            post("/") {
                call.respond(raceApiService.addSeries(call.receive()))
            }
            put("/") {
                call.respond(raceApiService.updateSeries(call.receive()))
            }
            delete("/{id}") {
                checkId { id ->
                    call.respond(raceApiService.deleteSeries(id))
                }
            }
        }
    }

    route("/race") {
        get("/") {
            call.respond(raceApiService.getAllRaces())
        }
        get("/{id}") {
            checkId { id ->
                call.respond(raceApiService.getRace(id))
            }
        }
        authenticate {
            post("/") {
                call.respond(raceApiService.addRace(call.receive()))
            }
            put("/") {
                call.respond(raceApiService.updateRace(call.receive()))
            }
            delete("/{id}") {
                checkId { id ->
                    call.respond(raceApiService.deleteRace(id))
                }
            }
        }
    }

    route("/result") {
        get("/") {
            call.respond(raceApiService.getAllResults())
        }
        get("/{id}") {
            checkId{ id ->
                call.respond(raceApiService.getResult(id))
            }
        }
        authenticate {
            post("/") {
                call.respond(raceApiService.addResult(call.receive()))
            }
            put("/") {
                call.respond(raceApiService.updateResult(call.receive()))
            }
            delete("/{id}") {
                checkId { id -> call.respond(raceApiService.deleteResult(id)) }
            }
        }
    }

    route("/individual") {
        get("/") {
            call.respond(raceApiService.getAllIndividuals())
        }
        get("/{id}") {
            checkId { id ->
                call.respond(raceApiService.getIndividual(id))
            }
        }
        authenticate {
            post("/") {
                call.respond(raceApiService.addIndividual(call.receive()))
            }
            put("/") {
                call.respond(raceApiService.updateIndividual(call.receive()))
            }
            delete("/{id}") {
                checkId { id -> call.respond(raceApiService.deleteIndividual(id)) }
            }
        }
    }

    route("/boattype") {
        get("/") {
            call.respond(raceApiService.getAllBoatTypes())
        }
        get("/{id}") {
            checkId { id ->
                call.respond(raceApiService.getBoatType(id))
            }
        }
        authenticate {
            post("/") {
                call.respond(raceApiService.addBoatType(call.receive()))
            }
            put("/") {
                call.respond(raceApiService.updateBoatType(call.receive()))
            }
            delete("/{id}") {
                checkId { id -> call.respond(raceApiService.deleteBoatType(id)) }
            }
        }
    }
}


class Main: CliktCommand() {
    private val url by option(help="database JDBC connection string").default("jdbc:mysql://localhost:3306/yxsecnyo_ysc")
    private val user by option(help="database user").default("root")
    private val password by option(help = "database password").default("example")
    private val port: Int by option(help = "The port on which to run the HTTP server").int().default(8080)
    private val host by option(help = "The host on which to run the HTTP server").default("0.0.0.0")
    override fun run() {
        val raceApiService = RaceApiService(url, user, password)
        embeddedServer(Netty, port, host) {
            install(DefaultHeaders)
            install(CallLogging)
            install(ContentNegotiation) {
                jackson {
                    configure(SerializationFeature.INDENT_OUTPUT, true)
                }
            }
            install(Authentication) {
                basic {
                    realm = "ktor"
                    validate {credentials ->
                        if (raceApiService.validateUser(credentials.name, credentials.password)) UserIdPrincipal(credentials.name) else null
                    }
                }
            }
            install(Routing) {
                raceApi(raceApiService)
            }
        }.start(wait = true)
    }
}

fun main(args: Array<String>) = Main().main(args)