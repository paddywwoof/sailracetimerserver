package net.mfashby.sailracetimerserver

import com.fasterxml.jackson.databind.SerializationFeature
import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.int
import io.ktor.application.ApplicationCall
import io.ktor.application.call
import io.ktor.application.install
import io.ktor.auth.*
import io.ktor.auth.jwt.JWTPrincipal
import io.ktor.auth.jwt.jwt
import io.ktor.features.CORS
import io.ktor.features.CallLogging
import io.ktor.features.ContentNegotiation
import io.ktor.features.DefaultHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.http.Parameters
import io.ktor.jackson.jackson
import io.ktor.pipeline.PipelineContext
import io.ktor.request.receive
import io.ktor.response.respond
import io.ktor.response.respondText
import io.ktor.routing.*
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty

fun main(args: Array<String>) = Main().main(args)

class Main: CliktCommand() {
    private val url by option(help="database JDBC connection string", envvar = "DB_URL")
            .default("jdbc:mysql://localhost:3306/yxsecnyo_ysc")

    private val user by option(help="database user", envvar = "DB_USER")
            .default("root")

    private val password by option(help = "database password", envvar = "DB_PASS")
            .default("example")

    private val port: Int by option(help = "The port on which to run the HTTP server", envvar = "SVR_PORT")
            .int().default(8080)

    private val host by option(help = "The host on which to run the HTTP server", envvar = "SVR_HOST")
            .default("0.0.0.0")

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
            install(CORS) {
                anyHost()
            }
            install(Authentication) {
                jwt {
                    verifier(JwtConfig.verifier)
                    realm = "sailracetimerserver"
                    validate {
                        it.payload
                                .getClaim("id").asInt()
                                ?.let(raceApiService::getUserById)
                    }
                }
            }
            install(Routing) {
                raceApi(raceApiService)
            }
        }.start(wait = true)
    }
}

fun Route.raceApi(raceApiService: RaceApiService) {
    post("login") {
        val credentials = call.receive<UserPasswordCredential>()
        val user = raceApiService.getAndValidateUser(credentials.name, credentials.password)
        if (user != null) {
            val token = JwtConfig.makeToken(user)
            call.respondText(token)
        } else {
            call.respond(HttpStatusCode.Unauthorized)
        }
    }

    route("/series") {
        get("/") {
            val params = call.request.queryParameters.toMap()
            call.respond(raceApiService.getSeries(params))
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
            val query = call.request.queryParameters.toMap()
            call.respond(raceApiService.getRaces(query))
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
            val query = call.request.queryParameters.toMap()
            call.respond(raceApiService.getResults(query))
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
            val query = call.request.queryParameters.toMap()
            call.respond(raceApiService.getIndividuals(query))
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
            val query = call.request.queryParameters.toMap()
            call.respond(raceApiService.getBoatTypes(query))
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

fun Parameters.toMap(): MutableMap<String, List<String>> {
    val map = mutableMapOf<String, List<String>>()
    forEach { s, list -> map[s] = list }
    return map
}