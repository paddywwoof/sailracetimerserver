package net.mfashby.sailracetimerserver


import com.auth0.jwt.*
import com.auth0.jwt.algorithms.*
import java.util.*

object JwtConfig {
    private const val secret = "jrnzq9UEiQYwNvYZ"
    private const val issuer = "mfashby.net"
    private const val validityInMs = 36_000_00 * 10 // 10 hours
    private val algorithm = Algorithm.HMAC512(secret)

    val verifier: JWTVerifier = JWT
            .require(algorithm)
            .withIssuer(issuer)
            .build()

    /**
     * Produce a token for this combination of User and Account
     */
    fun makeToken(user: User): String = JWT.create()
            .withSubject("Authentication")
            .withIssuer(issuer)
            .withClaim("id", user.id)
            .withExpiresAt(getExpiration())
            .sign(algorithm)

    /**
     * Calculate the expiration Date based on current time + the given validity
     */
    private fun getExpiration() = Date(System.currentTimeMillis() + validityInMs)
}