package de.spinscale.queue

import com.fasterxml.jackson.jr.ob.JSON
import com.natpryce.hamkrest.anyElement
import com.natpryce.hamkrest.assertion.assert
import com.natpryce.hamkrest.containsSubstring
import com.natpryce.hamkrest.endsWith
import com.natpryce.hamkrest.equalTo
import com.natpryce.hamkrest.hasElement
import com.natpryce.hamkrest.startsWith
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpHandler
import com.sun.net.httpserver.HttpServer
import de.spinscale.test.TestLambdaLogger
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it
import software.amazon.awssdk.services.sqs.model.Message
import java.net.InetAddress
import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicBoolean

object ElasticsearchMessageProcessorSpec : Spek({

    describe("ElasticsearchMessageProcessor") {

        val lambdaLogger = TestLambdaLogger()
        val message = Message.builder()
                .body(""" { "foo" : "bar", "lat": 1234.56, "lon": 65.4321, "tst": 1501054876, "_type" : "underscores" } """)
                .receiptHandle("baz")
                .messageId("myID")
                .build()

        val expectedMap = mapOf("foo" to "bar", "timestamp" to "2017-07-26T07:41:16Z",
                "type" to "underscores",
                "tst" to 1501054876,
                "location" to mapOf("lat" to 1234.56, "lon" to 65.4321))

        afterEachTest {
            lambdaLogger.lines.clear()
        }

        it("should send data to Elasticsearch to the owntracks index") {
            // send response
            val response = """ { "took": 30, "errors": false,
   "items": [
      {
         "index": {
            "_index": "owntracks-2017", "_type": "location", "_id": "1234", "_version": 1,
            "result": "created",
            "_shards": { "total": 1, "successful": 1, "failed": 0 },
            "created": true,
            "status": 201
         }
      }]} """

            val httpHandler = AssertingHttpHandler(message.messageId(), expectedMap, response)
            val server = createServer(httpHandler)
            try {
                val processor = ElasticsearchMessageProcessor("http://127.0.0.1:${server.address.port}", "abc", lambdaLogger)
                processor.process(listOf(message))
                assert.that(httpHandler.wasCalled.get(), equalTo(true))
                assert.that(lambdaLogger.lines, !hasElement("Response returned errors, logging whole response"))
            } finally {
                server.stop(0)
            }
        }

        it("should check for single failures and log them") {
            val response = """ { "took": 30, "errors": true, "items": [ { "index": "anything" }]} """
            val httpHandler = AssertingHttpHandler(message.messageId(), expectedMap, response)
            val server = createServer(httpHandler)
            try {
                val processor = ElasticsearchMessageProcessor("http://127.0.0.1:${server.address.port}", "abc", lambdaLogger)
                processor.process(listOf(message))
                assert.that(httpHandler.wasCalled.get(), equalTo(true))
                assert.that(lambdaLogger.lines, anyElement(startsWith("Response returned errors, logging whole response")))
            } finally {
                server.stop(0)
            }
        }
    }
})

fun createServer(handler: HttpHandler): HttpServer {
    val server = HttpServer.create()
    server.bind(InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0), 0)
    server.createContext("/", handler)
    server.start()
    return server
}

class AssertingHttpHandler(val id : String, val expectedData : Map<String, Any?>, val response : String) : HttpHandler {
    val wasCalled = AtomicBoolean(false)

    override fun handle(exchange: HttpExchange?) {
        exchange!!
        wasCalled.set(true)
        assert.that(exchange.requestMethod, equalTo("PUT"))
        assert.that(exchange.requestHeaders.getFirst("Authorization"), equalTo("Basic abc"))
        assert.that(exchange.requestURI.path, equalTo("/<owntracks-{now/d{YYYY}}>/location/_bulk"))
        val bodyAsString = exchange.requestBody.bufferedReader().use { it.readText() }
        assert.that(bodyAsString, containsSubstring("""{ "index": { "_id" : "${id}" } }"""))
        val map : Map<String, Any?> = JSON.std.mapFrom(bodyAsString.split("\n")[1])
        assert.that(map, equalTo(expectedData))
        assert.that(bodyAsString, endsWith("\n"))

        val byteArray = response.toByteArray(Charsets.UTF_8)
        exchange.sendResponseHeaders(200, byteArray.size.toLong())
        exchange.responseBody.use { it.write(byteArray) }
    }
}