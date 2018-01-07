package de.spinscale.http

import com.natpryce.hamkrest.assertion.assert
import com.natpryce.hamkrest.containsSubstring
import com.natpryce.hamkrest.equalTo
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import software.amazon.awssdk.core.regions.Region
import software.amazon.awssdk.services.sqs.SQSClient
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse
import software.amazon.awssdk.services.sqs.model.SendMessageRequest
import software.amazon.awssdk.services.sqs.model.SendMessageResponse

object HandlerSpec : Spek({

    given("A HTTP Handler") {

        describe("returns 403 with") {
            val sqsClient : SQSClient = SQSClient.builder()
                    .region(Region.US_EAST_1)
                    .build()

            val handler = Handler(sqsClient, "anyqueue", "anyauth")

            it("empty input map") {
                val response = handler.handleRequest(emptyMap(), null)
                assert.that(response.statusCode, equalTo(403))
            }

            it("missing authentication") {
                val input = mapOf("headers" to emptyMap<String, String>())
                val response = handler.handleRequest(input, null)
                assert.that(response.statusCode, equalTo(403))
            }

            it("wrong authentication") {
                val headers = mapOf("Authorization" to "invalid")
                val input = mapOf("headers" to headers)
                val response = handler.handleRequest(input, null)
                assert.that(response.statusCode, equalTo(403))
            }
        }

        describe("returns 200 and provdes valid JSON to send SQS message") {

            val headers = hashMapOf("Authorization" to "Basic anyauth")
            val body = """ { "tid":"foo", "lat": 1234.56, "lon": 65.4321, "tst" : 1234567890 } """

            it("when required parameters are supplied") {
                val sqsClient = MockSQSClient()
                val handler = Handler(sqsClient, "anyqueue", "anyauth")
                val input = mapOf("headers" to headers, "body" to body)
                val response = handler.handleRequest(input, null)
                assert.that(response.statusCode, equalTo(200))
                assert.that(sqsClient.messageBody, containsSubstring("lon"))
                assert.that(sqsClient.messageBody, containsSubstring("lat"))
                assert.that(sqsClient.messageBody, containsSubstring("tid"))
            }

            it("when user header x-limit-u is supplied") {
                val sqsClient = MockSQSClient()
                val handler = Handler(sqsClient, "anyqueue", "anyauth")
                headers.put("x-limit-u", "my_user")
                val input = mapOf("headers" to headers, "body" to body)
                val response = handler.handleRequest(input, null)
                assert.that(response.statusCode, equalTo(200))
                assert.that(sqsClient.messageBody, containsSubstring("my_user"))
            }

            it("when device header x-limit-d is supplied") {
                val sqsClient = MockSQSClient()
                val handler = Handler(sqsClient, "anyqueue", "anyauth")
                headers.put("x-limit-d", "my_device")
                val input = mapOf("headers" to headers, "body" to body)
                val response = handler.handleRequest(input, null)
                assert.that(response.statusCode, equalTo(200))
                assert.that(sqsClient.messageBody, containsSubstring("my_device"))
            }
        }
    }
})

class MockSQSClient : SQSClient {
    var messageBody : String = ""

    override fun close() {
    }

    override fun getQueueUrl(getQueueUrlRequest: GetQueueUrlRequest?): GetQueueUrlResponse {
        return GetQueueUrlResponse.builder().queueUrl("anyqueue").build()
    }

    override fun sendMessage(sendMessageRequest: SendMessageRequest?): SendMessageResponse {
        assert.that(sendMessageRequest?.queueUrl(), equalTo("anyqueue"))
        messageBody = sendMessageRequest?.messageBody() ?: ""
        return SendMessageResponse.builder().messageId("my-message-id").build()
    }
}
