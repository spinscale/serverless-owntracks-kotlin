package de.spinscale.http

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.fasterxml.jackson.jr.ob.JSON
import software.amazon.awssdk.core.auth.EnvironmentVariableCredentialsProvider
import software.amazon.awssdk.services.sqs.SQSClient
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest
import software.amazon.awssdk.services.sqs.model.SendMessageRequest

class Handler(
        private val sqsClient : SQSClient = SQSClient.builder().credentialsProvider(EnvironmentVariableCredentialsProvider.create()).build(),
        private val queue : String = System.getenv("AWS_QUEUE") ?: System.getProperty("AWS_QUEUE"),
        private val expectedBasicAuthHeader : String = System.getenv("BASIC_AUTH") ?: System.getProperty("BASIC_AUTH")
) : RequestHandler<Map<String, Any>, ApiGatewayResponse> {

    companion object {
        val AUTH_REQUIRED_RESPONSE = ApiGatewayResponse(403, "Please authenticate",
                mapOf("WWW-Authenticate" to "Basic realm=\"Owntracks realm\""))
    }

    override fun handleRequest(input: Map<String, Any>?, context: Context?): ApiGatewayResponse {
        context?.logger?.log("received: $input\n")
        // authorization check
        if (input!!.containsKey("headers").not() || input["headers"] !is Map<*, *>) {
            context?.logger?.log("No headers included, no auth header\n")
            return AUTH_REQUIRED_RESPONSE
        }

        val headers = input["headers"] as Map<*, *>
        if (!headers.containsKey("Authorization")) {
            context?.logger?.log("No authorization header included\n")
            return AUTH_REQUIRED_RESPONSE
        }

        val authHeader = headers["Authorization"]
        if (authHeader != "Basic $expectedBasicAuthHeader") {
            context?.logger?.log("Expected header [$expectedBasicAuthHeader], real header [$authHeader]\n")
            return AUTH_REQUIRED_RESPONSE
        }

        // body non-null check
        if (!input.containsKey("body") || input["body"] == null || input["body"].toString().isEmpty()) {
            return ApiGatewayResponse(400, "Please provide body", emptyMap())
        }

        val map : MutableMap<String, Any?> = JSON.std.mapFrom(input["body"])
        if (headers.containsKey("x-limit-u")) {
            map.put("user", headers["x-limit-u"])
        }
        if (headers.containsKey("x-limit-d")) {
            map.put("device", headers["x-limit-d"])
        }

        val output = JSON.std.asString(map)
        context?.logger?.log("Write JSON data to SQS $output\n")
        val messageResponse = sendSqsMessage(output.toString())
        context?.logger?.log("Sent message with id $messageResponse\n")
        return ApiGatewayResponse(200, "[]", emptyMap())
    }

    fun sendSqsMessage(message : String): String? {
        val queueUrlResponse = sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(queue).build())
        val request = SendMessageRequest.builder()
                .messageBody(message)
                .queueUrl(queueUrlResponse.queueUrl())
                .build()
        return sqsClient.sendMessage(request).messageId()
    }
}

data class ApiGatewayResponse(val statusCode: Int, val body: String, val headers: Map<String, String>,
        // API Gateway expects the property to be called "isBase64Encoded" => isIs
                         val isIsBase64Encoded: Boolean = false)
