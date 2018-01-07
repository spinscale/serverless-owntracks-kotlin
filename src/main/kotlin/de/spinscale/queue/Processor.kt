package de.spinscale.queue

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.LambdaLogger
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.fasterxml.jackson.jr.ob.JSON
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpPut
import org.apache.http.entity.ContentType
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.ObjectCannedACL
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.sqs.SQSClient
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest
import software.amazon.awssdk.services.sqs.model.Message
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest
import java.text.SimpleDateFormat
import java.util.*


/*
 *
 * This job runs every 10 minutes (144 a day):
 *  - creates a new file in s3
 *  - sends a bulk request to an Elastic Cloud instance
 *
This is what a scheduled event looks like
{
  "account": "123456789012",
  "region": "us-east-1",
  "detail": {},
  "detail-type": "Scheduled Event",
  "source": "aws.events",
  "time": "1970-01-01T00:00:00Z",
  "id": "cdc73f9d-aea9-11e3-9d5a-835b769c0d9c",
  "resources": [
    "arn:aws:events:us-east-1:123456789012:rule/my-schedule"
  ]
}
 */

open class Processor (
        private val sqsClient : SQSClient = SQSClient.builder().build(),
        private val queue : String = System.getenv("AWS_QUEUE") ?: System.getProperty("AWS_QUEUE")
): RequestHandler<Map<String, Any>, Unit> {

    companion object {
        val MAX_MESSAGES = 10
        val FETCH_TOTAL_MESSAGE = 250
    }

    override fun handleRequest(input: Map<String, Any>?, context: Context?) {
        context?.logger?.log("Got input: $input\n")

        val queueUrl = queueUrl()
        val messages = receiveMessages(queueUrl)

        if (messages.isEmpty()) {
            context?.logger?.log("No messages in queue, exiting\n")
            return
        }

        val messageProcessors = getProcessors(context!!)
        context.logger?.log("Going to process ${messages.size} messages with ${messageProcessors.size} processors\n")
        messageProcessors.forEach {
            try {
                it.process(messages)
            } catch (e : Exception) {
                context.logger?.log("Processor threw exception, continuing ${e.stackTrace.joinToString("\n")}\n")
            }
        }

        deleteMessages(queueUrl, messages, context.logger)
    }

    fun queueUrl() : String {
        val queueUrlResponse = sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(queue).build())
        return queueUrlResponse.queueUrl()
    }

    fun receiveMessages(queueUrl : String) : List<Message> {
        var continueFetching = true
        val messages = arrayListOf<Message>()
        while (continueFetching) {
            val requestMessages = receiveMessageBatch(queueUrl)
            messages += requestMessages
            continueFetching = requestMessages.size == MAX_MESSAGES && messages.size < FETCH_TOTAL_MESSAGE
        }

        return messages
    }

    fun receiveMessageBatch(queueUrl : String) : List<Message> {
        val receiveMessageRequest = ReceiveMessageRequest.builder().maxNumberOfMessages(MAX_MESSAGES).queueUrl(queueUrl).build()
        val receiveMessages = sqsClient.receiveMessage(receiveMessageRequest)
        return receiveMessages.messages() ?: Collections.emptyList()
    }

    fun deleteMessages(queueUrl : String, messages : List<Message>, logger : LambdaLogger) {
        var i = 0
        messages.split(10).forEach { msgs ->
            val deleteMessageEntries = msgs.map { message -> DeleteMessageBatchRequestEntry.builder()
                    .receiptHandle(message.receiptHandle())
                    .id((i++).toString())
                    .build()
            }
            val deleteMessageBatchRequest = DeleteMessageBatchRequest.builder().entries(deleteMessageEntries).queueUrl(queueUrl).build()
            val deleteMessageBatchResponse = sqsClient.deleteMessageBatch(deleteMessageBatchRequest)
            if (deleteMessageBatchResponse != null) {
                logger.log(String.format("Batch deletion of messages: %s successful, %s failed\n",
                        deleteMessageBatchResponse.successful()?.size, deleteMessageBatchResponse.failed()?.size))
            } else {
                logger.log("Deletion of message batch returned null message\n")
            }
        }
    }

    open fun getProcessors(context: Context) : List<MessageProcessor> {
        return listOf(S3StoreMessageProcessor(context.logger),
                ElasticsearchMessageProcessor(System.getenv("ELASTIC_MESSAGE_PROCESSOR_HOST"), System.getenv("ELASTIC_MESSAGE_PROCESSOR_AUTH"), context.logger))
    }
}

/**
 * splits a list into sublists
 * @param partitionSize the max size of each sublist. The last sub list may be shorter.
 * @return a List of Lists of T
 */
fun <T> List<T>.split(partitionSize: Int): List<List<T>> {
    if(this.isEmpty()) return emptyList()
    if(partitionSize < 1) throw IllegalArgumentException("partitionSize must be positive")

    val result = ArrayList<List<T>>()
    var entry = ArrayList<T>(partitionSize)
    for (item in this) {
        if(entry.size == partitionSize) {
            result.add(entry)
            entry = ArrayList()
        }
        entry.add(item)
    }
    result.add(entry)
    return result
}


interface MessageProcessor {
    fun process(messages : List<Message>)
}

class ElasticsearchMessageProcessor(
        private val host : String,
        private val authorizationHeader : String,
        private val logger: LambdaLogger,
        private val enabled: Boolean = !host.isEmpty() && !authorizationHeader.isEmpty()
    ) : MessageProcessor {

    override fun process(messages: List<Message>) {
        if (!enabled) {
            return
        }

        HttpClients.createDefault().use { client ->
            // the index name resembles owntracks-{now/d{YYYY}}, so it becomes owntracks-2017
            val host = "${host}/%3Cowntracks-%7Bnow%2Fd%7BYYYY%7D%7D%3E/location/_bulk"
            val request = HttpPut(host)
            val sb = StringBuilder()
            messages.forEach { message ->
                val inputJson : Map<String, Any> = JSON.std.mapFrom(message.body())
                // replace all fields starting with underscore
                val regex = Regex("^_")
                val map = HashMap(inputJson.entries.map { e ->
                    if (e.key.startsWith("_")) {
                        e.key.replace(regex, "") to e.value
                    } else {
                        e.key to e.value
                    }
                }.toMap())

                val lon = map.remove("lon") as Double
                val lat = map.remove("lat") as Double
                map.put("location", mapOf("lon" to lon, "lat" to lat))

                val df = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'") // Quoted "Z" to indicate UTC, no timezone offset
                df.timeZone = TimeZone.getTimeZone("UTC")
                val epochFromJson = map.get("tst") as Int
                val epochSeconds = Date(epochFromJson.toLong() * 1000)
                map.put("timestamp", df.format(epochSeconds))

                // use the message id to prevent duplicates, even if processed more than once
                sb.append("{ \"index\": { \"_id\" : \"${message.messageId()}\" } }\n").append(JSON.std.asString(map)).append("\n")
            }
            request.entity = StringEntity(sb.toString(), ContentType.APPLICATION_JSON)
            request.addHeader("Authorization", "Basic ${authorizationHeader}")
            request.config = RequestConfig.custom()
                    .setConnectionRequestTimeout(10000)
                    .setConnectTimeout(10000)
                    .setSocketTimeout(10000)
                    .build()

            val response = client.execute(request)
            logger.log("Response from sending bulk to elastic cluster: ${response.statusLine}\n")

            val responseBody = EntityUtils.toString(response.entity)
            val json : Map<String, Any> = JSON.std.mapFrom(responseBody)
            val hasErrors = json["errors"] as Boolean
            if (hasErrors) {
                logger.log("Response returned errors, logging whole response: $responseBody\n")
            }
        }
    }
}

class S3StoreMessageProcessor(private val logger: LambdaLogger,
                              private val bucket : String = System.getenv("AWS_BUCKET") ?: System.getProperty("AWS_BUCKET")
) : MessageProcessor {

    override fun process(messages: List<Message>) {
        // create new S3 file
        val s3Client = S3Client.builder().build()
        val path = getKey()
        val putObjectRequest = PutObjectRequest.builder()
                .acl(ObjectCannedACL.PRIVATE)
                .bucket(bucket)
                .contentType("text/json")
                .key(path)
                .build()
        s3Client.putObject(putObjectRequest, getData(messages))
        logger.log("Stored ${messages.size} messages in file $path\n")
    }

    fun getData(messages : List<Message>) : RequestBody {
        val data = StringBuilder()
        messages.forEach { data.append(it.body()); data.append("\n") }
        return RequestBody.of(data.toString())
    }

    fun getKey(date: Date = Date()): String {
        val dateFormat = SimpleDateFormat("'/data/'yyyy/MM/dd/HH:mm.'json'")
        return dateFormat.format(date)
    }
}