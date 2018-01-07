package de.spinscale.queue

import com.amazonaws.services.lambda.runtime.Context
import com.natpryce.hamkrest.*
import com.natpryce.hamkrest.assertion.assert
import de.spinscale.test.TestContext
import de.spinscale.test.TestLambdaLogger
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it
import software.amazon.awssdk.services.sqs.SQSClient
import software.amazon.awssdk.services.sqs.model.*
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.collections.ArrayList

object ProcessorSpec : Spek({

    describe("AWS Lambda Event Scheduled Event Processor") {

        val lambdaLogger = TestLambdaLogger()
        val context = TestContext(lambdaLogger)
        val message = Message.builder().body(""" { "foo" : "bar" } """).receiptHandle("baz").build()
        val messages = listOf(message)

        beforeGroup {
            System.setProperty("AWS_QUEUE", "anyqueue")
        }

        // make sure we start with empty instances
        beforeEachTest {
            lambdaLogger.lines.clear()
        }

        afterEachTest {
            lambdaLogger.lines.forEach { print(it) }
        }

        it("should log if no messages were processed") {
            val processor = TestProcessor(MockSQSClient(), emptyList())
            processor.handleRequest(emptyMap(), context)
            assert.that(lambdaLogger.lines, hasSize(equalTo(2)))
            assert.that(lambdaLogger.lines, hasElement("No messages in queue, exiting\n"))
        }

        it("should process messages") {
            val client = MockSQSClient(listOf(messages))
            val processor = TestProcessor(client, listOf(LoggingMessageProcessor(messages)))
            processor.handleRequest(emptyMap(), context)
            assert.that(lambdaLogger.lines, anyElement(startsWith("Got input")))
            assert.that(lambdaLogger.lines, anyElement(startsWith("Going to process 1 messages with 1 processors")))
            assert.that(lambdaLogger.lines, hasElement("Batch deletion of messages: 1 successful, 0 failed\n"))
        }

        it("should ignore exceptions in a processor") {
            val client = MockSQSClient(listOf(messages))
            val loggingMessageProcessor = LoggingMessageProcessor(messages)
            val messageProcessors = listOf(ExceptionThrowingProcessor(), loggingMessageProcessor)
            val processor = TestProcessor(client, messageProcessors)
            processor.handleRequest(emptyMap(), context)
            assert.that(loggingMessageProcessor.processed.get(), equalTo(true))
        }

        it("should fetch more than 10 messages in batches") {
            val firstBatch = arrayListOf<Message>()
            repeat(10) { firstBatch += Message.builder().body(""" { "foo" : "bar" } """).receiptHandle(it.toString()).build() }
            val secondBatch = arrayListOf<Message>()
            repeat(5) { secondBatch += Message.builder().body(""" { "foo" : "bar" } """).receiptHandle(it.toString()).build() }
            val client = MockSQSClient(listOf(firstBatch, secondBatch))
            val processor = TestProcessor(client, emptyList())
            processor.handleRequest(emptyMap(), context)
            client.assertAllMessagesDeleted()
            assert.that(client.deletedMessageIds, hasSize(equalTo(15)))
        }

        it("should fetch up to 250 messages") {
            val lotsOfMessages = arrayListOf<List<Message>>()
            val batch = arrayListOf<Message>()
            repeat(10) { batch += message }
            repeat(26) { lotsOfMessages += batch }
            val client = MockSQSClient(lotsOfMessages)
            val processor = TestProcessor(client, emptyList());
            processor.handleRequest(emptyMap(), context)
            assert.that(client.deletedMessageIds, hasSize(equalTo(250)))
        }
    }

    describe("Test List.split") {
        it("should not split when less than specified elements") {
            val list = listOf(1, 2, 3, 4, 5)
            assert.that(list.split(10).size, equalTo(1))
            assert.that(list.split(10).first(), equalTo(list))
        }

        it("should split when more than specified elements") {
            val list = listOf(1, 2, 3, 4, 5)
            assert.that(list.split(2).size, equalTo(3))
            assert.that(list.split(2), equalTo(listOf(listOf(1, 2), listOf(3, 4), listOf(5))))
        }
    }

    describe("MessageProcessor: S3") {

        val lambdaLogger = TestLambdaLogger()

        beforeGroup {
            System.setProperty("AWS_BUCKET", "somebucket")
            System.setProperty("AWS_QUEUE", "anyqueue")
        }

        // make sure we start with empty instances
        beforeEachTest {
            lambdaLogger.lines.clear()
        }

        it("should write messages separated by newline") {
            val first = Message.builder().body("""{ "foo" : "first" }""").build()
            val second = Message.builder().body("""{ "foo" : "second" }""").build()
            val messages = listOf(first, second)
            val body = S3StoreMessageProcessor(lambdaLogger, "my_bucket").getData(messages)
            assert.that(body.contentLength, greaterThan(0L))
            body.asStream().bufferedReader().use { r ->
                assert.that(r.readLine(), containsSubstring("first"))
                assert.that(r.readLine(), containsSubstring("second"))
            }
        }

        it("should create correct key path") {
            val cal = Calendar.getInstance()
            cal.set(2017, 3, 21, 22, 30)
            val key = S3StoreMessageProcessor(lambdaLogger, "my_bucket").getKey(cal.time)
            assert.that(key, equalTo("/data/2017/04/21/22:30.json"))
        }

    }
})

open class TestProcessor : Processor {

    private var messageProcessors: List<MessageProcessor>

    constructor(
            client: SQSClient,
            messageProcessors: List<MessageProcessor>
    ) : super(client) {
        this.messageProcessors = messageProcessors;
    }

    override fun getProcessors(context: Context): List<MessageProcessor> {
        return messageProcessors
    }
}

class LoggingMessageProcessor(private val messages: List<Message>) : MessageProcessor {
    val processed = AtomicBoolean(false)

    override fun process(messages: List<Message>) {
        assert.that(messages, equalTo(this.messages))
        processed.set(true)
    }
}

class ExceptionThrowingProcessor : MessageProcessor {
    override fun process(messages: List<Message>) {
        throw Exception("just throwing exception")
    }
}

class MockSQSClient(
        private val messages : List<List<Message>> = listOf(emptyList())
) : SQSClient {

    private val index = AtomicInteger(0)
    private val deletedMessageHandles = ArrayList<String>()
    val deletedMessageIds = ArrayList<String>()

    override fun close() {
    }

    override fun getQueueUrl(getQueueUrlRequest: GetQueueUrlRequest?): GetQueueUrlResponse {
        return GetQueueUrlResponse.builder().queueUrl("whatever").build()
    }

    override fun receiveMessage(receiveMessageRequest: ReceiveMessageRequest?): ReceiveMessageResponse {
        return ReceiveMessageResponse.builder().messages(messages.get(index.getAndIncrement())).build()
    }

    override fun deleteMessageBatch(deleteMessageBatchRequest: DeleteMessageBatchRequest?): DeleteMessageBatchResponse {
        val result : MutableList<DeleteMessageBatchResultEntry> = arrayListOf()
        deleteMessageBatchRequest!!.entries().forEach {
            deletedMessageHandles.add(it.receiptHandle())
            deletedMessageIds.add(it.id())
            val entry = DeleteMessageBatchResultEntry.builder().id(it.id()).build()
            result.add(entry)
        }
        return DeleteMessageBatchResponse.builder().failed(emptyList()).successful(result).build()
    }

    fun assertAllMessagesDeleted() {
        assert.that(index.get(), equalTo(messages.size))
        assert.that(deletedMessageIds.last().toInt(), equalTo(messages.flatten().size-1))
        // ensure all configured messages have been properly deleted
        val receiptHandles = messages.flatten().map { it.receiptHandle() }.sorted()
        assert.that(receiptHandles, equalTo(deletedMessageHandles.sorted()))
    }
}
