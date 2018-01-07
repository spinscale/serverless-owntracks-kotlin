package de.spinscale.s3

import akka.actor.CoordinatedShutdown
import com.natpryce.hamkrest.assertion.assert
import com.natpryce.hamkrest.equalTo
import com.natpryce.hamkrest.throws
import de.spinscale.test.TestContext
import de.spinscale.test.TestLambdaLogger
import io.findify.s3mock.S3Mock
import io.findify.s3mock.provider.InMemoryProvider
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import software.amazon.awssdk.core.regions.Region
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3AdvancedConfiguration
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.CreateBucketRequest
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.s3.model.S3Exception
import java.net.URI
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Calendar
import java.util.zip.GZIPInputStream


object ReducerSpec : Spek({

    given("A S3 Reducer") {

        val lambdaLogger = TestLambdaLogger()
        val context = TestContext(lambdaLogger)
        val bucket = "owntracks-bucket"

        var api : S3Mock? = null
        var cs : CoordinatedShutdown? = null
        var s3Client = S3Client.builder().region(Region.US_EAST_1).build()

        beforeGroup {
            System.setProperty("aws.accessKeyId", "foo")
            System.setProperty("aws.secretAccessKey", "bar")
            System.setProperty("AWS_BUCKET", bucket)
            val actor = S3Mock.`$lessinit$greater$default$3`(12345, InMemoryProvider())
            api = S3Mock(12345, InMemoryProvider(), actor)
            api?.start()
            cs = CoordinatedShutdown.get(actor);
            s3Client = S3Client.builder()
                    .endpointOverride(URI("http://localhost:12345")).region(Region.US_EAST_1)
                    .advancedConfiguration(S3AdvancedConfiguration.builder().pathStyleAccessEnabled(true).build())
                    .build()
            s3Client?.createBucket(CreateBucketRequest.builder().bucket(bucket).build())
        }

        afterGroup {
            s3Client.close()
            // workaround because of https://github.com/findify/s3mock/issues/67
            cs!!.runAll()
            api?.stop()
        }

        afterEachTest {
            lambdaLogger.lines.forEach { println(it) }
        }

        beforeEachTest {
            lambdaLogger.lines.clear()
        }

        describe("should be called to") {

            it("create a single compressed file") {
                val firstFileName = "/data/2017/07/31/10:34.json"
                s3Client.putObject(PutObjectRequest.builder().bucket(bucket).key(firstFileName).build(),
                        RequestBody.of("this is my second content\n"))
                val secondFileName = "/data/2017/07/31/09:23.json"
                s3Client.putObject(PutObjectRequest.builder().bucket(bucket).key(secondFileName).build(),
                        RequestBody.of("this is my content\n"))
                // this will fail when this test runs on midnight, good enough for now
                val format = DateTimeFormatter.ofPattern("YYYY/MM/dd").format(LocalDate.now())
                val todaysFilename = "/data/$format/12:12.json"
                s3Client.putObject(PutObjectRequest.builder().bucket(bucket).key(todaysFilename).build(),
                        RequestBody.of("not to be included\n"))

                val reducer = Reducer(s3Client)
                reducer.handleRequest(emptyMap(), context)

                val calendar = Calendar.getInstance()
                val year = calendar.get(Calendar.YEAR)
                val weekOfYear = calendar.get(Calendar.WEEK_OF_YEAR)
                val expectedKeyName = "/archives/$year-$weekOfYear.json.gz"

                // check contents of gzipped data
                val stream = s3Client.getObject(GetObjectRequest.builder().key(expectedKeyName).bucket(bucket).build())
                GZIPInputStream(stream).use { inputStream ->
                    val text = inputStream.reader().readText()
                    assert.that(text, equalTo("this is my content\nthis is my second content\n"))
                }

                // test that old files have been deleted
                assert.that({
                    s3Client.getObject(GetObjectRequest.builder().key(firstFileName).bucket(bucket).build())
                }, throws<S3Exception>())
                assert.that({
                    s3Client.getObject(GetObjectRequest.builder().key(secondFileName).bucket(bucket).build())
                }, throws<S3Exception>())

                s3Client.getObject(GetObjectRequest.builder().key(todaysFilename).bucket(bucket).build()).use { stream ->
                    checkNotNull(stream)
                }
            }
        }
    }
})