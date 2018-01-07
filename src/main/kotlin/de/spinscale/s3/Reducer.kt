package de.spinscale.s3

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.LambdaLogger
import com.amazonaws.services.lambda.runtime.RequestHandler
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.core.sync.StreamingResponseHandler
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.Delete
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.GetObjectResponse
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response
import software.amazon.awssdk.services.s3.model.ObjectCannedACL
import software.amazon.awssdk.services.s3.model.ObjectIdentifier
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.s3.model.S3Object
import java.io.ByteArrayOutputStream
import java.text.SimpleDateFormat
import java.util.ArrayList
import java.util.Date
import java.util.zip.GZIPOutputStream


class Reducer(
        val s3Client: S3Client = S3Client.builder().build(),
        val bucket : String = System.getenv("AWS_BUCKET") ?: System.getProperty("AWS_BUCKET")
) : RequestHandler<Map<String, Any>, Unit> {

    companion object {
        val MAX_FILES = 5000
    }

    override fun handleRequest(input: Map<String, Any>?, context: Context?) {
        val objects = getObjectsFromBucket()

        if (objects.isEmpty()) {
            context?.logger?.log(String.format("No s3 files to process, exiting\n"))
            return
        }

        reduceObjectsToOne(objects, context?.logger)
        deleteObjects(objects, context?.logger)
    }

    private fun getObjectsFromBucket() : List<S3Object> {
        var v2Response = ListObjectsV2Response.builder().build()
        val date = SimpleDateFormat("yyyy/MM/dd").format(Date())
        val objects = ArrayList<S3Object>()
        do {
            val requestBuilder = ListObjectsV2Request.builder()
                    .bucket(bucket)
                    .prefix("data/")
            if (v2Response.nextContinuationToken() != null) {
                requestBuilder.continuationToken(v2Response.nextContinuationToken())
            }
            v2Response = s3Client.listObjectsV2(requestBuilder.build())
            if (v2Response.contents() == null) {
                break
            }
            // exclude current day
            objects.addAll(v2Response.contents().filter {
                !it.key().contains(date)
            })
        } while (v2Response.isTruncated() || objects.size > MAX_FILES)

        // sort and return
        return objects.sortedWith(compareBy(S3Object::key))
    }

    private fun reduceObjectsToOne(objects: List<S3Object>, logger: LambdaLogger?) {
        val key = getKey()
        val putObjectRequest = PutObjectRequest.builder()
                .acl(ObjectCannedACL.PRIVATE)
                .bucket(bucket)
                .key(key)
                .build()

        ByteArrayOutputStream().use { bos ->
            GZIPOutputStream(bos).use { os ->
                objects.forEach { s3Obj ->
                    val getObjectRequest = GetObjectRequest.builder().bucket(bucket).key(s3Obj.key()).build()
                    s3Client.getObject(getObjectRequest, StreamingResponseHandler.toOutputStream<GetObjectResponse>(os))
                }
            }
            logger?.log("Wrote s3 file ${key}\n")

            s3Client.putObject(putObjectRequest, RequestBody.of(bos.toByteArray()))
        }
    }

    private fun deleteObjects(sortedObjects: List<S3Object>, logger : LambdaLogger?) {
        val deleteBuilder = DeleteObjectsRequest.builder().bucket(bucket)
        val ids = sortedObjects.map { ObjectIdentifier.builder().key(it.key()).build() }.toMutableList()
        deleteBuilder.delete(Delete.builder().objects(ids).build())
        val deleteObjectResponse = s3Client.deleteObjects(deleteBuilder.build())
        if (deleteObjectResponse.errors() != null && !deleteObjectResponse.errors().isEmpty()) {
            logger?.log(String.format("Deleted %s files, but had errors: %s\n", deleteObjectResponse.deleted().size, deleteObjectResponse.errors()))
        } else {
            logger?.log("Deleted ${deleteObjectResponse.deleted().size} files\n")
        }
    }

    private fun getKey(date: Date = Date()): String {
        val dateFormat = SimpleDateFormat("'/archives/'yyyy-w'.json.gz'")
        return dateFormat.format(date)
    }
}