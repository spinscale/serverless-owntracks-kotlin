package de.spinscale.test

import com.amazonaws.services.lambda.runtime.ClientContext
import com.amazonaws.services.lambda.runtime.CognitoIdentity
import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.LambdaLogger

class TestContext(private val logger : LambdaLogger) : Context {
    override fun getAwsRequestId(): String {
        TODO("not implemented")
    }

    override fun getLogStreamName(): String {
        TODO("not implemented")
    }

    override fun getClientContext(): ClientContext {
        TODO("not implemented")
    }

    override fun getFunctionName(): String {
        TODO("not implemented")
    }

    override fun getRemainingTimeInMillis(): Int {
        TODO("not implemented")
    }

    override fun getLogger(): LambdaLogger {
        return logger
    }

    override fun getInvokedFunctionArn(): String {
        TODO("not implemented")
    }

    override fun getMemoryLimitInMB(): Int {
        TODO("not implemented")
    }

    override fun getLogGroupName(): String {
        TODO("not implemented")
    }

    override fun getFunctionVersion(): String {
        TODO("not implemented")
    }

    override fun getIdentity(): CognitoIdentity {
        TODO("not implemented")
    }
}

class TestLambdaLogger : LambdaLogger {

    val lines = ArrayList<String>()

    override fun log(message: ByteArray?) {
        if (message is ByteArray) {
            lines += message.toString()
        }
    }

    override fun log(line: String?) {
        if (line is String) {
            lines += line
        }
    }

}