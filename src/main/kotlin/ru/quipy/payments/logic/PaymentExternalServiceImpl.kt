package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.*
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine


class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests
    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1L))
    private val nonBlockingOngoingWindow = NonBlockingOngoingWindow(parallelRequests)


    private val timeout = Duration.ofSeconds(120)
    private val client = HttpClient
        .newBuilder()
        .version(HttpClient.Version.HTTP_2)
        .connectTimeout(timeout)
        .build()

    private val dispatcher = Dispatchers.IO
    private val coroutineScope = CoroutineScope(dispatcher + SupervisorJob())

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
        coroutineScope.launch {
            rateLimiter.tickBlocking()
            nonBlockingOngoingWindow.putIntoWindow()
            var success = false
            var retry = 0
            while (!success && retry++ < 2) {
                try {
                    logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

                    val response = call(paymentId, amount, transactionId)
                    val body = try {
                        mapper.readValue(response.body(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.statusCode()}, reason: ${response.body()}")
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                    }
                    success = body.result
                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")
                } catch (error: Exception) {
                    when (error) {
                        is SocketTimeoutException -> {
                            logger.error(
                                "[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId",
                                error
                            )
                        }
                        else -> {
                            logger.error(
                                "[$accountName] Payment failed for txId: $transactionId, payment: $paymentId",
                                error.message
                            )
                        }
                    }
                }

            }
            nonBlockingOngoingWindow.releaseWindow()
        }
    }

    private suspend fun call(paymentId: UUID, amount: Int, transactionId: UUID) = suspendCoroutine<HttpResponse<String>> { continuation ->
        val uri = URI("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
        val request = HttpRequest
            .newBuilder()
            .uri(uri)
            .POST(HttpRequest.BodyPublishers.noBody())
            .timeout(timeout)
            .build()

        client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).whenComplete { response, error ->
            if (error != null) {
                continuation.resumeWithException(error)
            } else {
                continuation.resume(response)
            }
        }
    }
    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()