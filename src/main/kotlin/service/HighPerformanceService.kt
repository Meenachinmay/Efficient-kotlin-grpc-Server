package org.polarmeet.service

import io.grpc.stub.StreamObserver
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import org.polarmeet.proto.HighPerformanceServiceGrpc
import org.polarmeet.proto.Request
import org.polarmeet.proto.Response
import kotlin.coroutines.CoroutineContext

class HighPerformanceService(
    private val coroutineContext: CoroutineContext
) : HighPerformanceServiceGrpc.HighPerformanceServiceImplBase() {

    companion object {
        private val OK_RESPONSE = Response.newBuilder()
            .setStatus("OK")
            .build()
    }

    // Coroutine scope for managing all service operations
    private val serviceScope = CoroutineScope(coroutineContext + SupervisorJob())

    override fun process(request: Request, responseObserver: StreamObserver<Response>) {
        // Launch a coroutine for each request
        serviceScope.launch {
            try {
                // Process the request in a non-blocking way
                withContext(coroutineContext) {
                    responseObserver.onNext(OK_RESPONSE)
                    responseObserver.onCompleted()
                }
            } catch (e: Exception) {
                responseObserver.onError(e)
            }
        }
    }

    override fun processStream(responseObserver: StreamObserver<Response>): StreamObserver<Request> {
        // Create a channel for handling the stream
        val channel = Channel<Request>(Channel.UNLIMITED)

        // Launch a coroutine to process the stream
        serviceScope.launch {
            try {
                channel.consumeAsFlow()
                    .buffer(Channel.UNLIMITED)
                    .collect { _ ->
                        responseObserver.onNext(OK_RESPONSE)
                    }
                responseObserver.onCompleted()
            } catch (e: Exception) {
                responseObserver.onError(e)
            }
        }

        // Return a StreamObserver that feeds into our channel
        return object : StreamObserver<Request> {
            override fun onNext(request: Request) {
                serviceScope.launch {
                    channel.send(request)
                }
            }

            override fun onError(error: Throwable) {
                channel.close(error)
            }

            override fun onCompleted() {
                channel.close()
            }
        }
    }
}