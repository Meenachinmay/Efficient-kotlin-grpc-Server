package org.polarmeet.service

import io.grpc.Server
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioServerSocketChannel
import io.grpc.stub.StreamObserver
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import org.jetbrains.exposed.sql.batchInsert
import org.jetbrains.exposed.sql.transactions.experimental.newSuspendedTransaction
import org.polarmeet.proto.HighPerformanceServiceGrpc
import org.polarmeet.proto.Request
import org.polarmeet.proto.Response
import org.polarmeet.service.config.DatabaseConfig
import org.polarmeet.service.config.Requests

class HighPerformanceServer {
    private val server: Server
    private val coroutineScope = CoroutineScope(Dispatchers.Default + SupervisorJob())

    companion object {
        private const val PORT = 9090
    }

    init {
        // Configure Netty event loop groups for handling network operations
        val bossGroup: EventLoopGroup = NioEventLoopGroup(2)
        val workerGroup: EventLoopGroup = NioEventLoopGroup(32)

        // Build the gRPC server with optimized settings
        val builder = NettyServerBuilder.forPort(PORT)
            .channelType(NioServerSocketChannel::class.java)
            .bossEventLoopGroup(bossGroup)
            .workerEventLoopGroup(workerGroup)
            .maxConcurrentCallsPerConnection(100000)
            .maxInboundMessageSize(1024 * 1024)
            .maxInboundMetadataSize(1024 * 64)
            .addService(HighPerformanceServiceImpl(coroutineScope))

        server = builder.build()
    }

    private class HighPerformanceServiceImpl(
        private val scope: CoroutineScope
    ) : HighPerformanceServiceGrpc.HighPerformanceServiceImplBase() {

        // Pre-build the response object for better performance
        private val okResponse = Response.newBuilder().setStatus("OK").build()

        // Channel for buffering requests before batch processing
        private val requestChannel = Channel<Request>(Channel.UNLIMITED)

        init {
            // Start the background batch processing
            scope.launch {
                processBatchInserts()
            }
        }

        private suspend fun processBatchInserts() {
            while (true) {
                try {
                    val batch = mutableListOf<Request>()

                    // Collect up to 1000 requests or wait 100ms
                    withTimeoutOrNull(100) {
                        while (batch.size < 1000) {
                            batch.add(requestChannel.receive())
                        }
                    }

                    // Process the batch if we have any requests
                    if (batch.isNotEmpty()) {
                        newSuspendedTransaction(Dispatchers.IO) {
                            Requests.batchInsert(batch) { request ->
                                this[Requests.requestData] = request.data
                                this[Requests.timestamp] = System.currentTimeMillis()
                            }
                        }
                    }
                } catch (e: Exception) {
                    // Log error but continue processing
                    println("Error processing batch: ${e.message}")
                }
            }
        }

        override fun process(
            request: Request,
            responseObserver: StreamObserver<Response>
        ) {
            // Immediately respond to the client
            responseObserver.onNext(okResponse)
            responseObserver.onCompleted()

            // Queue the request for batch processing
            scope.launch {
                requestChannel.send(request)
            }
        }
    }

    fun start() {
        server.start()
        println("Server started on port $PORT")
    }

    fun blockUntilShutdown() {
        server.awaitTermination()
    }
}

fun main() {
    // Initialize the database before starting the server
    DatabaseConfig.init()

    // Create and start the server
    val server = HighPerformanceServer()
    server.start()
    server.blockUntilShutdown()
}