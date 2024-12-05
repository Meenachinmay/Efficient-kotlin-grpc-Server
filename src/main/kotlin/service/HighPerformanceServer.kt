package org.polarmeet.service

import io.grpc.Server
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioServerSocketChannel
import io.grpc.stub.StreamObserver
import kotlinx.coroutines.*
import java.util.concurrent.*
import org.jetbrains.exposed.sql.batchInsert
import org.jetbrains.exposed.sql.transactions.experimental.newSuspendedTransaction
import org.polarmeet.proto.HighPerformanceServiceGrpc
import org.polarmeet.proto.Request
import org.polarmeet.proto.Response
import org.polarmeet.service.config.DatabaseConfig
import org.polarmeet.service.config.Requests

class HighPerformanceServer {
    private val server: Server

    // Using a custom dispatcher for better control over thread allocation
    private val serverDispatcher = Executors.newVirtualThreadPerTaskExecutor().asCoroutineDispatcher()
    private val coroutineScope = CoroutineScope(serverDispatcher + SupervisorJob())

    companion object {
        private const val PORT = 9090
        private const val BATCH_SIZE = 1000
        private const val BATCH_TIMEOUT_MS = 100L
    }

    init {
        val bossGroup: EventLoopGroup = NioEventLoopGroup(2)
        val workerGroup: EventLoopGroup = NioEventLoopGroup(32)

        val builder = NettyServerBuilder.forPort(PORT)
            .channelType(NioServerSocketChannel::class.java)
            .bossEventLoopGroup(bossGroup)
            .workerEventLoopGroup(workerGroup)
            .executor(serverDispatcher.asExecutor())
            .maxConcurrentCallsPerConnection(100000)
            .maxInboundMessageSize(1024 * 1024)
            .maxInboundMetadataSize(1024 * 64)
            .addService(HighPerformanceServiceImpl(coroutineScope))

        server = builder.build()
    }

    private class HighPerformanceServiceImpl(
        private val scope: CoroutineScope
    ) : HighPerformanceServiceGrpc.HighPerformanceServiceImplBase() {

        private val okResponse = Response.newBuilder().setStatus("OK").build()

        // Using ArrayBlockingQueue for better performance than Channel
        private val requestQueue = ArrayBlockingQueue<Request>(100000)

        init {
            // Start multiple batch processors for parallel processing
            repeat(4) {
                scope.launch {
                    processBatchInserts()
                }
            }
        }

        private suspend fun processBatchInserts() {
            val batch = ArrayList<Request>(BATCH_SIZE)

            while (true) {
                try {
                    val deadline = System.currentTimeMillis() + BATCH_TIMEOUT_MS

                    // Collect batch using traditional blocking queue
                    while (batch.size < BATCH_SIZE && System.currentTimeMillis() < deadline) {
                        val request = withContext(Dispatchers.IO) {
                            requestQueue.poll(
                                deadline - System.currentTimeMillis(),
                                TimeUnit.MILLISECONDS
                            )
                        }
                        if (request != null) {
                            batch.add(request)
                        }
                    }

                    if (batch.isNotEmpty()) {
                        newSuspendedTransaction(Dispatchers.IO) {
                            Requests.batchInsert(batch) { request ->
                                this[Requests.requestData] = request.data
                                this[Requests.timestamp] = System.currentTimeMillis()
                            }
                        }
                        batch.clear()
                    }
                } catch (e: Exception) {
                    println("Error processing batch: ${e.message}")
                }
            }
        }

        override fun process(
            request: Request,
            responseObserver: StreamObserver<Response>
        ) {
            responseObserver.onNext(okResponse)
            responseObserver.onCompleted()

            // Directly offer to queue instead of launching a new coroutine
            requestQueue.offer(request)
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
    try {
        // Initialize the database first to ensure connections are ready
        // This should set up connection pools and verify database access
        DatabaseConfig.init()

        // Create runtime shutdown hook to handle graceful shutdown
        Runtime.getRuntime().addShutdownHook(Thread {
            println("Shutting down server...")
            try {
                // Add any cleanup code here if needed
                println("Server shutdown completed")
            } catch (e: Exception) {
                println("Error during shutdown: ${e.message}")
            }
        })

        // Create and start the server with error handling
        val server = HighPerformanceServer()

        try {
            server.start()
            // Block until shutdown is triggered
            server.blockUntilShutdown()
        } catch (e: Exception) {
            println("Failed to start server: ${e.message}")
            // Perform any necessary cleanup
            System.exit(1)
        }
    } catch (e: Exception) {
        println("Fatal error during server initialization: ${e.message}")
        System.exit(1)
    }
}