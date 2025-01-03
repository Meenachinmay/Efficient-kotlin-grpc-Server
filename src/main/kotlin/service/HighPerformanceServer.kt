package org.polarmeet.service

import io.grpc.Server
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioServerSocketChannel
import io.grpc.stub.StreamObserver
import kotlinx.coroutines.*
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicLong
import org.jetbrains.exposed.sql.batchInsert
import org.jetbrains.exposed.sql.exists
import org.jetbrains.exposed.sql.selectAll
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
        private const val BATCH_SIZE = 5000
        private const val BATCH_TIMEOUT_MS = 500L
        private const val NUM_PROCESSORS = 8
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
        private val totalProcessed = AtomicLong(0)

        // Using larger ArrayBlockingQueue for better performance
        private val requestQueue = ArrayBlockingQueue<Request>(500000)

        init {
            // Start multiple batch processors for parallel processing
            repeat(NUM_PROCESSORS) {
                scope.launch {
                    processBatchInserts()
                }
            }

            // Start monitoring coroutine
            scope.launch {
                while (true) {
                    delay(1000)
                    println("Queue size: ${requestQueue.size}, Total processed: ${totalProcessed.get()}")
                }
            }
        }

        private suspend fun processBatchInserts() {
            val batch = ArrayList<Request>(BATCH_SIZE)

            while (true) {
                try {
                    withTimeout(BATCH_TIMEOUT_MS) {
                        while (batch.size < BATCH_SIZE) {
                            val request = requestQueue.poll()
                            if (request != null) {
                                batch.add(request)
                            } else {
                                delay(1) // Small delay if queue is empty
                            }
                        }
                    }
                } catch (e: TimeoutCancellationException) {
                    // Timeout reached, process what we have
                }

                if (batch.isNotEmpty()) {
                    try {
                        newSuspendedTransaction(Dispatchers.IO) {
                            Requests.batchInsert(batch) { request ->
                                this[Requests.requestData] = request.data
                                this[Requests.timestamp] = System.currentTimeMillis()
                            }
                            totalProcessed.addAndGet(batch.size.toLong())
                        }
                    } catch (e: Exception) {
                        println("Batch insert failed: ${e.message}")
                        e.printStackTrace()
                    }
                    batch.clear()
                }
            }
        }

        override fun process(
            request: Request,
            responseObserver: StreamObserver<Response>
        ) {
            // Return response immediately
            responseObserver.onNext(okResponse)
            responseObserver.onCompleted()

            // Add to queue with timeout to prevent blocking
            if (!requestQueue.offer(request, 100, TimeUnit.MILLISECONDS)) {
                println("Warning: Request queue full, dropping request")
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
    try {
        // Initialize and verify database connection
        DatabaseConfig.init()

        // Verify table existence
        org.jetbrains.exposed.sql.transactions.transaction {
            val tableExists = Requests.exists()
            println("Requests table exists: $tableExists")

            // If table doesn't exist, this will throw an exception
            Requests.selectAll().limit(1).toList()
            println("Database connection and table structure verified")
        }

        // Create runtime shutdown hook
        Runtime.getRuntime().addShutdownHook(Thread {
            println("Shutting down server...")
            try {
                // Add cleanup code here if needed
                println("Server shutdown completed")
            } catch (e: Exception) {
                println("Error during shutdown: ${e.message}")
                e.printStackTrace()
            }
        })

        // Create and start the server
        val server = HighPerformanceServer()

        try {
            server.start()
            server.blockUntilShutdown()
        } catch (e: Exception) {
            println("Failed to start server: ${e.message}")
            e.printStackTrace()
            System.exit(1)
        }
    } catch (e: Exception) {
        println("Fatal error during server initialization: ${e.message}")
        e.printStackTrace()
        System.exit(1)
    }
}