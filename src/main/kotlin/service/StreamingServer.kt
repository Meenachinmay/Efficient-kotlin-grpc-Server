package org.polarmeet.service

import io.grpc.Server
import io.grpc.Status
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioServerSocketChannel
import io.grpc.stub.StreamObserver
import kotlinx.coroutines.*
import kotlinx.coroutines.NonCancellable.isActive
import org.polarmeet.proto.StreamRequest
import org.polarmeet.proto.StreamResponse
import org.polarmeet.proto.StreamingServiceGrpc
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.cancellation.CancellationException
import kotlin.time.Duration.Companion.seconds

class StreamingServer {
    private val server: Server
    private val activeStreams = AtomicInteger(0)
    private val totalStreamsCreated = AtomicInteger(0)
    private val streamsSinceLastReport = AtomicInteger(0)
    private val receivedClientMessages = AtomicInteger(0)  // Track client messages

    // Virtual thread executor for better scalability
    private val serverDispatcher = Executors.newVirtualThreadPerTaskExecutor().asCoroutineDispatcher()
    private val streamScope = CoroutineScope(serverDispatcher + SupervisorJob())

    companion object {
        private const val PORT = 9090
        private const val MAX_CONCURRENT_STREAMS = 100000
        private const val WORKER_THREADS = 128
        private const val MONITORING_INTERVAL_SECONDS = 5L
        private const val QUEUE_CAPACITY = 1000
        private const val QUEUE_TIMEOUT_MS = 100L
    }

    init {
        val bossGroup: EventLoopGroup = NioEventLoopGroup(4)
        val workerGroup: EventLoopGroup = NioEventLoopGroup(WORKER_THREADS)

        streamScope.launch {
            monitorStreams()
        }

        val builder = NettyServerBuilder.forPort(PORT)
            .channelType(NioServerSocketChannel::class.java)
            .bossEventLoopGroup(bossGroup)
            .workerEventLoopGroup(workerGroup)
            .executor(serverDispatcher.asExecutor())
            .maxConcurrentCallsPerConnection(MAX_CONCURRENT_STREAMS)
            .maxInboundMessageSize(1024 * 1024)
            .maxInboundMetadataSize(1024 * 64)
            .flowControlWindow(1024 * 1024 * 8)
            .addService(StreamingServiceImpl(streamScope, activeStreams, totalStreamsCreated, streamsSinceLastReport, receivedClientMessages))

        server = builder.build()
    }

    private suspend fun monitorStreams() {
        while (true) {
            delay(MONITORING_INTERVAL_SECONDS.seconds)
            val currentActive = activeStreams.get()
            val newStreams = streamsSinceLastReport.getAndSet(0)
            val total = totalStreamsCreated.get()

            println("""
                |=== Stream Statistics ===
                |Active Streams: $currentActive
                |New Streams (last ${MONITORING_INTERVAL_SECONDS}s): $newStreams
                |Total Streams Created: $total
                |Stream Creation Rate: ${newStreams / MONITORING_INTERVAL_SECONDS}/s
                |Memory Usage: ${Runtime.getRuntime().totalMemory() / 1024 / 1024}MB
                |======================
            """.trimMargin())
        }
    }

    private class StreamingServiceImpl(
        private val scope: CoroutineScope,
        private val activeStreams: AtomicInteger,
        private val totalStreamsCreated: AtomicInteger,
        private val streamsSinceLastReport: AtomicInteger,
        private val receivedClientMessages: AtomicInteger
    ) : StreamingServiceGrpc.StreamingServiceImplBase() {

        // Using ArrayBlockingQueue instead of Channel for better performance
        private val messageBuffer = ArrayBlockingQueue<StreamMessage>(QUEUE_CAPACITY)

        init {
            // Message producer coroutine
            scope.launch(Dispatchers.Default) {
                while (isActive) {
                    try {
                        val message = StreamMessage("Sample data ${System.currentTimeMillis()}")
                        messageBuffer.offer(message, QUEUE_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                        delay(100) // Control message production rate
                    } catch (e: Exception) {
                        println("Message production error: ${e.message}")
                    }
                }
            }
        }

        override fun streamData(
            responseObserver: StreamObserver<StreamResponse>
        ): StreamObserver<StreamRequest> {
            if (activeStreams.get() >= MAX_CONCURRENT_STREAMS) {
                println("❌ Connection rejected: Max streams (${MAX_CONCURRENT_STREAMS}) reached")
                responseObserver.onError(
                    Status.RESOURCE_EXHAUSTED
                        .withDescription("Max streams reached")
                        .asException()
                )
                return createNoOpRequestObserver()
            }

            val streamId = totalStreamsCreated.incrementAndGet()
            streamsSinceLastReport.incrementAndGet()
            val currentActive = activeStreams.incrementAndGet()

            println("✅ New stream connected (ID: $streamId, Active: $currentActive)")

            // Create request observer for client messages
            val requestObserver = object : StreamObserver<StreamRequest> {
                override fun onNext(request: StreamRequest) {
                    receivedClientMessages.incrementAndGet()
                    println("📥 Received from client $streamId: ${request.message}")

                    // Send acknowledgment response
                    scope.launch {
                        val response = StreamResponse.newBuilder()
                            .setStreamId(streamId)
                            .setData("Received: ${request.message}")
                            .setTimestamp(System.currentTimeMillis())
                            .build()
                        responseObserver.onNext(response)
                    }
                }

                override fun onError(t: Throwable) {
                    println("❌ Error from client in stream $streamId: ${t.message}")
                    activeStreams.decrementAndGet()
                }

                override fun onCompleted() {
                    println("❎ Stream $streamId completed by client")
                    activeStreams.decrementAndGet()
                    responseObserver.onCompleted()
                }
            }

            // Launch stream handling
            scope.launch {
                try {
                    handleStream(streamId, responseObserver, requestObserver)
                } finally {
                    val remainingStreams = activeStreams.decrementAndGet()
                    println("❎ Stream disconnected (ID: $streamId, Remaining: $remainingStreams)")
                }
            }

            return requestObserver
        }

        private fun createNoOpRequestObserver(): StreamObserver<StreamRequest> {
            return object : StreamObserver<StreamRequest> {
                override fun onNext(value: StreamRequest) {}
                override fun onError(t: Throwable) {}
                override fun onCompleted() {}
            }
        }

        private suspend fun handleStream(
            streamId: Int,
            responseObserver: StreamObserver<StreamResponse>,
            requestObserver: StreamObserver<StreamRequest>
        ) {
            // Queue for individual stream messages
            val streamQueue = ArrayBlockingQueue<StreamMessage>(QUEUE_CAPACITY)

            try {
                // Launch message forwarding coroutine
                val forwarderJob = scope.launch {
                    while (isActive) {
                        val message = messageBuffer.poll(QUEUE_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                        if (message != null) {
                            streamQueue.offer(message, QUEUE_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                        }
                    }
                }

                // Process messages from the stream queue
                while (isActive) {
                    val message = streamQueue.poll(QUEUE_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                    if (message != null) {
                        val response = StreamResponse.newBuilder()
                            .setStreamId(streamId)
                            .setData(message.data)
                            .setTimestamp(System.currentTimeMillis())
                            .build()

                        withContext(Dispatchers.IO) {
                            responseObserver.onNext(response)
                        }
                    }
                }

                forwarderJob.cancelAndJoin()

            } catch (e: CancellationException) {
                println("⚠️ Stream $streamId cancelled")
            } catch (e: Exception) {
                println("❌ Error in stream $streamId: ${e.message}")
                responseObserver.onError(e)
            } finally {
                responseObserver.onCompleted()
            }
        }
    }

    fun start() {
        server.start()
        println("""
            |🚀 Streaming Server started on port $PORT
            |Maximum concurrent streams: $MAX_CONCURRENT_STREAMS
            |Worker threads: $WORKER_THREADS
            |Monitoring interval: ${MONITORING_INTERVAL_SECONDS}s
        """.trimMargin())
    }

}