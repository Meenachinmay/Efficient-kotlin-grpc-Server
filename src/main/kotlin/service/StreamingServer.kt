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
            .addService(StreamingServiceImpl(streamScope, activeStreams, totalStreamsCreated, streamsSinceLastReport))

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
        private val streamsSinceLastReport: AtomicInteger
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
            request: StreamRequest,
            responseObserver: StreamObserver<StreamResponse>
        ) {
            if (activeStreams.get() >= MAX_CONCURRENT_STREAMS) {
                println("❌ Connection rejected: Max streams (${MAX_CONCURRENT_STREAMS}) reached")
                responseObserver.onError(
                    Status.RESOURCE_EXHAUSTED
                        .withDescription("Max streams reached")
                        .asException()
                )
                return
            }

            val streamId = totalStreamsCreated.incrementAndGet()
            streamsSinceLastReport.incrementAndGet()
            val currentActive = activeStreams.incrementAndGet()

            println("✅ New stream connected (ID: $streamId, Active: $currentActive)")

            scope.launch {
                try {
                    handleStream(streamId, responseObserver)
                } finally {
                    val remainingStreams = activeStreams.decrementAndGet()
                    println("❎ Stream disconnected (ID: $streamId, Remaining: $remainingStreams)")
                }
            }
        }

        private suspend fun handleStream(
            streamId: Int,
            responseObserver: StreamObserver<StreamResponse>
        ) {
            // Queue for individual stream messages
            val streamQueue = ArrayBlockingQueue<StreamMessage>(QUEUE_CAPACITY)

            try {
                // Launch message forwarding coroutine
                scope.launch {
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
                            .build()

                        withContext(Dispatchers.IO) {
                            responseObserver.onNext(response)
                        }
                    }
                }
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