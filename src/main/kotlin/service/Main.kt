package org.polarmeet.service

import kotlinx.coroutines.runBlocking
import java.util.concurrent.CountDownLatch
import kotlin.system.exitProcess

fun main() = runBlocking {
    // Create a latch that will help us manage graceful shutdown
    val shutdownLatch = CountDownLatch(1)

    try {
        // Initialize system properties for optimal performance
        System.setProperty("io.netty.leakDetection.level", "disabled")
        System.setProperty("io.netty.recycler.maxCapacity", "1000")
        System.setProperty("io.netty.allocator.numHeapArenas",
            Runtime.getRuntime().availableProcessors().toString())

        // Create and start the streaming server
        val server = StreamingServer()

        // Add shutdown hook for graceful termination
        Runtime.getRuntime().addShutdownHook(Thread {
            println("\n📍 Initiating graceful shutdown...")
            try {
                // Release the latch to trigger shutdown
                shutdownLatch.countDown()

                // Add any cleanup code here
                println("📍 Server shutdown completed successfully")
            } catch (e: Exception) {
                println("❌ Error during shutdown: ${e.message}")
                e.printStackTrace()
            }
        })

        println("""
            |================================================
            |🚀 Starting High-Performance Streaming Server
            |================================================
            |Press Ctrl+C to shutdown
            |------------------------------------------------
        """.trimMargin())

        // Start the server
        server.start()

        // Wait for shutdown signal
        shutdownLatch.await()

    } catch (e: Exception) {
        println("""
            |❌ Fatal error during server initialization:
            |   ${e.message}
            |   Stack trace:
        """.trimMargin())
        e.printStackTrace()
        exitProcess(1)
    }
}