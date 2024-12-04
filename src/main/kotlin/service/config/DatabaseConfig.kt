package org.polarmeet.service.config

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.transaction

// Define our database table structure
object Requests : Table("requests") {
    // UUID column that auto-generates values for new records
    val id = uuid("id").autoGenerate()
    // Column to store the request data with max length of 255 characters
    val requestData = varchar("request_data", 255)
    // Timestamp column stored as a long (BIGINT in PostgreSQL)
    val timestamp = long("timestamp")

    // Define the primary key constraint on the id column
    override val primaryKey = PrimaryKey(id)
}

object DatabaseConfig {
    // Initialize database connection with optimized settings
    fun init() {
        val config = HikariConfig().apply {
            // Basic connection settings
            jdbcUrl = "jdbc:postgresql://localhost:5432/grpctest"
            username = "postgres"
            password = "password"

            // Connection pool optimization
            maximumPoolSize = 70       // Maximum number of connections in the pool
            minimumIdle = 10           // Minimum number of idle connections to maintain

            // Connection timeout settings
            connectionTimeout = 10000   // Maximum time to wait for connection (10 seconds)
            validationTimeout = 5000    // Maximum time to validate connection (5 seconds)

            // Connection lifecycle settings
            maxLifetime = 1800000      // Maximum lifetime of a connection (30 minutes)
            idleTimeout = 600000       // Maximum idle time for connection (10 minutes)

            // Statement and connection optimizations
            addDataSourceProperty("cachePrepStmts", "true")
            addDataSourceProperty("prepStmtCacheSize", "250")
            addDataSourceProperty("prepStmtCacheSqlLimit", "2048")
            addDataSourceProperty("useServerPrepStmts", "true")

            // Batch operation optimizations
            addDataSourceProperty("rewriteBatchedStatements", "true")
            addDataSourceProperty("useBulkStmts", "true")
            addDataSourceProperty("useWriteBehindBuffer", "true")
            addDataSourceProperty("writeBufferSize", "16384")

            // Fast connection testing
            connectionTestQuery = "SELECT 1"
        }

        // Create the database connection with our optimized configuration
        Database.connect(HikariDataSource(config))

        // Ensure our table exists with the correct structure
        transaction {
            SchemaUtils.create(Requests)
        }
    }
}