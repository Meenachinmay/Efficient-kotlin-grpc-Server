package org.polarmeet.service.config

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.transaction
import java.sql.SQLException

object Requests : Table() {
    // Table definition remains the same
    val id = long("id").autoIncrement()
    val requestData = text("request_data")
    val timestamp = long("timestamp").index()

    override val primaryKey = PrimaryKey(id)

    init {
        index(isUnique = false, requestData)
    }
}

object DatabaseConfig {
    // Adding environment variables with defaults for flexibility
    private val dbHost = System.getenv("DB_HOST") ?: "localhost"  // Changed from localhost
    private val dbPort = System.getenv("DB_PORT") ?: "5432"
    private val dbName = System.getenv("DB_NAME") ?: "grpctest"
    private val dbUser = System.getenv("DB_USER") ?: "postgres"
    private val dbPassword = System.getenv("DB_PASSWORD") ?: "password"

    fun init() {
        var retries = 5
        var lastException: Exception? = null

        while (retries > 0) {
            try {
                val config = HikariConfig().apply {
                    // Using environment variables in the JDBC URL
                    jdbcUrl = "jdbc:postgresql://$dbHost:$dbPort/$dbName"
                    username = dbUser
                    password = dbPassword

                    // Your existing connection pool settings...
                    maximumPoolSize = 70
                    minimumIdle = 20
                    connectionTimeout = 5000L
                    validationTimeout = 3000L
                    maxLifetime = 1800000L
                    idleTimeout = 10000L

                    // Your existing optimization properties...
                    addDataSourceProperty("cachePrepStmts", "true")
                    addDataSourceProperty("prepStmtCacheSize", "250")
                    addDataSourceProperty("prepStmtCacheSqlLimit", "2048")
                    addDataSourceProperty("useServerPrepStmts", "true")
                    addDataSourceProperty("rewriteBatchedStatements", "true")
                    addDataSourceProperty("useBulkStmts", "true")
                    addDataSourceProperty("useWriteBehindBuffer", "true")
                    addDataSourceProperty("writeBufferSize", "16384")

                    // Simple connection test
                    connectionTestQuery = "SELECT 1"
                }

                println("Attempting to connect to database at $dbHost:$dbPort...")
                val dataSource = HikariDataSource(config)

                // Test the connection explicitly
                dataSource.connection.use { conn ->
                    conn.createStatement().executeQuery("SELECT 1").close()
                }

                Database.connect(dataSource)

                // Create tables
                transaction {
                    SchemaUtils.create(Requests)
                }

                println("Database connection established successfully!")
                return

            } catch (e: Exception) {
                lastException = e
                println("Failed to connect to database (${retries - 1} retries left): ${e.message}")
                retries--
                if (retries > 0) {
                    Thread.sleep(5000) // Wait 5 seconds before retrying
                }
            }
        }

        throw IllegalStateException("Could not connect to database after multiple attempts", lastException)
    }
}