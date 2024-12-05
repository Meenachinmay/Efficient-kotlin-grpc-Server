package org.polarmeet.service

// Basic message type for streaming
data class StreamMessage(
    val data: String,
    val timestamp: Long = System.currentTimeMillis()
)

// Request and Response classes matching our protobuf definitions
data class StreamRequest(
    val clientId: String
)

data class StreamResponse(
    val streamId: Int,
    val data: String,
    val timestamp: Long = System.currentTimeMillis()
) {
    companion object {
        fun newBuilder(): Builder = Builder()
    }

    // Builder pattern to match gRPC style
    class Builder {
        private var streamId: Int = 0
        private var data: String = ""

        fun setStreamId(id: Int): Builder {
            this.streamId = id
            return this
        }

        fun setData(data: String): Builder {
            this.data = data
            return this
        }

        fun build(): StreamResponse = StreamResponse(streamId, data)
    }
}