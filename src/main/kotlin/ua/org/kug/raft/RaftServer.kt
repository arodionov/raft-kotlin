package ua.org.kug.raft

import io.grpc.ServerBuilder
import io.javalin.Javalin
import io.ktor.application.call
import io.ktor.http.ContentType
import io.ktor.response.respondText
import io.ktor.routing.get
import io.ktor.routing.routing
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.channels.consumeEach
import mu.KotlinLogging
import ua.org.kug.raft.RequestAppendEntriesRPC.LogEntry
import ua.org.kug.raft.State.*
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.fixedRateTimer


enum class State {
    FOLLOWER, CANDIDATE, LEADER
}

class RaftServer(val id: Int, val servers: List<RaftClient>) :
        RaftGrpcKt.RaftImplBase(
                coroutineContext = newFixedThreadPoolContext(4, "server-pool"),
                sendChannelCapacity = 4) {

    val kLogger = KotlinLogging.logger("server")

    @Volatile
    var currentTerm = 0

    @Volatile
    var votedFor = -1

    @Volatile
    var state = FOLLOWER

    private val majority = servers.size / 2 + 1

    private var commitIndex = 0
    var lastApplied = 0
    private val log = Log<LogEntry>()

    private val channel = Channel<State>()

    init {
        //javalinServer()
        ktorServer()

        kLogger.info { "Server $id is in $state at term $currentTerm" }

        val waitingForHeartbeat = waitingForHeartbeatFromLeaderTimer()

        launch {
            channel.consumeEach {
                //kLogger.info { "$id state $state" }
                when (it) {
                    FOLLOWER -> waitingForHeartbeat.reset()
                    CANDIDATE -> leaderElection()
                    LEADER -> appendRequestAndLeaderHeartbeat()
                }
            }
        }
    }

    private fun javalinServer() {
        val app = Javalin.create().start(7000 + id)
        app.get("/") { ctx ->
            ctx.result("Server $id log ${entries()}")
        }
        app.get("/cm/:command") {ctx ->
            ctx.json(appendCommand(ctx.pathParam("command")))}
    }

    private fun ktorServer() {
        val server = embeddedServer(Netty, port = 7000 + id) {
            routing {
                get("/") {
                    call.respondText("Server $id log ${entries()}", ContentType.Text.Plain)
                }
                get("/cmd/{command}") {
                    appendCommand(call.parameters["command"]!!)
                    call.respondText("Server $id log ${entries()}", ContentType.Text.Plain)
                }
            }
        }
        server.start(wait = false)
    }

    fun entries() =
            log.entries().map { "${it.term}: ${it.command}"}


    public fun appendCommand(command: String): String {
        val logEntry = LogEntry.newBuilder()
                .setTerm(currentTerm)
                .setCommand(command)
                .build()
        log.add(log.lastIndex, logEntry)
        return command
    }

     private fun appendRequestAndLeaderHeartbeat() {
        kLogger.info { "Leader elected $id" }

        val nextIndex = Array(servers.size) { _ -> commitIndex + 1 }
        val matchIndex = Array(servers.size) { _ -> 0 }

        fixedRateTimer(period = 2000) {
            runBlocking {
            if (state == FOLLOWER) cancel()
//
//            println(nextIndex.toList())
//            println(matchIndex.toList())

            servers.forEach {
                launch {
                    try {
                        val entries = mutableListOf<LogEntry>()
                        val i = nextIndex[it.id - 1]
                        val prevLogIndex = i - 2
                        val prevLogTerm = if (prevLogIndex >= 0) log.get(prevLogIndex).term else -1

                        if (log.lastIndex >= nextIndex[it.id - 1]) {
                            entries.add(log.get(i - 1))
                        }

                        println("Append id: ${it.id} currentTerm: ${currentTerm} " +
                                    "prevLogIndex ${prevLogIndex} prevLogTerm ${prevLogTerm} entries ${entries} commitIndex ${commitIndex}")

                        val response = it.append(
                                leaderId = id,
                                term = currentTerm,
                                prevLogIndex = prevLogIndex,
                                prevLogTerm = prevLogTerm,
                                entries = entries,
                                leaderCommit = commitIndex)


                        if (response.term > currentTerm) {
                            currentTerm = response.term
                            state = FOLLOWER
                            kLogger.info {
                                "Server $id is converted to $state at term $currentTerm"
                            }
                            launch { channel.offer(state) }
                            return@launch
                        }

                        if (response.success) {
                            if (entries.size > 0) {
                                nextIndex[it.id - 1] += 1
                                matchIndex[it.id - 1] += 1

                                val count = matchIndex.filter { it > commitIndex }.count()
                                if (count >= majority) commitIndex += 1
                            } else {
                                matchIndex[it.id - 1] = prevLogIndex + 1
                            }
                        } else {
                                nextIndex[it.id - 1] -= 1
                        }

                    } catch (e: Exception) {
                        kLogger.info { "Server ${it.id} ${e.message}" }
                    }
                }
            }
        }
        }

    }

    private fun waitingForHeartbeatFromLeaderTimer() =
            ResettableCountdownTimer {
                state = CANDIDATE
                kLogger.info { "Server $id is in $state at term $currentTerm" }
                channel.offer(state)
            }

    private suspend fun leaderElection() {

        val electionTimeout = 25L

        while (state == CANDIDATE) {
            currentTerm += 1
            votedFor = id
            val votesGranted = AtomicInteger()
            kLogger.info { "term: $currentTerm" }
            val countDownLatch = CountDownLatch(majority)
            coroutineScope {
                servers.forEach {
                    launch {
                        val responseVote = retry {
                            val lastIndex = log.lastIndex
                            val lastTerm = if (lastIndex == 0) 0 else log.get(log.lastIndex - 1).term
                            it.vote(
                                    currentTerm,
                                    id,
                                    lastIndex,
                                    lastTerm)
                        }
                        countDownLatch.countDown()
                        if (currentTerm < responseVote.term) state = FOLLOWER
                        if (responseVote.voteGranted) votesGranted.incrementAndGet()
                    }
                }
                countDownLatch.await(electionTimeout, TimeUnit.SECONDS)
                coroutineContext.cancelChildren()
            }

            if (state == CANDIDATE && votesGranted.get() >= majority)
                state = LEADER
            else if (state == CANDIDATE)
                delay((2_000..3_000).random().toLong())
            kLogger.info { "Server $id is $state at term $currentTerm votes ${votesGranted.get()}" }
        }

        launch { channel.send(state) }
    }

    override suspend fun vote(request: RequestVoteRPC): ResponseVoteRPC {
        val granted = if (request.term < currentTerm) false
        else if (currentTerm == request.term) votedFor == request.candidateId
        else {
            if (log.lastIndex >= 1 &&
                    request.lastLogTerm < log.get(log.lastIndex - 1).term) false
            else if (log.lastIndex >= 1 &&
                    request.lastLogTerm == log.get(log.lastIndex - 1).term &&
                    request.lastLogIndex < log.lastIndex) false
            else {
                currentTerm = request.term
                votedFor = request.candidateId
                state = FOLLOWER
                launch { channel.send(state) }
                true
            }
        }

        return ResponseVoteRPC.newBuilder()
                    .setTerm(currentTerm)
                    .setVoteGranted(granted)
                    .build()

    }

    override suspend fun append(request: RequestAppendEntriesRPC):
            ResponseAppendEntriesRPC {
        kLogger.info { "leader heartbeat: ${request.leaderId} term: ${request.term}" }

        if (request.term > currentTerm) {
            currentTerm = request.term
            votedFor = -1
            state = FOLLOWER
            launch {  channel.send(state) }
        }

        if (request.leaderId != id) {
            state = FOLLOWER
            launch {  channel.send(state) }

        }

        if (request.leaderCommit > commitIndex) {
            commitIndex = minOf(request.leaderCommit, log.lastIndex)
        }

        val success = request.prevLogIndex == -1 ||
                (log.lastIndex > request.prevLogIndex &&
                log.get(request.prevLogIndex).term == request.prevLogTerm)

        if (success && request.entriesCount > 0) log.add(request.prevLogIndex + 1, request.getEntries(0))

        kLogger.info { "State: term ${currentTerm} commitIndex $commitIndex " }

        return ResponseAppendEntriesRPC.newBuilder()
                    .setTerm(currentTerm)
                    .setSuccess(success)
                    .build()

    }
}

private fun raftInstance(id: Int, port: Int, servers: List<RaftClient>) {
    val log = KotlinLogging.logger("server")

    log.info { "Start server $id at $port" }

    ServerBuilder.forPort(port)
            .addService(RaftServer(id, servers))
            .build()
            .start()
            //.awaitTermination()
}

fun main(args: Array<String>) {
    val raftClient1 = RaftClient(port = 8081, id = 1)
    val raftClient2 = RaftClient(port = 8082, id = 2)
    val raftClient3 = RaftClient(port = 8083, id = 3)

    val servers = listOf(raftClient1, raftClient2, raftClient3)

    raftInstance(id = 1, port = 8081, servers = servers)
}