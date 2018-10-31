package ua.org.kug.raft

import io.grpc.ManagedChannelBuilder
import kotlinx.coroutines.experimental.runBlocking
import mu.KotlinLogging

class RaftClient(val host: String = "localhost", val port: Int, val id: Int) {

    val log = KotlinLogging.logger("client")

    val raft = raftInstance()

    private fun raftInstance(): RaftGrpcKt.RaftKtStub {
        val server = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build()

        val raftClient = RaftGrpcKt.newStub(server)
        log.info { "Connecting port: $port" }
        return raftClient
    }

    public suspend fun vote(term: Int,
                            candidateId: Int,
                            lastLogIndex: Int,
                            lastLogTerm: Int): ResponseVoteRPC {
        log.info { "Call vote - host: $host, port: $port, candidateId: $candidateId, term: $term" }
        return raft.vote(
                RequestVoteRPC.newBuilder()
                        .setTerm(term)
                        .setCandidateId(candidateId)
                        .setLastLogIndex(lastLogIndex)
                        .setLastLogTerm(lastLogTerm)
                        .build()
        )
    }

    public suspend fun append(
            leaderId: Int,
            term: Int,
            prevLogIndex: Int,
            prevLogTerm: Int,
            entries: List<RequestAppendEntriesRPC.LogEntry>,
            leaderCommit: Int):
            ResponseAppendEntriesRPC {
        log.info { "Call append - host: $host, port: $port, term: $term" }
        return raft.append(RequestAppendEntriesRPC.newBuilder()
                .setTerm(term)
                .setLeaderId(leaderId)
                .setPrevLogIndex(prevLogIndex)
                .setPrevLogTerm(prevLogTerm)
                .addAllEntries(entries)
                .setLeaderCommit(leaderCommit)
                .build()
        )
    }

}

fun main(args: Array<String>) = runBlocking {


 }



