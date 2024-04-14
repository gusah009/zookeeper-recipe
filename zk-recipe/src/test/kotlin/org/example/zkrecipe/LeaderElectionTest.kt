package org.example.zkrecipe

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.recipes.leader.LeaderLatch
import org.apache.curator.framework.recipes.leader.LeaderSelector
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter
import org.apache.curator.retry.ExponentialBackoffRetry
import org.junit.jupiter.api.Test
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class LeaderElectionTest {
    private val path = "/election"
    private val zkConnString = "127.0.0.1:2181"

    @Test
    fun leaderLatchTest() {
        val qty = 3
        val service = Executors.newFixedThreadPool(qty)
        val clients = mutableListOf<Pair<CuratorFramework, LeaderLatch>>()
        for (index in 1..qty) {
            service.submit {
                val client = getClientAndStart(zkConnString)!!
                val leaderLatch = LeaderLatch(client, path, index.toString())
                leaderLatch.start()
                clients.add(Pair(client, leaderLatch))
            }
        }

        Thread.sleep(5000)
        for ((client, leaderLatch) in clients) {
            if (leaderLatch.hasLeadership()) {
                println("#${leaderLatch.id} is leader")
                println("#${leaderLatch.id} path: ${leaderLatch.ourPath}")
                Thread.sleep(2000)
                leaderLatch.close()
                client.close()
                println("#${leaderLatch.id} is not leader anymore")
                break
            }
        }
        Thread.sleep(5000)
        for ((client, leaderLatch) in clients) {
            if (leaderLatch.hasLeadership()) {
                println("#${leaderLatch.id} is leader")
                println("#${leaderLatch.id} path: ${leaderLatch.ourPath}")
                client.close()
                println("#${leaderLatch.id} is not leader anymore")
                break
            }
        }
        Thread.sleep(5000)

        println("new client ${qty + 1} is join")
        val client = getClientAndStart(zkConnString)!!
        val leaderLatch = LeaderLatch(client, path, (qty + 1).toString())
        leaderLatch.start()
        Thread.sleep(10000)

        service.shutdown()
        service.awaitTermination(10, TimeUnit.MINUTES)
    }


    @Test
    fun leaderElectionTest() {
        val qty = 3
        val service = Executors.newFixedThreadPool(qty)
        val clients = mutableListOf<Pair<CuratorFramework, LeaderSelector>>()
        for (index in 0 until qty) {
            service.submit {
                val client = getClientAndStart(zkConnString)!!
                val leaderSelector = getLeaderSelectorAndStart(client, index)
                clients.add(Pair(client, leaderSelector))
            }
        }

        Thread.sleep(1000)

        clients.forEachIndexed { index, (_, leaderSelector) ->
            println("#${index} think #${leaderSelector.leader.id} is leader")
        }

        Thread.sleep(2000)

        clients.forEachIndexed { index, (_, leaderSelector) ->
            println("#${index} think #${leaderSelector.leader.id} is leader")
        }

        println("new client ${qty + 1} is join")
        val client = getClientAndStart(zkConnString)!!
        val leaderSelector = getLeaderSelectorAndStart(client, qty + 1)
        clients.add(Pair(client, leaderSelector))

        Thread.sleep(2000)

        clients.forEachIndexed { index, (_, leaderSelector) ->
            println("#${index} think #${leaderSelector.leader.id} is leader")
        }

        Thread.sleep(10000)

        clients.forEach {
            it.second.close()
            it.first.close()
        }
        service.shutdown()
        service.awaitTermination(10, TimeUnit.MINUTES)
    }

    private fun getLeaderSelectorAndStart(
        client: CuratorFramework,
        index: Int
    ): LeaderSelector {
        val leaderSelector = LeaderSelector(client, path, object : LeaderSelectorListenerAdapter() {
            override fun takeLeadership(client: CuratorFramework?) {
                println("#${index} has leaderShip!")
                println("#${index} execute some process...")
                Thread.sleep(2000)
                println("#${index} done!")
            }
        })
        leaderSelector.id = index.toString()
        leaderSelector.start()
        return leaderSelector
    }

    private fun getClientAndStart(zkConnString: String): CuratorFramework? {
        val client = CuratorFrameworkFactory.newClient(zkConnString, ExponentialBackoffRetry(1000, 3))
        client.start()
        return client
    }
}
