package org.example.zkrecipe

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.recipes.locks.InterProcessMutex
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.utils.CloseableUtils
import org.junit.jupiter.api.Test
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class LockTest {
    private val lockPath = "/lock"
    private val zkConnString = "127.0.0.1:2181"

    @Test
    fun lockTest() {
        val client = getClientAndStart(zkConnString)
        val firstMutex = InterProcessMutex(client, lockPath)
        firstMutex.acquire()
        println("Client #1 get Lock!")
        Thread.sleep(10000)

        val qty = 2
        val service = Executors.newFixedThreadPool(qty)
        for (index in 2..qty + 1) {
            service.submit {
                val newClient = getClientAndStart(zkConnString)
                val newMutex = InterProcessMutex(newClient, lockPath)
                getLockAndRelease(index.toString(), newMutex)
                CloseableUtils.closeQuietly(newClient)
            }
        }

        Thread.sleep(15000)
        println("Client #1 release lock.")
        firstMutex.release()
        Thread.sleep(30000)

        service.shutdown()
        service.awaitTermination(10, TimeUnit.MINUTES)
        CloseableUtils.closeQuietly(client)
    }

    private fun getClientAndStart(zkConnString: String): CuratorFramework? {
        val client = CuratorFrameworkFactory.newClient(zkConnString, ExponentialBackoffRetry(1000, 3))
        client.start()
        return client
    }

    private fun getLockAndRelease(
        nodeName: String,
        newMutex: InterProcessMutex
    ) {
        println("Client #$nodeName waits on lock...")
        newMutex.acquire()
        println("Client #$nodeName get Lock!")
        Thread.sleep(5000)
        newMutex.release()
        println("Client #$nodeName release lock.")
    }
}
