package net.server

import org.zeromq.ZMQ
import java.util.Random

object RouterToReq {

  class WorkerTask extends Runnable {
    override def run: Unit = {
      val rand = new Random(System.currentTimeMillis())
      val context = ZMQ.context(1)
      val worker = context.socket(ZMQ.REQ)
      worker.connect("tcp://localhost:5555")
      var total = 0
      var workload = ""

      do {
        worker.send("Ready".getBytes, 0)
        workload = new String(worker.recv(0))
        Thread.sleep (rand.nextInt(1) * 1000)
        total += 1
      } while (workload.equalsIgnoreCase("END") == false)
      printf("Completed: %d tasks\n", total)
    }
  }

  def main(args: Array[String]): Unit = {
    val NBR_WORKERS = 5
    val context = ZMQ.context(1)
    val client = context.socket(ZMQ.ROUTER)

    assert(client.getType > -1)
    client.bind("tcp://*:5555")
    val workers = List.fill(NBR_WORKERS)(new Thread(new WorkerTask))
    workers.foreach (_.start)

    for (i <- 1 to (NBR_WORKERS * 10)) {
      // LRU worker is next waiting in queue
      val address = client.recv(0)
      val empty = client.recv(0)
      val ready = client.recv(0)

      client.send(address, ZMQ.SNDMORE)
      client.send("".getBytes, ZMQ.SNDMORE)
      client.send("This is the workload".getBytes, 0)
    }

    for (i <- 1 to NBR_WORKERS) {
      val address = client.recv(0)
      val empty = client.recv(0)
      val ready = client.recv(0)

      client.send(address, ZMQ.SNDMORE)
      client.send("".getBytes, ZMQ.SNDMORE)
      client.send("END".getBytes, 0)
    }
  }
}
