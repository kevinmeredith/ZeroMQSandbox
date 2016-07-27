package net.broker

import java.util.concurrent.TimeUnit

import org.zeromq.ZMQ
import org.zeromq.ZMQ.Poller

import scala.concurrent.duration.Duration

object LruQueue2 {

  val PollTimeout = Duration(10, TimeUnit.SECONDS)

  class ClientTask(name: String) extends Runnable {
    override def run(): Unit = {
      val context = ZMQ.context(1)
      val client = context.socket(ZMQ.REQ)
      client.setIdentity(name.getBytes)
      client.connect("tcp://localhost:5555")

      // send request, get reply
      client.send("HELLO".getBytes, 0)
      val reply = client.recv(0)
      println(s"${new String(client.getIdentity)} received: ${new String(reply)}")
    }
  }

  class WorkerTask(name: String) extends Runnable {
    override def run(): Unit = {
      val context = ZMQ.context(1)
      val worker  = context.socket(ZMQ.REQ)
      worker.connect("tcp://localhost:5556")
      worker.setIdentity(name.getBytes)
      worker.send("READY".getBytes, 0)
      while(true) {
        val clientAddr = worker.recv(0)
        val empty      = worker.recv(0)
        val clientMsg  = worker.recv(0)

        worker.send(clientAddr, ZMQ.SNDMORE)
        worker.send("".getBytes, ZMQ.SNDMORE)
        worker.send("WORLD".getBytes, 0)

        println(s"${new String(worker.getIdentity)}: 3-frames to client: ${new String(clientAddr)}")
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val NOFLAGS = 0

    // worker using REQ socket to do LRU routing
    val NBR_CLIENTS = 10
    val NBR_WORKERS = 3

    val context  = ZMQ.context(1)
    val frontend = context.socket(ZMQ.ROUTER)
    val backend  = context.socket(ZMQ.ROUTER)

    frontend.bind("tcp://*:5555")
    backend.bind("tcp://*:5556")

    val clients = (1 to NBR_CLIENTS).toList.map{ i => new Thread(new ClientTask(s"CLIENT$i"))}
    val workers = (1 to NBR_CLIENTS).toList.map{ i => new Thread(new WorkerTask(s"WORKER$i"))}

    clients.foreach(_.start)
    workers.foreach(_.start)

    val workerQueue = scala.collection.mutable.Queue[Array[Byte]]()

    val poller =  new Poller(2)

    poller.register(backend,  ZMQ.Poller.POLLIN)
    poller.register(frontend, ZMQ.Poller.POLLIN)

    var clientNbr = NBR_CLIENTS

    while(true) {

      println("clientNbr:" + clientNbr)
      println("workerQueue.length: " + workerQueue.length)

      poller.poll(PollTimeout.toMillis)

      if(clientNbr == 0) {
        sys.exit(0)
      }
      else if(poller.pollin(0) && clientNbr > 0) {
        val workerAddr = backend.recv(NOFLAGS)

        val empty                = backend.recv(NOFLAGS)
        val clientAddrOrReadyMsg = backend.recv(NOFLAGS)

        workerQueue.enqueue(workerAddr)

        if(new String(clientAddrOrReadyMsg) == "READY") {
         // nothing to do - worker is letting us know that he's ready to work
        }
        else {
          // retrieve remaining 2 frames of client message
          // [Empty][Client Message  of "HELLO"]
          val empty          = backend.recv(0)
          val workerResponse = backend.recv(0)

          frontend.send(clientAddrOrReadyMsg, ZMQ.SNDMORE)
          frontend.send("".getBytes,          ZMQ.SNDMORE)
          frontend.send(workerResponse,       NOFLAGS)
          clientNbr -= 1
        }
      }
      else if (poller.pollin(1) && workerQueue.nonEmpty) {
        val clientAddr = frontend.recv(0)
        val empty      = frontend.recv(0)
        val clientMsg  = frontend.recv(0)

        backend.send(workerQueue.dequeue(), ZMQ.SNDMORE)
        backend.send("".getBytes, ZMQ.SNDMORE)
        backend.send(clientAddr, ZMQ.SNDMORE)
        backend.send("".getBytes, ZMQ.SNDMORE)
        backend.send(clientMsg, NOFLAGS)
      }
      else {}
    }
  }
}
