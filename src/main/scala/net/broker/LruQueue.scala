package net.broker

import org.zeromq.ZMQ

// Basic request-reply client using REQ socket
class ClientTask(worker: String) extends Runnable {
  def run(): Unit = {
    val context = ZMQ.context(1)
    val client  = context.socket(ZMQ.REQ)
    client.setIdentity(worker.getBytes)
    client.connect("tcp://localhost:5555")

    // send request, get reply
    client.send("HELLO".getBytes, 0)
    val reply = client.recv(0)
    println(s"Client: ${new String(reply)}")
  }

}

// Worker using REQ
// socket to do LRU routing
class WorkerTask(name: String) extends Runnable {
  override def run: Unit = {
    val context = ZMQ.context(1)
    val worker  = context.socket(ZMQ.REQ)
    worker.setIdentity(name.getBytes)
    worker.connect("tcp://localhost:5556")
    worker.send("READY".getBytes, 0)

    while(true) {
      // Read and save all frames until we get an empty frame
      // In this example there is only 1, but could be more in reality
      val address = worker.recv(0)
      val empty   = worker.recv(0)

      // Get request, send reply
      val request = worker.recv(0)
      println(s"Worker: ${new String(request)}")

      worker.send(address,       ZMQ.SNDMORE)
      worker.send("".getBytes,   ZMQ.SNDMORE)
      worker.send("OK".getBytes, 0)
    }
  }
}

object LruQueue {

  def main(args: Array[String]): Unit = {
    // worker using REQ socket to do LRU routing
    val NBR_CLIENTS = 10

    // Prepare our context and sockets
    val context  = ZMQ.context(1)
    val frontend = context.socket(ZMQ.ROUTER)
    val backend  = context.socket(ZMQ.ROUTER)

    frontend.bind("tcp://*:5555")
    backend.bind("tcp://*:5556")

    val clients: List[Thread] =
      List("A", "B", "C", "D", "E", "F", "G", "H", "I", "J").map(x => new Thread(new ClientTask(x)))
    clients.foreach(_.start)

    val workers: List[Thread] = List("W1", "W2", "W3").map(x => new Thread(new WorkerTask(x)))
    workers.foreach(_.start)

    // Logic of LRU Loop:
    //  -- poll backend always, frontend only if 1+ worker ready
    //  -- if worker replies, queue worker as ready and forward reply to client if necessary
    //  -- if client requests, pop next worker and send request to it

    val workerQueue      = scala.collection.mutable.Queue[Array[Byte]]()
    var availableWorkers = 0
    val poller           = context.poller(2)

    poller.register(backend, ZMQ.Poller.POLLIN)
    poller.register(frontend, ZMQ.Poller.POLLIN)

    var clientNbr = NBR_CLIENTS
    while(true) {
      poller.poll()

      if(poller.pollin(0) && clientNbr > 0) {
        val workerAddr = backend.recv(0)
        availableWorkers += 1

        // queue worker address for LRU routing
        workerQueue.enqueue(workerAddr)

        // second frame is empty
        val empty      = backend.recv(0)

        //  Third frame is READY or else a client reply address
        val clientAddr = backend.recv(0)
        if(!new String(clientAddr).equals("READY")) {
          val reply = backend.recv(0)
          frontend.send(clientAddr, ZMQ.SNDMORE)
          frontend.send("".getBytes, ZMQ.SNDMORE)
          frontend.send(reply, 0)
          clientNbr -= 1 // exit after N messages
        }
      }
      if(availableWorkers > 0 && poller.pollin(1)) {
        // now get client request, router to LRU worker.
        // client request is [address][empty][request]
        val clientAddr = frontend.recv(0)
        val empty      = frontend.recv(0)
        val request    = frontend.recv(0)

        backend.send(workerQueue.dequeue, ZMQ.SNDMORE)
        backend.send("".getBytes(),       ZMQ.SNDMORE)
        backend.send(clientAddr,          ZMQ.SNDMORE)
        backend.send("".getBytes(),       ZMQ.SNDMORE)
        backend.send(request,             0)
        backend.send(request,             0)
        availableWorkers -= 1

      }
    }
  }

}
