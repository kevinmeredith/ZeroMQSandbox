package broker

/*
*  Least-recently used (LRU) queue device
*  Clients and workers are shown here in-process
*
*  While this example runs in a single process, that is just to make
*  it easier to start and stop the example. Each thread has its own
*  context and conceptually acts as a separate process.
*
*
*  Author:     Giovanni Ruggiero
*  Email:      giovanni.ruggiero@gmail.com
*/

import org.zeromq.ZMQ

//  Basic request-reply client using REQ socket
//
class ClientTask(name: String) extends Runnable {
  def run() {
    val ctx = ZMQ.context(1)
    val client = ctx.socket(ZMQ.REQ)
    client.setIdentity(name.getBytes)
    client.connect("tcp://localhost:5555")

    //  Send request, get reply
    client.send("HELLO".getBytes, 0)
    val reply = client.recv(0)
    println(s"Client received: ${new String(reply)}")
  }
}

//  Worker using REQ socket to do LRU routing
//
class WorkerTask(name: String) extends Runnable {
  def run() {
    // Thread.sleep(1000)
    val ctx = ZMQ.context(1)
    val worker = ctx.socket(ZMQ.REQ)
    worker.setIdentity(name.getBytes)
    worker.connect("tcp://localhost:5556")
    //  Tell broker we're ready for work
    worker.send("READY".getBytes, 0)
    while (true) {
      //  Read and save all frames until we get an empty frame
      //  In this example there is only 1 but it could be more
      val address = worker.recv(0)

      println(s"${new String(worker.getIdentity)}: received message from address: ${new String(address)}")

      val empty = worker.recv(0)

      println(s"${new String(worker.getIdentity)}: received message 'empty': ${new String(empty)}")

      //  Get request, send reply
      val request = worker.recv(0)
      println(s"${new String(worker.getIdentity)}: received request: ${new String(request)}")

      worker.send(address, ZMQ.SNDMORE)
      worker.send("".getBytes, ZMQ.SNDMORE)
      worker.send("WORLD".getBytes, 0)
    }
  }
}

object LruQueue   {
  def main(args : Array[String]) {
    val NOFLAGS = 0

    //  Worker using REQ socket to do LRU routing
    //
    val NBR_CLIENTS = 1 // 10
    val NBR_WORKERS = 1 //3

    //  Prepare our context and sockets
    val ctx      = ZMQ.context(1)
    val frontend = ctx.socket(ZMQ.ROUTER)
    val backend  = ctx.socket(ZMQ.ROUTER)

    frontend.bind("tcp://*:5555")
    backend.bind("tcp://*:5556")

    val clientsName: List[String] = (1 to NBR_CLIENTS).toList.map(i => s"CLIENT$i")

    val clients = clientsName.map(n => new Thread(new ClientTask(n)))
    clients foreach (_.start)

    val workersName: List[String] = (1 to NBR_WORKERS).toList.map(i => s"WORKER$i")

    val workers = workersName.map(n => new Thread(new WorkerTask(n)))
    workers foreach (_.start)

    //  Logic of LRU loop
    //  - Poll backend always, frontend only if 1+ worker ready
    //  - If worker replies, queue worker as ready and forward reply
    //    to client if necessary
    //  - If client requests, pop next worker and send request to it
    val workerQueue = scala.collection.mutable.Queue[Array[Byte]]()
    var availableWorkers = 0

    val poller = ctx.poller(2)

    // Always poll for worker activity on backend
    poller.register(backend,ZMQ.Poller.POLLIN)

    // Poll front-end only if we have available workers
    poller.register(frontend,ZMQ.Poller.POLLIN)

    var clientNbr = NBR_CLIENTS
    while (true) {
      poller.poll()

      if(poller.pollin(0) && clientNbr > 0) {
        val workerAddr = backend.recv(NOFLAGS)
        println("Backend: workerAddr: " + new String(workerAddr))
        assert (availableWorkers < NBR_WORKERS)

        availableWorkers += 1

        //  Queue worker address for LRU routing
        workerQueue.enqueue(workerAddr)

        //  Second frame should be empty
        val empty = backend.recv(NOFLAGS)
        if(new String(empty).nonEmpty) {
          println("Backend: non-empty second frame: " + new String(empty))
          sys.exit(-1)
        }
        //  Third frame is READY or else a client reply address
        val clientAddr = backend.recv(NOFLAGS)

        println(s"Back-end: cliendAddr: ${new String(clientAddr)}")

        if (new String(clientAddr).equals("READY")) {
          println("Backend: received 'READY'.")
        }
        else {
          val empty = backend.recv(NOFLAGS)
          println(s"Back-end: ${new String(empty)}")
          val reply = backend.recv(NOFLAGS)
          println(s"Back-end: received: ${new String(reply)}")
          frontend.send(clientAddr, ZMQ.SNDMORE)
          frontend.send("".getBytes, ZMQ.SNDMORE)
          frontend.send(reply, NOFLAGS)
          clientNbr -=1 //  Exit after N messages
        }
      }
      if(availableWorkers > 0 && poller.pollin(1)) {
        //  Now get next client request, route to LRU worker
        //  Client request is [address][empty][request]
        val clientAddr = frontend.recv(NOFLAGS)
        val empty      = frontend.recv(NOFLAGS)
        val request    = frontend.recv(NOFLAGS)

        println(s"Front-end: clientAddr: ${new String(clientAddr)}")
        println(s"Front-end: empty: ${new String(empty)}")
        println(s"Front-end: request: ${new String(request)}")

        backend.send(workerQueue.dequeue, ZMQ.SNDMORE)
        backend.send("".getBytes, ZMQ.SNDMORE)
        backend.send(clientAddr, ZMQ.SNDMORE)
        backend.send("".getBytes, ZMQ.SNDMORE)
        backend.send(request, NOFLAGS)
        availableWorkers -= 1
      }
      if(clientNbr == 0) {
        println("no more clients left...")
        sys.exit(0)
      }

    }
  }
}