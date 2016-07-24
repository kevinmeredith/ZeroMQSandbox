package net.broker

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
import org.zeromq.ZMQ.{Poller, Socket}

import scala.collection.immutable.Queue

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
  }
}

//  Worker using REQ socket to do LRU routing
//
class WorkerTask(name: String) extends Runnable {

  @scala.annotation.tailrec
  final def runHelperForever(worker: Socket): Unit = {
    //  Read and save all frames until we get an empty frame
    //  In this example there is only 1 but it could be more
    val address = worker.recv(0)
    val empty = worker.recv(0)

    //  Get request, send reply
    val request = worker.recv(0)
    println(s"${new String(worker.getIdentity)}: received request: ${new String(request)}")

    worker.send(address, ZMQ.SNDMORE)
    worker.send("".getBytes, ZMQ.SNDMORE)
    worker.send("WORLD".getBytes, 0)
    runHelperForever(worker)
  }

  override def run() {
    // Thread.sleep(1000)
    val ctx = ZMQ.context(1)
    val worker = ctx.socket(ZMQ.REQ)
    worker.setIdentity(name.getBytes)
    worker.connect("tcp://localhost:5556")
    //  Tell broker we're ready for work
    worker.send("READY".getBytes, 0)
    runHelperForever(worker)
  }
}

object LruQueueRefactored   {

  val NOFLAGS = 0

  private def workerHelper(clientAddr: String,
                   workerAddr: String,
                   workers: Queue[String],
                   backend: Socket,
                   frontend: Socket,
                   remainingClients: Int): (Queue[String], Int) = {
    if (new String(clientAddr).equals("READY")) {
      (workers.enqueue(workerAddr), remainingClients)
    }
    else {
      val empty = backend.recv(NOFLAGS)
      val reply = backend.recv(NOFLAGS)
      frontend.send(clientAddr, ZMQ.SNDMORE)
      frontend.send("".getBytes, ZMQ.SNDMORE)
      frontend.send(reply, NOFLAGS)
      (workers.enqueue(workerAddr), remainingClients - 1)
    }
  }

  private def pollFrontendHelper(workers: Queue[String],
                                 frontend: Socket,
                                 backend: Socket,
                                 remainingClients: Int,
                                 poller: Poller): Unit =
    workers match {
      case w +: ws =>
        //  Now get next client request, route to LRU worker
        //  Client request is [address][empty][request]
        val clientAddr = frontend.recv(NOFLAGS)
        val empty      = frontend.recv(NOFLAGS)
        val request    = frontend.recv(NOFLAGS)

        backend.send(w, ZMQ.SNDMORE)
        backend.send("".getBytes, ZMQ.SNDMORE)
        backend.send(clientAddr, ZMQ.SNDMORE)
        backend.send("".getBytes, ZMQ.SNDMORE)
        backend.send(request, NOFLAGS)
        pollHelper(poller, remainingClients, ws, backend, frontend)
      case empty   =>
        pollHelper(poller, remainingClients, empty, backend, frontend)
    }

  private def pollHelper(poller: Poller,
                         remainingClients: Int,
                         workers: Queue[String],
                         backend: Socket, frontend: Socket): Unit = {
    poller.poll()

    val result: (Queue[String], Int) =
     if(remainingClients == 0 ) sys.exit(0)
      else if(poller.pollin(0) && remainingClients > 0) {
        val workerAddr = backend.recv(NOFLAGS)

        //  Second frame should be empty
        val empty = backend.recv(NOFLAGS)
        if(new String(empty).nonEmpty) {
          sys.exit(-1)
        }
        //  Third frame is READY or else a client reply address
        val clientAddr = backend.recv(NOFLAGS)

        workerHelper(new String(clientAddr), new String(workerAddr), workers, backend, frontend, remainingClients)
      }
      else {
        (workers, remainingClients)
      }

    val (updatedWorkers, updatedClients) = result

    pollFrontendHelper(updatedWorkers, frontend, backend, updatedClients, poller)
  }

  def main(args : Array[String]) {

    //  Worker using REQ socket to do LRU routing
    //
    val NBR_CLIENTS = 10
    val NBR_WORKERS = 3

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

    val poller = ctx.poller(2)

    // Always poll for worker activity on backend
    poller.register(backend,ZMQ.Poller.POLLIN)

    // Poll front-end only if we have available workers
    poller.register(frontend,ZMQ.Poller.POLLIN)

    pollHelper(poller, NBR_CLIENTS, Queue.empty, backend, frontend)
  }
}