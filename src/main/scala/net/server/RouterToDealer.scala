package net.server

import org.zeromq.ZMQ

import java.util._

import scala.annotation.tailrec

/**
  * Router-to-dealer custom routing demo.
  *
  * The router, in this case the main function, uses ROUTER. The dealers, in this
  * case two worker threads, use DEALER.
  */
object RouterToDealer {

  val NOFLAGS = 0

  // Worker runnable consumes messages until it receives an END
  class Worker(name: String) extends Runnable {
    override def run(): Unit =  {
      val context = ZMQ.context(1)
      val socket = context.socket(ZMQ.DEALER)
      socket.setIdentity(name.getBytes)
      socket.connect("tcp://localhost:5555")

      @tailrec
      def workUntilEndMsg(total: Int, workload: String): Unit = {
        if(workload == "END") {
          println(s"Worker $name is shutting down. Received a total of $total messages")
          ()
        }
        else {
          val wl = new String(socket.recv(NOFLAGS))
          println(s"Worker $name received message: $wl")
          workUntilEndMsg(total + 1, wl)
        }
      }

      workUntilEndMsg(0, "")
      socket.close()
      context.term()
    }
  }

  val rand = new Random()

  def main(args: Array[String]): Unit = {
    val context = ZMQ.context(1)
    val socket = context.socket(ZMQ.ROUTER)
    socket.bind("tcp://*:5555")

    val workerA = new Thread(new Worker("A"))
    val workerB = new Thread(new Worker("B"))
    workerA.start()
    workerB.start()

    println("Workers started, sleeping 1 second for warmup.")
    Thread.sleep(1000)

    for(i <- 1 to 10) {
      val address = if (rand.nextInt() % 2 == 0) "B".getBytes else "A".getBytes
      socket.send(address, ZMQ.SNDMORE)
      socket.send("this is the workload".getBytes, NOFLAGS)
    }

    socket.send("A".getBytes, ZMQ.SNDMORE)
    socket.send("END".getBytes, NOFLAGS)

    socket.send("B".getBytes, ZMQ.SNDMORE)
    socket.send("END".getBytes, NOFLAGS)

    socket.close
    context.term()
  }

}
