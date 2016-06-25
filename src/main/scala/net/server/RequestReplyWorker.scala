package net.server

import org.zeromq.ZMQ

object RequestReplyWorker {

  def main(args: Array[String]): Unit = {

    val context = ZMQ.context(1)
    val receiver = context.socket(ZMQ.REP)
    receiver.connect("tcp://localhost:5560")

    while(true) {
      val request = receiver.recv(0)
      println("Received: " +
        new String(request,0,request.length-1))

      try {
        Thread.sleep (1000)
      } catch  {
        case e: InterruptedException => e.printStackTrace()
      }

      val reply = "World ".getBytes
      reply(reply.length-1)=0
      receiver.send(reply, 0)
    }

  }

}
