package net.broker

import org.zeromq.{ZMQ, ZMsg}

class ClientTask(name: String) extends Runnable {
  override def run(): Unit = {
    val context = ZMQ.context(1)
    val client  = context.socket(ZMQ.DEALER)
    client.setIdentity(name.getBytes)
    client.connect("tcp://localhost:5570")
    val poller = context.poller(1)

    poller.register(client, ZMQ.Poller.POLLIN)
    var requestNbr = 0
    while(true) {
      for (centitick <- 1 to 100) {
        poller.poll(10000)
        if(poller.pollin(0) {
          val msg = new ZMsg()
        })
      }
    }
  }
}

object AsyncServer {



}
