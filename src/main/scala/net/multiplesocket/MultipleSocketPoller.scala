package net.multiplesocket

import org.zeromq.ZMQ

/**
  * Created by kmmere on 5/8/16.
  */
object MultipleSocketPoller {

  def main(args: Array[String]): Unit = {
    val context = ZMQ.context(1)

    // Connect to task ventilator
    val receiver = context.socket(ZMQ.PULL)
    receiver.connect("tcp://localhost:5557")

    //  Connect to weather server
    val subscriber = context.socket(ZMQ.SUB)
    subscriber.connect("tcp://localhost:5556")
    subscriber.subscribe("10001 ".getBytes())

    //  Initialize poll set
    val items = context.poller(2)
    items.register(receiver, 0)
    items.register(subscriber, 0)

    println("entering infinite loop...")

    //  Process messages from both sockets
    while (true) {
      items.poll()
      if (items.pollin(0)) {
        val message0 = receiver.recv(0)
        println("ventillator message:" + message0)
      }
      if (items.pollin(1)) {
        val message1 = subscriber.recv(0)
        println("weather message:" + message1)
      }
    }
  }
}
