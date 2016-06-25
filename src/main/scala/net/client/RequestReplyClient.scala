package net.client

import org.zeromq.ZMQ
import org.zeromq.ZMQ.{Context,Socket}

object RequestReplyClient {

  def main(args: Array[String]): Unit = {

    val context = ZMQ.context(1)
    val requester = context.socket(ZMQ.REQ)

    requester.connect("tcp://localhost:5559")

    for(request_nbr <- 1 to 10) {
      val request = "Hello ".getBytes
      request(request.length-1) = 0
      println("Sending request " + request_nbr +  "...")
      requester.send(request, 0)

      // get the reply
      val reply = requester.recv(0)

      println("Received reply " + request_nbr + ": [" + new String(reply,0,reply.length-1) + "]")
    }

  }

}
