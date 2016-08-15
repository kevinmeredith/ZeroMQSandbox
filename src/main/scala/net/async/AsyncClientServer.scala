package net.async

import org.zeromq.ZMQ
import org.zeromq.ZMQ.Socket

import scala.annotation.tailrec

object Client {
  def message(x: Int) = s"HELLO_#$x".getBytes
  val Count   = 5
}

class Client(name: String) extends Runnable {

  import Client._
  import AsyncClientServer._

  override def run(): Unit = {
    val context = ZMQ.context(1)
    val dealer = context.socket(ZMQ.DEALER)
    dealer.setIdentity(name.getBytes)
    dealer.connect(s"tcp://localhost:$Port")
    initiate(dealer)
  }

  private def initiate(dealer: Socket): Unit = {
    dealer.send("".getBytes, ZMQ.SNDMORE)
    dealer.send("READY".getBytes, 0)
    val reply = new String(dealer.recv(0))
    println(s"DEALER: ${new String(dealer.getIdentity)} received $reply")
    if(reply == Ack) {println("DEALER: received ACK!"); runHelper(dealer, Count)}
    else              initiate(dealer)
  }

  @tailrec
  private def runHelper(dealer: Socket, count: Int): Unit = {
    val msg = if(count <= 0 ) End.getBytes else message(count)
    dealer.send(Empty, ZMQ.SNDMORE)
    dealer.send(msg, 0)
    val id = new String(dealer.getIdentity)
    println(s"DEALER ${id} sent message: ${new String(msg)}.")
    runHelper(dealer, count - 1)
  }
}

object AsyncClientServer {

  val End = "END"
  val Ack = "WORLD"
  val Port = 5555
  val ClientReady = "READY"
  val Empty = "".getBytes

  val context = ZMQ.context(1)
  val router  = context.socket(ZMQ.ROUTER)

  def main(args: Array[String]): Unit = {
    router.bind(s"tcp://*:$Port")
    new Thread(new Client("JOE")).start()
    new Thread(new Client("JILL")).start()
    mainHelper(Set.empty)
  }

  private def mainHelper(activeClients: Set[String]): Unit = {
    val identity = new String( router.recv(0) )
    println(s"ROUTER: Received message from $identity.")
    val empty   = router.recv(0)
    println("ROUTER: received empty: " + new String(empty))
    val message = new String( router.recv(0) )
    println(s"ROUTER: received message: $message")

    checkMessage(identity, message, activeClients) match {
      case Normal(msg)     => mainHelper(activeClients)
      case Ready(id)       => ackDealer(router, id); mainHelper(activeClients + id)
      case Kill(id)        => handleKill(id, activeClients)
      case UnknownIdentity => mainHelper(activeClients)
    }
  }

  private def handleKill(id: String, activeClients: Set[String]): Unit = {
    val minus = activeClients - id
    if (minus.isEmpty) {
      sys.exit(0)
    }
    else {
      mainHelper(minus)
    }
  }

  private def ackDealer(router: Socket, identity: String): Unit = {
    router.send(identity.getBytes, ZMQ.SNDMORE)
    router.send(Empty,             ZMQ.SNDMORE)
    router.send(Ack.getBytes,      0)
  }

  private def checkMessage(identity: String, message: String, activeClients: Set[String]): Message = {
    if(message == ClientReady) Ready(identity)
    else {
      activeClients.find(_ == identity) match {
        case Some(_) =>
          if (message == End) Kill(identity)
          else                Normal(message)
        case None    =>       UnknownIdentity
      }
    }
  }
  sealed trait Message
  case class Normal(value: String) extends Message
  case class Ready(id: String)     extends Message
  case class Kill(id: String)     extends Message
  case object UnknownIdentity extends Message

}
