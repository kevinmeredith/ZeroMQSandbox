package net.pipeline

import java.util.concurrent.TimeUnit

import org.zeromq.ZMQ
import ZMQ.Socket

import scalaz.concurrent.Task
import scalaz._
import Scalaz._
import scala.concurrent.duration.Duration

object Ventilator {

  val MessagesToSend = 100

  val Timeout = Duration(2, TimeUnit.MINUTES)

  def main(args: Array[String]): Unit =
    mainHelper().unsafePerformAsync {
      case \/-(_) => println("Completed successfully."); sys.exit(0)
      case -\/(t) => println(s"Error occurred. Message: ${t.getMessage} and Cause: ${t.getCause}."); sys.exit(1)
    }

  def mainHelper(): Task[Unit] = for {
    context <- Task { ZMQ.context(1) }
    sender  <- Task { context.socket(ZMQ.PUSH) }
    _       <- Task { sender.bind("tcp://*:5557") }
    _       <- Task { println("Press enter when the workers are ready.") }
    _       <- Task { System.in.read() }
    _       <- Task { println("Sending tasks to workers.") }
    _       <- sendTasks(sender, MessagesToSend)
  } yield ()

  def sendTasks(sender: Socket, n: Int): Task[Unit] = {
    if(n < 0 ) Task.now( () )
    else       sendTask(sender, buildMessage(n)) *> Task.now { println(s"sent $n") } *> sendTasks(sender, n - 1)
  }

  private def buildMessage(n: Int): String =
    s"${n}\u0000"

  private def sendTask(sender: Socket, message: String): Task[Unit] =
    Task { sender.send(message, 0) }

}
