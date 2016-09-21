package net.async

import org.zeromq.ZMQ
import org.zeromq.ZMQ.Socket
import scalaz._, Scalaz._, effect._, IO._

class Worker(name: String) extends Runnable {

  import DealerToDealer._

  val WorkerResponse = "Foo!".getBytes
  val id             = name.getBytes

  override def run(): Unit =
    runIO.unsafePerformIO()

  def runIO: IO[Unit] = for {
    context <- IO { ZMQ.context(1) }
    worker  <- IO { context.socket(ZMQ.DEALER) }
    _       <- IO { worker.setIdentity(id) }
    _       <- IO { worker.connect(s"tcp://localhost:$Port") }
    _       <- runIOHelper(worker)
  } yield ()

  private def runIOHelper(worker: Socket): IO[Unit] = for {
    _     <- IO.putStrLn(s"Worker ${name}: sending Empty + ${show(WorkerResponse)}")
    _     <- IO { worker.send(Empty,          ZMQ.SNDMORE) }
    _     <- IO { worker.send(WorkerResponse, 0) }
    _     <- IO.putStrLn(s"Worker ${name}: sent")
    empty <- IO { show(worker.recv(0)) }
    _     <- IO.putStrLn(s"Worker ${show(worker.getIdentity)}: received message: $empty.")
    msg   <- IO { show(worker.recv(0)) }
    _     <- IO.putStrLn(s"Worker ${show(worker.getIdentity)}: received message: $msg.")
    _     <- runIOHelper(worker)
  } yield ()
}

object DealerToDealer {

  def show(xs: Array[Byte]): String =
    new String(xs)

  val Port          = 5555
  val Empty         = "".getBytes
  val WorkerMessage = "Bar!".getBytes
  val WorkLimit     = 25

  def main(xs: Array[String]): Unit =
    mainIO().unsafePerformIO()

  def mainIO(): IO[Unit] = for {
    context <- IO { ZMQ.context(1) }
    server  <- IO { context.socket(ZMQ.DEALER) }
    _       <- IO { server.bind(s"tcp://*:$Port") }
    _       <- IO { new Thread(new Worker("Bob")).run() }
    _       <- runHelper(server, 0)
  } yield ()

  private def runHelper(server: Socket, sent: Int): IO[Unit] = {
    if (sent > WorkLimit ) {
      IO.putStrLn(s"SERVER: sent the '$WorkerMessage' message ${WorkLimit} times. shutting down.") >> IO (sys.exit(0))
    }
    else {
      for {
        empty <- IO { server.recv(0) }
        _     <- IO.putStrLn(s"SERVER: received message: ${show(empty)}.")
        msg   <- IO { server.recv(0) }
        _     <- IO.putStrLn(s"SERVER: received message: ${show(msg)}.")
        _     <- IO.putStrLn(s"SERVER: sending Empty + ${show(WorkerMessage)}.")
        _     <- IO { server.send(Empty,         ZMQ.SNDMORE) }
        _     <- IO { server.send(WorkerMessage, 0) }
        _     <- IO.putStrLn(s"SERVER: sent")
        _     <- runHelper(server, sent + 1)
      } yield ()
    }
  }
}
