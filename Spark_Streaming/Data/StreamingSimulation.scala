import java.io.PrintWriter
import java.net.ServerSocket

import scala.io.Source



object StreamingSimulation {

  def main(args: Array[String]) {

    // Three args: directory of file, port #, time interval(millisecond)

    if (args.length != 3) {

      System.err.println("Usage: <filename> <port> <millisecond>")

      System.exit(1)

    }


    val filename = args(0)

    val lines = Source.fromFile(filename).getLines.toList

    val filerow = lines.length


    val listener = new ServerSocket(args(1).toInt)

    while (true) {

      val socket = listener.accept()

      new Thread() {

        override def run = {

          println("Got client connected from: " + socket.getInetAddress)

          val out = new PrintWriter(socket.getOutputStream(), true)
          var start = 0

          while (start < filerow) {

            Thread.sleep(args(2).toLong)

            val content = lines(start)

            println(content)
            out.write(content + '\n')
            out.flush()

            start = start + 1

          }

          socket.close()

        }

      }.start()

    }

  }

}
