package main.scala.ExecutionTime

object Timer {
  def timer[R](block: => R): (Long, R) = {
    val startTime = System.nanoTime
    val result = block
    val finishTime = System.nanoTime
    (finishTime - startTime, result)
  }
}