package main.scala.gSpan

class EdgeCode(val vFrom: Int, val vTo: Int, val lbFrom: Int, val lbEdge: Int, val lbTo: Int) extends Serializable {
  def toTuple = (vFrom, vTo, lbFrom, lbEdge, lbTo)
}