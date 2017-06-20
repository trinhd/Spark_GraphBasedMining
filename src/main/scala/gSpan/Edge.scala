package main.scala.gSpan

class Edge(val vFrom: String, val vTo: String) extends Serializable {
  /**
   * Hàm so sánh cạnh hiện tại với một cạnh khác
   * @param edge: cạnh cần so sánh
   * @return <code>true</code> nếu 2 cạnh có đỉnh bắt đầu và kết thúc giống nhau, <code>false</code> nếu ngược lại
   */
  def equalTo(edge: Edge): Boolean = {
    (vFrom.equals(edge.vFrom) && vTo.equals(edge.vTo))
  }
}