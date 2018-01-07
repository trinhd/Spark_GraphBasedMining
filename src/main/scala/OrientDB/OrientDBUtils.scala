package main.scala

import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory
import com.tinkerpop.blueprints.impls.orient.OrientGraph
import com.tinkerpop.blueprints.impls.orient.OrientVertexType
import com.orientechnologies.orient.core.metadata.schema.OType
import com.tinkerpop.blueprints.impls.orient.OrientEdgeType
import com.tinkerpop.blueprints.Vertex
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx
import scala.collection.mutable.ListBuffer
import com.tinkerpop.blueprints.Edge
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.orientechnologies.orient.core.db.OPartitionedDatabasePool
import com.orientechnologies.orient.client.remote.OServerAdmin
import com.orientechnologies.orient.core.record.impl.ODocument
import com.orientechnologies.orient.core.exception.OConfigurationException

class OrientDBUtils(hostType: String, hostAddress: String, database: String, dbUser: String, dbPassword: String, userRoot: String, pwdRoot: String) {
  /*
  OrientDB supports three different kinds of storages, depending on the Database URL used:
   - Persistent Embedded Graph Database: Links to the application as a JAR, (that is, with no network transfer). Use PLocal with the plocal prefix. For instance, plocal:/tmp/graph/test.
   - In-Memory Embedded Graph Database: Keeps all data in memory. Use the memory prefix, for instance memory:test.
   - Persistent Remote Graph Database Uses a binary protocol to send and receive data from a remote OrientDB server. Use the remote prefix, for instance remote:localhost/test Note that this requires an OrientDB server instance up and running at the specific address, (in this case, localhost). Remote databases can be persistent or in-memory as well.
   */
  //plocal: or remote: or memory:
  //var hostType = "remote:"
  //if hostType is "memory:", set it empty
  //var hostAddress = "localhost" //"/home/duytri/Downloads/Apps/orientdb-community-importers-2.2.30/databases"
  //var database = "CoOccurrenceGraph"
  //var dbUser = "admin"
  //var dbPassword = "admin"
  var labelSubject = "subject"
  var labelName = "name"
  var tab_v = "dinh"
  var tab_e = "cooccurr_with"
  //var port     = 2424
  //var userRoot = "root"
  //var pwdRoot = "12345"

  //Constructor for graph
  def this(hostType: String, hostAddress: String, database: String, dbUser: String, dbPassword: String) = {
    this(hostType, hostAddress, database, dbUser, dbPassword, "", "")
  }

  //Constructor default
  def this() = {
    this("remote:","localhost", "CoOccurrenceGraph","admin", "admin", "root", "12345")
  }

  /**
   * Hàm tạo chuỗi kết nối cơ sở dữ liệu theo thông tin sẵn có
   * @return Hàm trả về chuỗi kết nối đã tạo nếu các thông tin chính xác, ngược lại trả về <code>null</code>
   */
  def createConnectURI(): String = {
    var uri = hostType
    if ((uri == "plocal:") || (uri == "remote:")) {
      uri += hostAddress + "/" + database
    } else if ((uri == "memory:")) {
      uri += database
    } else {
      println("Thông tin kết nối cơ sở dữ liệu không chính xác. Vui lòng kiểm tra lại!")
      return null
    }
    uri
  }

  //---------------------------PART OF GRAPH DATABASE--------------------------------

  /**
   * Hàm thiết lập kết nối đến OrientDB sử dụng Java Native Driver Graph API
   * @return GraphFactory để khởi tạo kết nối database khi cần
   */
  def connectDBUsingGraphAPI(): OrientGraphFactory = {
    var uri = createConnectURI()

    if (uri == null) {
      return null
    }

    var factory: OrientGraphFactory = null
    if ((hostType == "plocal:") || (hostType == "memory:")) {
      factory = new OrientGraphFactory(uri)
    } else {
      factory = new OrientGraphFactory(uri, dbUser, dbPassword)
    }

    if ((hostType != "remote:") && (!factory.exists))
      println("Database chưa tồn tại, chương trình sẽ tiến hành khởi tạo!") //Remote databases must already exist

    /*
    Prior to version 2.1.7, to work with a graph always use transactional OrientGraph instances and never the non-transactional instances to avoid graph corruption from multi-threaded updates.
		Non-transactional graph instances are created with .getNoTx()
		This instance is only useful when you don't work with data, but want to define the database schema or for bulk inserts.
		*/

    //Using Transaction Instance of Database
    val graph: OrientGraph = factory.getTx

    try {
      //Nếu chưa có database thì khởi tạo cấu trúc database
      if (graph.getVertexType(tab_v) == null) {
        val vertexType: OrientVertexType = graph.createVertexType(tab_v)
        vertexType.createProperty(labelSubject, OType.STRING)
        vertexType.createProperty(labelName, OType.STRING)

        val edgeType: OrientEdgeType = graph.createEdgeType(tab_e)
      }
    } catch {
      case t: Throwable => {
        println("************************ ERROR ************************")
        println("Có LỖI xảy ra!!")
        t.printStackTrace() // TODO: handle error
        println("************************ ERROR ************************")
        graph.rollback
      }
    } finally {
      println("Kết nối thành công với cơ sở dữ liệu!")
      graph.shutdown
      println("All Done!!!")
    }

    //Using Non-Transaction Instance of Database
    /*val graph: OrientGraphNoTx = factory.getNoTx

    try {
      //Nếu chưa có database thì khởi tạo cấu trúc database
      if (graph.getVertexType(tab_v) == null) {
        val vertexType: OrientVertexType = graph.createVertexType(tab_v)
        vertexType.createProperty(labelSubject, OType.STRING)
        vertexType.createProperty(labelName, OType.STRING)

        val edgeType: OrientEdgeType = graph.createEdgeType(tab_e)
      }
    } catch {
      case t: Throwable => {
        println("************************ ERROR ************************")
        println("Có LỖI xảy ra!!")
        t.printStackTrace() // TODO: handle error
        println("************************ ERROR ************************")
      }
    } finally {
      println("Kết nối thành công với cơ sở dữ liệu!")
      graph.shutdown
      println("All Done!!!")
    }*/

    factory
  }

  /**
   * Hàm thêm một đỉnh mới cho đồ thị
   * @param factory: Object dùng để tạo liên kết tới database
   * @param subject: Chủ đề của đỉnh cần thêm vào
   * @param Name: Tên đỉnh cần thêm
   * @return Trả về đỉnh mới thêm vào nếu thành công, <code>null</code> nếu thất bại
   */
  def insertVertex(factory: OrientGraphFactory, Subject: String, Name: String): Vertex = {
    var vertex: Vertex = null
    val graph: OrientGraph = factory.getTx
    try {
      vertex = graph.addVertex("class:" + tab_v, labelSubject, Subject, labelName, Name)

      //Cách thêm cạnh khác
      /*vertex = graph.addVertex("class:" + tab_v, Nil: _*)
      vertex.setProperty(labelSubject, Subject)
      vertex.setProperty(labelName, Name)*/

      graph.commit
    } catch {
      case t: Throwable => {
        println("************************ ERROR ************************")
        println("Có LỖI xảy ra!!")
        t.printStackTrace() // TODO: handle error
        println("************************ ERROR ************************")
        graph.rollback
      }
    } finally {
      println("Thực hiện thành công lệnh thêm dữ liệu đỉnh!")
      graph.shutdown
      println("All Done!!!")
    }
    vertex
  }

  /**
   * Hàm thêm đỉnh mới theo lô cho đồ thị
   * @param factory: Object dùng để tạo liên kết tới database
   * @param vertices: Map chứa thông tin đỉnh cần thêm các phần tử có cấu trúc (name -> subject)
   * @return Danh sách đỉnh mới thêm vào nếu thành công, danh sách rỗng nếu thất bại
   */
  def insertVertexInBatches(factory: OrientGraphFactory, vertices: Map[String, String]): ListBuffer[Vertex] = {
    var lVertex: ListBuffer[Vertex] = ListBuffer()
    val graph: OrientGraphNoTx = factory.getNoTx
    try {
      for ((name, sub) <- vertices) {
        val vertex = graph.addVertex("class:" + tab_v, labelSubject, sub, labelName, name)

        //Cách thêm cạnh khác
        /*val vertex: Vertex = graph.addVertex("class:" + tab_v, Nil: _*)
        vertex.setProperty(labelSubject, sub)
        vertex.setProperty(labelName, name)*/

        lVertex += vertex
      }
      graph.commit
    } catch {
      case t: Throwable => {
        println("************************ ERROR ************************")
        println("Có LỖI xảy ra!!")
        t.printStackTrace() // TODO: handle error
        println("************************ ERROR ************************")
      }
    } finally {
      println("Thực hiện thành công lệnh thêm dữ liệu đỉnh theo lô!")
      graph.shutdown
      println("All Done!!!")
    }
    lVertex
  }

  /**
   * Hàm thêm một cạnh mới cho đồ thị
   * @param factory: Object dùng để tạo liên kết tới database
   * @param vFrom: đỉnh đầu cạnh
   * @param vTo: đỉnh cuối cạnh
   * @return Trả về cạnh mới thêm vào nếu thành công, <code>null</code> nếu thất bại
   */
  def insertEdge(factory: OrientGraphFactory, vFrom: Vertex, vTo: Vertex): Edge = {
    var edge: Edge = null
    val graph: OrientGraph = factory.getTx
    try {
      edge = graph.addEdge("class:" + tab_e, vFrom, vTo, null)
      //Cách khác để thêm cạnh
      //edge = vFrom.addEdge(tab_e, vTo)

      graph.commit
    } catch {
      case t: Throwable => {
        println("************************ ERROR ************************")
        println("Có LỖI xảy ra!!")
        t.printStackTrace() // TODO: handle error
        println("************************ ERROR ************************")
        graph.rollback
      }
    } finally {
      println("Thực hiện thành công lệnh thêm dữ liệu cạnh!")
      graph.shutdown
      println("All Done!!!")
    }
    edge
  }

  /**
   * Hàm thêm cạnh mới theo lô cho đồ thị
   * @param factory: Object dùng để tạo liên kết tới database
   * @param edges: Map chứa thông tin cạnh cần thêm, các phần tử có cấu trúc (vFrom -> vTo)
   * @return Danh sách cạnh mới thêm vào nếu thành công, danh sách rỗng nếu thất bại
   */
  def insertEdgeInBatches(factory: OrientGraphFactory, edges: Map[Vertex, Vertex]): ListBuffer[Edge] = {
    var lEdge: ListBuffer[Edge] = ListBuffer()
    val graph: OrientGraphNoTx = factory.getNoTx
    try {
      for ((vFrom, vTo) <- edges) {
        val edge: Edge = graph.addEdge("class:" + tab_e, vFrom, vTo, null)

        lEdge += edge
      }
      graph.commit
    } catch {
      case t: Throwable => {
        println("************************ ERROR ************************")
        println("Có LỖI xảy ra!!")
        t.printStackTrace() // TODO: handle error
        println("************************ ERROR ************************")
      }
    } finally {
      println("Thực hiện thành công lệnh thêm dữ liệu cạnh theo lô!")
      graph.shutdown
      println("All Done!!!")
    }
    lEdge
  }

  //---------------------------PART OF DOCUMENT DATABASE--------------------------------

  def connectDBUsingDocAPI(): OPartitionedDatabasePool = {
    var uri = createConnectURI()

    if (uri == null) {
      return null
    }

    var factory = new OPartitionedDatabasePool(uri, userRoot, pwdRoot)

    try {
      var db = factory.acquire()
      db.close
    } catch {
      case x: OConfigurationException => {
        println("Database chưa tồn tại, chương trình sẽ tiến hành khởi tạo!")
        if ((hostType == "plocal:") || (hostType == "memory:")) {
          factory.setAutoCreate(true)
          var db = factory.acquire
          db.close
        } else {
          new OServerAdmin(uri).connect(userRoot, pwdRoot).createDatabase(database, "document", "plocal").close
          factory.close
          factory = new OPartitionedDatabasePool(uri, userRoot, pwdRoot)
        }
      }
      case t: Throwable => {
        println("************************ ERROR ************************")
        println("Có LỖI xảy ra!!")
        t.printStackTrace() // TODO: handle error
        println("************************ ERROR ************************")
      }
    } finally {
      println("Kết nối thành công với cơ sở dữ liệu!")
      //factory.close
      println("All Done!!!")
    }

    factory
  }

  def insertDoc(factory: OPartitionedDatabasePool, docName: String, data: List[(String, String)]) = {
    val db = factory.acquire()
    db.begin
    try {
      val doc = new ODocument(docName)
      data.foreach(item => {
        doc.field(item._1, item._2)
      })
      doc.save
      db.commit
    } catch {
      case t: Throwable => {
        db.rollback
        println("************************ ERROR ************************")
        println("Có LỖI xảy ra!!")
        t.printStackTrace() // TODO: handle error
        println("************************ ERROR ************************")
      }
    } finally {
      println("Thực hiện thành công lệnh thêm dữ liệu!")
      db.close
      println("All Done!!!")
    }
  }

  def insertDocInBatches(factory: OPartitionedDatabasePool, data: List[(String, List[(String, String)])]) = {
    val db = factory.acquire()
    db.begin
    try {
      data.foreach(item => {
        val doc = new ODocument(item._1)
        item._2.foreach(subItem => {
          doc.field(subItem._1, subItem._2)
        })
        doc.save
      })
      db.commit
    } catch {
      case t: Throwable => {
        db.rollback
        println("************************ ERROR ************************")
        println("Có LỖI xảy ra!!")
        t.printStackTrace() // TODO: handle error
        println("************************ ERROR ************************")
      }
    } finally {
      println("Thực hiện thành công lệnh thêm dữ liệu!")
      db.close
      println("All Done!!!")
    }
  }
}