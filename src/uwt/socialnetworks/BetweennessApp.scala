package uwt.socialnetworks
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import scala.reflect.ClassTag
import org.apache.velocity.runtime.directive.Foreach
import akka.dispatch.Foreach

object BetweennessApp extends App {

	val sc: SparkContext =
		new SparkContext("local", "SparkPi", System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))
	val s = System.nanoTime
	val nodes: RDD[(VertexId, Int)] = sc.parallelize(Array(
		(1L, 0),
		(2L, 0),
		(3L, 0),
		(4L, 0),
		(6L, 0),
		(7L, 0),
		(8L, 0),
		(5L, 0)))

	// Create an RDD for edges
	val edges: RDD[Edge[Double]] = sc.parallelize(Array(
		Edge(1L, 2L, 0.0),
		//Edge(1L, 2L, 0.0),
		Edge(2L, 3L, 0.0),
		Edge(1L, 3L, 0.0),
		Edge(2L, 4L, 0.0),
		Edge(3L, 4L, 0.0),
		Edge(2L, 5L, 0.0),
		Edge(5L, 6L, 0.0),
		Edge(1L, 8L, 0.0),
		Edge(8L, 4L, 0.0),
		Edge(5L, 7L, 0.0)))
		
	
	var roots: List[VertexId] = List()
	nodes.collect.foreach(v=>roots::=v._1)
	var broadcastedRoots = sc.broadcast(roots)
	val initialVertices = nodes.map(v => {
		var roots = broadcastedRoots.value
		var vdata: VertexData = new VertexData()
		var currentVertexId: VertexId = v._1
		roots.foreach(i => {
			if (i == currentVertexId) {
				vdata.levels += (i -> 0)
			}
			else {
				vdata.levels += (i -> Int.MaxValue)
			}
			vdata.shortestPaths += (i -> 0)
			vdata.isLeaf += (i -> true)
			vdata.numOfMessagesSent += (i -> 0)

		})
		(currentVertexId, vdata)
	})

	var graph = Graph(initialVertices, edges).cache
	var msg = new Message(-1, -1, -1)

	graph = graph.pregel(msg)(BetweennessLib.vertexProgramOfPass1, BetweennessLib.messageSenderOfPass1, BetweennessLib.messageMerger)
	//println(graph.vertices.collect.mkString("\n"))
	graph = graph.mapVertices((vid, vdata) => {

		roots.foreach(i => {
			/*var creditVal = Double.PositiveInfinity
			if (vdata.isLeaf(i))
				creditVal = 1*/
			vdata.credits += (i -> 1)
			vdata.isCreditPropegated += (i -> false)
		})
		vdata
	})

	graph = graph.pregel(msg)(BetweennessLib.vertexProgramOfPass2, BetweennessLib.messageSenderOfPass2, BetweennessLib.messageMerger)
	//println(graph.vertices.collect.mkString("\n"))

	graph = graph.mapTriplets(t => {
		var roots = broadcastedRoots.value
		var credit: Double = 0
		roots.foreach(i => {
			var src = t.srcAttr
			var dst = t.dstAttr
			if (src.levels(i) < dst.levels(i)) {
				src = t.dstAttr
				dst = t.srcAttr

			}

			var srcLevel = src.levels(i)
			var dstLevel = dst.levels(i)
			if (srcLevel > dstLevel) {
				var srcPathesCount = src.shortestPaths(i)
				var dstPathesCount = dst.shortestPaths(i)
				var srcCredit = src.credits(i)
				var dstCredit = dst.credits(i)
				var creditWeight = dstPathesCount / (srcPathesCount + 0.0)
				if (creditWeight == 0)
					creditWeight = 1
				credit += srcCredit * creditWeight
			}

		})

		credit/2
	})
	println(graph.edges.collect.mkString("\n"))
	println("time: " + (System.nanoTime - s) / 1e9 + "seconds")

}