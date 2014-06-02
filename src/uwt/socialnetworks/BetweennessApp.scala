package uwt.socialnetworks
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import scala.reflect.ClassTag
import org.apache.velocity.runtime.directive.Foreach
import akka.dispatch.Foreach
import org.apache.spark.broadcast.Broadcast

object BetweennessApp extends App {
	
	var broadcastedRoots:Broadcast[List[Long]] = null;
	var output: String = ""
	override def main(args: Array[String]) =
		{
			val sc: SparkContext =
				new SparkContext("local", "SparkPi", System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))
			val s = System.nanoTime
			//var roots: List[VertexId] = List(1L, 2L, 3L, 4L, 5L)
			//
			//var graph = getTestGraph(sc)
			//var graphAndRoots = loadGraphFromFile("D:\\uwt\\socialnetworks\\edges.txt", sc)
			var graphAndRoots =BetweennessLib.loadGraphFromFile(args(0), sc)
			var graph = graphAndRoots._1
			var roots = graphAndRoots._2
			//graph.vertices.collect.foreach(v=>roots::=v._1)
			broadcastedRoots = sc.broadcast(roots)
			var msg = new Message(-1, -1, -1)
			graph = graph.cache
			graph = graph.pregel(msg)(BetweennessLib.vertexProgramOfPass1, BetweennessLib.messageSenderOfPass1, BetweennessLib.messageMerger)
			BetweennessApp.printToOutput(graph.vertices.collect.mkString("\n") + "\n\nDone with pass1\n\n")
			graph = graph.mapVertices((vid, vdata) => {

				roots.foreach(i => {
					vdata.credits += (i -> 1)
					vdata.isCreditPropegated += (i -> false)
				})
				vdata
			})
			graph = graph.cache
			graph = graph.pregel(msg)(BetweennessLib.vertexProgramOfPass2, BetweennessLib.messageSenderOfPass2, BetweennessLib.messageMerger)
			//graph = BetweennessLib.myPregel(graph,msg)(BetweennessLib.vertexProgramOfPass2, BetweennessLib.messageSenderOfPass2, BetweennessLib.messageMerger)

			BetweennessApp.printToOutput(graph.vertices.collect.mkString("\n"))

			graph = graph.mapTriplets(t => BetweennessLib.edgeBetweennessReducer(t))

			BetweennessApp.printToOutput(graph.edges.collect.mkString("\n"))
			var finalOut: String = ""
			graph.edges.collect.foreach(e => finalOut += e.srcId + " " + e.dstId + " " + e.attr + "\r\n")
			//scala.tools.nsc.io.File("D:\\uwt\\socialnetworks\\edgeBetweenness.txt").writeAll(finalOut)
			scala.tools.nsc.io.File(args(1)).writeAll(finalOut)
			BetweennessApp.printToOutput("time: " + (System.nanoTime - s) / 1e9 + "seconds")
			println(output)
		}

	def printToOutput(str: String) {
		output += str + "\n"
	}
}