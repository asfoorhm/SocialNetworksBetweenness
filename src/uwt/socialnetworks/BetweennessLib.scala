package uwt.socialnetworks
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import scala.reflect.ClassTag
import org.apache.velocity.runtime.directive.Foreach
import akka.dispatch.Foreach
object BetweennessLib {
	def vertexProgramOfPass1(vid: VertexId, vertex: VertexData, message: Message): VertexData =
		{
			println("vid=" + vid + " node=" + vertex + "; received msg: " + message)
			var vdata = new VertexData()
			vdata.levels = vertex.levels
			vdata.shortestPaths = vertex.shortestPaths
			vdata.isLeaf = vertex.isLeaf
			vdata.numOfMessagesSent = vertex.numOfMessagesSent

			message.messages.foreach(msg => {
				if (msg.messageId != -1) {
					if (msg.isBackwardMessage) {
						vdata.numOfMessagesSent(msg.messageId) += 1
						vdata.isLeaf(msg.messageId)= false
					}
					else {
						vdata.levels(msg.messageId) = msg.level
						//println("msgid="+msg.messageId+"; msg.shortestPathCount="+msg.shortestPathCount+"; vdata.shortestPaths="+vdata.shortestPaths
						vdata.shortestPaths(msg.messageId) += msg.shortestPathCount
					}
				}
			})

			vdata
		}
	
	def messageSenderOfPass1(triplet:EdgeTriplet[VertexData, Double]):  Iterator[(VertexId, Message)] =
	{
		//println("triplets:" + triplet)
			var resultList: List[(VertexId, Message)] = List[(VertexId, Message)]()
			var roots = triplet.srcAttr.levels;
			if (roots.size <= 0) {
				roots = triplet.dstAttr.levels;
			}
			if (roots.size > 0) {

				//println("roots exist")

				roots.keys.foreach(i => {
					var src = triplet.srcAttr
					var dst = triplet.dstAttr
					var srcId = triplet.srcId
					var dstId = triplet.dstId
					if (src.levels(i) > dst.levels(i)) {
						src = triplet.dstAttr
						dst = triplet.srcAttr
						srcId = triplet.dstId
						dstId = triplet.srcId
					}
					var srcLevel = src.levels(i)
					var dstLevel = dst.levels(i)
					var srcPathesCount = src.shortestPaths(i)
					//println("srcLevel="+srcLevel+"; dstLevel="+dstLevel+"; root="+i)
					if (srcLevel != dstLevel && (srcLevel == Int.MaxValue || dstLevel == Int.MaxValue)) {
						var pathcount = srcPathesCount
						if (pathcount == 0)
							pathcount = 1
						var msg = new Message(i, srcId, dstId)
						msg.level = srcLevel + 1
						msg.shortestPathCount = pathcount
						
						var backwardMsg = new Message(i, dstId, srcId)
						backwardMsg.isBackwardMessage = true
						
						println("sending msg from src=" + srcId + " (lvl=" + srcLevel + " to dstId=" + dstId + " (lvl=" + dstLevel + "; msg=" + msg)
						println("sending back msg from src=" + dstId + " to dstId=" + srcId + " ; msg=" + msg)
						resultList ::= (dstId, msg)
						resultList ::= (srcId, backwardMsg)
					}
				})

			}

			resultList.iterator
	}
	
	def messageMerger(a:Message, b:Message): Message=
	{
		a.messages ::= b
				a
	}
	
	def vertexProgramOfPass2(vid: VertexId, vertex: VertexData, message: Message): VertexData =
		{
			println("vid=" + vid + " node=" + vertex + "; received msg: " + message)
			var vdata = new VertexData()
			vdata.levels = vertex.levels
			vdata.shortestPaths = vertex.shortestPaths
			vdata.isLeaf = vertex.isLeaf
			vdata.numOfMessagesSent = vertex.numOfMessagesSent
			vdata.credits = vertex.credits
			vdata.isCreditPropegated = vertex.isCreditPropegated

			message.messages.foreach(msg => {
				if (msg.messageId != -1) {
					if (msg.isBackwardMessage) {
						vdata.isCreditPropegated(msg.messageId)= true
						
					}
					else {
						vdata.numOfMessagesSent(msg.messageId)-=1
						vdata.credits(msg.messageId)+=msg.credit
					}
				}
			})
			vdata
		}
	
	def messageSenderOfPass2(triplet:EdgeTriplet[VertexData, Double]):  Iterator[(VertexId, Message)] =
	{
		//println("triplets:" + triplet)
			var resultList: List[(VertexId, Message)] = List[(VertexId, Message)]()
			var roots = triplet.srcAttr.levels;
			if (roots.size <= 0) {
				roots = triplet.dstAttr.levels;
			}
			if (roots.size > 0) {

				//println("roots exist")

				roots.keys.foreach(i => {
					var src = triplet.srcAttr
					var dst = triplet.dstAttr
					var srcId = triplet.srcId
					var dstId = triplet.dstId
					//println("pass2- before swap - msg from src=" + srcId + "; dstId=" + dstId )
					if (src.levels(i) < dst.levels(i)) {
						src = triplet.dstAttr
						dst = triplet.srcAttr
						srcId = triplet.dstId
						dstId = triplet.srcId
					}
					//println("pass2- after swap - msg from src=" + srcId + "; dstId=" + dstId )
					var srcLevel = src.levels(i)
					var dstLevel = dst.levels(i)
					var srcPathesCount = src.shortestPaths(i)
					var dstPathesCount = dst.shortestPaths(i)
					var srcCredit = src.credits(i)
					var dstCredit = dst.credits(i)
					var creditWeight = dstPathesCount/(srcPathesCount+0.0)
					//println("srcLevel="+srcLevel+"; dstLevel="+dstLevel+"; root="+i)
					if (!src.isCreditPropegated(i) && src.numOfMessagesSent(i)== 0 && srcLevel>dstLevel) {
						var pathcount = srcPathesCount
						if (pathcount == 0)
							pathcount = 1
						var msg = new Message(i, srcId, dstId)
						if(creditWeight == 0)
							creditWeight = 1
						msg.credit = srcCredit * creditWeight 
						var backwardMsg = new Message(i, dstId, srcId)
						backwardMsg.isBackwardMessage = true
						
						println("pass2-sending msg from src=" + srcId + " to dstId=" + dstId + "; msg=" + msg)
						println("pass2-sending back msg from src=" + dstId + " to dstId=" + srcId + " ; msg=" + msg)
						resultList ::= (dstId, msg)
						resultList ::= (srcId, backwardMsg)
					}
				})

			}

			resultList.iterator
	}
	
	def getGraphFromFile(fileName:String, sc:SparkContext): Graph[Int,Double] = 
	{
		var g = GraphLoader.edgeListFile(sc, "/home/spark/apps/graphx/edges.txt", true, 7)
		var edges = g.edges.mapValues(v=>0.0)
		var vertices = g.vertices.mapValues(v=>v.toInt)
		var grf = Graph(vertices,edges)
		return grf
	}
}