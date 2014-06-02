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
			var vdata = new VertexData()
			vdata.levels = vertex.levels
			vdata.shortestPaths = vertex.shortestPaths
			vdata.isLeaf = vertex.isLeaf
			vdata.numOfMessagesSent = vertex.numOfMessagesSent
			//println("received msg: " + message)
			if (message.messageId != -1) {
				BetweennessApp.printToOutput("before: vid=" + vid + " node=" + vertex)
				println("before: vid=" + vid + " node=" + vertex)

				message.messages.foreach(msg => {
					println("received msg: " + msg)
					BetweennessApp.printToOutput("received msg: " + msg)
					if (msg.isBackwardMessage) {
						vdata.numOfMessagesSent(msg.messageId) += 1
						vdata.isLeaf(msg.messageId) = false
					}
					else {
						println("level before = " + vdata.levels(msg.messageId))
						vdata.levels(msg.messageId) = msg.level
						println("level after = " + vdata.levels(msg.messageId))
						//println("msgid="+msg.messageId+"; msg.shortestPathCount="+msg.shortestPathCount+"; vdata.shortestPaths="+vdata.shortestPaths
						vdata.shortestPaths(msg.messageId) += msg.shortestPathCount
					}
				})

				BetweennessApp.printToOutput("after: vid=" + vid + " node=" + vertex)
				println("after: vid=" + vid + " node=" + vertex)
			}

			vdata
		}

	def messageSenderOfPass1(triplet: EdgeTriplet[VertexData, Double]): Iterator[(VertexId, Message)] =
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
						msg.content.level = srcLevel + 1
						msg.content.shortestPathCount = pathcount

						var backwardMsg = new Message(i, dstId, srcId)
						backwardMsg.content.isBackwardMessage = true

						BetweennessApp.printToOutput("sending msg from src=" + srcId + " (lvl=" + srcLevel + " to dstId=" + dstId + " (lvl=" + dstLevel + "; msg=" + msg)
						BetweennessApp.printToOutput("sending back msg from src=" + dstId + " to dstId=" + srcId + " ; msg=" + backwardMsg)
						resultList ::= (dstId, msg)
						resultList ::= (srcId, backwardMsg)
					}
				})

			}

			resultList.iterator
		}

	def messageMerger(a: Message, b: Message): Message =
		{
			//the problem is here. You need to fix it!
			/*var msgToSent: Message = a
			if (b.messages.size > b.messages.size) {
				msgToSent = b
				msgToSent.messages.::(a)
			}
			else {
				msgToSent.messages.::(b)

			}
			if (!msgToSent.messages.contains(msgToSent))
				msgToSent.messages.::(msgToSent)
			BetweennessApp.printToOutput("a=" + a+"; b="+b+"; msgToSent="+msgToSent)
			msgToSent*/

			/*var msg:Message = null
			//
			if(a.isAggregator)
			{
				msg = a;
				msg.messages::=b
			}
			else if(b.isAggregator)
			{
				msg = b;
				msg.messages::=a
			}
			else
			{
				msg = new Message(-5,-5,-5)
				msg.isAggregator = true
				msg.messages ::= a
				msg.messages ::= b
			}
			msg*/
			a.messages = a.messages ::: b.messages
			a

		}

	def vertexProgramOfPass2(vid: VertexId, vertex: VertexData, message: Message): VertexData =
		{

			var vdata = new VertexData()
			vdata.levels = vertex.levels
			vdata.shortestPaths = vertex.shortestPaths
			vdata.isLeaf = vertex.isLeaf
			vdata.numOfMessagesSent = vertex.numOfMessagesSent
			vdata.credits = vertex.credits
			vdata.isCreditPropegated = vertex.isCreditPropegated

			if (message.messageId != -1) {
				BetweennessApp.printToOutput("vid=" + vid + " node=" + vertex)
				message.messages.foreach(msg => {
					BetweennessApp.printToOutput("received msg: " + message)
					if (msg.isBackwardMessage) {
						vdata.isCreditPropegated(msg.messageId) = true

					}
					else {
						//if(msg.messageId == 1L)
						//BetweennessApp.printToOutput("before numOfMessagesSent decrement -  node="+vid+"; "+vdata)
						vdata.numOfMessagesSent(msg.messageId) -= 1
						//if(msg.messageId == 1L)
						//BetweennessApp.printToOutput("after numOfMessagesSent decrement -  node="+vid+"; "+vdata)
						vdata.credits(msg.messageId) += msg.credit
					}
					//if(msg.messageId == 1L)
					/*BetweennessApp.printToOutput("node=" + vid + " received msg: " + msg)
				BetweennessApp.printToOutput("after- vid=" + vid + " node=" + vertex )*/
				})
			}
			vdata
		}

	def messageSenderOfPass2(triplet: EdgeTriplet[VertexData, Double]): Iterator[(VertexId, Message)] =
		{
			//BetweennessApp.printToOutput("pass2-triplets:" + triplet)
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
					var creditWeight = dstPathesCount / (srcPathesCount + 0.0)
					//println("srcLevel="+srcLevel+"; dstLevel="+dstLevel+"; root="+i)
					if (!src.isCreditPropegated(i) && src.numOfMessagesSent(i) == 0 && srcLevel > dstLevel) {
						var pathcount = srcPathesCount
						if (pathcount == 0)
							pathcount = 1
						var msg = new Message(i, srcId, dstId)
						if (creditWeight == 0)
							creditWeight = 1
						msg.content.credit = srcCredit * creditWeight
						var backwardMsg = new Message(i, dstId, srcId)
						backwardMsg.content.isBackwardMessage = true

						resultList ::= (dstId, msg)
						resultList ::= (srcId, backwardMsg)
						//BetweennessApp.printToOutput("pass2-sending msg from src=" + srcId + " to dstId=" + dstId + "; msg=" + msg)
						//BetweennessApp.printToOutput("pass2-sending back msg from src=" + dstId + " to dstId=" + srcId + " ; msg=" + msg)

					}
				})

			}

			resultList.iterator
		}

	def edgeBetweennessReducer(t: EdgeTriplet[VertexData, Double]) =
		{
			var roots = BetweennessApp.broadcastedRoots.value
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

			credit / 2
		}

	def getGraphFromFile(fileName: String, sc: SparkContext): Graph[Int, Double] =
		{
			var g = GraphLoader.edgeListFile(sc, fileName, true, 7)
			var edges = g.edges.mapValues(v => 0.0)
			var vertices = g.vertices.mapValues(v => v.toInt)
			var grf = Graph(vertices, edges)
			return grf
		}

	def loadGraphFromFile(filename: String, sc: SparkContext): (Graph[VertexData, Double], List[Long]) =
		{
			var gr = BetweennessLib.getGraphFromFile(filename, sc)
			var roots = List[Long]()
			gr.vertices.collect.foreach(v => roots ::= v._1)

			var graph = gr.mapVertices((vid, vdata) => {
				var vdata: VertexData = new VertexData()
				var currentVertexId: VertexId = vid
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
				vdata
			}).cache
			(graph, roots)
		}

	def myPregel[VD: ClassTag, ED: ClassTag, A: ClassTag](grf: Graph[VD, ED],
		initialMsg: A,
		maxIterations: Int = Int.MaxValue,
		activeDirection: EdgeDirection = EdgeDirection.Either)(vprog: (VertexId, VD, A) => VD,
			sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
			mergeMsg: (A, A) => A): Graph[VD, ED] =
		{
			println("Executing vprog...")
			var g = grf.mapVertices((vid, vdata) => vprog(vid, vdata, initialMsg)).cache()
			//g.vertices.collect.foreach(println(_))
			println("vertices after executing vprog: " + g.vertices.collect.mkString("\n"))
			// compute the messages
			var messages = g.mapReduceTriplets(sendMsg, mergeMsg)
			BetweennessApp.printToOutput("new msgs: \n" + messages.collect.mkString("\n"))

			var activeMessages = messages.count()
			println(activeMessages)
			// Loop
			var prevG: Graph[VD, ED] = null
			var i = 0
			while (activeMessages > 0 && i < maxIterations) {
				// Receive the messages. Vertices that didn't get any messages do not appear in newVerts.
				println("Executing vprog...")
				val newVerts = g.vertices.innerJoin(messages)(vprog).cache()
				BetweennessApp.printToOutput("Vertices updated by vprog: \n" + newVerts.collect.mkString("\n"))
				// Update the graph with the new vertices.
				prevG = g
				g = g.outerJoinVertices(newVerts) { (vid, old, newOpt) => newOpt.getOrElse(old) }

				println("vertices after executing vprog: " + g.vertices.collect.mkString("\n"))
				g.cache()
				val oldMessages = messages
				// Send new messages. Vertices that didn't get any messages don't appear in newVerts, so don't
				// get to send messages. We must cache messages so it can be materialized on the next line,
				// allowing us to uncache the previous iteration.
				g.triplets.map(t => {
					println("checking triplets!: " + t)
					t
				}).first
				messages = g.mapReduceTriplets(sendMsg, mergeMsg, Some((newVerts, activeDirection))).cache()
				// The call to count() materializes `messages`, `newVerts`, and the vertices of `g`. This
				// hides oldMessages (depended on by newVerts), newVerts (depended on by messages), and the
				// vertices of prevG (depended on by newVerts, oldMessages, and the vertices of g).
				activeMessages = messages.count()
				// Unpersist the RDDs hidden by newly-materialized RDDs
				oldMessages.unpersist(blocking = false)
				newVerts.unpersist(blocking = false)
				prevG.unpersistVertices(blocking = false)
				// count the iteration
				i += 1
			}

			g
		}

}