object Hello extends App {
	val s = System.nanoTime
println("hi")
	println("time: "+(System.nanoTime-s)/1e6+"ms")
}