
import java.io.File


object Main extends App {

  val nmappers = 4
  val nreducers = 2
  val f1 = new java.io.File("f1")
  val f2 = new java.io.File("f2")
  val f3 = new java.io.File("f3")
  val f4 = new java.io.File("f4")
  val f5 = new java.io.File("f5")
  val f6 = new java.io.File("f6")
  val f7 = new java.io.File("f7")
  val f8 = new java.io.File("f8")

  val fitxers: List[(File, List[String])] = List(
    (f1, List("hola", "adeu", "per", "palotes", "hola", "hola", "adeu", "pericos", "pal", "pal", "pal")),
    (f2, List("hola", "adeu", "pericos", "pal", "pal", "pal")),
    (f3, List("que", "tal", "anem", "be")),
    (f4, List("be", "tal", "pericos", "pal")),
    (f5, List("doncs", "si", "doncs", "quin", "pal", "doncs")),
    (f6, List("quin", "hola", "vols", "dir")),
    (f7, List("hola", "no", "pas", "adeu")),
    (f8, List("ahh", "molt", "be", "adeu")))

  // A continuació definim el que seria una funció d'ordre superior i polimòrfica per simular el MapReduce de forma seqencial.
  // La instanciació dels tipus en el cas del Word Count seria aquesta
 /* def mapReduce(
                 input: List[(File, List[String])],
                 mapping: ((File, List[String])) => List[(String, Int)],
                 reducing: ((String, List[Int])) => (String, Int)) = {
*/

    def mapReduce[K1,V1,K2,V2,V3](
                 input: List[(K1, List[V1])],
                 mapping: ((K1, List[V1])) => List[(K2, V2)],
                 reducing: ((K2, List[V2])) => (K2, V3)): List[(K2,V3)] = {

    val inter: List[List[(K2, V2)]] = input.map(mapping)

    var dict: Map[K2, List[V2]] = Map().withDefault(k => List())
    for ((w, f) <- inter.flatten) dict += (w -> (f :: dict(w)))

    val result: List[(K2, V3)] = dict.toList.map(reducing)
    result

  }

// ------------- WORD COUNT ----------------
// Hem de definir la funció de mapping
  def mappingWC(tupla:(File, List[String])) :List[(String, Int)] =
    tupla match {
      case (file, words) =>
        for (word <- words) yield (word, 1) // Canvi file per 1
    }

  // i la de reducing
  def reducingWC(tupla:(String,List[Int])):(String,Int) =
    tupla match {
      case (word, nums) => (word, nums.sum)
    }

  val resultatWordcount: List[(String, Int)] = mapReduce(fitxers, mappingWC, reducingWC )
  println("------------- RESULTAT FINAL DEL MAPREDUCE  pel WordCount ----------------")
  resultatWordcount.map(println)


// ------------- INVERTED INDEX -------------
  // el mateix per l'índex invertit
  def mappingInvInd(tupla:(File, List[String])) :List[(String, File)] =
    tupla match {
      case (file, words) =>
        for (word <- words) yield (word, file)
    }

  def reducingInvInd(tupla:(String,List[File])):(String,Set[File]) =
    tupla match {
      case (word, files) => (word, files.toSet)
    }

  val resultatInvetedIndex: List[(String, Set[File])] = mapReduce(fitxers, mappingInvInd, reducingInvInd )
  println("------------- RESULTAT FINAL DEL MAPREDUCE  per l'Inverted Index ----------------")
  resultatInvetedIndex.map(println)








  println("Fi!")

}


