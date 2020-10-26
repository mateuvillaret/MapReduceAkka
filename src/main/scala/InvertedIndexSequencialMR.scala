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
    (f1, List("hola", "adeu", "per", "palotes", "hola")),
    (f2, List("hola", "adeu", "pericos", "pal", "pal", "pal")),
    (f3, List("que", "tal", "anem", "be")),
    (f4, List("be", "tal", "pericos", "pal")),
    (f5, List("doncs", "si", "doncs", "quin", "pal", "doncs")),
    (f6, List("quin", "hola", "vols", "dir")),
    (f7, List("hola", "no", "pas", "adeu")),
    (f8, List("ahh", "molt", "be", "adeu")))


  // SCALA inverted index version

  val flattenedInvertedInput: List[(String, File)] = for ((file, lwords) <- fitxers; word <- lwords) yield (word, file)
  //Versio desugared anonymous functions
  val resultPlain: Map[String, List[(String, File)]] = flattenedInvertedInput.groupBy({ case ( w: String,f: File) => w })
  val resultPlainFinal: Map[String, Set[String]] = resultPlain.map(  { case (w,lfs) => (w, lfs.map(_._1).toSet)})

  //Versio alternativa anonymous functions
  //val resultPlain = flattenedInvertedInput.groupBy(_._1)
  //val resultPlainFinal: Map[String, Set[File]] = resultPlain.map(x => (x._1, x._2.map(_._1).toSet))

  resultPlainFinal.map(println)

  println("---------------------------------")

  // SCALA inverted index version a la map reduce
  // Input:  List[(File, List[String])]

  // Part del Mapping:  List[(File, List[String])] => List[List[(String, File)]]
  // La funció que se li passa al map te per tipus: (File, List[String]) => List[(String, File)]

  def mapping(tupla: (File, List[String])): List[(String, File)] =
    tupla match {
      case (file, words) =>
        for (word <- words) yield (word, file)
    }

  // Part del map del MapReduce
  val inter: List[List[(String, File)]] = fitxers.map(mapping)

  println("------------RESULTAT del MAP --------------")
  inter.map(println)

  // Part intermitja del MapReduce
  // Map[String,List[File]]
  var dict: Map[String, List[File]] = Map().withDefault(k => List())
  for ((w, f) <- inter.flatten) dict += (w -> (f :: dict(w)))


  // Part del Reducing: Map[String,List[File]] => Map[String,Set[File]]

  // la funció que farà el reducing te per tipus: (String,List[File]) => (String,Set[File])
  def reducing(tupla: (String, List[File])): (String, Set[File]) =
    tupla match {
      case (word, files) => (word, files.toSet)
    }

  // Es fa un "reducing" a cada element del MAP.
  var result: Map[String, Set[File]] = dict.map(reducing)

  println("------------- RESULTAT FINAL DEL MAPREDUCE ----------------")
  // Veiem Com ha quedat el resultat final
  result.map(println)


  println("tot enviat, esperant... a veure si triga en PACO")

}

