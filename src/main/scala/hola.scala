//package main
import java.io.File
//import scala.language.postfixOps
import akka.actor
import akka.actor.{Actor, ActorRef, ActorSystem, Props}

case class IndexInvertit(nmappers:Int, nreducers:Int, corpus: List[(File,List[String])])
case class toMapper(fitxer: File, text: List[String])
case class fromMapper(intemig: List[(String,File)])
case class toReducer(word:String, fitxers:List[File])
case class fromReducer(word:String, fitxers:List[File])

class Master extends Actor {
  var nmappers = 0 // adaptar per poder tenir menys mappers
  var mappersPendents = 0
  var reducersPendents = 0
  var nreducers = 0 // adaptar per poder tenir menys reducers
  var nfiles = 0
  var num_files_mapper = 0
  var dict = Map[String, List[File]]() withDefault (k => List())
  var resultatFinal = Map[String, List[File]]()

  def receive: Receive = {
    case IndexInvertit(nm,nr,c) =>
      nmappers = nm
      nreducers = nr

      nfiles = c.length
      nmappers = nfiles
      val mappers = for (i <- 0 until nmappers) yield
        context.actorOf(Props[Mapper], "mapper"+i)

      for(i<- 0 until nmappers) mappers(i) ! toMapper(c(i)._1, c(i)._2)
      mappersPendents = nmappers

      println("All sent to Mappers")

    case fromMapper(list_string_file) =>
      for ((word, file) <- list_string_file)
        dict += (word -> (file :: dict(word)))
      mappersPendents -= 1

      if (mappersPendents==0)
        {
          nreducers = dict.size
          reducersPendents = nreducers
          val reducers = for (i <- 0 until nreducers) yield
            context.actorOf(Props[Reducer], "reducer"+i)
          for ((i,(key, value)) <-  (0 to nreducers-1) zip dict)
            reducers(i) ! toReducer(key, value)
          println("All sent to Reducers")
        }



    case fromReducer(s,lf) =>
      resultatFinal += (s-> lf)
      reducersPendents -= 1
      if (reducersPendents == 0) {
        for ((s,lf)<- resultatFinal) println(s+" -> " + lf)
        println("All Done from Reducers!")
      }
  }
}

class Mapper extends Actor {
  def receive: Receive = {
    case toMapper(fitxer,text)=>
      sender ! fromMapper(for (word <- text) yield (word, fitxer))
      println("Work Done by Mapper")
  }
}

class Reducer extends Actor {
  def receive: Receive = {
    case toReducer(w,lf)=>
      sender ! fromReducer(w, lf.distinct)
    println("Work Done by Reducer")
  }
}

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

  val fitxers = List(
    (f1, List("hola", "adeu", "per", "palotes", "hola")),
    (f2, List("hola", "adeu", "pericos", "pal", "pal", "pal")),
    (f3, List("que", "tal", "anem", "be")),
    (f4, List("be", "tal", "pericos", "pal")),
    (f5, List("doncs", "si", "doncs", "quin", "pal", "doncs")),
    (f6, List("quin", "hola", "vols", "dir")),
    (f7, List("hola", "no", "pas", "adeu")),
    (f8, List("ahh", "molt", "be", "adeu")))



  val systema = ActorSystem("sistema")

  val master = systema.actorOf(Props[Master], name = "master")

  master ! IndexInvertit(nmappers,nreducers, fitxers)



  println("tot enviat, esperant... a veure si triga en PACO")

}

