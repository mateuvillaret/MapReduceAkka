//package main
import java.io.File
//import scala.language.postfixOps
import akka.actor
import akka.actor.{Actor, ActorRef, ActorSystem, Props}


// no caldrà... serà l'actor.
case class toMapper[K1,V1](fitxer: K1, text: List[V1])
case class fromMapper[K2,V2](intermig: List[(K2,V2)])
case class toReducer[K2,V2](word:K2, fitxers:List[V2])
case class fromReducer[K2,V3](finals: (K2,V3))



class Mapper[K1,V1,K2,V2](mapping:((K1,List[V1])) => List[(K2,V2)]) extends Actor {
  def receive: Receive = {
    case toMapper(clau,valor)=>
      val kk =mapping((clau,valor))
      sender ! fromMapper(mapping((clau,valor)))
      println("Work Done by Mapper")
  }
}

class Reducer extends Actor {
  def receive: Receive = {
    case toReducer(w,lf)=>
      sender ! fromReducer(reducing(w, lf))
      println("Work Done by Reducer")
  }
}
class MapReduce[K1,V1,K2,V2,V3](
                                 input:List[(K1,List[V1])],
                                 mapping:((K1,List[V1])) => List[(K2,V2)],
                                 reducing:((K2,List[V2]))=> (K2,V3),
                                  nm: Int,
                                  nr: Int) extends Actor {




  var nmappers = 0 // adaptar per poder tenir menys mappers
  var mappersPendents = 0
  var reducersPendents = 0
  var nreducers = 0 // adaptar per poder tenir menys reducers
  var nfiles = 0
  var num_files_mapper = 0
  var dict = Map[K2, List[V2]]() withDefault (k => List())
  var resultatFinal = Map[K2, V3]()



  nfiles = input.length
  nmappers = nfiles
  // nmappers = nm
  nreducers = nr

  println("Going to create MAPPERS!!")

  val mappers = for (i <- 0 until nmappers) yield
    context.actorOf(Props[Mapper], "mapper"+i)

  for(i<- 0 until nmappers) mappers(i) ! toMapper(input(i)._1:K1, input(i)._2: List[V1])
  mappersPendents = nmappers

  println("All sent to Mappers")

  def receive: Receive = {


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

    case fromReducer(entradaDictionari) =>
      resultatFinal += (entradaDictionari._1 -> entradaDictionari._2 )
      reducersPendents -= 1
      if (reducersPendents == 0) {
        for ((s,lf)<- resultatFinal) println(s+" -> " + lf)
        println("All Done from Reducers!")
      }
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

  def mappingInvInd(tupla:(File, List[String])) :List[(String, File)] =
    tupla match {
      case (file, words) =>
        for (word <- words) yield (word, file)
    }

  def reducingInvInd(tupla:(String,List[File])):(String,Set[File]) =
    tupla match {
      case (word, files) => (word, files.toSet)
    }

  val systema = ActorSystem("sistema")

  val master = systema.actorOf(Props(new MapReduce[File,String,String,File,Set[File]](fitxers,mappingInvInd,reducingInvInd,10,10 )), name = "master")

 // master ! IndexInvertit(nmappers,nreducers, fitxers)



  println("tot enviat, esperant... a veure si triga en PACO")

}

