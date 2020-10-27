//package main
import java.io.File
//import scala.language.postfixOps
import akka.actor
import akka.actor.{Actor, ActorRef, ActorSystem, Props}


// Les case class hauran de polimòrfiques en les "claus" i "valors" que s'utilitzen
// ja que  el MapReduce també serà polimòrfic, sinó, perdríem la genericitat.
case class toMapper[K1,V1](fitxer: K1, text: List[V1])
case class fromMapper[K2,V2](intermig: List[(K2,V2)])
case class toReducer[K2,V2](word:K2, fitxers:List[V2])
case class fromReducer[K2,V3](finals: (K2,V3))


// Els actors mappers són polimòrfics ja que  reben la funció de mapping polimòrfica que han d'aplicar
class Mapper[K1,V1,K2,V2](mapping:((K1,List[V1])) => List[(K2,V2)]) extends Actor {
  def receive: Receive = {
        // cal anotar clau:K1 i valor:List[V1] per tal d'instanciar adequadament el missatge toMapper amb les variables de tipus de Mapper
        // Compte, que no us enganyi que hagi donat els mateixos noms a les variables de tipus al Mapper que a les case class de fora. S'han
        // de vincular d'alguna manera perquè sinó l'inferidor de tipus no sap a quin dels paràmetres de tipus del mapper correspon el tipus de la clau
        // i el tipus del valor al missatge toMapper
    case toMapper(clau:K1,valor:List[V1])=>
      sender ! fromMapper(mapping((clau,valor)))
      // xivato per seguir l'evolució
      // println("Work Done by Mapper")
  }
}

// Els actors reducers són polimòrfics ja que reben la funció de reducing polimòrfica que han d'aplicar
class Reducer[K2,V2,V3](reducing:((K2,List[V2]))=> (K2,V3)) extends Actor {
  def receive: Receive = {
        // cal anotar també la clau i el valor com hem fet amb els mappers
    case toReducer(clau:K2,valor:List[V2])=>
      sender ! fromReducer(reducing(clau, valor))
      // println("Work Done by Reducer")
  }
}



// l'Actor MapReduce és polimòrfic amb els tipus de les claus valor de l'entrada [K1,V1], la clau i valor intermitjos [k2,v2]
// i la clau i valor finals [K2,V3].
// input és el paràmetre d'entrada (compte perquè depedent de la mida pot ser un problema)
// mapping és la funció dels mappers
// reducing és la funció dels reducers
class MapReduce[K1,V1,K2,V2,V3](
                                 input:List[(K1,List[V1])],
                                 mapping:((K1,List[V1])) => List[(K2,V2)],
                                 reducing:((K2,List[V2]))=> (K2,V3)) extends Actor {




  var nmappers = 0 // adaptar per poder tenir menys mappers
  var mappersPendents = 0
  var nreducers = 0 // adaptar per poder tenir menys reducers
  var reducersPendents = 0
  var nchunks = 0
  var num_files_mapper = 0
  // dict serà el diccionari amb el resultat intermedi
  var dict = Map[K2, List[V2]]() withDefault (k => List())
  // resultatFinal recollirà les respostes finals dels reducers
  var resultatFinal = Map[K2, V3]()


  // farem un mapper per parella (K1,List[V1]) de l'input
  nchunks = input.length
  nmappers = nchunks

  println("Going to create MAPPERS!!")

  // Al crear actors a dins d'altres actors, enlloc de crear el ActorSystem utilitzarem context i així es va
  // organitzant la jerarquia d'actors.
  // D'altra banda, quan els actors que creen tenen un contructor amb paràmetre, no passem el "tipus" de l'actor i prou
  // a Props sino que creem l'actor amb els paràmetres que necessita. En aquest cas, l'Actor mapping és paramètric en tipus
  // i necessita com a paràmetre una funció de mapping.
  val mappers = for (i <- 0 until nmappers) yield
    context.actorOf(Props(new Mapper[K1,V1,K2,V2](mapping)), "mapper"+i)

  // posar les anotacions de tipus aquí no és necessari però ajuda a llegir
  // a cada mapper li enviem una clau de tipus K1 i una llista de valors de tipus V1
  for(i<- 0 until nmappers) mappers(i) ! toMapper(input(i)._1:K1, input(i)._2: List[V1])
  // for(i<- 0 until nmappers) mappers(i) ! toMapper(input(i)._1, input(i)._2)

  // necessitem controlar quant s'han acabat tots els mappers per poder llençar els reducers després...
  mappersPendents = nmappers

  println("All sent to Mappers, now start listening...")


  def receive: Receive = {

        // Anem rebent les respostes dels mappers i les agrupem al diccionari per clau.
        // De nou ens cal etiquetar el tipus de la clau i el valor, sinó no es pot inferir... però ara ho fem
        // en el paràmetre del missatge directament. (comentat al for més envall, la versió alternativa).

    case fromMapper(list_clau_valor:List[(K2,V2)]) =>
      //for ((word:K2, file:V2) <- list_string_file)
        for ((clau, valor) <- list_clau_valor)
          dict += (clau -> (valor :: dict(clau)))
      // Ja falta un mapper menys...
      mappersPendents -= 1

      // quan ja hem rebut tots els missatges dels mappers:
      if (mappersPendents==0)
        {
          // creem els reducers, tants com entrades al diccionari; fixeu-vos de nou que fem servir context i fem el new
          // pel constructor del Reducer amb paràmetres
          nreducers = dict.size
          reducersPendents = nreducers
          val reducers = for (i <- 0 until nreducers) yield
            context.actorOf(Props(new Reducer[K2,V2,V3](reducing)), "reducer"+i)

          // ara enviem a cada reducer una clau de tipus V2 i una llista de valors de tipus K2. Les anotacions de tipus
          // no calen
          for ((i,(key:K2, lvalue:List[V2])) <-  (0 to nreducers-1) zip dict)
            reducers(i) ! toReducer(key, lvalue)
          println("All sent to Reducers")
        }

      // a mesura que anem rebent respostes del reducer (tuples K2, V3) les anem afegint al Map del resultatfinal i
      // descomptem reducers pendents
    case fromReducer(entradaDictionari:(K2,V3)) =>
      resultatFinal += (entradaDictionari._1 -> entradaDictionari._2 )
      reducersPendents -= 1

      // En arribar a 0, mostrem el resultat
      if (reducersPendents == 0) {
        println("All Done from Reducers! Showing RESULTS!!!")
        for ((s,lf)<- resultatFinal) println(s+" -> " + lf)

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

  val fitxers: List[(File, List[String])] = List(
    (f1, List("hola", "adeu", "per", "palotes", "hola","hola", "adeu", "pericos", "pal", "pal", "pal")),
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
println("Llencem l'index invertit")
 val indexinvertit = systema.actorOf(Props(new MapReduce[File,String,String,File,Set[File]](fitxers,mappingInvInd,reducingInvInd)), name = "masterinv")




  def mappingWC(tupla:(File, List[String])) :List[(String, Int)] =
    tupla match {
      case (file, words) =>
        for (word <- words) yield (word, 1) // Canvi file per 1
    }

  def reducingWC(tupla:(String,List[Int])):(String,Int) =
    tupla match {
      case (word, nums) => (word, nums.sum)
    }

  println("Llencem el wordCount")
  val wordcount = systema.actorOf(Props(new MapReduce[File,String,String,Int,Int](fitxers,mappingWC,reducingWC )), name = "mastercount")








  println("tot enviat, esperant... a veure si triga en PACO")

}

