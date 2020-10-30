

import java.io.File

import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps
import akka.actor.{Actor, ActorRef, ActorSystem, PoisonPill, Props}

// Les case class hauran de polimòrfiques en les "claus" i "valors" que s'utilitzen
// ja que  el MapReduce també serà polimòrfic, sinó, perdríem la genericitat.

case class MapReduceCompute()
case class toMapper[K1,V1](fitxer: K1, text: List[V1])
case class fromMapper[K2,V2](intermig: List[(K2,V2)])
case class toReducer[K2,V2](word:K2, fitxers:List[V2])
case class fromReducer[K2,V3](finals: (K2,V3))


// Els actors mappers són polimòrfics ja que  reben la funció de mapping polimòrfica que han d'aplicar
class Mapper[K1,V1,K2,V2](mapping:(K1,List[V1]) => List[(K2,V2)]) extends Actor {
  def receive: Receive = {
    // cal anotar clau:K1 i valor:List[V1] per tal d'instanciar adequadament el missatge toMapper amb les variables de tipus de Mapper
    // Compte, que no us enganyi que hagi donat els mateixos noms a les variables de tipus al Mapper que a les case class de fora. S'han
    // de vincular d'alguna manera perquè sinó l'inferidor de tipus no sap a quin dels paràmetres de tipus del mapper correspon el tipus de la clau
    // i el tipus del valor al missatge toMapper
    case toMapper(clau:K1,valor:List[V1])=>
      sender ! fromMapper(mapping(clau,valor))
  }
}

// Els actors reducers són polimòrfics ja que reben la funció de reducing polimòrfica que han d'aplicar
class Reducer[K2,V2,V3](reducing:(K2,List[V2])=> (K2,V3)) extends Actor {
  def receive: Receive = {
    // cal anotar també la clau i el valor com hem fet amb els mappers
    case toReducer(clau:K2,valor:List[V2])=>
      sender ! fromReducer(reducing(clau, valor))
  }
}



// L'Actor MapReduce és polimòrfic amb els tipus de les claus valor de l'entrada [K1,V1], la clau i valor intermitjos [k2,v2]
// i la clau i valor finals [K2,V3].
// - input és el paràmetre d'entrada (compte perquè depedent de la mida pot ser un problema)
// - mapping és la funció dels mappers
// - reducing és la funció dels reducers
class MapReduce[K1,V1,K2,V2,V3](
                                 input:List[(K1,List[V1])],
                                 mapping:(K1,List[V1]) => List[(K2,V2)],
                                 reducing:(K2,List[V2])=> (K2,V3)) extends Actor {


  var nmappers = 0 // adaptar per poder tenir menys mappers
  var mappersPendents = 0
  var nreducers = 0 // adaptar per poder tenir menys reducers
  var reducersPendents = 0

  var num_files_mapper = 0
  // dict serà el diccionari amb el resultat intermedi
  var dict: Map[K2, List[V2]] = Map[K2, List[V2]]() withDefaultValue List()
  // resultatFinal recollirà les respostes finals dels reducers
  var resultatFinal: Map[K2, V3] = Map()

  // Ens apuntem qui ens ha demanat la feina
  var client:ActorRef = null



  def receive: Receive = {

    // En rebre el missatge MapReduceCompute engeguem el procés.
    case MapReduceCompute() =>
      println("Hem rebut lencarrec")
      client = sender() // Ens apuntem qui ens ha fet l'encàrrec per enviar-li el missatge més tard.

       // farem un mapper per parella (K1,List[V1]) de l'input

      nmappers = input.length

      println("Going to create MAPPERS!!")

      // Al crear actors a dins d'altres actors, enlloc de crear el ActorSystem utilitzarem context i així es va
      // organitzant la jerarquia d'actors.
      // D'altra banda, quan els actors que creem tenen un contructor amb paràmetre, no passem el "tipus" de l'actor i prou
      // a Props sino que creem l'actor amb els paràmetres que necessita. En aquest cas, l'Actor mapping és paramètric en tipus
      // i necessita com a paràmetre una funció de mapping.

      val mappers: Seq[ActorRef] = for (i <- 0 until nmappers) yield {
        context.actorOf(Props(new Mapper(mapping)), "mapper" + i)
      }
      // No és necessari passar els tipus K1,V1, ... ja que els infereix SCALA pel paràmetre mapping
      // D'altra banda compte pq  "0 until n" és el mateix que "0 to n-1".
      // val mappers = for (i <- 0 to nmappers-1) yield {
      //      context.actorOf(Props(new Mapper[K1,V1,K2,V2](mapping)), "mapper"+i)
      // }

      // Posar les anotacions de tipus aquí no és necessari però ajuda a llegir que
      // a cada mapper li enviem una clau de tipus K1 i una llista de valors de tipus V1
      for(i<- 0 until nmappers) mappers(i) ! toMapper(input(i)._1:K1, input(i)._2: List[V1])
      // Per tant, alternativament...
      // for(i<- 0 until nmappers) mappers(i) ! toMapper(input(i)._1, input(i)._2)

      // Necessitem controlar quant s'han acabat tots els mappers per poder llençar els reducers després...
      mappersPendents = nmappers

      println("All sent to Mappers, now start listening...")


    // Anem rebent les respostes dels mappers i les agrupem al diccionari per clau.
    // Tornem a necessitar anotar els tipus del paràmetre que reb fromMapper tal com ho hem fet
    // o be en el codi comentat al generar les tuples.

    case fromMapper(list_clau_valor:List[(K2,V2)]) =>
       for ((clau, valor) <- list_clau_valor)
        dict += (clau -> (valor :: dict(clau)))

      mappersPendents -= 1

      // Quan ja hem rebut tots els missatges dels mappers:
      if (mappersPendents==0)
      {
        // creem els reducers, tants com entrades al diccionari; fixeu-vos de nou que fem servir context i fem el new
        // pel constructor del Reducer amb paràmetres
        nreducers = dict.size
        reducersPendents = nreducers // actualitzem els reducers pendents
        val reducers = for (i <- 0 until nreducers) yield
           context.actorOf(Props(new Reducer(reducing)), "reducer"+i)
          // No cal anotar els tipus ja que els infereix de la funció reducing
          //context.actorOf(Props(new Reducer[K2,V2,V3](reducing)), "reducer"+i)

        // Ara enviem a cada reducer una clau de tipus V2 i una llista de valors de tipus K2. Les anotacions de tipus
        // no caldrien perquè ja sabem de quin tipus és dict, però ens ajuden a documentar.
        for ((i,(key:K2, lvalue:List[V2])) <-  (0 until nreducers) zip dict)
          reducers(i) ! toReducer(key, lvalue)
        println("All sent to Reducers")
      }

    // A mesura que anem rebent respostes del reducer, tuples (K2, V3), les anem afegint al Map del resultatfinal i
    // descomptem reducers pendents. Tornem a necessitar anotar el tipus.
    case fromReducer(entradaDictionari:(K2,V3)) =>
      resultatFinal += entradaDictionari
      reducersPendents -= 1


      // En arribar a 0 enviem a qui ens ha encarregat el MapReduce el resultat. De fet l'està esperant pq haurà fet un ask.
      if (reducersPendents == 0) {
        client ! resultatFinal
        println("All Done from Reducers!")

      }
  }

}


object exampleMapreduce extends App {

  val nmappers = 1
  val nreducers = 1
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

  // Dades per a fer els exercicis del final...
  val compres: List[(String,List[(String,Double, String)])] = List(
    ("bonpeu",List(("pep", 10.5, "1/09/20"), ("pep", 13.5, "2/09/20"), ("joan", 30.3, "2/09/20"), ("marti", 1.5, "2/09/20"), ("pep", 10.5, "3/09/20"))),
    ("sordi", List(("pep", 13.5, "4/09/20"), ("joan", 30.3, "3/09/20"), ("marti", 1.5, "1/09/20"), ("pep", 7.1, "5/09/20"), ("pep", 11.9, "6/09/20"))),
    ("canbravo", List(("joan", 40.4, "5/09/20"), ("marti", 100.5, "5/09/20"), ("pep", 10.5, "7/09/20"), ("pep", 13.5, "8/09/20"), ("joan", 30.3, "7/09/20"), ("marti", 1.5, "6/09/20"))),
    ("maldi", List(("pepa", 10.5, "3/09/20"), ("pepa", 13.5, "4/09/20"), ("joan", 30.3, "8/09/20"), ("marti", 0.5, "8/09/20"), ("pep", 72.1, "9/09/20"), ("mateu", 9.9, "4/09/20"), ("mateu", 40.4, "5/09/20"), ("mateu", 100.5, "6/09/20")))
  )



  // funcions per si volem fer l'index invers
  def mappingInvInd(file:File, words:List[String]) :List[(String, File)] =
    for (word <- words) yield (word, file)

  def reducingInvInd(word:String,lfiles:List[File]):(String,Set[File]) =
    (word, lfiles.toSet)

  // ActorSystem per a crear els actors
  val systema: ActorSystem = ActorSystem("sistema")

  // Al crear l'actor MapReduce no cal passar els tipus com a paràmetres ja que amb els propis paràmetres dels constructor SCALA ja els pot inferir.
  //  val indexinvertit = systema.actorOf(Props(new MapReduce[File,String,String,File,Set[File]](fitxers,mappingInvInd,reducingInvInd)), name = "masterinv")
  val indexinvertit = systema.actorOf(Props(new MapReduce(fitxers,mappingInvInd,reducingInvInd)), name = "masterinv")


  implicit val timeout = Timeout(10000 seconds) // L'implicit permet fixar el timeout per a la pregunta que enviem al indexinvertit. És obligagori.
  var futureresultInvertedIndex = indexinvertit ? MapReduceCompute()

  println("Awaiting")
  // En acabar el MapReduce ens envia un missatge amb el resultat
  val indexinvertitresult:Map[String,List[File]] = Await.result(futureresultInvertedIndex,Duration.Inf).asInstanceOf[Map[String,List[File]]]


  println("Results Inverted Index Obtained")
  for(v<-indexinvertitresult) println(v)

  // Podem aturar l'actor i els seus descendents amb un stop
  println("Stopping: " + indexinvertit.path.name )
  systema.stop(indexinvertit)





  // funcions per poder fer un word count
  def mappingWC(file:File, words:List[String]) :List[(String, Int)] =
    for (word <- words) yield (word, 1)

  def reducingWC(word:String, nums:List[Int]):(String,Int) =
    (word, nums.sum)


  println("Creem l'actor MapReduce per fer el wordCount")
  // Al crear l'actor MapReduce no cal passar els tipus com a paràmetres ja que amb els propis paràmetres dels constructor SCALA ja els pot inferir.
  // val wordcount = systema.actorOf(Props(new MapReduce[File,String,String,Int,Int](fitxers,mappingWC,reducingWC )), name = "mastercount")
  val wordcount = systema.actorOf(Props(new MapReduce(fitxers,mappingWC,reducingWC )), name = "mastercount")

  // Els Futures necessiten que se'ls passi un temps d'espera, un pel future i un per esperar la resposta.
  // La idea és esperar un temps limitat per tal que el codi no es quedés penjat ja que si us fixeu preguntar
  // i esperar denota sincronització. En el nostre cas, al saber que el codi no pot avançar fins que tinguem
  // el resultat del MapReduce, posem un temps llarg (100000s) al preguntar i una Duration.Inf a l'esperar la resposta.

  // Enviem un missatge com a pregunta (? enlloc de !) per tal que inicii l'execució del MapReduce del wordcount.
  //var futureresutltwordcount = wordcount.ask(mapreduce.MapReduceCompute())(100000 seconds)

 // implicit val timeout = Timeout(10000 seconds) // L'implicit permet fixar el timeout per a la pregunta que enviem al wordcount. És obligagori.
  var futureresultwordcount = wordcount ? MapReduceCompute()

  println("Awaiting")
  // En acabar el MapReduce ens envia un missatge amb el resultat
  val wordCountResult:Map[String,Int] = Await.result(futureresultwordcount,Duration.Inf).asInstanceOf[Map[String,Int]]

  // També podem aturar un actor enviant-li un missatge de destruccio:
  println("Poisoning: " + wordcount.path.name )
  wordcount ! PoisonPill

  println("Results Obtained")
  for(v<-wordCountResult) println(v)

  // Fem el shutdown del actor system
  println("shutdown")
  systema.terminate()
  println("ended shutdown")
  // com tancar el sistema d'actors.


  /*

  EXERCICIS:

  Useu el MapReduce per saber quant ha gastat cada persona.

  Useu el MapReduce per saber qui ha fet la compra més cara a cada supermercat

  Useu el MapReduce per saber quant s'ha gastat cada dia a cada supermercat.
   */


  println("tot enviat, esperant... a veure si triga en PACO")
}



