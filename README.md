# MapReduce Sequencial i Generalitzat

##Excerpt from MapReduce paper ##
> We realized that most of our computations involved applying a map operation to each logical “record”
 in our input in order to compute a set of intermediate key/value pairs, and then
 applying a reduce operation to all the values that shared the same key, in order to combine the
 derived data appropriately.
 Our use of a functional model with userspecified map and reduce operations allows us to parallelize
 large computations easily...
 
 
 ### Descripció ###
 En aquesta branca donem una versió seqüencial del MapReduce en forma de funció d'ordre superior i polimòrfica.
 També ensenyem com s'instanciaria pels dos casos que hem estudiat. La funció de mapping i de reducing treballen amb tuples tal i com s'ha ilustrat en els casos adhoc de WordCount i Inverted Index.
 A la versió general amb Actors d'AKKA aquestes funcions tindran dos paràmetres enlloc de treballar amb tuples.