# MapReduceAkka

En aquest repositori estudiarem el patró map-reduce.

Partiré de dos exemples originals com són wordcount i inverted index i els generalitzarem utilitzant una funció polimórfica i d'ordre superior que anomenarem map-reduce.

Exploreu successivament les següents branques:

* a la branca [IndexInvertitSequencial](https://github.com/mateuvillaret/MapReduceAkka/tree/IndexinvertitSequencial) trobem una versió funcional d'index invertit i la seva reescriptura cap al map-reduce.
* a la branca [WordCountSequencial](https://github.com/mateuvillaret/MapReduceAkka/tree/WordCountSequencial) trobem una versió funcional del word count i la seva reescriptura cap al map-reduce. 
* a la branca [MapReduceSequencialAbstracte](https://github.com/mateuvillaret/MapReduceAkka/tree/MapReduceSequencialAbstracte)  trobem una versió del map-reduce abstracta, concretament definim una funció polimórfica i d'ordre superior que implementa el map-reduce i implementem el word count i l'index invertit fent servir aquesta funció.
* a la branca [IndexInvertitAkkaMapReduceAdhoc](https://github.com/mateuvillaret/MapReduceAkka/tree/IndexInvertitAkkaMapReduceAdhoc) trobem l'index invertit a la map-reduce amb actors i akka.
* **finalment a [MapReduceAkkaGeneralitzat](https://github.com/mateuvillaret/MapReduceAkka/tree/MapReduceAkkaGeneralitzat) hi trobem el MapReduce general amb Akka.**



