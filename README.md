# MapReduceAkka

En aquesta branca donem la versió polimòrfica i general del Map Reduce i n'ilustrem el seu us amb els casos de l'Inverted Index i el Word Count.

Cal destacar algun aspecte:

* utilitzar un actor com una funció (és a dir, que ens pugui "retornar" un resultat) no entra molt directament dins la filosofia d'Actors. Per tant usarem un patró, el patró Ask, que ens permetrà des del programa principal enviar un missatge "en forma de pregunta" (usant ? enlloc de !) i poder esperar la resposta (com a Future amb un timeout).
* hem simplificat els tipus de les funcions de mapping i reducing i ara enlloc de rebre tuples reben dos paràmetres.
* no està fet el balanceig de càrrega ni el control de fallades.


Llegiu els comentaris del codi per acabar d'entendre'l adequadament.

