# Nubeam-Dedup
Il progetto presentato consiste in un'implementazione dell'algoritmo Nubeam Dedup (Dai, Guan) per il corso di Gestione ed Elaborazione di Big Data (prof. Umberto Ferraro Petrillo) nel corso di Laurea Magistrale "Statistical Sciences" della Sapienza, Università di Roma. 
In un primo momento vengono create le funzioni per trasformare una sequenza di nucleotidi (A, T, C, G) in una stringa binaria per poi renderla un numero Nubeam tramite la logica dell'algoritmo. 
Successivamente, si passa a processare dataset composti da quantità elevatissime di elementi tramite Apache Spark, con collegamento da Python. 
Infine, le richieste prevedevano l'implementazione del lavoro su un databse NoSQL a nostra scelta. Tale scelta è ricaduta sul database Neo4J, nuovamente con collegamento tramite Python, poiché è ottimale da un punto di vista visivo per comprendere al meglio dove sono presenti duplicati e la loro quantità.
