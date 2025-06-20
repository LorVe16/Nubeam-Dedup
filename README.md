# Nubeam-Dedup on Spark-Neo4J
Il progetto presentato consiste in un'implementazione su Spark dell'algoritmo Nubeam Dedup (Dai, Guan) per il corso di Gestione ed Elaborazione di Big Data (prof. Umberto Ferraro Petrillo) nel corso di Laurea Magistrale "Statistical Sciences" della Sapienza, Università di Roma. 
In un primo momento vengono create le funzioni per trasformare una sequenza di nucleotidi (A, T, C, G) in una stringa binaria per poi renderla un numero Nubeam tramite la logica dell'algoritmo. 
Successivamente, si passa a processare dataset composti da quantità elevatissime di elementi tramite Apache Spark, con collegamento da Python. 
Infine, le richieste prevedevano l'implementazione del lavoro su un databse NoSQL a nostra scelta. Tale scelta è ricaduta sul database Neo4J, nuovamente con collegamento tramite Python, poiché è ottimale da un punto di vista visivo per comprendere al meglio dove sono presenti duplicati e la loro quantità.

Nei file è possibile trovare:
- Codice Python creato su PyCharm;
- Due file zippati contenenti rispettivamente letture iniziali e finali dei frammenti di DNA trovati un una libreria pubblica online;
- File PDF contenente la nostra relazione finale presentata in sede di esame;
- File PDF contenente la presentazione in PowerPoint;
- Articolo di Dai e Guan che spiega l'algoritmo.

L'intero progetto è stato curato da me, Martina Sestito e Federica Saguto
