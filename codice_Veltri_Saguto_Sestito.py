import os
import math
import pyspark as ps


from pyspark.sql import SparkSession
from neo4j import GraphDatabase, basic_auth

#Funzione che restituisce il numero Nubeam di un filamento di DNA
def numero_Nubeam_da_sequenza_basi_azotate(seq_DNA):
    def sequenza_binaria(sequenza_basi_azotate):
        seq_bin = []
        for i in sequenza_basi_azotate:
            if i == 'A':
                seq_bin.append('1')
            else:
                seq_bin.append('0')
        for j in sequenza_basi_azotate:
            if j == 'T':
                seq_bin.append('1')
            else:
                seq_bin.append('0')
        for z in sequenza_basi_azotate:
            if z == 'C':
                seq_bin.append('1')
            else:
                seq_bin.append('0')
        for g in sequenza_basi_azotate:
            if g == 'G':
                seq_bin.append('1')
            else:
                seq_bin.append('0')
        # Converte la lista in una stringa binaria (non un intero)
        sequenza_binaria = ''.join(seq_bin)
        return sequenza_binaria

    sb = sequenza_binaria(seq_DNA)

    # Calcolo il prodotto di due matrici quadrate 2x2.

    def prodotto_due_matrici(A, B):
        return [
            [(A[0][0] * B[0][0]) + (A[0][1] * B[1][0]), (A[0][0] * B[0][1]) + (A[0][1] * B[1][1])],
            [(A[1][0] * B[0][0]) + (A[1][1] * B[1][0]), (A[1][0] * B[0][1]) + (A[1][1] * B[1][1])]
        ]

    # Trasformo una sequenza numerica binaria nella relativa sequenza di matrici
    # utilizzando le matrici equivalenti a 0 e 1.
    def sequenza_numerica_in_sequenza_matrici(sb):
        M1 = [[1, 1],
              [0, 1]]

        M0 = [[1, 0],
              [1, 1]]

        sequenza_matrici = []

        for n in sb:
            if n == '1':
                sequenza_matrici.append(M1)
            elif n == '0':
                sequenza_matrici.append(M0)
        return sequenza_matrici

    seq_matrici = sequenza_numerica_in_sequenza_matrici(sb)

    def produttoria_matrici(seq_matrici):
        risultato = seq_matrici[0]
        for x in seq_matrici[1:]:
            risultato = prodotto_due_matrici(risultato, x)
        return risultato

    matrice_prodotto = produttoria_matrici(seq_matrici)

    matrice_peso = [[1, math.sqrt(3)],
                    [math.sqrt(2), math.sqrt(5)]]

    mat_pesata = prodotto_due_matrici(matrice_peso, matrice_prodotto)

    traccia = mat_pesata[0][0] + mat_pesata[1][1]

    traccia_arrotondata_2 = round(traccia, 2)

    return traccia_arrotondata_2
#Funzione che restituisce il numero Nubeam del complementare di un filamento di DNA
def complementare_sequenza(sequenza_basi_azotate):
    seq_complementare=[]
    for i in range(0,len(sequenza_basi_azotate)):
        if sequenza_basi_azotate[i] == 'A':
            seq_complementare.append('T')
        if sequenza_basi_azotate[i] == 'T':
            seq_complementare.append('A')
        if sequenza_basi_azotate[i] == 'C':
            seq_complementare.append('G')
        if sequenza_basi_azotate[i] == 'G':
            seq_complementare.append('C')

    seq_complementare="".join(seq_complementare)

    return seq_complementare


sc = ps.SparkContext('local[*]', appName='Nubeam-Dedup1')
spark = SparkSession(sc)  # Associa SparkSession a SparkContext
dataset_filepath='/Users/federica/PycharmProjects/GEBDproject/data/1.fq' #insert the filepath of the dataset you're using
dDb = sc.textFile(dataset_filepath)

#Divido le righe del testo in tuple con relativo indice crescente(0, 1, 2, ..., numero_righe_testo)
dDati = dDb.zipWithIndex()

#Raggruppo le righe in blocchi da 4 utilizzando la divisione intera (es:1//4 = 0, 2//4 = 0,
# 3//4 = 0, 4//4 = 1, 5//4 = 1, ...) e raggruppo in base alla chiave.
dDati1 = dDati.map(lambda x: (x[1]//4, [x[0]])).reduceByKey(lambda x, y: x + y)

#RDD formata solo da letture /1
dDati1_lecture1 = dDati1.filter(lambda x: '/1' in x[1][0] or '1:N:0:CAGATC' in x[1][0])

#RDD formata solo da letture /2
dDati1_lecture2 = dDati1.filter(lambda x: '/2' in x[1][0] or '2:N:0:CAGATC' in x[1][0])


##### METODO SINGLE-END #####

#Creo 2 RDD con tuple (nubeam, sequenza) e (nubeam_complementare, sequenza)
dNubeamSE = dDati1_lecture1.map(lambda x: (numero_Nubeam_da_sequenza_basi_azotate(x[1][1]), (x[1][1])))

dNubeam_compSE = dDati1_lecture1.map(lambda x: (numero_Nubeam_da_sequenza_basi_azotate(complementare_sequenza(x[1][1])), (complementare_sequenza(x[1][1]))))

#Unisco le due RDD in una sola.
dSE = dNubeamSE.union(dNubeam_compSE)

#Con il distinct lascio solo le sequenze uniche.
dUnordered_set = dSE.distinct()



##### METODO PAIRED-END #####

#sostiuisco /1 e /2 con un vuoto per uniformare gli ID.
#NEL CODICE SOSTITUIRE "/1" CON "1:N:0:CAGATC" e "/2" con "2:N:0:CAGATC" NEL CASO DI CAMBIO DI STILE DI FASTQ

dDati1_lecture1_unif = dDati1_lecture1.map(lambda x: (x[1][0].replace("/1", ""), [x[1][1]]))

dDati1_lecture2_unif = dDati1_lecture2.map(lambda x: (x[1][0].replace("/2", ""), [x[1][1]]))

#PER ESEGUIRE IL DATASET NON GIOCATTOLO TOGLIERE # DALLE PROSSIME DUE RIGHE
#E INSERIRLO NELLE DUE RIGHE PRECEDENTI
#dDati1_lecture1_unif = dDati1_lecture1.map(lambda x: (x[1][0].replace("1:N:0:CAGATC", ""), [x[1][1]]))
#dDati1_lecture2_unif = dDati1_lecture2.map(lambda x: (x[1][0].replace("2:N:0:CAGATC", ""), [x[1][1]]))

dDati1_unif = dDati1_lecture1_unif.union(dDati1_lecture2_unif).reduceByKey(lambda x,y: x+y)


#RDD composta da tuple (Nubeam_/1, Nubeam_/2)
dNubeamPE = dDati1_unif.filter(lambda x: len(x[1])>1).map(lambda x: (x[1][0], numero_Nubeam_da_sequenza_basi_azotate(x[1][0]),
                                               x[1][1], numero_Nubeam_da_sequenza_basi_azotate(x[1][1])))


#RDD composta da tuple di complementari (N_comp_/1, N_comp_/2)
dNubeam_compPE = dDati1_unif.filter(lambda x: len(x[1])>1).map(lambda x: (complementare_sequenza(x[1][0]), numero_Nubeam_da_sequenza_basi_azotate(complementare_sequenza(x[1][0])),
                                                               complementare_sequenza(x[1][1]), numero_Nubeam_da_sequenza_basi_azotate(complementare_sequenza(x[1][1]))))



#Unisco le RDD appena create per formarne una sola formata solo da sequenze uniche
dPE = dNubeamPE.union(dNubeam_compPE)

dPE_Unici = dPE.distinct()


##### IMPLEMENTAZIONE IN NEO4J ######
# dopo avere importato il pacchetto neo4j
# importo i moduli da neo4j

# creo i parametri per la connessione a Neo4j
URL = "bolt://localhost:7687"
username = "neo4j"
password = "12345678"

# stabilisco la connessione
driver = GraphDatabase.driver(URL, auth=basic_auth(username, password))
session = driver.session(database="neo4j")

#sistemo i dati dalla rdd di interesse per renderli adatti al salvataggio come file .csv
dDatiNeo= dPE.zipWithIndex().map(lambda x: (x[0][0], x[0][1], x[0][2], x[0][3], x[1]))
DatiNeo=dDatiNeo.collect()

#definisco il percorso di salvataggio
file= "/Users/federica/Library/Application Support/Neo4j Desktop/Application/relate-data/dbmss/dbms-6778afb6-d63d-4b6a-a680-1f49ce0df360/import/data1.csv"
#apro un file in modalità scrittura
f= open(file,"w")
#scrivo nel file la riga di intestazione
f.write("sequenza1,nubeam1,sequenza2,nubeam2,riga\n")
#trasformo ogni riga della rdd in una stringa e la scrivo nel file
for line in DatiNeo:
    rettifica = ",".join(map(str,line))
    f.write(rettifica + "\n")
f.close()

#creo i nodi per le sequenze /1 e /2 in neo4j
query1 = "load csv with headers from 'file:///data1.csv' as line create (s1:SEQUENZA1{ seq: line.sequenza1, n1:line.nubeam1,r1:line.riga}), (s2:SEQUENZA2{seq:line.sequenza2,n2:line.nubeam2,r2:line.riga})"
risposta_seq = session.run(query1)

#creo i nodi per i numeri nubeam /1 e nubeam /2 eliminando i duplicati dei nubeam
query2 = "load csv with headers from 'file:///data1.csv' as line with distinct line.nubeam1 as NB1 create (n1:NUBEAM1{nubeam_id1:NB1})"
query3 = "load csv with headers from 'file:///data1.csv' as line with distinct line.nubeam2 as NB2 create (n2:NUBEAM2{nubeam_id2:NB2})"
risposta_nub1 = session.run(query2)
risposta_nub2 = session.run(query3)

#creo gli archi Ass1 tra le sequenze /1 e i nubeam /1
query4 = "MATCH (n1),(s1) where s1.n1=n1.nubeam_id1 create (s1)-[a:ASs1]->(n1)"
risposta_archi1 = session.run(query4)

#creo gli archi Ass2 tra le sequenze /2 e i nubeam /2
query5 = "MATCH (n2),(s2) where s2.n2=n2.nubeam_id2 create (s2)-[a:ASs2]->(n2)"
risposta_archi2 = session.run(query5)

#creo gli archi Coppia tra le sequenze /1 e /2
query6 = "MATCH (s1),(s2) where s1.r1=s2.r2 create (s1)-[c:Coppia]->(s2)"
risposta_archi3 = session.run(query6)



'''#per unire 2 file .fastq

doc_1 = "percorsofile1"
doc_2 = "percorsofile2"
doc_totale = "percorsofilebianco.csv"

with open(doc_totale,"w") as output:
    with open(doc_1,"r") as f1:
        output.write(f1.read())
    with open(doc_2,"r") as f2:
        output.write(f2.read()+"\n")'''


