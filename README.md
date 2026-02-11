# Agregator de Stiri – Procesare Paralela in Java

## Descriere

Acest proiect implementeaza un program paralel in Java care simuleaza functionarea unui agregator de stiri. Aplicatia proceseaza un volum mare de articole, le organizeaza eficient pe categorii si limbi si genereaza statistici si rapoarte agregate intr-un format clar, structurat si determinist.

Rezultatul final consta intr-un set de fisiere text care reflecta informatiile extrase si prelucrate din articolele de intrare, intr-o maniera coerenta si reproductibila.

---

## Obiectivele proiectului

Prin dezvoltarea acestei aplicatii se urmareste:

- Implementarea procesarii paralele folosind Java Threads
- Descompunerea unei probleme descrise in limbaj natural in subprobleme executabile in paralel
- Identificarea unei solutii paralele scalabile
- Organizarea articolelor in functie de categorie si limba
- Generarea de statistici agregate
- Generarea de rapoarte deterministe
- Producerea de fisiere text structurate

---

## Functionalitati principale

Aplicatia:

- Citeste un volum mare de articole de intrare
- Proceseaza articolele in paralel
- Grupeaza articolele pe categorii si limbi
- Genereaza statistici relevante
- Produce fisiere de iesire structurate si deterministe


---
 ## 1 Impartirea muncii intre thread-uri

Implementarea este organizata in trei faze principale, toate executate de acelasi pool de thread-uri, creat o singura data la inceputul programului:

### Faza 1 – Identificarea articolelor duplicate

- Pentru fiecare fisier JSON se creeaza un task.
- Thread-urile citesc fisierele in paralel si actualizeaza:
  - numarul de aparitii al fiecarui uuid,
  - numarul de aparitii al fiecarui title.
- Aceasta faza este complet paralelizata si se termina doar cand toate fisierele au fost procesate.

### Faza 2 – Procesarea articolelor unice

- Se genereaza din nou un task per fisier.
- Thread-urile proceseaza articolele ramase, filtrate dupa regulile din faza 1.
- In paralel se actualizeaza:
  - statisticile pe limbi,
  - statisticile pe categorii,
  - lista articolelor valide,
  - structurile necesare pentru generarea rapoartelor,
  - seturile de cuvinte-cheie in limba engleza.

### Faza 3 – Generarea fisierelor finalizate

- Se executa exclusiv in thread-ul principal.
- Include generarea: all_articles.txt, fisierele pe categorii si limbi, reports.txt, keywords_count.txt.
- Este secventiala deoarece reprezinta doar alipirea finala a datelor colectate.

---

## 2 Mecanisme de sincronizare folosite

Pentru a mentine corectitudinea in paralel, sunt utilizate mai multe mecanisme de sincronizare:

- **BlockingQueue**: Folosita pentru a distribui task-urile catre thread-uri. Asigura preluarea corecta si sigura a muncii in paralel.
- **AtomicInteger**: Folosit atat pentru tasksRemaining (controlul finalizarii fazelor), cat si pentru diverse numarari paralele. Operatiile atomice permit actualizari sigure fara blocari explicite.
- **Obiectul monitor + wait() / notifyAll()**: Implementeaza barierele dintre faze: main-ul asteapta finalizarea tuturor task-urilor din faza curenta inainte de a continua.
- **ConcurrentHashMap si ConcurrentHashMap.newKeySet()**: Structuri de date thread-safe folosite pentru numarari, mapari si colectarea informatiilor necesare pentru rapoarte.
- **Synchronized doar pentru delimitarea fazelor**: Nu exista blocari excesive; sincronizarea este folosita strict pentru controlul tranzitiei intre faze.
