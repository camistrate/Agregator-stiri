import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;


public class Tema1 {
    private static volatile boolean shutdown = false;

    // structuri pt identificarea duplicatelor (Faza 1)

    // nr-ul de aparitii pt fiecare uuid si fiecare title
    private static ConcurrentHashMap<String, Integer> uuidCount =  new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String, Integer> titleCount =  new ConcurrentHashMap<>();

    // structuri pt procesarea articolelor unice (Faza 2)

    // contoare globale
    private static ConcurrentHashMap<String, Integer> authorCount =  new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String, Integer> languageCount =  new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String, Integer> categoryCount =  new ConcurrentHashMap<>();

    // contor pt cuvinte limba engleza
    private static ConcurrentHashMap<String, Integer> keywordCount =  new ConcurrentHashMap<>();

    // uuid-urille puse pe categorie/limba
    private static ConcurrentHashMap<String, List<String>> categoryToUUIDs =  new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String, List<String>> languageToUUIDs =  new ConcurrentHashMap<>();

    // lista cu toate articolele unice
    private static List<ArticleInfo> allArticles = Collections.synchronizedList(new ArrayList<>());

    // numar total de articole si numar total de articole unice
    private static AtomicInteger totalArticles = new AtomicInteger(0);
    private static AtomicInteger uniqueArticles = new AtomicInteger(0);

    static AtomicInteger tasksRemaining;
    static Object monitor;

    static Set<String> linkingWords = new HashSet<>();

    static class ArticleInfo {
        String uuid;
        String published;
        String url;

        ArticleInfo(String uuid, String published, String url) {
            this.uuid = uuid;
            this.published = published;
            this.url = url;
        }
    }

    static class Worker implements Runnable {
        private final BlockingQueue<Runnable> queue;
        private final AtomicInteger tasksRemaining;
        private final Object monitor;

        private volatile boolean running = true;

        public Worker(BlockingQueue<Runnable> queue, AtomicInteger tasksRemaining ,Object monitor) {
            this.queue = queue;
            this.tasksRemaining = tasksRemaining;
            this.monitor = monitor;
        }

        @Override
        public void run() {
            while (running) {
                try {
                    Runnable task = queue.take();

                    task.run();

                    int remaining = tasksRemaining.decrementAndGet();
                    if (remaining == 0) {
                        synchronized (monitor) {
                            monitor.notifyAll();
                        }
                    }
                } catch (InterruptedException e) {
                    if (shutdown) {
                        break;
                    }
                }
            }
        }
    }

    static class TaskFaza1 implements Runnable {
        private final String filename;

        public TaskFaza1(String filename) {
            this.filename = filename;
        }

        @Override
        public void run() {
            try {
                ObjectMapper mapper = new ObjectMapper();

                // citim fisierul ca o lista generica de obiecte
                List<Map<String, Object>> articles =
                        mapper.readValue(new File(filename), new TypeReference<List<Map<String, Object>>>() {});

                // iteram prin articole
                for (Map<String, Object> article : articles) {
                    String uuid = (String) article.get("uuid");
                    String title = (String) article.get("title");

                    // incrementam contoarele
                    uuidCount.merge(uuid, 1, Integer::sum);
                    titleCount.merge(title, 1, Integer::sum);
                }
            } catch (Exception e) {
                System.err.println("Eroare la citirea fisierului " + filename);
                e.printStackTrace();
            }
        }
    }

    static class TaskFaza2 implements Runnable {
        private final String filePath;

        public TaskFaza2(String filePath) {
            this.filePath = filePath;
        }

        @Override
        public void run() {
            ObjectMapper mapper = new ObjectMapper();

            try {
                List<Map<String, Object>> articles =
                        mapper.readValue(new File(filePath), new TypeReference<>() {});
                for (Map<String, Object> article : articles) {
                    String uuid = (String) article.get("uuid");
                    String title = (String) article.get("title");

                    // verificare duplicate
                    if (uuidCount.get(uuid) != 1 || titleCount.get(title) != 1) {
                        continue;
                    }

                    // extragem campurile necesare
                    String author = (String) article.get("author");
                    String language = (String) article.get("language");
                    String url = (String) article.get("url");
                    String published = (String) article.get("published");

                    // categorii este lista
                    List<String> cats = (List<String>) article.get("categories");

                    // update autori
                    authorCount.merge(author, 1, Integer::sum);

                    //updat limbi
                    languageCount.merge(language, 1, Integer::sum);

                    // update categorii
                    for (String cat : cats) {
                        categoryCount.merge(cat, 1, Integer::sum);
                    }

                    // uuid -> limba
                    List<String> list = languageToUUIDs.get(language);
                    if (list == null) {
                        list = Collections.synchronizedList(new ArrayList<>());
                        languageToUUIDs.put(language, list);
                    }
                    list.add(uuid);

                    // uuid -> categorii
                    for (String cat : cats) {
                        List<String> list2 = categoryToUUIDs.get(cat);
                        if (list2 == null) {
                            list2 = Collections.synchronizedList(new ArrayList<>());
                            categoryToUUIDs.put(cat, list2);
                        }
                        list2.add(uuid);
                    }

                    // lista allArticles
                    allArticles.add(new ArticleInfo(uuid, published, url));

                    // Keywords
                    if (language.equals("english")) {
                        String text = (String) article.get("text");
                        processEnglishKeywords(text);
                    }
                }
            } catch (Exception e) {
                System.err.println("Eroare la citirea fisierului " + filePath);
                e.printStackTrace();
            }

            // decrementam
            if (tasksRemaining.decrementAndGet() == 0) {
                synchronized (monitor) {
                    monitor.notifyAll();
                }
            }
        }
    }

    static void processEnglishKeywords(String text) {
        String[] tokens = text.toLowerCase().split("[^a-z]+");
        for (String token : tokens) {
            if (token.isEmpty()) continue;
            if (linkingWords.contains(token)) continue;

            keywordCount.merge(token, 1, Integer::sum);
        }
    }

    public static void main(String[] args) {
        // PASLUL 1 - validarea argumentelor in linia de comanda
        if (args.length < 2) {
            System.err.println("Usage: java Tema1 <num_threads> <json_file1> <json_file2> ...");
            return;
        }

        int numThreads = 0;
        try {
            numThreads = Integer.parseInt(args[0]);
        } catch (NumberFormatException e) {
            System.err.println("Primul argument trebuie sa fie un numar intreg pozitiv (nr-ul de threaduri)");
            return;
        }

        if (numThreads < 1) {
            System.err.println("Nr-ul de thraduri trebuie sa fie > 0");
            return;
        }

        // args[1] = path către articles.txt
        String articlesListPath = args[1];
        File articlesListFile = new File(articlesListPath);

        if (!articlesListFile.exists()) {
            System.err.println("Eroare: fisierul articles.txt nu exista: " + articlesListPath);
            return;
        }

        List<String> jsonInputFiles = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(articlesListFile))) {

            // Prima linie: număr de fișiere JSON
            int n = Integer.parseInt(br.readLine().trim());

            for (int i = 0; i < n; i++) {
                String relPath = br.readLine().trim();

                // Construim absolut calea pornind de la directorul lui articles.txt
                File base = articlesListFile.getParentFile();
                File resolved = new File(base, relPath);

                if (!resolved.exists()) {
                    System.err.println("Fisier JSON inexistent: " + resolved.getAbsolutePath());
                    return;
                }

                jsonInputFiles.add(resolved.getAbsolutePath());
            }

        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        System.out.println("Fisiere JSON pentru test: " + jsonInputFiles.size());
        for (String f : jsonInputFiles) System.out.println(" - " + f);


        // PASUL 2 - infrastructura workerilor

        // coada de taskuri
        BlockingQueue<Runnable> taskQueue = new LinkedBlockingQueue<>();
        // contorul de taskuri ramase in faza curenta
        tasksRemaining = new AtomicInteger(0);
        // monitor pentru wait/notifyAll
        monitor = new Object();
        // crearea si pornirea workerilor
        List<Thread> workers = new ArrayList<>();

        for (int i = 0; i < numThreads; i++) {
            Thread thread = new Thread(new Worker(taskQueue, tasksRemaining, monitor));
            thread.start();
            workers.add(thread);
        }

        // FAZA 1 - numararea aparitiilor de uuid si title
        tasksRemaining.set(jsonInputFiles.size()); // atatea taskuri avem in faza 1

        for (String file :  jsonInputFiles) {
            taskQueue.add(new TaskFaza1(file));
        }

        synchronized (monitor) {
            while (tasksRemaining.get() > 0) {
                try {
                    monitor.wait();
                } catch (InterruptedException e) {}
            }
        }

        System.out.println("Workeri creati si porniti: " + numThreads);

        // FAZA 2 - procesarea articolelor unice
        tasksRemaining.set(jsonInputFiles.size());

        for (String file : jsonInputFiles) {
            taskQueue.add(new TaskFaza2(file));
        }

        synchronized (monitor) {
            while (tasksRemaining.get() > 0) {
                try {
                    monitor.wait();
                } catch (InterruptedException e) {}
            }
        }



        shutdown = true;
        for (Thread thread : workers) {
            thread.interrupt();
        }

        for (Thread thread : workers) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                // ignoram
            }
        }
    }
}