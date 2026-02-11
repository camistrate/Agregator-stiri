import java.io.*;
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
    private static ConcurrentHashMap<String, Set<String>> categoryToUUIDs =  new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String, Set<String>> languageToUUIDs =  new ConcurrentHashMap<>();

    // lista cu toate articolele unice
    private static List<ArticleInfo> allArticles = Collections.synchronizedList(new ArrayList<>());

    // numar total de articole si numar total de articole unice
    private static AtomicInteger totalArticles = new AtomicInteger(0);
    private static AtomicInteger uniqueArticles = new AtomicInteger(0);

    static AtomicInteger tasksRemaining;
    static Object monitor;

    static Set<String> linkingWords = new HashSet<>();

    // limbile si categoriile din fisierele de intrare
    static List<String> languagesOrder =  new ArrayList<>();
    static List<String> categoriesOrder =  new ArrayList<>();

    static Set<String> languagesSet = new HashSet<>();
    static Set<String> categoriesSet = new HashSet<>();

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
                    totalArticles.incrementAndGet();

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

                    // articol unic (dupa uuid si title)
                    uniqueArticles.incrementAndGet();

                    // extragem campurile necesare
                    String author = (String) article.get("author");
                    String language = (String) article.get("language");
                    String url = (String) article.get("url");
                    String published = (String) article.get("published");

                    // categorii este lista
                    List<String> cats = (List<String>) article.get("categories");

                    // update autori
                    authorCount.merge(author, 1, Integer::sum);

                    // uuid -> limba
                    if (languagesSet.contains(language)) {
                        //update limbi
                        languageCount.merge(language, 1, Integer::sum);

                        languageToUUIDs
                                .computeIfAbsent(language, k -> ConcurrentHashMap.newKeySet())
                                .add(uuid);

                    }

                    // procesare categorii
                    // extragem lista bruta
                    Object rawCats = article.get("categories");
                    List<?> catsRaw = (rawCats instanceof List<?>) ? (List<?>) rawCats : Collections.emptyList();

                    // eliminam duplicatele interne si filtram categoriile invalide
                    Set<String> uniqueCats = new HashSet<>();

                    for (Object oc : catsRaw) {
                        if (oc instanceof String s) {
                            String cat = s.trim();
                            if (!cat.isEmpty() && categoriesSet.contains(cat)) {
                                uniqueCats.add(cat);     // elimina duplicate interne
                            }
                        }
                    }

                    // update categoryCount si mapare uuid -> categorie
                    for (String cat : uniqueCats) {
                        categoryCount.merge(cat, 1, Integer::sum);

                        categoryToUUIDs
                                .computeIfAbsent(cat, k -> ConcurrentHashMap.newKeySet())
                                .add(uuid);
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
        }
    }

    static void processEnglishKeywords(String text) {
        if (text == null) return;

        // lowercase
        text = text.toLowerCase();

        // split doar dupa spatiu
        String[] tokens = text.split("\\s+");

        Set<String> wordsInArticle = new HashSet<>();

        for (String token : tokens) {
            if (token.isEmpty()) continue;

            // eliminam tot ce nu e litera
            token = token.replaceAll("[^a-z]", "");

            if (token.isEmpty()) continue;
            if (linkingWords.contains(token)) continue;

            wordsInArticle.add(token);
        }

        // contorizam
        for (String word : wordsInArticle) {
            keywordCount.merge(word, 1, Integer::sum);
        }
    }

    static void loadAuxFiles(String inputsFilePath) {
        File inputsFile = new File(inputsFilePath);

        if (!inputsFile.exists()) {
            System.err.println("Input file " + inputsFilePath + " nu exista");
            return;
        }

        try (BufferedReader br = new BufferedReader(new FileReader(inputsFile))) {
            String line = br.readLine();
            if (line == null) {
                System.err.println("Input file " + inputsFilePath + " e gol");
                return;
            }

            int n = Integer.parseInt(line.trim());

            if (n < 3) {
                System.err.println("Input file " + inputsFilePath + " nu e valid");
                return;
            }

            String langPath = br.readLine().trim();
            String catPath = br.readLine().trim();
            String linkPath = br.readLine().trim();

            File base = inputsFile.getParentFile();

            loadLanguages(new File(base, langPath));
            loadCategories(new File(base, catPath));
            loadLinkingWords(new File(base, linkPath));
        } catch (Exception e) {
            System.err.println("Eroare la citirea fisierului " + inputsFilePath);
            e.printStackTrace();
        }
    }

    static void loadLanguages(File file) {
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line = br.readLine();
            if (line == null)
                return;

            int n = Integer.parseInt(line.trim());
            for (int i = 0; i < n; i++) {
                String lang = br.readLine();
                if (lang == null)
                    break;

                lang = lang.trim();
                if (lang.isEmpty())
                    continue;
                languagesOrder.add(lang);
                languagesSet.add(lang);
            }
        } catch (Exception e) {
            System.err.println("Eroare la citirea fisierului de limbi: " + file.getAbsolutePath());
            e.printStackTrace();
        }
    }

    static void loadCategories(File file) {
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line = br.readLine();
            if (line == null)
                return;

            int n = Integer.parseInt(line.trim());
            for (int i = 0; i < n; i++) {
                String cat = br.readLine();
                if (cat == null)
                    break;
                cat = cat.trim();

                if (cat.isEmpty())
                    continue;
                categoriesOrder.add(cat);
                categoriesSet.add(cat);
            }
        } catch (Exception e) {
            System.err.println("Eroare la citirea fisierului de categorii: " + file.getAbsolutePath());
            e.printStackTrace();
        }
    }

    static void loadLinkingWords(File file) {
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line = br.readLine();

            if (line == null)
                return;
            int n = Integer.parseInt(line.trim());

            for (int i = 0; i < n; i++) {
                String w = br.readLine();

                if (w == null)
                    break;
                w = w.trim().toLowerCase();

                if (w.isEmpty())
                    continue;
                linkingWords.add(w);
            }
        } catch (Exception e) {
            System.err.println("Eroare la citirea fisierului de linking words: " + file.getAbsolutePath());
            e.printStackTrace();
        }
    }

    static String normalizeCategoryName(String categoryName) {
        if (categoryName == null)
            return "";

        String noCommas = categoryName.replace(",", "");
        String trimmed = noCommas.trim();
        return trimmed.replaceAll("\\s+", "_");
    }

    static void writeUuidList(String filename, List<String> uuids) {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(filename))) {
            for (String u : uuids) {
                bw.write(u);
                bw.newLine();
            }
        } catch (IOException e) {
            System.err.println("Eroare la scrierea fisierului " + filename);
            e.printStackTrace();
        }
    }

    static void generateAllArticlesFile() {
        List<ArticleInfo> list = new ArrayList<>(allArticles);

        // sortam descrescator dupa published, la egalitate dupa uuid
        Collections.sort(list, (a, b) -> {
            int cmp = b.published.compareTo(a.published);
            if (cmp != 0) return cmp;
            return a.uuid.compareTo(b.uuid);
        });

        try (BufferedWriter bw = new BufferedWriter(new FileWriter("all_articles.txt"))) {
            for (ArticleInfo art : list) {
                bw.write(art.uuid);
                bw.write(" ");
                bw.write(art.published);
                bw.newLine();
            }
        } catch (IOException e) {
            System.err.println("Eroare la scrierea all_articles.txt");
            e.printStackTrace();
        }
    }

    static void generateLanguageFiles() {
        for (String lang : languagesOrder) {
            Set<String> uuids = languageToUUIDs.get(lang);
            if (uuids == null || uuids.isEmpty()) {
                // daca nu exista articole pentru limba respectiva, nu generam fisier
                continue;
            }

            List<String> copy = new ArrayList<>(uuids);
            Collections.sort(copy); // lexicografic dupa uuid

            String filename = lang + ".txt";
            writeUuidList(filename, copy);
        }
    }

    static void generateCategoryFiles() {
        for (String cat : categoriesOrder) {
            Set<String> uuids = categoryToUUIDs.get(cat);
            if (uuids == null || uuids.isEmpty()) {
                // daca nu exista articole pentru categoria respectiva, nu generam fisier
                continue;
            }

            List<String> copy = new ArrayList<>(uuids);
            Collections.sort(copy); // lexicografic dupa uuid

            String filename = normalizeCategoryName(cat) + ".txt";
            writeUuidList(filename, copy);
        }
    }

    static void generateKeywordsFile() {
        // extragem cuvintele si le sortam alfabetic
        List<String> words = new ArrayList<>(keywordCount.keySet());
        Collections.sort(words);

        List<Map.Entry<String,Integer>> entries = new ArrayList<>();
        for (String w : words) {
            entries.add(Map.entry(w, keywordCount.get(w)));
        }

        Collections.sort(entries, (a, b) -> {
            int cmp = Integer.compare(b.getValue(), a.getValue());  // count descrescator
            if (cmp != 0) return cmp;
            return a.getKey().compareTo(b.getKey()); // lexicografic crescator
        });

        // scierea rezultatelor in keywords_count.txt
        try (BufferedWriter bw = new BufferedWriter(new FileWriter("keywords_count.txt"))) {
            for (Map.Entry<String, Integer> e : entries) {
                bw.write(e.getKey());
                bw.write(" ");
                bw.write(Integer.toString(e.getValue()));
                bw.newLine();
            }
        } catch (IOException e) {
            System.err.println("Eroare la scrierea keywords.txt");
            e.printStackTrace();
        }
    }

    static void generateReportsFile() {
        int total = totalArticles.get();
        int unique = uniqueArticles.get();
        int duplicates = total - unique;

        try (BufferedWriter bw = new BufferedWriter(new FileWriter("reports.txt"))) {

            bw.write("duplicates_found - " + duplicates);
            bw.newLine();

            bw.write("unique_articles - " + unique);
            bw.newLine();

            // best_author
            String bestAuthor = "-";
            int bestAuthorCount = 0;
            List<String> authors = new ArrayList<>(authorCount.keySet());
            Collections.sort(authors);
            for (String name : authors) {
                int cnt = authorCount.get(name);
                if (cnt > bestAuthorCount ||
                        (cnt == bestAuthorCount && name.compareTo(bestAuthor) < 0)) {
                    bestAuthorCount = cnt;
                    bestAuthor = name;
                }
            }
            bw.write("best_author - " + bestAuthor + " " + bestAuthorCount);
            bw.newLine();

            // top_language
            String bestLang = "-";
            int bestLangCount = 0;
            for (String lang : languagesOrder) {
                int cnt = languageCount.getOrDefault(lang, 0);
                if (cnt > bestLangCount ||
                        (cnt == bestLangCount && cnt > 0 && lang.compareTo(bestLang) < 0)) {
                    bestLangCount = cnt;
                    bestLang = lang;
                }
            }
            bw.write("top_language - " + bestLang + " " + bestLangCount);
            bw.newLine();

            // top_category
            String bestCat = "-";
            int bestCatCount = 0;
            for (String cat : categoriesOrder) {
                int cnt = categoryCount.getOrDefault(cat, 0);
                if (cnt == 0) continue;

                String norm = normalizeCategoryName(cat);
                if (cnt > bestCatCount ||
                        (cnt == bestCatCount &&
                                norm.compareTo(normalizeCategoryName(bestCat)) < 0)) {
                    bestCat = norm;
                    bestCatCount = cnt;
                }
            }
            String bestCatNorm = (bestCat == null) ? "-" : normalizeCategoryName(bestCat);
            bw.write("top_category - " + bestCatNorm + " " + bestCatCount);
            bw.newLine();

            // most_recent_article
            ArticleInfo mostRecent = null;
            for (ArticleInfo art : allArticles) {
                if (mostRecent == null) {
                    mostRecent = art;
                } else {
                    int cmp = art.published.compareTo(mostRecent.published);
                    if (cmp > 0 || (cmp == 0 && art.uuid.compareTo(mostRecent.uuid) < 0)) {
                        mostRecent = art;
                    }
                }
            }
            if (mostRecent == null) {
                bw.write("most_recent_article - - -");
            } else {
                bw.write("most_recent_article - " +
                        mostRecent.published + " " + mostRecent.url);
            }
            bw.newLine();

            // top_keyword_en
            String bestWord = "-";
            int bestWordCount = 0;
            for (Map.Entry<String, Integer> e : keywordCount.entrySet()) {
                String w = e.getKey();
                int cnt = e.getValue();
                if (cnt > bestWordCount ||
                        (cnt == bestWordCount && w.compareTo(bestWord) < 0)) {
                    bestWordCount = cnt;
                    bestWord = w;
                }
            }
            bw.write("top_keyword_en - " + bestWord + " " + bestWordCount);
            bw.newLine();

        } catch (IOException e) {
            System.err.println("Eroare la scrierea reports.txt");
            e.printStackTrace();
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

        String articlesListPath = args[1];
        File articlesListFile = new File(articlesListPath);

        if (!articlesListFile.exists()) {
            System.err.println("Eroare: fisierul articles.txt nu exista: " + articlesListPath);
            return;
        }

        List<String> jsonInputFiles = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(articlesListFile))) {

            int n = Integer.parseInt(br.readLine().trim());

            for (int i = 0; i < n; i++) {
                String relPath = br.readLine().trim();

                // Construim calea pornind de la directorul lui articles.txt
                File resolved = new File(articlesListFile.getParentFile(), relPath).getCanonicalFile();

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

        String inputsPath = args[2];
        loadAuxFiles(inputsPath);

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

        // FAZA 3 - generare fisiere de iesire
        generateAllArticlesFile();
        generateCategoryFiles();
        generateLanguageFiles();
        generateKeywordsFile();
        generateReportsFile();


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