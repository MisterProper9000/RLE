import javafx.util.Pair;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Даниил on 29.09.2018.
 */
public class PipInterpreter {
    private final String sp = ":";

    /**
     * Opens given file
     * @param fileName -- name of file to be opened
     * @return
     */
    private BufferedReader OpenFile(String fileName) {
        BufferedReader BufReader;

        try {
            File file = new File(fileName);
            FileReader cfgReader = new FileReader(file);
            BufReader = new BufferedReader(cfgReader);
        } catch (IOException e) {
            Log.report("Can't open \"" + fileName + "\" file\n");
            return null;
        }
        return BufReader;
    }

    /**
     * Processing string with config syntax
     * @param s -- processing string
     * @return
     */
    private String[] ProcessString(String s) {
        String[] pair = s.split(sp);

        if (pair.length != 2) {
            Log.report("Invalid config format");
            return null;
        } else if (pair[0].equals("") || pair[1].equals("")) {
            Log.report("Invalid config data");
            return null;
        }

        for (int i = 0; i < pair.length; i++) {
            pair[i] = pair[i].trim();
        }
        return pair;
    }

    /**
     * Interprets pipeline config file
     * @param config -- name of pipeline's config file
     * @return pipeline config map
     * @throws IOException
     */
    public Map<Grammar.FILE_LEXEMES, String> Interp(String config) throws IOException {
        BufferedReader cfgBufReader;
        if ((cfgBufReader = OpenFile(config)) == null)
            return null;

        String string;

        Map<Grammar.FILE_LEXEMES, String> map = new HashMap<>();

        while ((string = cfgBufReader.readLine()) != null) {

            if (string.trim().isEmpty())
                continue;

            String[] pair;
            if ((pair = ProcessString(string)) == null)
                return null;

            if (Pipeline.grammarMap.get(pair[0]) == Grammar.FILE_LEXEMES.WORKERS_FILE) {
                map.put(Grammar.FILE_LEXEMES.WORKERS_FILE, pair[1]);
            } else if (Pipeline.grammarMap.get(pair[0]) == Grammar.FILE_LEXEMES.PIPSEQ) {
                map.put(Grammar.FILE_LEXEMES.PIPSEQ, pair[1]);
            } else if (Pipeline.grammarMap.get(pair[0]) == Grammar.FILE_LEXEMES.INPUT_FILE) {
                map.put(Grammar.FILE_LEXEMES.INPUT_FILE, pair[1]);
            } else if (Pipeline.grammarMap.get(pair[0]) == Grammar.FILE_LEXEMES.OUTPUT_FILE) {
                map.put(Grammar.FILE_LEXEMES.OUTPUT_FILE, pair[1]);
            } else if (Pipeline.grammarMap.get(pair[0]) == Grammar.FILE_LEXEMES.CHUNK_SIZE) {
                map.put(Grammar.FILE_LEXEMES.CHUNK_SIZE, pair[1]);
            } else {
                Log.report("Wrong lexem in " + config + " file");
                return null;
            }
        }

        if (map.putIfAbsent(Grammar.FILE_LEXEMES.CHUNK_SIZE, "10") == null) {
            Log.report("Missing block length, using default block length - 10 symbols");
        }
        if (map.putIfAbsent(Grammar.FILE_LEXEMES.OUTPUT_FILE, "output.txt") == null) {
            Log.report("Missing output file, using default file - 10 output.txt");
        }

        return map;
    }

    /**
     * Interprets file with workers enum
     * @param config -- name of config file
     * @return list of pairs -- worker's name, worker's config file name
     * @throws IOException
     */
    public ArrayList<Pair<String, String>> InterpWorkers(String config) throws IOException {
        BufferedReader cfgBufReader;
        if ((cfgBufReader = OpenFile(config)) == null)
            return null;

        String string;
        ArrayList<Pair<String, String>> map = new ArrayList<>();
        while ((string = cfgBufReader.readLine()) != null) {
            if (string.trim().isEmpty())
                continue;

            String[] pair = string.split(sp);

            if (pair.length != 3) {
                Log.report("Invalid config format");
                return null;
            } else if (pair[0].equals("") || pair[1].equals("") || pair[2].equals("")) {
                Log.report("Invalid config data");
                return null;
            }

            for (int i = 0; i < pair.length; i++) {
                pair[i] = pair[i].trim();
            }

            if (Pipeline.workerGrammarMap.get(pair[0]) == Grammar.WORKER_CFG_FILE_LEXEMES.WORKER) {
                map.add(new Pair<>(pair[1], pair[2]));
            } else {
                Log.report("Wrong lexem in " + config + " file");
                return null;
            }
        }

        return map;
    }

    /**
     * Setting sequence of pipeline
     * @param fileName -- sequence config file name
     * @return list of pairs -- sending worker, receiving worker
     * @throws IOException
     */
    public ArrayList<Pair<Integer, Integer>> SetPipSeq(String fileName) throws IOException {

        BufferedReader cfgBufReader;
        if ((cfgBufReader = OpenFile(fileName)) == null)
            return null;

        String string;

        ArrayList<Pair<Integer, Integer>> pairs = new ArrayList<>();
        while ((string = cfgBufReader.readLine()) != null) {
            if (string.trim().isEmpty())
                continue;

            String[] pair;
            if ((pair = ProcessString(string)) == null)
                return null;
            int a, b;
            if (((a = Integer.parseInt(pair[0])) < -2 ) ||
                    ((b = Integer.parseInt(pair[1])) < -2 )) {
                Log.report("Worker index out of bound");
                return null;
            }
            pairs.add(new Pair<>(a, b));
        }

        return pairs;
    }
}
