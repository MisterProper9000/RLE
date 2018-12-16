import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Даниил on 24.11.2018.
 */

/**
 * Interpreter for RLEWorkItem
 */
public class WorkerInterpreter {
    private final String sp = ":";
    public enum WORKER_LEXEMES {CODE_MODE, SHIFT, LEN, TYPE}
    public static final Map<String, WORKER_LEXEMES> grammarMap;
    static {
        grammarMap = new HashMap<>();
        grammarMap.put("CODE_MODE", WORKER_LEXEMES.CODE_MODE);
        grammarMap.put("SHIFT", WORKER_LEXEMES.SHIFT);
        grammarMap.put("LEN", WORKER_LEXEMES.LEN);
        grammarMap.put("TYPE", WORKER_LEXEMES.TYPE);
    }

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
     *
     * @param s -- string to be processing by a config syntax
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
     * Interprets worker cofig file
     * @param fileName -- name of worker's config file
     * @return map with data for initializing worker
     * @throws IOException
     */
    public Map<WORKER_LEXEMES, String> InterpWorker(String fileName) throws IOException {
        BufferedReader cfgBufReader;
        if ((cfgBufReader = OpenFile(fileName)) == null)
            return null;

        String string;

        Map<WORKER_LEXEMES, String> map = new HashMap<>();

        while ((string = cfgBufReader.readLine()) != null) {
            if (string.trim().isEmpty())
                continue;

            String[] pair;
            if ((pair = ProcessString(string)) == null)
                return null;

            if (grammarMap.get(pair[0]) == WORKER_LEXEMES.CODE_MODE) {
                map.put(WORKER_LEXEMES.CODE_MODE, pair[1]);
            } else if (grammarMap.get(pair[0]) == WORKER_LEXEMES.SHIFT) {
                map.put(WORKER_LEXEMES.SHIFT, pair[1]);
            } else if (grammarMap.get(pair[0]) == WORKER_LEXEMES.LEN) {
                map.put(WORKER_LEXEMES.LEN, pair[1]);
            }else if (grammarMap.get(pair[0]) == WORKER_LEXEMES.TYPE) {
                ;
            }else {
                Log.report("Wrong lexem in " + fileName + " file");
                return null;
            }
        }


        if (map.putIfAbsent(WORKER_LEXEMES.CODE_MODE, "0") == null) {
            Log.report("Missing code mode, using default encoding mode");
        }
        if (map.putIfAbsent(WORKER_LEXEMES.TYPE, "BYTE") == null) {
            Log.report("Missing worker's input type, using default byte type");
        }

        return map;
    }

}
