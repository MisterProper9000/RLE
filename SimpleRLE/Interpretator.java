import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Даниил on 29.09.2018.
 */
public class Interpretator {
    private static final String sp = ":";
    public enum LEXEMES {INPUT_FILE, OUTPUT_FILE, PARAM_FILE, CODE_MODE, MIN_LENGTH}
    public static final Map<String, LEXEMES> grammarMap;
    static
    {
        grammarMap = new HashMap<>();
        grammarMap.put( "IN", LEXEMES.INPUT_FILE);
        grammarMap.put("OUT", LEXEMES.OUTPUT_FILE);
        grammarMap.put("PARAM", LEXEMES.PARAM_FILE);
        grammarMap.put("CODE_MODE", LEXEMES.CODE_MODE);
        grammarMap.put("MIN_LENGTH", LEXEMES.MIN_LENGTH);
    }





    public static boolean Interp(String config) throws IOException
    {
        BufferedReader cfgBReader;
        try {
            File file = new File(config);
            FileReader cfgReader = new FileReader(file);
            cfgBReader = new BufferedReader(cfgReader);
        }
        catch (IOException e)
        {
            Log.report("Can't open \"" + config + "\" file\n");
            return false;
        }

        String sas;

        while((sas = cfgBReader.readLine()) != null)
        {

            if(sas.trim().isEmpty())
                continue;

            String[] pair = sas.split(sp);

            if (pair.length != 2) {
                Log.report("Invalid config format");
                return false;
            } else if (pair[0].equals("") || pair[1].equals("")) {
                Log.report("Invalid config data");
                return false;
            }

            for(int i = 0; i  < pair.length; i++)
            {
                pair[i] = pair[i].trim();
            }

            LEXEMES l;
            if (grammarMap.get(pair[0]) == LEXEMES.PARAM_FILE) {
                if((l = grammarMap.get(pair[0])) != null)
                    RLE.resultMap.put(l, pair[1]);
                else {
                    Log.report("Invalid lexem");
                    return false;
                }
                if(!Interp(pair[1]))
                    return false;

            }
            else if(grammarMap.get(pair[0]) == LEXEMES.INPUT_FILE)
                RLE.resultMap.put(LEXEMES.INPUT_FILE, pair[1]);
            else if(grammarMap.get(pair[0]) == LEXEMES.OUTPUT_FILE)
                RLE.resultMap.put(grammarMap.get(pair[0]), pair[1]);
            else
                if((l = grammarMap.get(pair[0])) != null)
                    RLE.resultMap.put(l, pair[1]);
                else {
                    Log.report("Invalid lexem");
                    return false;
                }

        }

        if(!config.equals(RLE.resultMap.get(LEXEMES.PARAM_FILE)))
            if(RLE.resultMap.putIfAbsent(LEXEMES.OUTPUT_FILE, "output.txt") == null)
                Log.report("Missing output file, using default output file -- output.txt\n");
        if(config.equals(RLE.resultMap.get(LEXEMES.PARAM_FILE))) {
            if (RLE.resultMap.putIfAbsent(LEXEMES.CODE_MODE, "0") == null)
                Log.report("Missing code mode, using default encode mode\n");
            if (RLE.resultMap.putIfAbsent(LEXEMES.MIN_LENGTH, "2") == null)
                Log.report("Missing min length of repeated byte series, using default min length\n");
        }
        return true;
    }
}
