import com.sun.org.apache.xpath.internal.operations.Bool;
import javafx.util.Pair;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Даниил on 18.11.2018.
 */

/**
 * Pipeline manager class
 */
public class Pipeline {

    //Pipeline config file grammar
    public static final Map<String, Grammar.FILE_LEXEMES> grammarMap;

    static {
        grammarMap = new HashMap<>();
        grammarMap.put("IN", Grammar.FILE_LEXEMES.INPUT_FILE);
        grammarMap.put("OUT", Grammar.FILE_LEXEMES.OUTPUT_FILE);
        grammarMap.put("CHUNK_SIZE", Grammar.FILE_LEXEMES.CHUNK_SIZE);
        grammarMap.put("WORKERS", Grammar.FILE_LEXEMES.WORKERS_FILE);
        grammarMap.put("PIP_SEQUENCE", Grammar.FILE_LEXEMES.PIPSEQ);
    }

    //signs of input and output in workers file
    public static final String input = "-1", output = "-2";

    //Workers config file grammar
    public static final Map<String, Grammar.WORKER_CFG_FILE_LEXEMES> workerGrammarMap;

    static {
        workerGrammarMap = new HashMap<>();
        workerGrammarMap.put("WORKER", Grammar.WORKER_CFG_FILE_LEXEMES.WORKER);
        workerGrammarMap.put(output, Grammar.WORKER_CFG_FILE_LEXEMES.INPUT);
        workerGrammarMap.put(input, Grammar.WORKER_CFG_FILE_LEXEMES.OUTPUT);
    }

    //Worker's name and corresponding worker
    private Map<String, Executor> workersList;

    //Pipeline config interpreted
    private Map<Grammar.FILE_LEXEMES, String> configMap;

    //Index of first worker in pipeline queue
    private String startIdx;

    /**
     * Constructor
     *
     * @param cfg -- name of pipeline config file
     * @throws IOException
     */
    public Pipeline(String cfg) throws IOException {
        workersList = new HashMap<>();
        configMap = new HashMap<>();

        //Creating an interpreter for this pipeline
        PipInterpreter interp = new PipInterpreter();

        //put interpreted pipeline config map
        if ((configMap = interp.Interp(cfg)) == null)
            System.exit(1);

        if (!CreateWorkers(interp.InterpWorkers(configMap.get(Grammar.FILE_LEXEMES.WORKERS_FILE)))) {
            System.exit(1);
        }

        //Set queue of workers
        if (!ConnectWorkers(interp.SetPipSeq(configMap.get(Grammar.FILE_LEXEMES.PIPSEQ)))) {

            System.exit(1);
        }
    }

    /**
     * Start pipeline
     */
    public void Start() {
        if (workersList.get(startIdx).run() != 0) {
            Log.report("Pipeline crashed");
        }
    }

    /**
     * Fill the map of workers
     *
     * @param names -- list of worker's name and corresponding worker's config file
     * @return
     */
    private boolean CreateWorkers(ArrayList<Pair<String, String>> names) {
        if (names == null)
            return false;

        for (int i = 0; i < names.size(); i++) {
            try {
                workersList.put(names.get(i).getKey(), new RLEWorkItem(names.get(i).getValue(), Integer.parseInt(configMap.get(Grammar.FILE_LEXEMES.CHUNK_SIZE))));
            } catch (IOException e) {
                Log.report("Error in worker creation");
                return false;
            }
        }

        return true;
    }

    /**
     * @param seq -- sequence of workers
     * @return
     */
    private boolean ConnectWorkers(ArrayList<Pair<Integer, Integer>> seq) {
        if (seq == null)
            return false;
        int check = 0;
        for (int i = 0; i < seq.size(); i++) {
            //set input point
            if (seq.get(i).getKey() == Integer.parseInt(input)) {

                try {
                    if (workersList.get(Integer.toString(seq.get(i).getValue())) == null) {
                        Log.report("Wrong worker name " + seq.get(i).getValue());
                        System.exit(1);
                    }
                    check++;
                    startIdx = Integer.toString(seq.get(i).getValue());
                    workersList.get(Integer.toString(seq.get(i).getValue())).setInput(new DataInputStream(new FileInputStream(configMap.get(Grammar.FILE_LEXEMES.INPUT_FILE))));
                } catch (IOException e) {
                    Log.report("Can't open input file");
                    System.exit(1);
                }
            }
            //set output point
            else if (seq.get(i).getValue() == Integer.parseInt(output)) {

                try {
                    if (workersList.get(Integer.toString(seq.get(i).getKey())) == null) {
                        Log.report("Wrong worker name " + seq.get(i).getKey());
                        System.exit(1);
                    }
                    check++;
                    workersList.get(Integer.toString(seq.get(i).getKey())).setOutput(new DataOutputStream(new FileOutputStream(configMap.get(Grammar.FILE_LEXEMES.OUTPUT_FILE))));
                } catch (IOException e) {
                    Log.report("Can't open output file");
                    System.exit(1);
                }

            }
            //set usual worker
            else {

                if (workersList.get(Integer.toString(seq.get(i).getKey())) == null || workersList.get(Integer.toString(seq.get(i).getValue())) == null) {
                    Log.report("Wrong worker name " + seq.get(i).getKey());
                    System.exit(1);
                }
                workersList.get(Integer.toString(seq.get(i).getKey())).setConsumer(workersList.get(Integer.toString(seq.get(i).getValue())));

            }
        }

        if (check != 2) {
            Log.report("Missing input or output in pipeline's queue");
            System.exit(1);
        }

        //check for cycle
        Boolean workers[] = new Boolean[seq.size() - 1];
        for(int i = 0; i < seq.size() -1; i++)
            workers[i] = false;

        int start = Integer.parseInt(startIdx);

        workers[start] = true;
        int first = start;
        int second;
        while (true) {
            second = seq.get(Find(first,seq)).getValue();
            if(second == Integer.parseInt(output))
            {
                break;
            }
            if (workers[second])
            {
                Log.report("Detected cycle in workers queue");
                System.exit(1);
                break;
            }
            workers[first] = true;
            first = second;
        }

        for(int i = 0; i < workers.length; i++)
            if(!workers[i])
            {
                Log.report("Found a pair of workers which is doesn't belongs to pipeline queue");
                break;
            }
        return true;
    }

    private int Find(int k, ArrayList<Pair<Integer, Integer>> seq)
    {
        for(int i = 0; i < seq.size(); i++)
        {
            if(seq.get(i).getKey() == k)
                return i;
        }
        return -1;
    }

}
