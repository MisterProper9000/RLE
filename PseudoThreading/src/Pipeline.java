import javafx.util.Pair;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IllegalFormatCodePointException;
import java.util.Map;

/**
 * Created by ������ on 18.11.2018.
 */

/**
 * Pipeline manager class
 */
public class Pipeline {


    //Worker's name and corresponding worker
    private ArrayList<Pair<Grammar.WORKERS_CFG_FILE_LEXEMES, Pair<String, Executor>>> workersList;

    //Pipeline config interpreted
    private Map<Grammar.FILE_LEXEMES, String> configMap;

    /**
     * Constructor
     *
     * @param cfg -- name of pipeline config file
     * @throws IOException
     */
    public Pipeline(String cfg) throws IOException {
        workersList = new ArrayList<>();
        configMap = new HashMap<>();

        //Creating an interpreter for this pipeline
        PipInterpreter interp = new PipInterpreter();

        //put interpreted pipeline config map
        if ((configMap = interp.Interp(cfg)) == null)
            System.exit(1);


        //Set queue of workers
        if (!ConnectWorkers(CreateWorkers(interp.InterpWorkers(configMap.get(Grammar.FILE_LEXEMES.WORKERS_FILE))),interp.SetPipSeq(configMap.get(Grammar.FILE_LEXEMES.PIPSEQ)))) {

            System.exit(1);
        }
    }

    /**
     * Start pipeline
     */
    public void Start() {
        for(int i =0; i < workersList.size(); i++)
            if(workersList.get(i).getKey() == Grammar.WORKERS_CFG_FILE_LEXEMES.INPUT_WORKER) {
                if (workersList.get(i).getValue().getValue().run() != 0) {
                    Log.report("Pipeline crashed");
                }
                break;
            }
    }

    /**
     * Fill the map of workers
     *
     * @param workerArrayMap -- list of worker's status and corresponding worker's config file
     * @return
     */
    private ArrayList<Pair<Grammar.WORKERS_CFG_FILE_LEXEMES, String>> CreateWorkers(ArrayList<Pair<Grammar.WORKERS_CFG_FILE_LEXEMES, String>> workerArrayMap) {
        if (workerArrayMap == null)
            return null;

        for (int i = 0; i < workerArrayMap.size(); i++) {
            Object sas = null;
            try {
                Class<?> clazz = Class.forName(workerArrayMap.get(i).getValue().split(PipInterpreter.sp)[2]);
                sas = clazz.newInstance();
            }
            catch (ClassNotFoundException e)
            {
                Log.report("ClassNotFoundException");
                return null;
            }
            catch (IllegalAccessException e)
            {
                Log.report("IllegalAccessException");
                return null;
            }
            catch (InstantiationException e)
            {
                Log.report("InstantiationException");
                return null;
            }

            workersList.add(new Pair<>(workerArrayMap.get(i).getKey(), new Pair<>(workerArrayMap.get(i).getValue().split(PipInterpreter.sp)[0], (Executor)sas)));
            if (workersList.get(i).getValue().getValue().setConfig(workerArrayMap.get(i).getValue().split(PipInterpreter.sp)[1]) != 0)
                return null;
        }

        return workerArrayMap;
    }

    /**
     * @return
     */
    private boolean ConnectWorkers(ArrayList<Pair<Grammar.WORKERS_CFG_FILE_LEXEMES, String>> workerArrayData,ArrayList<Pair<String, String>> conn) {
        if(conn == null || workerArrayData == null)
            return false;

        int check = 0;
        for (Pair<Grammar.WORKERS_CFG_FILE_LEXEMES, Pair<String,Executor>> entry : workersList) {

            for (int i = 0; i < conn.size(); i++) {
                {
                    if (entry.getValue().getKey().equals(conn.get(i).getKey())) {
                        Executor cons=null;
                        for (Pair<Grammar.WORKERS_CFG_FILE_LEXEMES, Pair<String,Executor>> entryCons : workersList)
                        {
                            if(entryCons.getValue().getKey().equals(conn.get(i).getValue()))
                                cons = entryCons.getValue().getValue();
                        }
                        if(cons == null && entry.getKey() != Grammar.WORKERS_CFG_FILE_LEXEMES.OUTPUT_WORKER)
                        {
                            Log.report("null cons in ConnectWorkers and it's not an output worker");
                            return  false;
                        }
                        entry.getValue().getValue().setConsumer(cons);
                    }
                }
            }

            if (entry.getKey() == Grammar.WORKERS_CFG_FILE_LEXEMES.INPUT_WORKER) {
                check++;
                try {
                    entry.getValue().getValue().setInput(new DataInputStream(new FileInputStream(configMap.get(Grammar.FILE_LEXEMES.INPUT_FILE))));
                } catch (IOException e) {
                    Log.report("can't set input");
                    return false;
                }
            } else if (entry.getKey() == Grammar.WORKERS_CFG_FILE_LEXEMES.OUTPUT_WORKER) {
                check++;
                try {
                    entry.getValue().getValue().setOutput(new DataOutputStream(new FileOutputStream(configMap.get(Grammar.FILE_LEXEMES.OUTPUT_FILE))));
                } catch (IOException e) {
                    Log.report("can't set output");
                    return false;
                }

            }

        }
        if (check != 2) {
            Log.report("Something wrong with input or output worker items");
            return false;
        }

        return true;
    }
}
