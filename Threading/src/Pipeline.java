import javafx.util.Pair;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by ������ on 18.11.2018.
 */

/**
 * Pipeline manager class
 */
public class Pipeline {

    private DataInputStream inputStream;
    private DataOutputStream outputStream;

    private ExecutorService[] exServices;     /* array of ExecutorService from interface to work with streams */

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
        if(openStreams() == -1)
            System.exit(1);

        //Set queue of workers
        if (!ConnectWorkers(CreateWorkers(interp.InterpWorkers(configMap.get(Grammar.FILE_LEXEMES.WORKERS_FILE))),interp.SetPipSeq(configMap.get(Grammar.FILE_LEXEMES.PIPSEQ)))) {

            System.exit(1);
        }
    }

    private int openStreams() {
        try {
            FileInputStream inStream = new FileInputStream(configMap.get(Grammar.FILE_LEXEMES.INPUT_FILE));
            this.inputStream = new DataInputStream(inStream);
            FileOutputStream outStream = new FileOutputStream(configMap.get(Grammar.FILE_LEXEMES.OUTPUT_FILE));
            this.outputStream = new DataOutputStream(outStream);
        } catch (NullPointerException e) {
            //Log.report("Exception thrown during stream open process.\n");
            return -1;
        } catch (IOException e) {
            //Log.report("Exception thrown during stream open process.\n");
            return -1;
        }
        return 0;
    }

    /**
     * Start pipeline
     */
    public void Start() {
        try {
            exServices = new ExecutorService[workersList.size()];
            System.out.println(workersList.size());
            for (int i = 0; i < workersList.size(); i++) {
                System.out.println(i);
                exServices[i] = Executors.newSingleThreadExecutor();
                int finalI = i;
                exServices[i].submit(() ->
                {
                    int a;
                    if ((a = workersList.get(finalI).getValue().getValue().run()) != 0) {
                        System.out.print("shut down " + a);
                        //Log.sendMessage("the pipeline work was incorrect");
                        ShutDownNowAllExecutors();
                    }
                    if (finalI == workersList.size()-1) {
                        System.out.print("shut down " + a);
                        ShutDownNowAllExecutors();
                    }
                    System.out.println(a);
                });
            }
            System.out.println("I'm here");
        }
        catch (Exception ex) {
            //Log.sendMessage("the pipeline work was incorrect");
        }
    }

    /**
     * Shut down all executors
     */
    private void ShutDownNowAllExecutors()
    {
        System.out.println("kill");
        for (ExecutorService ex : exServices)
            ex.shutdownNow();
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
                //Log.report("ClassNotFoundException");
                return null;
            }
            catch (IllegalAccessException e)
            {
                //Log.report("IllegalAccessException");
                return null;
            }
            catch (InstantiationException e)
            {
                //Log.report("InstantiationException");
                return null;
            }

            workersList.add(new Pair<>(workerArrayMap.get(i).getKey(), new Pair<>(workerArrayMap.get(i).getValue().split(PipInterpreter.sp)[0], (Executor)sas)));
            workersList.get(i).getValue().getValue().setConfig(workerArrayMap.get(i).getValue().split(PipInterpreter.sp)[1]);

        }

        return workerArrayMap;
    }

    /**
     * @return
     */
    private boolean ConnectWorkers(ArrayList<Pair<Grammar.WORKERS_CFG_FILE_LEXEMES, String>> workerArrayData,ArrayList<Pair<String, String>> conn) {
        if (conn == null || workerArrayData == null)
            return false;

        int check = 0;
        for (Pair<Grammar.WORKERS_CFG_FILE_LEXEMES, Pair<String, Executor>> entry : workersList) {

            for (int i = 0; i < conn.size(); i++) {
                {
                    if (entry.getValue().getKey().equals(conn.get(i).getKey())) {
                        Executor cons = null;
                        for (Pair<Grammar.WORKERS_CFG_FILE_LEXEMES, Pair<String, Executor>> entryCons : workersList) {
                            if (entryCons.getValue().getKey().equals(conn.get(i).getValue()))
                                cons = entryCons.getValue().getValue();
                        }
                        if (cons == null && entry.getKey() != Grammar.WORKERS_CFG_FILE_LEXEMES.OUTPUT_WORKER) {
                            //Log.report("null cons in ConnectWorkers and it's not an output worker");
                            return false;
                        }

                        entry.getValue().getValue().setConsumer(cons);

                    }
                }
            }

            if (entry.getKey() == Grammar.WORKERS_CFG_FILE_LEXEMES.INPUT_WORKER) {
                check++;
                entry.getValue().getValue().setInput(inputStream);
            } else if (entry.getKey() == Grammar.WORKERS_CFG_FILE_LEXEMES.OUTPUT_WORKER) {
                check++;
                entry.getValue().getValue().setOutput(outputStream);
            }

        }
        if (check != 2) {
            //Log.report("Something wrong with input or output worker items");
            return false;
        }

        return true;
    }
}
