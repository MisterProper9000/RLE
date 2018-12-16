import javafx.util.Pair;

import java.io.*;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.FileInputStream;
import java.io.BufferedInputStream;
import java.io.FileOutputStream;
import java.io.BufferedOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by ������ on 15.09.2018.
 */

/**
 * RLE coder/decoder entity
 */
public class RLEWorkItem implements Executor {

    public static final Map<String, Grammar.WORKER_LEXEMES> grammarMap;
    static {
        grammarMap = new HashMap<>();
        grammarMap.put("CODE_MODE", Grammar.WORKER_LEXEMES.CODE_MODE);
    }

    byte[] dataBlock;

    //private FileInputStream fin;
    //private BufferedInputStream in;
    private DataInputStream in;

    //private FileOutputStream fout;
    //private BufferedOutputStream out;
    private DataOutputStream out;

    private Executor consumer;

    private boolean isEnd;

    private int codeMode;
    private int chunkSize;

    //private EnumMap<PipInterpreter.LEXEMES, String> workerMap = new EnumMap<>(PipInterpreter.LEXEMES.class);

    /**
     *
     * @param config -- name of worker's config file
     * @param cSize -- data chunk size
     * @throws IOException
     */
    public RLEWorkItem(String config, int cSize) throws IOException{
        WorkerInterpreter workerInterpreter = new WorkerInterpreter();
        Map<Grammar.WORKER_LEXEMES, String> map = new HashMap<>();
        try {
            if ((map = workerInterpreter.InterpWorker(config)) == null) {
                Log.report("Worker creation failed");
                System.exit(1);
            }
        }catch (IOException e)
        {
            Log.report("IOException while creating worker");
            System.exit(1);
        }
        codeMode = Integer.parseInt(map.get(Grammar.WORKER_LEXEMES.CODE_MODE));
        chunkSize = cSize;
        isEnd = false;
    }

    @Override
    public int setConsumer(Executor ex) {
        if(consumer == null)
            consumer = ex;
        return 0;
    }

    @Override
    public int setOutput(DataOutputStream dos) {
        out = dos;
        return 0;
    }

    @Override
    public int setInput(DataInputStream dis) {
        in = dis;
        return 0;
    }

    @Override
    public int run() {
        byte[] result;
        byte tChunk[];
        byte chunk[];
        if(in != null) {
            tChunk = new byte[chunkSize];

            try {
                in.read(tChunk, 0, chunkSize);
            }
            catch (IOException e )
            {
                Log.report("Can't read from input file");
                return -1;
            }
            int cut = 0;
            for(int i = 0; i < tChunk.length; i++)
            {
                if(tChunk[i] == 0) {
                    cut = i;
                    isEnd = true;
                    break;
                }
            }
            if(cut + 1 == tChunk.length)
                chunk = tChunk;
            else {
                chunk = new byte[cut];
                for (int i = 0; i < cut; i++)
                    chunk[i] = tChunk[i];
            }
        }
        else
        {
            chunk = dataBlock;
        }

        try {
            result = sortRLE(chunk);
        }
        catch (IOException e) {
            Log.report("error in sortRLE");
            return -1;
        }

        if(result == null)
        {
            Log.report("null result");
            return -1;
        }

        if(out != null) {
            try {
                out.write(result);
                out.flush();
                if(isEnd)
                    out.close();
            }
            catch (IOException e)
            {
                Log.report("Can't write to output file");
                return -1;
            }
        }
        else if(consumer != null){
            ArrayList<Byte> Result = new ArrayList<>();
            for(int i = 0; i < result.length; i++)
            {
                Result.add(result[i]);
            }
            Pair<ArrayList<Byte>,Boolean> RESILT = new Pair<>(Result,isEnd);
            consumer.put(RESILT);
            return consumer.run();
        }
        else
        {
            Log.report("No consumer or output file");
            return -1;
        }

        return 0;
    }

    @Override
    public int put(Object input) {
        try {

            Pair r = (Pair)input;
            ArrayList b = (ArrayList)r.getKey();
            isEnd = (Boolean) r.getValue();
            dataBlock = new byte[b.size()];
            for(int i = 0; i < b.size(); i++)
            {
                dataBlock[i] = (byte)b.get(i);
            }
        }
        catch (ClassCastException ex) {
            Log.report("Can't cast object to data\n");
            return -1;
        }
        return 0;
    }

    /**
     *
     * @param chunk -- byte array to be processed
     * @return processed byte array
     * @throws IOException
     */
    private byte[] sortRLE(byte[] chunk) throws  IOException
    {
        if (codeMode == 0)
            return toRLE(chunk);
        else
            return fromRLE(chunk);
    }

    /**
     *
     * @param chunk -- byte array to be encoded
     * @return encoded byte array
     * @throws IOException
     */
    private byte[] toRLE(byte[] chunk) throws IOException
    {
        final int maxByte = 255;
        ArrayList<Byte> result = new ArrayList<>();

        for(int i = 0; i < chunk.length; i++)
        {
            int pb = chunk[i];
            int j = 1;
            while(j < maxByte && i < chunk.length-1)
            {
                if(chunk[i+1] == pb) {
                    ++j;
                    i++;
                }
                else
                    break;
            }
            result.add(new Byte((byte)j));
            result.add(new Byte((byte)pb));
        }
        byte[] res = new byte[result.size()];
        for(int i =0; i < result.size(); i++)
        {
            res[i] = result.get(i);
        }
        return res;

    }

    /**
     *
     * @param chunk -- byte array to be decoded
     * @return -- decoded byte array
     * @throws IOException
     */
    private byte[] fromRLE(byte[] chunk) throws IOException
    {
        ArrayList<Byte> result = new ArrayList<>();

        for(int i = 0; i < chunk.length; i++)
        {
            int count;

            count = chunk[i];
            if(count < 0)
                break;

            if(chunk[i] < 0){
                Log.report("Unexpected end of input file");
                return null;
            }

            for(int j = 0; j < count; j++)
            {
                result.add(chunk[i+1]);
            }
            i++;
        }
        byte[] res = new byte[result.size()];
        for(int i = 0; i < result.size(); i++)
        {
            res[i] = result.get(i);
        }
        return  res;
    }
}