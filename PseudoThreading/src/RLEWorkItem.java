import javafx.util.Pair;

import java.io.*;
import java.io.IOException;
import java.util.*;

/**
 * Created by ������ on 15.09.2018.
 */

/**
 * RLE coder/decoder entity
 */
public class RLEWorkItem implements Executor {

    ArrayList<APPROPRIATE_TYPES> types;

    public class AdapterByte implements InterfaceByteTransfer {
        private int startIndex;
        public void setStartIndex(int index) {
            startIndex = index;
        }
        public Byte getNextByte()
        {
            try {
                byte next = dataBlock[startIndex];
                startIndex++;
                return next;
            } catch(Exception e) {
                return 0;
            }
        }
    }

    public class AdapterChar implements InterfaceCharTransfer{
        private int startIndex;
        public void setStartIndex(int index) {
            startIndex = index;
        }

        public Character getNextChar()
        {
            try {
                char next = (char)dataBlock[startIndex];
                startIndex++;
                return next;
            } catch(Exception e) {
                return 0;
            }
        }
    }

    public class AdapterDouble  implements InterfaceDoubleTransfer{
        private int startIndex;
        public void setStartIndex(int index) {
            startIndex = index;
        }
        public Double getNextDouble()
        {
            try {
                double next = (double)dataBlock[startIndex];
                startIndex++;
                return next;
            } catch(Exception e) {
                return 0d;
            }
        }
    }

    Map<Executor, Object> connections;



    byte[] dataBlock;

    //private FileInputStream fin;
    //private BufferedInputStream in;
    private DataInputStream in;

    //private FileOutputStream fout;
    //private BufferedOutputStream out;
    private DataOutputStream out;

    private ArrayList<Executor> consumers;
    private Map<Executor, Pair<APPROPRIATE_TYPES,Object>> adapterMap;


    private Map<WorkerInterpreter.WORKER_LEXEMES, String> configMap;

    //private EnumMap<PipInterpreter.LEXEMES, String> workerMap = new EnumMap<>(PipInterpreter.LEXEMES.class);

    public  Pair<Integer, Integer> getBlockMetrics()
    {
        return new Pair<>(Integer.parseInt(configMap.get(WorkerInterpreter.WORKER_LEXEMES.LEN)),Integer.parseInt(configMap.get(WorkerInterpreter.WORKER_LEXEMES.SHIFT)));
    }

    public int setConfig(String config)
    {
        WorkerInterpreter workerInterpreter = new WorkerInterpreter();
        configMap = new HashMap<>();
        try {
            if ((configMap = workerInterpreter.InterpWorker(config)) == null) {
                Log.report("Worker creation failed");
                return -1;
            }
        }catch (IOException e)
        {
            Log.report("IOException while creating worker");
            return -1;
        }

        return 0;
    }

    @Override
    public int setConsumer(Executor consumer) {
        if(consumers == null)
            consumers = new ArrayList<>();
        consumers.add(consumer);
        //switch по типам??
        switch (consumer.getConsumedTypes()[0])
        {
            case CHAR:
                consumer.setAdapter(this, new AdapterChar(),APPROPRIATE_TYPES.CHAR);
                break;
            case BYTE:
                consumer.setAdapter(this, new AdapterByte(),APPROPRIATE_TYPES.BYTE);
                break;
            case DOUBLE:
                consumer.setAdapter(this, new AdapterDouble(),APPROPRIATE_TYPES.DOUBLE);
                break;
        }

        return 0;
    }

    @Override
    public void setAdapter(Executor executor, Object o, APPROPRIATE_TYPES appropriate_types) {
        if(adapterMap == null)
            adapterMap = new HashMap<>();
        adapterMap.put(executor, new Pair<>(appropriate_types, o));
    }

    @Override
    public APPROPRIATE_TYPES[] getConsumedTypes() {
        APPROPRIATE_TYPES a[] = new APPROPRIATE_TYPES[1];
        a[0] = APPROPRIATE_TYPES.valueOf(configMap.get(WorkerInterpreter.WORKER_LEXEMES.TYPE));
        return a;
    }

    @Override
    public void setOutput(DataOutputStream dos) {
        out = dos;
    }

    @Override
    public void setInput(DataInputStream dis) {
        in = dis;
    }

    @Override
    public int run() {
        if(in != null) {
            byte[] block;
            try {
                block = Read();
            } catch (IOException e) {
                Log.report("Cannot read input file");
                return -1;
            }
            if(block == null) return -1;
            dataBlock = block;
        }
        if(out == null && in == null)
            try {
                dataBlock = sortRLE(dataBlock);
            }
            catch (IOException e)
            {
                Log.report("sasaj");
                return -1;
            }
        if(out != null) {
            Write(dataBlock);
        }
        else {
            ArrayList<Pair<Integer,Integer>> lenAndShift = new ArrayList<>();
            int sumLen = 0;
            for(Executor consumer: consumers)
            {
                lenAndShift.add(consumer.getBlockMetrics());
                sumLen += consumer.getBlockMetrics().getKey();
            }
            if(sumLen != Integer.parseInt(configMap.get(WorkerInterpreter.WORKER_LEXEMES.LEN)) && consumers.size() != 1)
            {
                Log.report("Wrong block metrics");
                return -1;
            }

            Collections.sort(lenAndShift, new Compare());
            for(int i = 0; i < lenAndShift.size()-1; i++)
            {
                if(lenAndShift.get(i).getValue() + lenAndShift.get(i).getKey() != lenAndShift.get(i+1).getValue() && consumers.size() != 1)
                {
                    Log.report("interects nah");
                    return -1;
                }
            }

            for(Executor consumer: consumers)
            {
                consumer.put(this);
                if(consumer.run() == -1) return -1;
            }
        }
        return 0;
    }

    private class Compare implements  Comparator<Pair<Integer, Integer>>{
        @Override
        public int compare(Pair<Integer, Integer> p1, Pair<Integer, Integer> p2)
        {
            if(p1.getValue() < p2.getValue())
                return -1;
            if(p1.getValue() > p2.getValue())
                return 1;

            return 0;

        }
    }


    private byte[] Read() throws IOException {
        int i = 0;
        byte[] block = new byte[Integer.parseInt(configMap.get(WorkerInterpreter.WORKER_LEXEMES.LEN))];
        int symbol = in.read();
        while(symbol != -1) {
            block[i] = (byte) symbol;
            i++;
            if(i == Integer.parseInt(configMap.get(WorkerInterpreter.WORKER_LEXEMES.LEN))) break;
            symbol = in.read();
        }
        if(symbol == -1 && block[0] == 0) return null;
        return block;
    }

    private void Write(byte[] str) {
        try {
            for (int i = 0; i < str.length; i++) {
                if (i % 2 == 0 && str[i] == 0)
                    i++;
                else if(i<str.length)
                    out.write(str[i]);
            }
            out.flush();
        } catch (IOException e) {
            Log.report("Can't write to output file");
        }
    }

    @Override
    public int put(Executor provider) {
        try {

            Pair<APPROPRIATE_TYPES, Object> pair = adapterMap.get(provider);
            switch (pair.getKey())
            {
                case BYTE:
                    InterfaceByteTransfer adapterByte = (InterfaceByteTransfer) pair.getValue();
                    dataBlock = new byte[Integer.parseInt(configMap.get(WorkerInterpreter.WORKER_LEXEMES.LEN))];
                    adapterByte.setStartIndex(Integer.parseInt(configMap.get(WorkerInterpreter.WORKER_LEXEMES.SHIFT)));
                    for(int i = 0; i< Integer.parseInt(configMap.get(WorkerInterpreter.WORKER_LEXEMES.LEN)); i++) {
                        dataBlock[i] = (byte)adapterByte.getNextByte();
                    }
                    break;
                case CHAR:
                    InterfaceCharTransfer adapterChar = (InterfaceCharTransfer) pair.getValue();
                    dataBlock = new byte[Integer.parseInt(configMap.get(WorkerInterpreter.WORKER_LEXEMES.LEN))];
                    adapterChar.setStartIndex(Integer.parseInt(configMap.get(WorkerInterpreter.WORKER_LEXEMES.SHIFT)));
                    for(int i = 0; i< Integer.parseInt(configMap.get(WorkerInterpreter.WORKER_LEXEMES.LEN)); i++) {
                        dataBlock[i] = (byte)adapterChar.getNextChar().charValue();
                    }
                    break;
                case DOUBLE:
                    InterfaceDoubleTransfer adapterDouble = (InterfaceDoubleTransfer) pair.getValue();
                    dataBlock = new byte[Integer.parseInt(configMap.get(WorkerInterpreter.WORKER_LEXEMES.LEN))];
                    adapterDouble.setStartIndex(Integer.parseInt(configMap.get(WorkerInterpreter.WORKER_LEXEMES.SHIFT)));
                    for(int i = 0; i< Integer.parseInt(configMap.get(WorkerInterpreter.WORKER_LEXEMES.LEN)); i++) {
                        dataBlock[i] = (byte)adapterDouble.getNextDouble().doubleValue();
                    }
                    break;
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
        if (Integer.parseInt(configMap.get(WorkerInterpreter.WORKER_LEXEMES.CODE_MODE)) == 0)
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