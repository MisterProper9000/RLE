import javafx.util.Pair;

import java.io.IOException;
import java.util.Map;
import java.util.ArrayList;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.concurrent.TimeUnit;

/**
 * Created by ������ on 15.09.2018.
 */

/**
 * RLE coder/decoder entity
 */
public class RLEWorkItem implements Executor{

    private byte[] buf;
    private int posShift = 0;
    private int firstPosShift = 0;
    private APPROPRIATE_TYPES[] operatedTypes = new APPROPRIATE_TYPES[2];

    private Map<WorkerInterpreter.WORKER_LEXEMES, String> configMap;

    private Executor provider;

    private Object adapter;
    private APPROPRIATE_TYPES consumedType;
    private Executor consumer;

    private boolean isReadyConsumer = true;
    private boolean isReadyProvider = false;
    private boolean isEnd = false;

    RLEWorkItem(){
        this.operatedTypes[0] = APPROPRIATE_TYPES.BYTE;
        this.operatedTypes[1] = APPROPRIATE_TYPES.CHAR;
    }

    @Override
    public int setConfig(String config)
    {
        WorkerInterpreter workerInterpreter = new WorkerInterpreter();

        try {
            if ((configMap = workerInterpreter.InterpWorker(config)) == null) {
                //Log.report("Worker creation failed");
                return 1;
            }
            else
            {
                int blockSize = Integer.parseInt(configMap.get(WorkerInterpreter.WORKER_LEXEMES.LEN));
                this.posShift = Integer.parseInt(configMap.get(WorkerInterpreter.WORKER_LEXEMES.SHIFT));
                this.firstPosShift = Integer.parseInt(configMap.get(WorkerInterpreter.WORKER_LEXEMES.GLOB_SHIFT));

                if(blockSize > 0 && posShift >= 0 && firstPosShift >= 0) {
                    this.buf = new byte[blockSize];
                    return 0;
                } else {
                    return 1;
                }
            }
        }catch (IOException e)
        {
            //System.out.println("6");
            //Log.report("IOException while creating worker");
            return 1;
        }

    }

    @Override
    public APPROPRIATE_TYPES[] getConsumedTypes() {
        return operatedTypes;
    }

    public int getGlobalOffset() {
        return this.firstPosShift;
    }

    public void setAdapter(Executor provider, Object adapter, APPROPRIATE_TYPES type) {
        this.provider = provider;
        for(APPROPRIATE_TYPES appType: operatedTypes) {
            if(appType == type) {
                this.adapter = adapter;
                this.consumedType = type;
                return;
            }
        }

    }

    @Override
    public void setOutput(DataOutputStream dos) {}

    @Override
    public void setInput(DataInputStream dis) {}

    @Override
    public int setConsumer(Executor consumer) {
        if(consumer == null) {
            return 1;
        }
        else {
            APPROPRIATE_TYPES[] consumersTypes = consumer.getConsumedTypes();

            int zeroOffset = consumer.getIndex();
            int blockSize = consumer.getLength();


            //switch по типам??
            switch (this.getAppropriateType(consumersTypes)) {
                case CHAR:
                    consumer.setAdapter(this, new CharTransferAdapter(zeroOffset, blockSize), APPROPRIATE_TYPES.CHAR);
                    break;
                case BYTE:
                    consumer.setAdapter(this, new ByteTransferAdapter(zeroOffset, blockSize), APPROPRIATE_TYPES.BYTE);
                    break;
                default:
                    return 1;
            }



            this.consumer = consumer;
        }
        return 0;
    }

    private APPROPRIATE_TYPES getAppropriateType(APPROPRIATE_TYPES[] consumersTypes) {
        for(APPROPRIATE_TYPES providedType: operatedTypes) {
            for(APPROPRIATE_TYPES consumedType: consumersTypes) {
                if (providedType == consumedType) {
                    return providedType;
                }
            }
        }
        return null;
    }

    @Override
    public int run() {
        int i = 0;
        while(true)
        {
            if(!report("run usual")) return 0;

            if(this.isReadyProvider && this.isReadyConsumer) {
                synchronized(this) {
                    isReadyConsumer = false;
                    isReadyProvider = false;
                }

                int ind = 0;
                Arrays.fill(buf, (byte) 0);
                switch (consumedType) {
                    case BYTE:
                        InterfaceByteTransfer adapterByte = (InterfaceByteTransfer) adapter;
                        Byte b;
                        while ((b = adapterByte.getNextByte()) != null) {
                            buf[ind++] = b;
                        }
                        break;
                    case CHAR:
                        InterfaceCharTransfer adapterChar = (InterfaceCharTransfer) adapter;
                        Character c;
                        while ((c = adapterChar.getNextChar()) != null) {
                            buf[ind++] = (byte) (char) c;
                        }
                        break;
                    default:
                }

                try{
                    System.out.println("sort rle");
                    buf = sortRLE(buf);
                }
                catch (IOException e)
                {
                    return 1;
                }
                this.provider.setConsumerIsReady(this);
                this.consumer.setProviderIsReady(this);
                if(this.isEnd) {
                    this.consumer.setEnd(this);
                    return 0;
                }
            } else {
                try {
                    TimeUnit.MILLISECONDS.sleep(10);
                } catch (InterruptedException e) {}
            }
        }
    }

    public int getIndex() {
        return posShift;
    }

    public int getLength() {
        return buf.length;
    }

    /**
     * set provider's state to "ready"
     * @param provider
     */
    public synchronized void setProviderIsReady(Executor provider) {
        isReadyProvider = true;
    }

    /**
     * set consumer's state to "ready"
     * @param consumer
     */
    public synchronized void setConsumerIsReady(Executor consumer) {
        isReadyConsumer = true;
    }

    public synchronized void setEnd(Executor provider) {
        this.isEnd = true;
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
                //Log.report("Unexpected end of input file");
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




    private class CharTransferAdapter implements InterfaceCharTransfer {
        private int pos;
        private int startPos;
        private int batchLen;
        private int packageNum = 0;

        public CharTransferAdapter(int shift, int blockSize) {
            this.startPos = shift;
            this.pos = this.startPos;
            this.batchLen = blockSize;
        }

        public Character getNextChar() {
            if(this.pos < buf.length && this.pos < this.startPos + this.batchLen) {
                return (char)buf[this.pos++];
            } else {
                this.pos = this.startPos;
                ++this.packageNum;
                return null;
            }
        }

        public int getNumber() {
            return this.packageNum;
        }

        public int getIndex() {
            return this.startPos;
        }
    }
    int i = 0;//for debug


    public boolean report(String s) //instead of static logger we simply put this method here
    {
        i++;
        System.out.println(s);
        if(i>40) return false; else return true;
    }

    private class ByteTransferAdapter implements InterfaceByteTransfer {
        private int pos;
        private int startPos;
        private int batchLen;
        private int packageNum = 0;

        public ByteTransferAdapter(int shift, int blockSize) {
            this.startPos = shift;
            this.pos = this.startPos;
            this.batchLen = blockSize;
        }

        public Byte getNextByte() {
            if(this.pos < buf.length && this.pos < this.startPos + this.batchLen) {
                return buf[this.pos++];
            } else {
                this.pos = this.startPos;
                ++this.packageNum;
                return null;
            }
        }

        public int getNumber() {
            return this.packageNum;
        }

        public int getIndex() {
            return this.startPos;
        }

    }
}