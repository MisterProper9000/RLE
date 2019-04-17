import java.awt.image.ImagingOpException;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by Даниил on 25.01.2019.
 */
public class InputReader implements Executor{
    private byte[] buf; //data storage

    private DataInputStream  input; //data stream for reading


    private APPROPRIATE_TYPES[] operatedTypes; //which types are acceptable
    private ArrayList<Executor> consumers; //who wil continue work

    private HashMap<Executor, Boolean> isReadyConsumer; //here stored consumer and his status (ready/not ready for work)

    /**
    * Creates object
    */
    public InputReader() {
        consumers = new ArrayList<>();
        isReadyConsumer = new HashMap<>();
        operatedTypes = new APPROPRIATE_TYPES[2];
        operatedTypes[0] = APPROPRIATE_TYPES.BYTE;
        operatedTypes[1] = APPROPRIATE_TYPES.CHAR;
    }

    /// BEGIN Executor interface
    public int setConfig(String config) {
        WorkerInterpreter workerInterpreter = new WorkerInterpreter();
        try {
            int len = Integer.parseInt(workerInterpreter.InterpWorker(config).get(WorkerInterpreter.WORKER_LEXEMES.LEN));
            buf = new byte[len];
        }
        catch (IOException e)
        {
            return 1;
        }
        return 0;
    }

    /**
     * set data stream for reading
     * @param in
     */
    public void setInput(DataInputStream in) { input = in; }

    /**
     * only OutputWriter has non-empty implementation of this method
     * @param output
     */
    public void setOutput(DataOutputStream output) {}

    /**
     * linking this to the consumer
     * @param consumer
     * @return
     */
    public int setConsumer(Executor consumer) {
        if(consumer == null) {
            return 1;
        }
        if(setConnection(consumer) == 0) {
            consumers.add(consumer);
            return 0;
        }
        return 1;
    }

    /**
     *
     * @return shift in the GLOBAL input of the first worker in branch (for RLEWorkItem
     */
    public int getGlobalOffset() { return 1; }

    /**
     * For correct data flow between different workers with different acceptable types
     * @return array of acceptable types for this particular worker
     */
    public APPROPRIATE_TYPES[] getConsumedTypes() {  return operatedTypes;  }

    /**
     *
     * @param provider who is provider for this
     * @param adapter who will deal with getting data from provider
     * @param type what type of data will be faced
     */
    public void setAdapter(Executor provider, Object adapter, APPROPRIATE_TYPES type){}

    /**
     * read chunks of data from input stream and distribute them to the consumers
     * @return
     */
    public int run() {
        updateAllConsumersReadyState(true);
        int readBytesCounter;

        while (true) {
            if(!report("run Reader")) return 0;
            if(areAllConsumersReady()) {
                readBytesCounter = readBytes();
                if (readBytesCounter < buf.length) {
                    for (Executor consumer : consumers) {
                        consumer.setProviderIsReady(this);
                    }

                    updateAllConsumersReadyState(false);

                    for (Executor consumer : consumers)
                        consumer.setEnd(this);
                    return 0;
                }

                for (Executor consumer : consumers) {
                    consumer.setProviderIsReady(this);
                }

                updateAllConsumersReadyState(false);
            }
            else
                try {
                    TimeUnit.MILLISECONDS.sleep(10);
                }
                catch(InterruptedException e) {}
        }
    }

    /**
     *
     * @return shift in LOCAL input data chunk
     */
    public int getIndex() { return 0; }

    /**
     *
     * @return return length of data chunk that stored in this worker
     */
    public int getLength() { return buf.length; }

    /**
     *
     * @param provider set consumer's field "isProviderReady" to true
     */
    public synchronized void setProviderIsReady(Executor provider) { }

    /**
     *
     * @param consumer set particular consumer to state "ready"
     */
    public synchronized void setConsumerIsReady(Executor consumer) {
        isReadyConsumer.put(consumer, true);
    }

    /**
     *
     * @param provider set flag "isEnd"
     */
    public synchronized void setEnd(Executor provider) { }
    /// END Executor interface

    /// BEGIN ByteTransferAdapter
    class ByteTransferAdapter implements InterfaceByteTransfer {

        /**
         * Creates object
         * @param shift x consumers's shift - the number of the first symbol to send
         * @param blockSize x consumer's block size
         */
        public ByteTransferAdapter(int shift, int blockSize) {
            startPos = shift;
            pos = startPos;
            batchLen = blockSize;
        }

        public Byte getNextByte() {
            if (pos >= buf.length || pos >= startPos + batchLen) {
                pos = startPos;
                packageNum++;
                return null;
            }
            else {
                return buf[pos++];
            }
        }

        public int getNumber() {
            return packageNum;
        }

        public int getIndex() {
            return startPos;
        }

        /* private fields*/
        private int pos;
        private int startPos;
        private int batchLen;
        private int packageNum = 0;
    }
    /// END ByteTransferAdapter

    /// BEGIN CharTransferAdapter
    class CharTransferAdapter implements InterfaceCharTransfer {

        /**
         * Creates object
         * @param shift x consumers's shift - the number of the first symbol to send
         * @param blockSize x consumer's block size
         */
        public CharTransferAdapter(int shift, int blockSize) {
            startPos = shift;
            pos = startPos;
            batchLen = blockSize;
        }

        public Character getNextChar() {
            if (pos >= buf.length || pos >= startPos + batchLen) {
                pos = startPos;
                packageNum++;
                return null;
            }
            else {
                return (char)buf[pos++];
            }
        }

        public int getNumber() {
            return packageNum;
        }

        public int getIndex() {
            return startPos;
        }

        /* private fields*/
        private int pos;
        private int startPos;
        private int batchLen;
        private int packageNum = 0;
    }
    /// END CharTransferAdapter

    /// BEGIN inner methods


    /**
     * Updates state of all consumers
     * @param state x state to set
     */
    private synchronized void updateAllConsumersReadyState(boolean state) {
        for(Executor consumer: consumers)
            isReadyConsumer.put(consumer, state);
    }

    /**
     * Check if all consumers are ready
     * @return x boolean true if all consumers are ready, false otherwise
     */
    private boolean areAllConsumersReady() {
        for(Boolean isReady: isReadyConsumer.values()) {
            if(isReady == false)
                return false;
        }
        return true;
    }

    /**
     * Search for compatible types
     * @param consumersTypes x APPROPRIATE_TYPES array of consumer's types
     * @return APPROPRIATE_TYPES x compatible type
     */
    private APPROPRIATE_TYPES getAppropriateType(APPROPRIATE_TYPES consumersTypes[]) {
        for(APPROPRIATE_TYPES providedType: operatedTypes) {
            for(APPROPRIATE_TYPES consumedType: consumersTypes) {
                if (providedType == consumedType) {
                    return providedType;
                }
            }
        }
        return null;
    }

    /**
     * Sets connection between consumer and provider
     * @param consumer x the consumer
     * @return 0 if success, otherwise 1
     */
    private int setConnection(Executor consumer) {
        APPROPRIATE_TYPES consumersTypes[] = consumer.getConsumedTypes();
        int zeroOffset = consumer.getIndex();
        int blockSize = consumer.getLength();

        switch(getAppropriateType(consumersTypes)) {
            case BYTE:
                consumer.setAdapter(this, new ByteTransferAdapter(zeroOffset, blockSize), APPROPRIATE_TYPES.BYTE);
                return 0;
            case CHAR:
                consumer.setAdapter(this, new CharTransferAdapter(zeroOffset, blockSize), APPROPRIATE_TYPES.CHAR);
                return 0;
            default:
                return 1;
        }
    }

    public boolean report(String s) //instead of static logger we simply put this method here
    {
        System.out.println(s);
        return true;
    }

    /**
     * @return x number of bytes which were read
     */
    private int readBytes() {
        try {
            Arrays.fill(buf, (byte) 0);
            int cnt = input.read(buf);
            return cnt;
        } catch (Exception e) {
            return -1;
        }
    }
}
