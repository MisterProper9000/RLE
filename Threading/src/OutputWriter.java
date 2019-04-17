import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by Даниил on 25.01.2019.
 */
public class OutputWriter implements Executor {
    private DataOutputStream output;

    private APPROPRIATE_TYPES[] operatedTypes;                          /* data types writer deals with */

    private HashMap<Executor, Object> adaptersMap;                      /* map from provider and its adapter */
    private HashMap<Executor, APPROPRIATE_TYPES> adaptersTypesMap;      /* map from provider and its appropriate types */

    private HashMap<Executor, Integer> providersShiftMap;               /* map from provider and its zero offset */
    private TreeMap<Integer, List<byte[]>> result;                      /* tree map from provider and its work result */

    private HashMap<Executor, Boolean> isReadyProviderMap;
    private HashMap<Executor, Boolean> isEndWorkProviderMap;

    /**
     * Creates object
     */
    public OutputWriter() {
        /* init fields */
        adaptersMap = new HashMap<>();
        adaptersTypesMap = new HashMap<>();
        providersShiftMap = new HashMap<>();
        result = new TreeMap<>();

        isReadyProviderMap = new HashMap<>();
        isEndWorkProviderMap = new HashMap<>();

        /* set data types writer deals with */
        operatedTypes = new APPROPRIATE_TYPES[3];
        operatedTypes[0] = APPROPRIATE_TYPES.BYTE;
        operatedTypes[1] = APPROPRIATE_TYPES.CHAR;
        operatedTypes[2] = APPROPRIATE_TYPES.DOUBLE;
    }

    /// BEGIN Executor interface
    public int setConfig(String config) {
        return 0;
    }

    public void setInput(DataInputStream input) { }

    public void setOutput(DataOutputStream out) { output = out; }

    public int setConsumer(Executor consumer) { return 1; }

    public int getGlobalOffset() { return 1; }

    public APPROPRIATE_TYPES[] getConsumedTypes() { return operatedTypes; }

    public void setAdapter(Executor provider, Object adapter, APPROPRIATE_TYPES type) {
        for(APPROPRIATE_TYPES appType: operatedTypes) {
            if(appType == type) {
                adaptersMap.put(provider, adapter);
                adaptersTypesMap.put(provider, type);
                providersShiftMap.put(provider, provider.getGlobalOffset());
                return;
            }
        }
    }

    public int run() {
        int i = 0;

        initResultContainer();
        setAllProvidersInitialState();

        while(true) {
            if(!report("run Writer")) return 0;

            for(Executor provider: adaptersMap.keySet()) {
                if (isReadyProvider(provider)) {
                    byte buf[] = new byte[provider.getLength()];
                    int ind = 0;
                    switch (getSetDataType(provider)) {
                        case BYTE:
                            InterfaceByteTransfer adapterByte = (InterfaceByteTransfer) getSetAdapter(provider);
                            Byte b;
                            while ((b = adapterByte.getNextByte()) != null) {
                                buf[ind++] = b;
                            }
                            break;
                        case CHAR:
                            InterfaceCharTransfer adapterChar = (InterfaceCharTransfer) getSetAdapter(provider);
                            Character c;
                            while ((c = adapterChar.getNextChar()) != null) {
                                buf[ind++] = (byte) (char) c;
                            }
                            break;
                        case DOUBLE:
                            InterfaceDoubleTransfer adapterDouble = (InterfaceDoubleTransfer) getSetAdapter(provider);
                            Double d;
                            while ((d = adapterDouble.getNextDouble()) != null) {
                                buf[ind++] = (byte) (double) d;
                            }
                            break;
                        default:
                    }
                    addProviderWorkResult(provider, buf);
                    provider.setConsumerIsReady(this);
                    updateProviderReadyState(provider, false);

                } else {
                    try {
                        TimeUnit.MILLISECONDS.sleep(10);
                    } catch (InterruptedException e) {
                        return 1;
                    }
                }
                if(isBatchToWrite()) {
                    System.out.println("Write result");
                    if (writeBatchToFile() != 0) {
                        return 1;
                    }
                }
                System.out.println(haveAllProvidersFinishedWork() + " " +  isEmptyResult() + " " + areAllProvidersNotReady());

                if(haveAllProvidersFinishedWork() && isEmptyResult() && areAllProvidersNotReady()) {
                    System.out.print("END");
                    return 0;
                }
            }
        }
    }

    public int getIndex() { return 0; }

    public int getLength() { return Integer.MAX_VALUE; }

    /**
     * set the given provider's state to "ready"
     * @param provider
     */
    public synchronized void setProviderIsReady(Executor provider) { isReadyProviderMap.put(provider, true); }

    /**
     * OutputWorker has no consumers...
     * @param consumer
     */
    public void setConsumerIsReady(Executor consumer) {   }

    public synchronized void setEnd(Executor provider) { isEndWorkProviderMap.put(provider, true); }
    /// END Executor interface

    /// BEGIN inner functions

    /**
     * Inits container to keep the result
     */
    private void initResultContainer() {
        for(Integer key: providersShiftMap.values()) {
            result.put(key, new ArrayList<>());
        }
    }

    /**
     * Adds the result of provider's work
     * @param provider x provider which holds the result to save
     * @param buf x the result of provider's work
     */
    private void addProviderWorkResult(Executor provider, byte[] buf) {
        int key = providersShiftMap.get(provider);
        result.get(key).add(buf);
    }

    /**
     * Checks if there is a full data set which can be written right now
     * @return boolean x true if there is, false otherwise
     */
    private boolean isBatchToWrite() {
        for(List<byte[]> list: result.values()) {
            if(list.isEmpty())
                return false;
        }
        return true;
    }

    /**
     * Checks if the result is empty now
     * @return boolean x true if it is, false otherwise
     */
    private boolean isEmptyResult() {
        for(List<byte[]> list: result.values()) {
            if(!list.isEmpty())
                return false;
        }
        return true;
    }


    /**
     * Writes batch of data to file
     */
    private int writeBatchToFile() {
        for(List<byte[]> list: result.values()) {
            byte[] buf = list.get(0);
            if(writeBytes(buf) != 0)
                return 1;
            list.remove(0);
        }
        return 0;
    }

    /**
     * Updates state of all providersShiftMap
     * @param state x state to set
     */
    private synchronized void updateAllProvidersReadyState(boolean state) {
        for(Executor provider: providersShiftMap.keySet())
            isReadyProviderMap.put(provider, state);
    }

    /**
     * Updates current provider's state
     * @param provider x provider to update
     * @param state x the state to set
     */
    private synchronized void updateProviderReadyState(Executor provider, boolean state) {
        isReadyProviderMap.put(provider, state);
    }

    /**
     * Check if all providersShiftMap are ready
     * @return x boolean true if all providersShiftMap are ready, false otherwise
     */
    private synchronized boolean areAllProvidersReady() {
        for(Boolean isReady: isReadyProviderMap.values()) {
            if(!isReady)
                return false;
        }
        return true;
    }

    /**
     * Check if all providersShiftMap are not ready
     * @return x boolean true if all providersShiftMap are not ready, false otherwise
     */
    private synchronized boolean areAllProvidersNotReady() {
        for(Boolean isReady: isReadyProviderMap.values()) {
            if(isReady)
                return false;
        }
        return true;
    }

    /**
     * Updates state of all providersShiftMap
     * @param state x state to set
     */
    private synchronized void updateAllProvidersEndState(boolean state) {
        for(Executor provider: providersShiftMap.keySet())
            isReadyProviderMap.put(provider, state);
    }

    /**
     * Sets all providers initial state (not ready to sync with)
     */
    private synchronized void setAllProvidersInitialState() {
        for(Executor provider: providersShiftMap.keySet()) {
            isReadyProviderMap.put(provider, false);
            isEndWorkProviderMap.put(provider, false);
        }
    }

    /**
     * Checks if the provider is ready to provide the data
     * @param provider x data provider
     * @return boolean x true if it is ready, false otherwise
     */
    private synchronized boolean isReadyProvider(Executor provider) {
        return isReadyProviderMap.get(provider);
    }

    int i = 0;//for debug

    /**
     * Gets set data type to take
     * @param provider x provider to take from
     * @return APPROPRIATE_TYPES x data type
     */
    private APPROPRIATE_TYPES getSetDataType(Executor provider) {
        return adaptersTypesMap.get(provider);
    }

    /**
     * Gets set adapter
     * @param provider x provider to take from
     * @return Object x set adapter
     */
    private Object getSetAdapter(Executor provider) {
        return adaptersMap.get(provider);
    }

    /**
     * Check if all providersShiftMap finished their work
     * @return x boolean true if all providersShiftMap finished their work, false otherwise
     */
    private boolean haveAllProvidersFinishedWork() {
        for(Boolean isReady: isEndWorkProviderMap.values()) {
            if(!isReady)
                return false;
        }
        return true;
    }

    public boolean report(String s) //instead of static logger we simply put this method here
    {
        i++;
        System.out.println(s);
        if(i>40) return false; else return true;
    }

    /**
     * Writes bytes in the output stream
     * @param buf x buffer to write from
     * @return int x 0 if the data was written successfully, 1 otherwise
     */
    private int writeBytes(byte[] buf) {
        try {
            output.write(buf, 0, buf.length);
            return 0;
        } catch (Exception e) {
            return 1;
        }
    }
}
