import java.io.DataInputStream;
import java.io.DataOutputStream;

/**
 * Created by Даниил on 17.11.2018.
 */

interface Executor {

    int setInput( DataInputStream input );

    int setOutput( DataOutputStream output );

    int setConsumer( Executor consumer );

    int put( Object obj );

    int run();
} /* End of 'Executor' interface */

/* END OF 'Executor.java' FILE */