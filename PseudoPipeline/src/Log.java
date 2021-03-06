import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by Äàíèèë on 27.09.2018.
 */
public class Log {

    private static FileWriter logWriter;

    public static boolean init()
    {
        try {
            logWriter = new FileWriter("log.log");
        }
        catch (IOException e)
        {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public static void report(String report)
    {
        try {
            logWriter.write(report);
            logWriter.flush();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    public static void close() throws IOException
    {
        logWriter.flush();
        logWriter.close();
    }
}
