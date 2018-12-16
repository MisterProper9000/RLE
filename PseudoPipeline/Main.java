import java.io.IOException;

/**
 * Created by ������ on 22.11.2018.
 */

/**
 *  Main class
 */
public class Main {
    public static void  main(String[] args) {

        Log.init();

        if(args.length == 1) {
            try {
                Pipeline pip = new Pipeline(args[0]);
                pip.Start();

            }
            catch (Exception e) {
                Log.report("Pipeline failed");
                System.exit(1);
            }
        } else {
            Log.report("Wrong program argument");
            System.exit(1);
        }
        System.exit(0);
    }
}
