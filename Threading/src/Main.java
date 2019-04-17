/**
 * Created by ������ on 22.11.2018.
 */

/**
 *  Main class
 */
public class Main {
    public static void  main(String[] args) {

       // Log.init();

        if(args.length == 1) {
            try {
                Pipeline pip = new Pipeline(args[0]);
                System.out.println("Start");
                pip.Start();
                System.out.println("end?");
            }
            catch (Exception e) {
             //   Log.report("Pipeline failed");
                return;
            }
        } else {
           // Log.report("Wrong program argument");
            return;
        }


    }

}
