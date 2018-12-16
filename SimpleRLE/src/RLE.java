import java.io.*;
import java.util.EnumMap;
import java.util.Map;
import java.util.HashMap;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.FileInputStream;
import java.io.BufferedInputStream;
import java.io.FileOutputStream;
import java.io.BufferedOutputStream;

/**
 * Created by ������ on 15.09.2018.
 */

public class RLE {

    private static FileInputStream fin;
    private static BufferedInputStream in;


    private static FileOutputStream fout;
    private static BufferedOutputStream out;


    public static EnumMap<Interpretator.LEXEMES, String> resultMap = new EnumMap<>(Interpretator.LEXEMES.class);

    public static void  main(String[] args)throws IOException {

        Log.init();

        if (args.length > 0) {

            if (Interpretator.Interp(args[0]) && OpenFile()) {


                sortRLE();
                Log.report("Processing complete successfully. See result of " + ((Integer.parseInt(resultMap.get(Interpretator.LEXEMES.CODE_MODE)) % 2 == 0) ? "encoding" : "decoding") + " in " + ((Integer.parseInt(resultMap.get(Interpretator.LEXEMES.CODE_MODE)) % 2 == 0) ? resultMap.get(Interpretator.LEXEMES.OUTPUT_FILE) : resultMap.get(Interpretator.LEXEMES.INPUT_FILE)) + " file");
            }

        } else {
            Log.report("Config file missing");
        }
        Log.close();
    }

    private static boolean OpenFile()throws IOException
    {
        if(resultMap.get(Interpretator.LEXEMES.INPUT_FILE) != null && resultMap.get(Interpretator.LEXEMES.OUTPUT_FILE) != null) {
            try
            {
                if(Integer.parseInt(resultMap.get(Interpretator.LEXEMES.CODE_MODE)) == 0)
                    fin = new FileInputStream(resultMap.get(Interpretator.LEXEMES.INPUT_FILE));
                else
                    fin = new FileInputStream(resultMap.get(Interpretator.LEXEMES.OUTPUT_FILE));
            }
            catch (FileNotFoundException e)
            {
                Log.report("input file not found");
                return false;
            }
            in = new BufferedInputStream(fin);

            try
            {
                if(Integer.parseInt(resultMap.get(Interpretator.LEXEMES.CODE_MODE)) == 0)
                    fout = new FileOutputStream(resultMap.get(Interpretator.LEXEMES.OUTPUT_FILE));
                else
                    fout = new FileOutputStream(resultMap.get(Interpretator.LEXEMES.INPUT_FILE));
            }
            catch (FileNotFoundException e)
            {
                Log.report("output file not found");
                return false;
            }
            out = new BufferedOutputStream(fout);

            return true;
        }
        else {
            Log.report("File name missing");
            return false;
        }
    }

    private static void sortRLE() throws  IOException
    {
        int codeMode = Integer.parseInt(resultMap.get(Interpretator.LEXEMES.CODE_MODE));
        if (codeMode == 0)
            toRLE();
        else
            fromRLE();
    }

    private static void toRLE() throws IOException
    {
        final int maxByte = 255;
        int b;

        try{
            b = in.read();
            while (b>=0)
            {
                int pb = b;
                int count = 1;

                while(count < maxByte)
                {
                    b = in.read();
                    if(b == pb)
                        ++count;
                    else
                        break;
                }

                try
                {
                    out.write(count);
                    out.write(pb);
                    out.flush();
                }
                catch (IOException e)
                {
                    Log.report("Error while writing to output file");
                    return;
                }
            }
        }
        catch (IOException e)
        {
            Log.report("Error while reading from input file");
            return;
        }
    }

    private static void fromRLE() throws IOException
    {
        while (true) {
            int count, b;

            try {
                count = in.read();

                if (count < 0) {
                    break;
                }

                b = in.read();

                if (b < 0) {
                    Log.report("Unexpected end of input file");
                    return;
                }
            } catch (IOException e) {
                Log.report("Error while reading input file");
                return;
            }

            for (int i = 0; i < count; ++i) {
                try {
                    out.write(b);
                    out.flush();
                } catch (IOException e) {
                    Log.report("Error while writing to output file");
                    return;
                }
            }
        }
    }
}
