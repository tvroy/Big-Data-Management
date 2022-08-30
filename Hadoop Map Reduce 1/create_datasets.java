import java.io.File;
import java.io.FileWriter;
import java.util.Random;
import java.io.IOException;

public class create_datasets {

    private static int random_int(int min, int max) {
        
        return((int)Math.floor(Math.random()*(max-min+1)+min));

    }
    private static float random_float(int min, int max) {
        
        return((float)(Math.random()*(max-min)+min));

    }
    private static String random_string(int min, int max) {
        int n = random_int(min, max);

        String Alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                    + "abcdefghijklmnopqrstuvxyz";

        String rand_text = "";
        for (int i = 0; i < n; i++) {
            int index = (int)(Alphabet.length()*Math.random());
            rand_text = rand_text + Alphabet.charAt(index);
        }
        return rand_text;
    }
    
    public static void main(String[] args) {
        try {
            File cust_file = new File("cust_file.txt");
            if (cust_file.createNewFile()) {
                cust_file.setReadable(true);
                cust_file.setWritable(true);
                System.out.println("File created: " + cust_file.getName() + " at " + cust_file.getAbsolutePath());
            } else {
                System.out.println("File already exists at " + cust_file.getAbsolutePath());
            }
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
     
        try {
            FileWriter cust_writer = new FileWriter("cust_file.txt", true);

            for(int id = 1; id <= 50000; id++) {
                String name = random_string(10, 20);
                int age = random_int(10, 70);
                String gender;
                if (random_int(0,1) == 0) {
                    gender = "male";
                }
                else {
                    gender = "female";
                }
                int country_code = random_int(1,10);
                float salary = random_float(100, 10000);
            
                cust_writer.append("" + id + "," + name + "," + age + "," + gender + "," + country_code + "," + salary + "\n");
            }
            cust_writer.close();        
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }  

        try {
            File trans_file = new File("trans_file.txt");
            if (trans_file.createNewFile()) {
                System.out.println("File created: " + trans_file.getName() + " at " + trans_file.getAbsolutePath());
            } else {
                System.out.println("File already exists at " + trans_file.getAbsolutePath());
            }
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
        
        try {
            FileWriter trans_writer = new FileWriter("trans_file.txt");
            for(int transid = 1; transid <= 5000000; transid++) {
                int cust_id = random_int(1, 50000);
                float trans_total = random_float(10, 1000);
                int trans_num_items = random_int(1,10);
                String trans_desc = random_string(20, 50);

                trans_writer.append("" + transid + "," + cust_id + "," + trans_total + "," + trans_num_items + "," + trans_desc + "\n");
            }
            trans_writer.close();
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
    }
}