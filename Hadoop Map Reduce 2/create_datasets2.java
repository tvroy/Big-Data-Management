import java.io.File;
import java.io.FileWriter;
import java.util.Random;
import java.io.IOException;

public class create_datasets2 {

    private static int random_int(int min, int max) {
        
        return((int)Math.floor(Math.random()*(max-min+1)+min));

    }

    
    public static void main(String[] args) {
        try {
            File p_file = new File("p_file.txt");
            if (p_file.createNewFile()) {
                System.out.println("File created: " + p_file.getName() + " at " + p_file.getAbsolutePath());
            } else {
                System.out.println("File already exists at " + p_file.getAbsolutePath());
            }
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
     
        try {
            FileWriter p_writer = new FileWriter("p_file.txt", true);

            for(int id = 1; id <= 15000000; id++) {
                int x = random_int(0, 10000);
                int y = random_int(0, 10000);
                p_writer.append("" + x + "," + y +"\n");
            }
            p_writer.close();        
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }  

        try {
            File r_file = new File("r_file.txt");
            if (r_file.createNewFile()) {
                System.out.println("File created: " + r_file.getName() + " at " + r_file.getAbsolutePath());
            } else {
                System.out.println("File already exists at " + r_file.getAbsolutePath());
            }
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
        
        try {
            FileWriter r_writer = new FileWriter("r_file.txt");
            for(int id = 1; id <= 5000000; id++) {
                int h = random_int(1,20);
                int w = random_int(1, 7);
                int x = random_int(0, (10000-w));
                int y = random_int(0, (10000-h));
                
                r_writer.append("r" + id + "," + x + "," + y + "," + h + "," + w + "\n");
            }
            r_writer.close();
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
    }
}