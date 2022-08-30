import java.io.File;
import java.io.FileWriter;
import java.util.Random;
import java.io.IOException;

public class create_dataset {

    private static int random_int(int min, int max) {
        
        return((int)Math.floor(Math.random()*(max-min+1)+min));

    }

    public static void main(String[] args) {
        try {
            File p_file = new File("C:/Users/tvroy/Documents/DS503/project_3/p_file.txt");
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
            FileWriter p_writer = new FileWriter("C:/Users/tvroy/Documents/DS503/project_3/p_file.txt", true);

            for(int id = 1; id <= 15000000; id++) {
                int x = random_int(1, 10000);
                int y = random_int(1, 10000);
                p_writer.append("" + x + "," + y +"\n");
            }
            p_writer.close();        
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }  
    }
}