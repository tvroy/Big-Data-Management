import java.io.*;
import java.util.HashMap;
import java.net.URI;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.filecache.DistributedCache;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class Query3DC {
    public static class CustTransMapper extends Mapper<Object, Text, Text, Text>{
        private static HashMap<String, String> CustIDMap = new HashMap<String, String>();

        public void setup(Context context) throws IOException, InterruptedException {

        URI[] cacheFile = context.getCacheFiles();
  
        if (cacheFile != null && cacheFile.length > 0) {
            try {
                String cust_record = "";
                FileSystem fs = FileSystem.get(context.getConfiguration());
                Path FilePath = new Path(cacheFile[0].toString());
                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(FilePath)));
                
                while ((cust_record = reader.readLine()) != null) {
                    String[] val_list = cust_record.split(",");
                    String cust_id = val_list[0];
                    if(!CustIDMap.containsKey(cust_id)) {
                        CustIDMap.put(cust_id,cust_record);
                    }
                }
            }
            catch (Exception e) {
                System.out.println("Unable to read the File");
                System.exit(1);
            }
        }
        }
        
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] trans_val_list = value.toString().split(",");
            String[] cust_val_list = CustIDMap.get(trans_val_list[1]).split(",");
            String age = cust_val_list[2];
            String gender = cust_val_list[3];
            String age_bracket = "";

            int intage = Integer.parseInt(age);
                    if (intage >= 10 && intage < 20) {
                        age_bracket = "[10, 20)";
                    } else if (intage >= 20 && intage < 30){
                        age_bracket = "[20, 30)";
                    } else if (intage >= 30 && intage < 40){
                        age_bracket = "[30, 40)";
                    } else if (intage >= 40 && intage < 50){
                        age_bracket = "[40, 50)";
                    } else if (intage >= 50 && intage < 60){
                        age_bracket = "[50, 60)";
                    } else if (intage >= 60 && intage <= 70){
                        age_bracket = "[60, 70]";
                    }
            String age_gender = "" + age_bracket + "," + gender;
            String output_value = trans_val_list[2];
            context.write(new Text(age_gender), new Text(output_value));
            }
        }

    public static class CustTransReducer extends Reducer<Text,Text,Text,Text>{
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            float trans_sum = 0;
            int trans_count = 0;
            float max_trans_total = 10;
            float min_trans_total = 1000;

            for (Text val : values) {
                    String valstring = val.toString();
                    trans_count ++;
                    float trans_total = Float.parseFloat(valstring);
                    trans_sum = trans_sum + trans_total;
                    if (trans_total < min_trans_total) {
                        min_trans_total = trans_total;
                    }
                    if (trans_total > max_trans_total) {
                        max_trans_total = trans_total;
                    }
                }
                float trans_avg = trans_sum/trans_count;
            String output_values = max_trans_total + "," + min_trans_total + "," + trans_avg;
            context.write(key, new Text(output_values));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Query3DC");
        job1.setJarByClass(Query3DC.class);
        job1.addCacheFile(new URI(args[0]));
        FileInputFormat.addInputPath(job1, new Path(args[1]));
        job1.setMapperClass(CustTransMapper.class);
        job1.setReducerClass(CustTransReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));
        job1.waitForCompletion(true);

    }
}


  