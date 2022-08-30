import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class Query3 {
    public static class CustomerMapper extends Mapper<Object, Text, Text, Text>{
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] cust_val_list = value.toString().split(",");
            String cust_id = cust_val_list[0];
            String cust_age = cust_val_list[2];
            String cust_gender = cust_val_list[3];
            context.write(new Text(cust_id), new Text("C" + "," + cust_age + "," + cust_gender));
        }
    }
    public static class TransactionMapper extends Mapper<Object, Text, Text, Text>{
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] trans_val_list = value.toString().split(",");
            String cust_id = trans_val_list[1];
            String trans_total = trans_val_list[2]; 
       
            context.write(new Text(cust_id), new Text("T" + "," + trans_total));
        }
    }
    public static class OneReducer
        extends Reducer<Text,Text,Text,Text>{
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String age_bracket = "";
            String gender = "";
            float trans_sum = 0;
            int trans_count = 0;
            float max_trans_total = 10;
            float min_trans_total = 1000;

            for (Text val : values) {
                String[] val_list = val.toString().split(",");
                if (val_list[0].equals("C")) {
                    gender = val_list[2];
                    String stringage = "" + val_list[1];
                    int intage = Integer.parseInt(stringage);
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
                } else {
                    trans_count ++;
                    float trans_total = Float.parseFloat(val_list[1]);
                    trans_sum = trans_sum + trans_total;
                    if (trans_total < min_trans_total) {
                        min_trans_total = trans_total;
                    }
                    if (trans_total > max_trans_total) {
                        max_trans_total = trans_total;
                    }
                }
            }
            String age_gender = "" + age_bracket + "," + gender;
            String output_values = max_trans_total + "," + min_trans_total + "," + trans_sum + "," + trans_count;
            context.write(new Text(age_gender), new Text(output_values));
        }
    }
    
    public static class TwoMapper extends Mapper<Object, Text, Text, Text>{
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] val_list = value.toString().split("\t");
            String age_gender = val_list[0];
            String trans_info = val_list[1]; 
            context.write(new Text(age_gender), new Text(trans_info));
        }
    }

    public static class TwoReducer
        extends Reducer<Text,Text,Text,Text>{
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int trans_count = 0;
            float max_trans_total = 10;
            float min_trans_total = 1000;
            float trans_sum = 0;
            float trans_avg = 0;
            String trans_info = "";
            String output_info = "";

            for (Text val : values) {
                String[] val_list = val.toString().split(",");
                trans_count = trans_count + Integer.parseInt(val_list[3]);
                trans_sum = trans_sum + Float.parseFloat(val_list[2]);
                if (Float.parseFloat(val_list[1]) < min_trans_total) {
                    min_trans_total = Float.parseFloat(val_list[1]);
                }
                if (Float.parseFloat(val_list[0]) > max_trans_total) {
                    max_trans_total = Float.parseFloat(val_list[0]);
                }
            }
            trans_avg = (trans_sum / trans_count);
            output_info = min_trans_total + "," + max_trans_total + "," + trans_avg;
            context.write(key, new Text(output_info));
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Query3");
        job1.setJarByClass(Query3.class);
        MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, CustomerMapper.class);
        MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, TransactionMapper.class);
        job1.setReducerClass(OneReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));
        job1.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Query3");
        job2.setJarByClass(Query3.class);
        FileInputFormat.addInputPath(job2, new Path(args[2]));
        job2.setMapperClass(TwoMapper.class);
        job2.setReducerClass(TwoReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}


  