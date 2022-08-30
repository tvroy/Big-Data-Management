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

public class Query2 {
    public static class CustomerMapper extends Mapper<Object, Text, Text, Text>{
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] cust_val_list = value.toString().split(",");
            String cust_id = cust_val_list[0];
            String cust_country_code = cust_val_list[4];

            context.write(new Text(cust_id), new Text("C" + "," + cust_country_code));
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
            String country_code = "";
            float max_trans_total = 10;
            float min_trans_total = 1000;

            for (Text val : values) {
                String[] val_list = val.toString().split(",");
                if (val_list[0].equals("C")) {
                    country_code = "" + val_list[1];
                } else {
                    float trans_total = Float.parseFloat(val_list[1]);
                    if (trans_total < min_trans_total) {
                        min_trans_total = trans_total;
                    }
                    if (trans_total > max_trans_total) {
                        max_trans_total = trans_total;
                    }
                }
            }
            String output_values = max_trans_total + "," + min_trans_total;
            context.write(new Text(country_code), new Text(output_values));
        }
    }
    
    public static class TwoMapper extends Mapper<Object, Text, Text, Text>{
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] val_list = value.toString().split("\t");
            String cc = val_list[0];
            String trans_info = val_list[1]; 
            context.write(new Text(cc), new Text(trans_info));
        }
    }

    public static class TwoReducer
        extends Reducer<Text,Text,Text,Text>{
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int cust_count = 0;
            float max_trans_total = 10;
            float min_trans_total = 1000;
            String trans_info = "";
            String output_info = "";

            for (Text val : values) {
                String[] val_list = val.toString().split(",");
                cust_count++;
                if (Float.parseFloat(val_list[1]) < min_trans_total) {
                    min_trans_total = Float.parseFloat(val_list[1]);
                }
                if (Float.parseFloat(val_list[0]) > max_trans_total) {
                    max_trans_total = Float.parseFloat(val_list[0]);
                }
            }
            output_info = cust_count + "," + min_trans_total + "," + max_trans_total;
            context.write(key, new Text(output_info));
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Query2");
        job1.setJarByClass(Query2.class);
        MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, CustomerMapper.class);
        MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, TransactionMapper.class);
        job1.setReducerClass(OneReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));
        job1.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Query2");
        job2.setJarByClass(Query2.class);
        FileInputFormat.addInputPath(job2, new Path(args[2]));
        job2.setMapperClass(TwoMapper.class);
        job2.setReducerClass(TwoReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}

  