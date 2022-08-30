import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class Query1 {
    public static class CustomerMapper
         extends Mapper<Object, Text, Text, Text>{
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] cust_val_list = value.toString().split(",");
            String cust_id = cust_val_list[0];
            String cust_name = cust_val_list[1];
            String cust_salary = cust_val_list[5];

            context.write(new Text(cust_id), new Text(cust_name + "," + cust_salary));
        }
    }
        
    public static class TransactionMapper
         extends Mapper<Object, Text, Text, Text>{
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] trans_val_list = value.toString().split(",");
            String cust_id = trans_val_list[1];
            String trans_total = trans_val_list[2]; 
            String trans_num_items = trans_val_list[3];
            
            context.write(new Text(cust_id), new Text("T" + "," + trans_total + "," + trans_num_items));
        }
    }

    public static class TransactionReducer
        extends Reducer<Text,Text,Text,Text>{
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String cust_info = "";
            int count_trans = 0;
            int min_items = 10;
            float sum_trans = 0;

            for (Text val : values) {
                String[] val_list = val.toString().split(",");
                if (val_list[0].equals("T")) {
                    count_trans ++;
                    int num_items = Integer.parseInt(val_list[2]);
                    if (min_items > num_items) {
                        min_items = num_items;
                    }
                    float trans_total = Float.parseFloat(val_list[1]);
                    sum_trans += trans_total;
                }
                else {
                    cust_info = key + "," + val_list[0] + "," + val_list[1];
                }
            }
            String output_values = count_trans + "," + sum_trans + "," + min_items;
            context.write(new Text(cust_info), new Text(output_values));
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Query1");
        job.setJarByClass(Query1.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CustomerMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TransactionMapper.class);
        job.setReducerClass(TransactionReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
  