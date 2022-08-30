import java.io.IOException;
import java.util.ArrayList;
import java.lang.Math;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class Problem2 {

    private static ArrayList<String> create_segments(int radius) {
        ArrayList<String> segments = new ArrayList<>();
        int seg_size = 999;
        int seg_num = 1;
        int seg_x_min = 0;
        int seg_y_min = 0;
        int seg_x_max = 0;
        int seg_y_max = 0;
        int supp_x_min = 0;
        int supp_y_min = 0;
        int supp_x_max = 0;
        int supp_y_max = 0;
        for (int x = 0; x < 10000; x = x + 1 + seg_size) {
            for (int y = 0; y < 10000; y = y + 1 + seg_size) {
                if (y == 9000 && x == 9000) {
                    seg_x_min = x;
                    seg_y_min = y;
                    seg_x_max = seg_x_min + seg_size + 1;
                    seg_y_max = seg_y_min + seg_size + 1;
                    supp_x_min = seg_x_min - radius;
                    supp_y_min = seg_y_min - radius;
                    supp_x_max = seg_x_max;
                    supp_y_max = seg_y_max;
                } else if (y == 9000) {
                    seg_x_min = x;
                    seg_y_min = y;
                    seg_x_max = seg_x_min + seg_size;
                    seg_y_max = seg_y_min + seg_size + 1;
                    supp_x_min = seg_x_min - radius;
                    supp_y_min = seg_y_min - radius;
                    supp_x_max = seg_x_max + radius;
                    supp_y_max = seg_y_max;
                } else if (x == 9000) {
                    seg_x_min = x;
                    seg_y_min = y;
                    seg_x_max = seg_x_min + seg_size + 1;
                    seg_y_max = seg_y_min + seg_size;
                    supp_x_min = seg_x_min - radius;
                    supp_y_min = seg_y_min - radius;
                    supp_x_max = seg_x_max;
                    supp_y_max = seg_y_max + radius;

                } else if(x == 0 && y ==0) {
                    seg_x_min = x;
                    seg_y_min = y;
                    seg_x_max = seg_x_min + seg_size;
                    seg_y_max = seg_y_min + seg_size;
                    supp_x_min = seg_x_min;
                    supp_y_min = seg_y_min;
                    supp_x_max = seg_x_max + radius;
                    supp_y_max = seg_y_max + radius;

                } else if(x == 0) {
                    seg_x_min = x;
                    seg_y_min = y;
                    seg_x_max = seg_x_min + seg_size;
                    seg_y_max = seg_y_min + seg_size;
                    supp_x_min = seg_x_min;
                    supp_y_min = seg_y_min - radius;
                    supp_x_max = seg_x_max + radius;
                    supp_y_max = seg_y_max + radius;

                } else if(y == 0) {
                    seg_x_min = x;
                    seg_y_min = y;
                    seg_x_max = seg_x_min + seg_size;
                    seg_y_max = seg_y_min + seg_size;
                    supp_x_min = seg_x_min - radius;
                    supp_y_min = seg_y_min;
                    supp_x_max = seg_x_max + radius;
                    supp_y_max = seg_y_max + radius;

                } else {
                    seg_x_min = x;
                    seg_y_min = y;
                    seg_x_max = seg_x_min + seg_size;
                    seg_y_max = seg_y_min + seg_size;
                    supp_x_min = seg_x_min - radius;
                    supp_y_min = seg_y_min - radius;
                    supp_x_max = seg_x_max + radius;
                    supp_y_max = seg_y_max + radius;
                }
                String segment = "s" + seg_num + "," + seg_x_min + "," + seg_y_min + "," + seg_x_max + "," + seg_y_max + "," + supp_x_min + "," + supp_y_min + "," + supp_x_max + "," + supp_y_max;
                segments.add(segment);
                seg_num++;
            }
        }
        return segments;
    }

    public static class PointMapper
         extends Mapper<Object, Text, Text, Text>{
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int rad = Integer.parseInt(conf.get("r"));
            String[] point = value.toString().split(",");
            int x = Integer.parseInt(point[0]);
            int y = Integer.parseInt(point[1]);
            ArrayList<String> segs = create_segments(rad);

            for (String seg:segs) {
                String[] seg_info = seg.split(",");
                String seg_num = seg_info[0];
                int s_x_min = Integer.parseInt(seg_info[1]);
                int s_y_min = Integer.parseInt(seg_info[2]);
                int s_x_max = Integer.parseInt(seg_info[3]);
                int s_y_max = Integer.parseInt(seg_info[4]);
                int supplement_x_min = Integer.parseInt(seg_info[5]);
                int supplement_y_min = Integer.parseInt(seg_info[6]);
                int supplement_x_max = Integer.parseInt(seg_info[7]);
                int supplement_y_max = Integer.parseInt(seg_info[8]);
                if (x >= s_x_min && x <= s_x_max && y >= s_y_min && y <=s_y_max) {
                    context.write(new Text(seg_num), new Text("core" + "," + x + "," + y));
                } else if (x >= supplement_x_min && x <= supplement_x_max && y >= supplement_y_min && y <=supplement_y_max) {
                    context.write(new Text(seg_num), new Text("supplemental" + "," + x + "," + y));
                }
            }
        }
    }

        
    public static class OutlierReducer
        extends Reducer<Text,Text,Text,Text>{
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int r_t = Integer.parseInt(conf.get("r"));
            int n_t = Integer.parseInt(conf.get("k"));
            ArrayList<String> points = new ArrayList<>();
            ArrayList<String> outliers = new ArrayList<>();
            
            for (Text val : values) {
                points.add(val.toString());
            }

            int[] neighbor_counts = new int[points.size()];

            for(int i = 0; i < points.size() - 1; i++) {
                String[] point_1 = points.get(i).split(",");
                int point_1_x = Integer.parseInt(point_1[1]);
                int point_1_y = Integer.parseInt(point_1[2]);

                for(int j = i + 1; j < points.size(); j++) {
                    String[] point_2 = points.get(j).split(",");
                    int point_2_x = Integer.parseInt(point_2[1]);
                    int point_2_y = Integer.parseInt(point_2[2]);
                    int ac = Math.abs(point_1_y - point_2_y);
                    int cb = Math.abs(point_1_x - point_2_x);
                    Double dist = Math.hypot(ac, cb);
                    
                    if(dist <= r_t) {
                        neighbor_counts[i] = neighbor_counts[i] + 1;
                        neighbor_counts[j] = neighbor_counts[j] + 1;
                    }
                }
            }

            for(int i = 0; i < points.size(); i++) {
                if (neighbor_counts[i] < n_t) {
                    outliers.add(points.get(i));
                }
            }

            for (String out : outliers) {
                String[] outlier = out.split(",");
                if (outlier[0].equals("core")) {
                    String outlier_info = "" + outlier[1] + "," + outlier[2];
                    context.write(null, new Text(outlier_info));
                }
            }
        } 
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            String error = "Either or both parameters r and k are missing. Please try again.";
            throw new Exception(error);
        }
        Configuration conf = new Configuration();
        conf.set("r", args[2]);
        conf.set("k", args[3]);
        Job job = Job.getInstance(conf, "Problem2");
        job.setJarByClass(Problem2.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setMapperClass(PointMapper.class);
        job.setReducerClass(OutlierReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
  