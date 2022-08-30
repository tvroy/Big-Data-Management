import java.io.IOException;
import java.util.ArrayList;

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

public class Problem1 {

    private static ArrayList<String> create_segments() {
        ArrayList<String> segments = new ArrayList<>();
        int seg_size = 499;
        int seg_num = 1;
        int seg_x_min = 0;
        int seg_y_min = 0;
        int seg_x_max = 0;
        int seg_y_max = 0;
        for (int x = 0; x < 10000; x = x + 1 + seg_size) {
            for (int y = 0; y < 10000; y = y + 1 + seg_size) {
                if (y == 9500 && x == 9500) {
                    seg_x_min = x;
                    seg_y_min = y;
                    seg_x_max = seg_x_min + seg_size + 1;
                    seg_y_max = seg_y_min + seg_size + 1;
                } else if (y == 9500) {
                    seg_x_min = x;
                    seg_y_min = y;
                    seg_x_max = seg_x_min + seg_size;
                    seg_y_max = seg_y_min + seg_size + 1;
                } else if (x == 9500) {
                    seg_x_min = x;
                    seg_y_min = y;
                    seg_x_max = seg_x_min + seg_size + 1;
                    seg_y_max = seg_y_min + seg_size;
                } else {
                    seg_x_min = x;
                    seg_y_min = y;
                    seg_x_max = seg_x_min + seg_size;
                    seg_y_max = seg_y_min + seg_size;
                }
                String segment = "s" + seg_num + "," + seg_x_min + "," + seg_y_min + "," + seg_x_max + "," + seg_y_max;
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
            String window = conf.get("window");
            boolean win = false;
            String[] point = value.toString().split(",");
            int x = Integer.parseInt(point[0]);
            int y = Integer.parseInt(point[1]);
            ArrayList<String> segs = create_segments();
            int w_x_min = 0;
            int w_y_min = 0;
            int w_x_max = 0;
            int w_y_max = 0;


            if(!window.equals("none")) {
                String[] window_space = window.split(",");
                w_x_min = Integer.parseInt(window_space[0]);
                w_y_min = Integer.parseInt(window_space[1]);
                w_x_max = Integer.parseInt(window_space[2]);
                w_y_max = Integer.parseInt(window_space[3]);
                win = true;
            }

            for (String seg:segs) {
                String[] seg_info = seg.split(",");
                String seg_num = seg_info[0];
                int s_x_min = Integer.parseInt(seg_info[1]);
                int s_y_min = Integer.parseInt(seg_info[2]);
                int s_x_max = Integer.parseInt(seg_info[3]);
                int s_y_max = Integer.parseInt(seg_info[4]);
                if (x >= s_x_min && x <= s_x_max && y >= s_y_min && y <=s_y_max) {
                    if (win == true) {
                        if (x >= w_x_min && x <= w_x_max && y >= w_y_min && y <= w_y_max) {
                            context.write(new Text(seg_num), new Text("P" + "," + x + "," + y));
                            break;
                        }
                    } else {
                        context.write(new Text(seg_num), new Text("P" + "," + x + "," + y));
                        break;
                    }
                }
            }
        }
    }
        
    public static class RectangleMapper
         extends Mapper<Object, Text, Text, Text>{
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String window = conf.get("window");
            boolean win = false;
            String[] rect = value.toString().split(",");
            String rect_num = rect[0];
            int bl_x = Integer.parseInt(rect[1]);
            int bl_y = Integer.parseInt(rect[2]);
            int h = Integer.parseInt(rect[3]);
            int w = Integer.parseInt(rect[4]);
            // bottom right corner
            int br_x = bl_x + w;
            int br_y = bl_y;
            //top left corner
            int tl_x = bl_x;
            int tl_y = bl_y + h;
            //top right corner
            int tr_x = bl_x + w;
            int tr_y = bl_y + h;
            ArrayList<String> segs = create_segments();
            int w_x_min = 0;
            int w_y_min = 0;
            int w_x_max = 0;
            int w_y_max = 0;


            if(!window.equals("none")) {
                String[] window_space = window.split(",");
                w_x_min = Integer.parseInt(window_space[0]);
                w_y_min = Integer.parseInt(window_space[1]);
                w_x_max = Integer.parseInt(window_space[2]);
                w_y_max = Integer.parseInt(window_space[3]);
                win = true;
            }

            for (String seg:segs) {
                String[] seg_info = seg.split(",");
                String seg_num = seg_info[0];
                int s_x_min = Integer.parseInt(seg_info[1]);
                int s_y_min = Integer.parseInt(seg_info[2]);
                int s_x_max = Integer.parseInt(seg_info[3]);
                int s_y_max = Integer.parseInt(seg_info[4]);
                if ((bl_x >= s_x_min && bl_x <= s_x_max && bl_y >= s_y_min && bl_y <= s_y_max) || 
                    (br_x >= s_x_min && br_x <= s_x_max && br_y >= s_y_min && br_y <= s_y_max) ||
                    (tl_x >= s_x_min && tl_x <= s_x_max && tl_y >= s_y_min && tl_y <= s_y_max) ||
                    (tr_x >= s_x_min && tr_x <= s_x_max && tr_y >= s_y_min && tr_y <= s_y_max)) {
                    
                    if (win == true) {
                        if ((bl_x >= w_x_min && bl_x <= w_x_max && bl_y >= w_y_min && bl_y <= w_y_max) || 
                            (br_x >= w_x_min && br_x <= w_x_max && br_y >= w_y_min && br_y <= w_y_max) ||
                            (tl_x >= w_x_min && tl_x <= w_x_max && tl_y >= w_y_min && tl_y <= w_y_max) ||
                            (tr_x >= w_x_min && tr_x <= w_x_max && tr_y >= w_y_min && tr_y <= w_y_max)) {
                            
                            context.write(new Text(seg_num), new Text("R" + "," + rect_num + "," + bl_x + "," + bl_y + "," + tr_x + "," + tr_y));
                        }
                    } else {
                        context.write(new Text(seg_num), new Text("R" + "," + rect_num + "," + bl_x + "," + bl_y + "," + tr_x + "," + tr_y));
                    }
                }
            }
        }
    }

    public static class SpatialJoinReducer
        extends Reducer<Text,Text,Text,Text>{
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<String> points = new ArrayList<>();
            ArrayList<String> rectangles = new ArrayList<>();
            for (Text val : values) {
                String[] val_info = val.toString().split(",");
                if (val_info[0].equals("P")) {
                    points.add(val.toString());
                } else {
                    rectangles.add(val.toString());
                }
            }

            for(String rect : rectangles) {
                String[] rect_info = rect.split(",");
                String rect_number = rect_info[1];
                int r_x_min = Integer.parseInt(rect_info[2]);
                int r_y_min = Integer.parseInt(rect_info[3]);
                int r_x_max = Integer.parseInt(rect_info[4]);
                int r_y_max = Integer.parseInt(rect_info[5]);
                for (String point : points) {
                    String[] point_info = point.split(",");
                    int p_x = Integer.parseInt(point_info[1]);
                    int p_y = Integer.parseInt(point_info[2]);
                    if (p_x >= r_x_min && p_x <= r_x_max && p_y >= r_y_min && p_y <= r_y_max) {
                        String r_key = "<" + rect_number + ",";
                        String p_value = "(" + p_x + "," + p_y + ")>";
                        context.write(new Text(r_key), new Text(p_value));
                    }
                }
            }
        } 
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("window", args[3]);
        Job job = Job.getInstance(conf, "Problem1");
        job.setJarByClass(Problem1.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, PointMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RectangleMapper.class);
        job.setReducerClass(SpatialJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
  