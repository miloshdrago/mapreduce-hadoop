import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SparkTripLength {

    public static double degrees2radians = Math.PI/180; // factor to convert degrees
    public static double EarthRadius = 6371.009; // earth radius in kilometers

    /**
     * Calculates the spherical earth distance in kilometers, input values are in degrees
     * https://en.wikipedia.org/wiki/Geographical_distance#Spherical_Earth_projected_to_a_plane
     */
    private static double SphericalEarthDistance(double lat1, double long1, double lat2, double long2) {
        double deltalat = (lat1 - lat2) * degrees2radians;
        double deltalong = (long1 - long2) * degrees2radians;
        double meanlat = (lat1 + lat2) * degrees2radians / 2;
        return EarthRadius * Math.sqrt(Math.pow(deltalat, 2) + Math.pow(Math.cos(meanlat) * deltalong, 2));
    }

    /**
     * The mapper will convert the following Text input :
     * "9 1267451562.0 37.61373 -122.39722 1267453549.0 37.34666 -121.99176"
     * by calculating the distance between he start and end point 46.497 km
     * then it will map the distance to a bin based on the intervals => bin 46 which covers distances from 46.000km up to 47.999 km
     * The output will be key = 46 & value = 1
     */
    public static class TripLengthMapper
                    extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            // convert string to an array of strings for each trip parameter
            String[] trip = value.toString().split("\\s+", -1);
            // parse the start and end latitudes and longitudes
            double lat1 = Double.parseDouble(trip[2]); // start position latitude
            double long1 = Double.parseDouble(trip[3]); // start position longitude
            double lat2 = Double.parseDouble(trip[5]); // end position latitude
            double long2 = Double.parseDouble(trip[6]); // end position longitude
            double distance = SphericalEarthDistance(lat1, long1, lat2, long2); //calculate the distance in km
            String str_bin;
            int interval = 2; // bin interval in km
            int max_dist = 80; // maximum distance in km
            int current_bin = (int) (distance/interval); // calculate to which bin this trip belongs
            // check if the trip is a normal trip or an outlier
            if(current_bin<(max_dist/interval)){
                str_bin = String.format("%02d",current_bin*interval);
            } else {
                // Outliers in the last bin
                str_bin = ">=" + String.format("%02d",max_dist);
            }
            context.write(new Text(str_bin),one);
        }
    }

    public static class TripLengthReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "SparkTripLength");
        job.setJarByClass(SparkTripLength.class);
        job.setMapperClass(TripLengthMapper.class);
        job.setCombinerClass(TripLengthReducer.class);
        job.setReducerClass(TripLengthReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}



