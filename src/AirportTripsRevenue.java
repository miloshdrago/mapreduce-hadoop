import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class AirportTripsRevenue {
    public static final double DEGREES_TO_RADIANS = Math.PI / 180; // factor to convert degrees to radians
    public static final double EARTH_RADIUS = 6371.009; // earth radius in kilometers
    public static final double SFO_LAT = 37.62131; // SFO Latitude
    public static final double SFO_LONG = -122.37896; // SFO Longitude
    public static final double SFO_MAX_DISTANCE = 1.0; // Max distance to consider trip an airport trip
    public static final double MIN_LAT = 36.50; // Minimal Latitude
    public static final double MAX_LAT = 39.80; // Maximal Latitude
    public static final double MIN_LONG = -123.80; // Minimal Longitude
    public static final double MAX_LONG = -119.10; // Maximal Longitude
    public static final double COASTLINE_EQ_PARAM_A = -1.3388206045302447; // Parameter a of line equation defining the coastline (a.x +b)
    public static final double COASTLINE_EQ_PARAM_B = -126.75838718367324; // Parameter b of line equation defining the coastline (a.x +b)
    public static final double START_FEE = 3.5; // Taxi trip start fee in $
    public static final double KM_FEE = 1.71; // Taxi trip kilometer fee in $
    public static final double MAX_SEGMENT_SPEED = 180.0; // Maximal speed between segment positions
    public static final double MAX_SEGMENT_DELTA_TIME = 210.0; // maximum time between busy data points beyond which we declare a new trip
    public static final double MIN_TRIP_DISTANCE = 0.100; // minimum trip distance for a valid trip (covers distance between terminals at SFO)

    // Class used by SegmentReducer to store and calculate the trip characteristics
    static class TaxiTrip {
        int taxiNumber = 0; // Taxi Number
        double startTime = 0.0; // Unix epoch representing the start time of the trip
        double startLat = 0.0; // Trip start latitude
        double startLong = 0.0; // Trip start longitude
        double stopTime = 0.0; // Unix epoch representing the end time of the trip
        double stopLat = 0.0; // Trip end latitude
        double stopLong = 0.0; // Trip end longitude
        boolean isAirportTrip = false; // If any trip position passes within airport radius set to true
        double cumulatedDistance = 0.0; // Trip cumulated distance from intermediate segments distances
        double tripRevenue = 0.0; // Trip revenue
        String startDate = ""; // Trip start date and time in human readable form, used for debug and to agregate by date
    }


    /**
     * Calculates the spherical earth distance in kilometers, input values are in degrees
     *
     * @param lat1  latitude point 1
     * @param long1 longitude point 1
     * @param lat2  latitude point 2
     * @param long2 longitude point 2
     * @return distance in km between the two points
     */
    private static double getSphericalEarthDistance(double lat1, double long1, double lat2, double long2) {
        double deltaLat = (lat1 - lat2) * DEGREES_TO_RADIANS;
        double deltaLong = (long1 - long2) * DEGREES_TO_RADIANS;
        double meanLat = (lat1 + lat2) * DEGREES_TO_RADIANS / 2;
        return EARTH_RADIUS * Math.sqrt(Math.pow(deltaLat, 2) + Math.pow(Math.cos(meanLat) * deltaLong, 2));
    }

    /**
     * Check if the segment position is within a radius of 1 km from SFO airport coordinates
     *
     * @param lat1  latitude
     * @param long1 longitude
     * @return true if position is within 1km of SFO airport center
     */
    private static boolean checkIsAirportTrip(double lat1, double long1) {
        return (getSphericalEarthDistance(lat1, long1, SFO_LAT, SFO_LONG) <= SFO_MAX_DISTANCE) ? true : false;
    }

    /**
     * Check if the segment position is within the limits of validity
     *
     * @param lat1  latitude
     * @param long1 longitude
     * @return true if position is valid
     */
    private static boolean checkIsValidPosition(double lat1, double long1) {
        // check if coordinates are west of the coastline which would mean a point in the sea
        if (lat1 < (COASTLINE_EQ_PARAM_A * long1 + COASTLINE_EQ_PARAM_B)) {
            return false;
        }
        // check if latitude is within limits
        if ((lat1 < MIN_LAT) || (lat1 > MAX_LAT)) {
            return false;
        }
        // check if longitude is within limits
        if ((long1 < MIN_LONG) || (long1 > MAX_LONG)) {
            return false;
        }
        return true;
    }

    /**
     * Converts a date in String format to a double representing the Unix Epoch
     * This timestamp from the 2010_03.segments file : '2010-03-01 00:10:25',37.79076,-122.40255,'M'
     * has been converted to this epoch in 2010 03.trips file : 9 1267402225.0 37.79076 -122.40255
     * Thereby we conclude that the conversion does not need any specific time zone and that we should
     * consider the timestamps as UTC dates
     *
     * @param strDate a String containing a taxi timestamp like 2010-03-01 00:10:25
     * @return a double like 1267402225.0
     */
    private static double strTimeToSystemTime(String strDate) {
        if (strDate.compareTo("NULL") == 0) {
            return 0.0;
        }
        // Declare the date format to process taxi trip timestamps like "2010-10-26 16:20:08"
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        // convert to a date-time without a time-zone in the ISO-8601 calendar system
        LocalDateTime zdt = LocalDateTime.parse(strDate, dtf);
        // convert the Unix Epoch expressed in seconds to double
        return (double) zdt.toEpochSecond(ZoneOffset.UTC);
    }

    /**
     * The segment Mapper will receive as input Text values like these :
     * "450,'2008-05-25 09:16:58',37.61611,-122.38888,'M','2008-05-25 09:17:00',37.61506,-122.39206,'E'"
     * "450,'2008-05-25 09:16:01',37.61799,-122.38608,'M','2008-05-25 09:16:58',37.61611,-122.38888,'M'"
     * "450,'2008-05-25 09:14:47',37.61798,-122.38606,'M','2008-05-25 09:16:01',37.61799,-122.38608,'M'"
     * "450,'2008-05-25 09:14:32',37.61799,-122.38607,'M','2008-05-25 09:14:47',37.61798,-122.38606,'M'"
     * "450,'2008-05-25 09:13:44',37.61661,-122.38425,'E','2008-05-25 09:14:32',37.61799,-122.38607,'M'"
     * it will check the validity of both positions and then output two <Text,NullWritable> key-value pairs per record as follows :
     * <"450,2008-05-25 09:16:58,37.61611,-122.38888,M",NullWritable>
     * <"450,2008-05-25 09:17:00,37.61506,-122.39206,E",NullWritable>
     * <"450,2008-05-25 09:16:01,37.61799,-122.38608,M",NullWritable>
     * <"450,2008-05-25 09:16:58,37.61611,-122.38888,M",NullWritable>
     * <"450,2008-05-25 09:14:47,37.61798,-122.38606,M",NullWritable>
     * <"450,2008-05-25 09:16:01,37.61799,-122.38608,M",NullWritable>
     * <"450,2008-05-25 09:14:32,37.61799,-122.38607,M",NullWritable>
     * <"450,2008-05-25 09:14:47,37.61798,-122.38606,M",NullWritable>
     * <"450,2008-05-25 09:13:44,37.61661,-122.38425,E",NullWritable>
     * <"450,2008-05-25 09:14:32,37.61799,-122.38607,M",NullWritable>
     */
    public static class SegmentMapper
            extends Mapper<Object, Text, Text, NullWritable> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            // remove quotes and split the line in an array of strings
            String[] segmentParams = value.toString().replace("'", "").split(",", -1);
            // a segment is defined by 9 elements, if we don't have exactly 9 we do not process this segment
            if (segmentParams.length != 9) {
                return;
            }
            // if the first position in the segment contains any other status than M (busy) we force the
            // taxi status at this position to E (empty)
            if (segmentParams[4].compareTo("M") != 0) {
                segmentParams[4] = "E";
            }
            // if the second position in the segment contains any other status than M (busy) we force the
            // taxi status at this position to E (empty)
            if (segmentParams[8].compareTo("M") != 0) {
                segmentParams[8] = "E";
            }
            // if we have an E-E segment (empty-empty) we do not process it as it has no useful value
            if (segmentParams[4].equals("E") && segmentParams[8].equals("E")) {
                return;
            }
            // check if in the first position the string NULL appears in which case we do not process this position
            // this is an example of such a record :
            // "926,'2008-05-28 00:40:54',37.75159,-122.39527,'E',NULL,NULL,NULL,NULL"
            if (segmentParams[1].compareTo("NULL") != 0) {
                // check the validity of first position in the segment
                double lat1 = Double.parseDouble(segmentParams[2]);
                double long1 = Double.parseDouble(segmentParams[3]);
                if (checkIsValidPosition(lat1, long1) == true) {
                    // first position in segment is valid, write it out as mapper output <Text,NullWritable> key-value pair
                    String str_start = segmentParams[0] + "," + segmentParams[1] + "," + segmentParams[2] + "," + segmentParams[3] + "," + segmentParams[4];
                    context.write(new Text(str_start), NullWritable.get());
                }
            }
            // check if in the second position the string NULL appears in which case we do not process this position
            if (segmentParams[5].compareTo("NULL") != 0) {
                // check the validity of second position in the segment
                double lat2 = Double.parseDouble(segmentParams[6]);
                double long2 = Double.parseDouble(segmentParams[7]);
                if (checkIsValidPosition(lat2, long2) == true) {
                    // second position in segment is valid, write it out as mapper output <Text,NullWritable> key-value pair
                    String str_stop = segmentParams[0] + "," + segmentParams[5] + "," + segmentParams[6] + "," + segmentParams[7] + "," + segmentParams[8];
                    context.write(new Text(str_stop), NullWritable.get());
                }
            }
        }
    }

    /**
     * The segment Combiner will receive from the segment Mapper as input a Text Key and a set of NullWritable Values like these :
     * <"450,2008-05-25 09:16:58,37.61611,-122.38888,M",Values={NullWritable,NullWritable}>
     * <"450,2008-05-25 09:17:00,37.61506,-122.39206,E",Values={NullWritable}>
     * <"450,2008-05-25 09:16:01,37.61799,-122.38608,M",Values={NullWritable,NullWritable}>
     * <"450,2008-05-25 09:14:47,37.61798,-122.38606,M",Values={NullWritable,NullWritable}>
     * <"450,2008-05-25 09:14:32,37.61799,-122.38607,M",Values={NullWritable,NullWritable}>
     * <"450,2008-05-25 09:13:44,37.61661,-122.38425,E",Values={NullWritable}>
     * it is simply used to remove the duplicate positions pairs
     * the output will be unique <Text,NullWritable> key-value pairs like :
     * <"450,2008-05-25 09:16:58,37.61611,-122.38888,M",NullWritable>
     * <"450,2008-05-25 09:17:00,37.61506,-122.39206,E",NullWritable>
     * <"450,2008-05-25 09:16:01,37.61799,-122.38608,M",NullWritable>
     * <"450,2008-05-25 09:14:47,37.61798,-122.38606,M",NullWritable>
     * <"450,2008-05-25 09:14:32,37.61799,-122.38607,M",NullWritable>
     * <"450,2008-05-25 09:13:44,37.61661,-122.38425,E",NullWritable>
     */
    public static class SegmentCombiner
            extends Reducer<Text, NullWritable, Text, NullWritable> {

        public void reduce(Text key, Iterable<NullWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            // write out just one unique <Text,NullWritable> key-value pair in the partition, removing any duplicates
            context.write(key, NullWritable.get());
        }
    }

    /**
     * The segment Partitioner will receive from the segment Combiner as input a <Text,NullWritable> key-value pair like this :
     * <"450,2008-05-25 09:16:58,37.61611,-122.38888,M",NullWritable>
     * and based on the taxi number and the number of reducers of the job it will assign the key-value pair
     * to the appropriate partition resulting in some load-balancing between reducers.
     * For example if taxi number is 450 and there are 21 reducers the key-value pair will be
     * assigned to the partition 450%21 = 9 ==> partition and reducer number 9
     * This ensures that all the segments positions of a specific taxi are assigned in ascending order to the same reducer
     */
    public static class SegmentPartitioner extends
            Partitioner<Text, NullWritable> {
        @Override
        public int getPartition(Text key, NullWritable value, int numReduceTasks) {
            // check if number of reducer is 0 in which case assign key-value pair to partition 0
            if (numReduceTasks == 0) {
                return 0;
            }
            // split the received key in an array of strings
            String[] str = key.toString().split(",", -1);
            // the first parameter is the taxi number
            int taxiNumber = Integer.parseInt(str[0]);
            // return the partition id by using the modulo of
            // the division of the taxi number by the number of reducers
            return taxiNumber % numReduceTasks;
        }
    }

    /**
     * The segment Reducer will receive from the segment Partitioner all the positions of one taxi
     * in ascending order in <Text,NullWritable> key-value pairs like this :
     * <"450,2008-05-25 09:13:44,37.61661,-122.38425,E",NullWritable>
     * <"450,2008-05-25 09:14:32,37.61799,-122.38607,M",NullWritable>
     * <"450,2008-05-25 09:14:47,37.61798,-122.38606,M",NullWritable>
     * <"450,2008-05-25 09:16:01,37.61799,-122.38608,M",NullWritable>
     * <"450,2008-05-25 09:16:58,37.61611,-122.38888,M",NullWritable>
     * <"450,2008-05-25 09:17:00,37.61506,-122.39206,E",NullWritable>
     * and based on this sequence of points and the status change it will identify the beginning
     * and the end of each trip.
     * It will perform some sanity checks like :
     * the speed between two points, the time interval between points, the minimum trip distance
     * the maximum trip time, the minimum trip speed
     * It will also check if any of the trip positions is within the radius if SFO airport and calculate
     * the total distance and total revenue of the trip.
     * The reducer will only output <Text,NullWritable> key-value pairs for the airport trips like this example :
     * <"450 1211706872.0 37.61799 -122.38607 1211707018.0 37.61611 -122.38888 true 0.327 4.06 2008-05-25",NullWritable>
     * and the Record Writer will automatically  write in the output file the string :
     * "450 1211706872.0 37.61799 -122.38607 1211707018.0 37.61611 -122.38888 true 0.327 4.06 2008-05-25"
     */
    public static class SegmentReducer
            extends Reducer<Text, NullWritable, Text, NullWritable> {

        // initialise variables for the previous taxi position to a fictive empty position
        private int previousTaxiNumber = -1; // fictive taxi number
        private String previousStatus = "E";
        private double previousLat = 0.0;
        private double previousLong = 0.0;
        private double previousTime = 0.0;
        // initialise a variable to collect all the tax trip properties
        private TaxiTrip currentTrip = new TaxiTrip();

        public void reduce(Text key, Iterable<NullWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

            // split the received key representing the taxi position in an array of strings
            String[] positionParams = key.toString().split(",", -1);
            int currentTaxiNumber = Integer.parseInt(positionParams[0]);
            double currentTime = strTimeToSystemTime(positionParams[1]);
            double currentLat = Double.parseDouble(positionParams[2]);
            double currentLong = Double.parseDouble(positionParams[3]);
            String currentStatus = positionParams[4];
            // compute the difference between timestamps of the two segment positions
            double segmentDeltaTime = currentTime - previousTime;

            // TRANSITION E TO M
            // if we load someone in the taxi initialise a new trip with current position
            // as starting point
            if (previousStatus.equals("E") && currentStatus.equals("M")) {
                currentTrip.taxiNumber = currentTaxiNumber;
                currentTrip.startTime = currentTime;
                currentTrip.startLat = currentLat;
                currentTrip.startLong = currentLong;
                currentTrip.isAirportTrip = checkIsAirportTrip(currentLat, currentLong); // check if start position is in radius of SFO airport
                currentTrip.cumulatedDistance = 0.0;
                currentTrip.stopTime = 0.0;
                currentTrip.stopLat = 0.0;
                currentTrip.stopLong = 0.0;
                currentTrip.startDate = positionParams[1]; // Date and time in human readable form "2008-05-25 09:13:44"
            }

            // TRANSITION M TO M - NORMAL CASE
            // if same taxi is still loaded and there is no too big time interval between the segment positions
            if (previousStatus.equals("M") && currentStatus.equals("M") && (currentTaxiNumber == previousTaxiNumber) && (segmentDeltaTime <= MAX_SEGMENT_DELTA_TIME)) {
                // compute the speed between the two segment positions
                double segmentSpeed = getSphericalEarthDistance(previousLat, previousLong, currentLat, currentLong) / (segmentDeltaTime / 3600);
                // check if speed is under the maximum limit
                if (segmentSpeed <= MAX_SEGMENT_SPEED) {
                    // then update the end position of the trip, check if current position is in radius of SFO airport
                    // and update the cumulated distance
                    currentTrip.stopTime = currentTime;
                    currentTrip.stopLat = currentLat;
                    currentTrip.stopLong = currentLong;
                    currentTrip.isAirportTrip = currentTrip.isAirportTrip || checkIsAirportTrip(currentLat, currentLong);
                    currentTrip.cumulatedDistance += getSphericalEarthDistance(previousLat, previousLong, currentLat, currentLong);
                } else {
                    // speed is too high, ignore current position
                    return;
                }
            }

            // TRANSITION M TO M - EXCEPTION CASE
            // if status indicates that the taxi is still loaded but the taxi number changed OR the time interval between positions is too big
            // then we need to save the current trip and initialise a new trip
            if (previousStatus.equals("M") && currentStatus.equals("M") && ((currentTaxiNumber != previousTaxiNumber) || (segmentDeltaTime > MAX_SEGMENT_DELTA_TIME))) {
                // the trip needs to have a minimal cumulated distance to be valid
                if (currentTrip.cumulatedDistance >= MIN_TRIP_DISTANCE) {
                    // update the trip end position on the previous known valid position, calculate the trip revenue
                    currentTrip.stopTime = previousTime;
                    currentTrip.stopLat = previousLat;
                    currentTrip.stopLong = previousLong;
                    currentTrip.tripRevenue = START_FEE + currentTrip.cumulatedDistance * KM_FEE;
                    // check if trip is and SFO airport trip and output a <Text,NullWritable> key-value pair
                    // the key respects a similar format as the one of the reference 2010_03.trips file
                    if (currentTrip.isAirportTrip) {
                        String str_key = currentTrip.taxiNumber + " " + String.format("%.1f", currentTrip.startTime) + " " + currentTrip.startLat + " " + currentTrip.startLong
                                + " " + String.format("%.1f", currentTrip.stopTime) + " " + currentTrip.stopLat + " " + currentTrip.stopLong
                                + " " + currentTrip.isAirportTrip + " " + String.format("%.3f", currentTrip.cumulatedDistance)
                                + " " + String.format("%.2f", currentTrip.tripRevenue)
                                + " " + currentTrip.startDate.substring(0, 10);
                        context.write(new Text(str_key), NullWritable.get());
                    }
                }
                // as either the taxi number changed OR the time interval between positions is too big we
                // need to initialize a new the trip with the current segment position
                currentTrip = new TaxiTrip();
                currentTrip.taxiNumber = currentTaxiNumber;
                currentTrip.startTime = currentTime;
                currentTrip.startLat = currentLat;
                currentTrip.startLong = currentLong;
                currentTrip.isAirportTrip = checkIsAirportTrip(currentLat, currentLong);
                currentTrip.cumulatedDistance = 0.0;
                currentTrip.stopTime = 0.0;
                currentTrip.stopLat = 0.0;
                currentTrip.stopLong = 0.0;
                currentTrip.startDate = positionParams[1];
            }

            // TRANSITION M TO E
            // check if taxi trip came to an end, save it and reset the trip variable
            if (previousStatus.equals("M") && currentStatus.equals("E")) {
                // the trip needs to have a minimal distance to be valid
                if (currentTrip.cumulatedDistance >= MIN_TRIP_DISTANCE) {
                    // update the trip end position on the previous known valid position, calculate the trip revenue
                    currentTrip.stopTime = previousTime;
                    currentTrip.stopLat = previousLat;
                    currentTrip.stopLong = previousLong;
                    currentTrip.tripRevenue = START_FEE + currentTrip.cumulatedDistance * KM_FEE;
                    // check if trip is and SFO airport trip and output a <Text,NullWritable> key-value pair
                    // the key respects a similar format as the one of the reference 2010_03.trips file
                    if (currentTrip.isAirportTrip) {
                        String str_key = currentTrip.taxiNumber + " " + String.format("%.1f", currentTrip.startTime) + " " + currentTrip.startLat + " " + currentTrip.startLong
                                + " " + String.format("%.1f", currentTrip.stopTime) + " " + currentTrip.stopLat + " " + currentTrip.stopLong
                                + " " + currentTrip.isAirportTrip + " " + String.format("%.3f", currentTrip.cumulatedDistance)
                                + " " + String.format("%.2f", currentTrip.tripRevenue)
                                + " " + currentTrip.startDate.substring(0, 10);
                        context.write(new Text(str_key), NullWritable.get());
                    }
                }
                // reset the trip
                currentTrip = new TaxiTrip();
            }

            // finally preserve current segment position parameters to have the necessary data
            // to compute the next trip segment
            previousTaxiNumber = currentTaxiNumber;
            previousLat = currentLat;
            previousLong = currentLong;
            previousTime = currentTime;
            previousStatus = currentStatus;
        }
    }

    /**
     * The revenue Mapper will read the output of the previous job containing all airport trips
     * in the form of a Text input like :
     * "450 1211706872.0 37.61799 -122.38607 1211707018.0 37.61611 -122.38888 true 0.327 4.06 2008-05-25"
     * The revenue Mapper will simply retrieve the date and the revenue of each trip and generate an output
     * <Text,DoubleWritable> key-value pair like:
     * <"2008-05-25",4.06>
     */
    public static class RevenueMapper
            extends Mapper<Object, Text, Text, DoubleWritable> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] tripParams = value.toString().split(" ", -1);
            String tripDate = tripParams[10];
            double revenue = Double.parseDouble(tripParams[9]);
            // write out the <Text,DoubleWritable> key-value pair
            context.write(new Text(tripDate), new DoubleWritable(revenue));
        }
    }

    /**
     * The revenue Reducer receives from the trip Mapper input in the form of a <Text,DoubleWritable> key-value pairs like  :
     * <"2008-05-25",{4.06,37.78,38.48}>
     * The revenue Combiner and revenue Reducer will sum up the revenue by date and output
     * another <Text,DoubleWritable> key-value pair containing the sum of revenue like  :
     * <"2008-05-25",80.32>
     */
    public static class RevenueReducer
            extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            double sum = 0;
            // sum up the revenues provided by Mapper or Combiner by date
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            // write out the date as key and the sum in a <Text,DoubleWritable> pair
            context.write(key, new DoubleWritable(sum));
        }
    }


    public static void main(String[] args) throws Exception {

        Configuration job1Configuration = new Configuration();
        // get the framework name to determine if we are running locally or on a cluster
        String frameworkName = job1Configuration.get("mapreduce.framework.name");
        System.err.println("JOB1 : mapreduce.framework.name " + job1Configuration.get("mapreduce.framework.name"));
        int runningNodes = 0;
        if (frameworkName != null && !frameworkName.isEmpty()) {
            System.err.println("==============================================================");
            System.err.println("RUNNING LOCALLY ON ONE NODE");
            System.err.println("==============================================================");
            runningNodes = 1;
        } else {
            // get the number of running nodes in the cluster
            YarnClient yarnClient = YarnClient.createYarnClient();
            Configuration clusterConfiguration = new YarnConfiguration();
            yarnClient.init(new YarnConfiguration(clusterConfiguration));
            yarnClient.start();
            try {
                List<NodeReport> reports = yarnClient.getNodeReports(NodeState.RUNNING);
                runningNodes = reports.size();
                System.err.println("==============================================================");
                System.err.println("RUNNING NODES IN THE CLUSTER : " + runningNodes);
                System.err.println("==============================================================");

            } catch (Exception ex) {
                System.err.println(ex.getMessage());
            }
            yarnClient.stop();
        }

        // job1 will process the segments file and reconstruct the trip and finally only output the
        // airport trips with their start and end positions, the trip distance, the trip revenue and the trip date
        // printout the most important parameters for the job performance
        System.err.println("JOB1 DEFAULT CONFIGURATION VALUES");
        System.err.println("==============================================================");
        System.err.println("JOB1 : mapreduce.map.memory.mb " + job1Configuration.get("mapreduce.map.memory.mb"));
        System.err.println("JOB1 : mapreduce.map.cpu.vcores " + job1Configuration.get("mapreduce.map.cpu.vcores"));
        System.err.println("JOB1 : mapreduce.reduce.memory.mb " + job1Configuration.get("mapreduce.reduce.memory.mb"));
        System.err.println("JOB1 : mapreduce.reduce.cpu.vcores " + job1Configuration.get("mapreduce.reduce.cpu.vcores"));
        System.err.println("JOB1 : mapreduce.reduce.shuffle.input.buffer.percent " + job1Configuration.get("mapreduce.reduce.shuffle.input.buffer.percent"));
        System.err.println("JOB1 : mapreduce.reduce.shuffle.memory.limit.percent " + job1Configuration.get("mapreduce.reduce.shuffle.memory.limit.percent"));
        System.err.println("JOB1 : mapreduce.reduce.shuffle.parallelcopies " + job1Configuration.get("mapreduce.reduce.shuffle.parallelcopies"));
        System.err.println("JOB1 : mapreduce.tasktracker.map.tasks.maximum " + job1Configuration.get("mapreduce.tasktracker.map.tasks.maximum"));
        System.err.println("JOB1 : mapreduce.tasktracker.reduce.tasks.maximum " + job1Configuration.get("mapreduce.tasktracker.reduce.tasks.maximum"));
        System.err.println("JOB1 : mapred.child.java.opts " + job1Configuration.get("mapred.child.java.opts"));
        System.err.println("JOB1 : mapreduce.job.heap.memory-mb.ratio " + job1Configuration.get("mapreduce.job.heap.memory-mb.ratio"));
        System.err.println("JOB1 : yarn.scheduler.minimum-allocation-mb " + job1Configuration.get("yarn.scheduler.minimum-allocation-mb"));
        System.err.println("JOB1 : yarn.scheduler.maximum-allocation-mb " + job1Configuration.get("yarn.scheduler.maximum-allocation-mb"));
        System.err.println("JOB1 : yarn.nodemanager.resource.memory-mb " + job1Configuration.get("yarn.nodemanager.resource.memory-mb"));

        // set the requested memory size for the mapper and reducer containers for job1
        String job1MapMemory = "512"; // allows up to 3 job1 mapper containers per node
        String job1ReduceMemory = "768"; // allows up to 2 job1 reducer containers per node
        // modify the default values to allow more containers to run in parallel
        job1Configuration.set("mapreduce.map.memory.mb", job1MapMemory);
        job1Configuration.set("mapreduce.reduce.memory.mb", job1ReduceMemory);
        // about 48% of memory is for the heap, value was modified as sometimes with the default of 0.8 some
        // containers were crashing. Safe limits were found to be between 0.35 and 0.6, we set it to the middle 0.48
        job1Configuration.set("mapreduce.job.heap.memory-mb.ratio", "0.48");

        System.err.println("==============================================================");
        System.err.println("JOB1 MODIFIED CONFIGURATION VALUES");
        System.err.println("==============================================================");
        System.err.println("JOB1 : mapreduce.map.memory.mb ==> " + job1Configuration.get("mapreduce.map.memory.mb"));
        System.err.println("JOB1 : mapreduce.reduce.memory.mb ==> " + job1Configuration.get("mapreduce.reduce.memory.mb"));
        System.err.println("JOB1 : mapreduce.job.heap.memory-mb.ratio ==> " + job1Configuration.get("mapreduce.job.heap.memory-mb.ratio"));

        System.err.println("==============================================================");
        System.err.println("JOB1 MAPPERS AND REDUCERS");
        System.err.println("==============================================================");
        long maxContainerMemory = Math.min(Long.parseLong(job1Configuration.get("yarn.nodemanager.resource.memory-mb")), Long.parseLong(job1Configuration.get("yarn.scheduler.maximum-allocation-mb")));
        System.err.println("JOB1 : Maximum container size is : " + maxContainerMemory + "MB");
        FileSystem fs = FileSystem.get(job1Configuration); //variable for file system access
        System.err.println("JOB1 : Input file block size " + fs.getDefaultBlockSize(new Path(args[0])));
        System.err.println("JOB1 : Input file size " + fs.getFileStatus(new Path(args[0])).getLen());

        long maxMapContainers = maxContainerMemory / Long.parseLong(job1MapMemory); // number of mappers fitting in memory
        long maxReducerContainers = maxContainerMemory / Long.parseLong(job1ReduceMemory); // number of reducers fitting in memory
        long fsBlockSize = fs.getDefaultBlockSize(new Path(args[0])); // get the block size of the filesystem of the input file
        long inputFileSize = fs.getFileStatus(new Path(args[0])).getLen(); // get the length in bytes of the input file
        long calculatedNumberMappers = runningNodes * maxMapContainers - 2; // calculate the ideal number of mappers
        long calculatedSplitSize = (long) (Math.ceil((double) inputFileSize / (double) calculatedNumberMappers / (double) fsBlockSize) * fsBlockSize); // Best split
        int calculatedReducers = runningNodes * (int) maxReducerContainers - 2;
        long job1Mappers = 0;
        // if we did not specify the split size in the command line then it is the one computed based on number of nodes
        long job1SplitSize = 0L;
        if (args.length >= 5) {
            job1SplitSize = Long.parseLong(args[4]);
            job1Mappers = (int) Math.ceil((double) inputFileSize / (double) job1SplitSize);
            System.err.println("JOB1 : Split size set from command line : " + job1SplitSize + " will start " + job1Mappers + " Mappers");
            System.err.println("JOB1 : Calculated split Size would have been " + calculatedSplitSize + " and would have started " + calculatedNumberMappers + " Mappers");
        } else {
            job1SplitSize = calculatedSplitSize;
            job1Mappers = calculatedNumberMappers;
            System.err.println("JOB1 : Using calculated split Size " + job1SplitSize + " for " + job1Mappers + " Mappers");
        }
        // if we did not specify the number of reducers
        int job1Reducers = 0;
        if (args.length >= 4) {
            job1Reducers = Integer.parseInt(args[3]);
            System.err.println("JOB1 : Number of Reducers set from command line : " + job1Reducers);
            System.err.println("JOB1 : Calculated number of Reducers would have been " + calculatedReducers);
        } else {
            job1Reducers = calculatedReducers;
            System.err.println("JOB1 : Using calculated number of Reducers " + job1Reducers);
        }
        System.err.println("JOB1 : STARTING WITH " + job1Mappers + " MAPPERS AND " + job1Reducers + " REDUCERS");
        System.err.println("==============================================================");
        long job1StartTime = System.currentTimeMillis(); // initialize variable to measure runtime
        Job job1 = Job.getInstance(job1Configuration, "job1-ReconstructTrips");
        job1.setNumReduceTasks(job1Reducers); // set the number of reducers
        job1.setJarByClass(AirportTripsRevenue.class);
        FileInputFormat.addInputPath(job1, new Path(args[0])); // input path
        FileInputFormat.setMinInputSplitSize(job1, job1SplitSize); // the min split size which will define number of mappers
        FileOutputFormat.setOutputPath(job1, new Path(args[1])); // output path
        job1.setMapperClass(SegmentMapper.class); // mapper will do GPS validity checks and split records
        job1.setCombinerClass(SegmentCombiner.class); // combiner will remove duplicates
        job1.setPartitionerClass(SegmentPartitioner.class); // partitioner will send positions to specific reducer
        job1.setReducerClass(SegmentReducer.class); // reducer reconstructs the trips based on status changes
        job1.setOutputKeyClass(Text.class); // output key is a string
        job1.setOutputValueClass(NullWritable.class); // no values generated by this job
        int job1Status = job1.waitForCompletion(true) ? 0 : 1; // wait job completion
        long job1EndTime = System.currentTimeMillis(); // timestamp when job end
        System.err.println("==============================================================");
        System.err.println("JOB1 : " + job1.getJobID().toString() + " Status : " + job1Status);
        System.err.println("JOB1 : Runtime " + (job1EndTime - job1StartTime) / 1000.0 + " seconds");
        System.err.println("==============================================================");

        // if job1 failed stop the execution, else move on to job2
        if (job1Status == 0) {
            // job2 will process the output of job1 and aggregate by day the revenue of taxi airport trips
            Configuration job2Configuration = new Configuration();
            String job2MapMemory = "768"; // allows up to 2 job2 mapper containers per node
            String job2ReduceMemory = "1536"; // use maximum of memory for the unique reducer in job2
            job2Configuration.set("mapreduce.map.memory.mb", job2MapMemory);
            job2Configuration.set("mapreduce.reduce.memory.mb", job2ReduceMemory);
            System.err.println("==============================================================");
            System.err.println("JOB2 MODIFIED CONFIGURATION VALUES");
            System.err.println("==============================================================");
            System.err.println("JOB2 : mapreduce.map.memory.mb ==> " + job2Configuration.get("mapreduce.map.memory.mb"));
            System.err.println("JOB2 : mapreduce.reduce.memory.mb ==> " + job2Configuration.get("mapreduce.reduce.memory.mb"));
            System.err.println("==============================================================");
            Job job2 = Job.getInstance(job2Configuration, "job2-CalculateRevenue");
            job2.setJarByClass(AirportTripsRevenue.class);
            FileInputFormat.addInputPath(job2, new Path(args[1])); // input path is output path of job1
            FileInputFormat.setMinInputSplitSize(job2, job1SplitSize); // set split size as in job1 to be sure not split any if the intermediate files
            FileOutputFormat.setOutputPath(job2, new Path(args[2])); // Output path for job2
            job2.setMapperClass(RevenueMapper.class); // mapper retrieves date and airport trip revenue
            job2.setCombinerClass(RevenueReducer.class); // combiner sums up revenues by date received from mapper
            job2.setReducerClass(RevenueReducer.class); // reducer sums up revenues by date received from combiner
            job2.setOutputKeyClass(Text.class); // output key is a string representind the date
            job2.setOutputValueClass(DoubleWritable.class); // output value is a double with the total revenue of airport trips on a specific date
            int job2Status = job2.waitForCompletion(true) ? 0 : 1; // wait on job completion
            long job2EndTime = System.currentTimeMillis(); // get timestamp for the end of job2
            System.err.println("==============================================================");
            System.err.println("JOB2 : " + job2.getJobID().toString() + " Status : " + job2Status);
            System.err.println("JOB2 : Runtime " + (job2EndTime - job1EndTime) / 1000.0 + " seconds");
            System.err.println("==============================================================");
            System.out.println("TOTAL RUNTIME : " + (job2EndTime - job1StartTime) / 1000.0 + " seconds");
            // check if job2 was successful
            if (job2Status == 0) {
                double revenueAirportTrips = 0; // Total airport trips revenue
                // open the output file to compute the total revenue
                FSDataInputStream inputStream = null;
                // Hadoop DFS Path - Input file
                Path revenueFile = new Path(args[2] + "/part-r-00000");
                // Check if input is valid
                if (!fs.exists(revenueFile)) {
                    System.out.println("File not found " + args[2] + "/part-r-00000");
                    throw new IOException("Input file not found");
                } else {
                    // open and read from file
                    inputStream = fs.open(revenueFile);
                    BufferedReader bufferedReader = new BufferedReader(
                            new InputStreamReader(inputStream, StandardCharsets.UTF_8));
                    String line = null;
                    // loop over each line and add the daily revenue of airport trips
                    while ((line = bufferedReader.readLine()) != null) {
                        String[] str = line.split("\t", -1);
                        revenueAirportTrips += Double.parseDouble(str[1]);
                    }
                    inputStream.close();
                }
                fs.close();
                System.err.println("==============================================================");
                System.err.println("TOTAL AIRPORT TRIPS REVENUE : " + String.format("%.2f", revenueAirportTrips));
                System.err.println("==============================================================");
            } else {
                System.err.println("==============================================================");
                System.err.println("JOB2 : FAILED ==> EXITING PROGRAM");
                System.err.println("==============================================================");
                System.exit(job2Status);
            }
        } else {
            System.err.println("==============================================================");
            System.err.println("JOB1 : FAILED ==> EXITING PROGRAM");
            System.err.println("==============================================================");
            System.exit(job1Status);
        }
    }
}
