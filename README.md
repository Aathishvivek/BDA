BigDataAnalytics-DA2
Project Overview
The Hadoop MapReduce program named DiscountCalculator processes customer purchase data, calculates discounted prices for purchases greater than 400, and outputs the original and discounted prices along with the customer name and ID.

Code
Java MapReduce Code for Discount Calculation

java
Copy code
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DiscountCalculator {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            if (tokens.length >= 3) {
                String customerId = tokens[1];
                String customerName = tokens[0];
                int originalPrice = Integer.parseInt(tokens[2]);
                int discountedPrice = originalPrice;
                if (originalPrice > 400) {
                    // Apply 10% discount
                    discountedPrice = (int) (originalPrice * 0.9);
                }
                String outputValue = customerName + "," + originalPrice + "," + discountedPrice;
                context.write(new Text(customerId), new Text(outputValue));
            }
        }
    }

    public static class DiscountReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key, value);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "discount calculator");

        job.setJarByClass(DiscountCalculator.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(DiscountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
Input Format
The input file should contain records in the following format:


customer_name,customer_id,original_price
Example Input:


Alice,101,500
Bob,102,300
Charlie,103,200
David,104,450
Eve,105,600
How It Works
Mapper Class (TokenizerMapper):

Reads each line of the input.
Splits the line into customer name, customer ID, and original price.
Applies a 10% discount if the original price is greater than 400.
Emits the customer ID as the key and the customer details (name, original price, and discounted price) as the value.
Reducer Class (DiscountReducer):

Aggregates all values for each customer ID.
Writes out the customer ID and their corresponding details.
Main Method:

Configures and sets up the Hadoop job.
Specifies the input and output paths.
Executes the MapReduce job.
Execution Process
Create Directory in HDFS:


hdfs dfs -mkdir /DA2
Copy Input File from Local to HDFS:


hdfs dfs -put input.csv /DA2/
Run the MapReduce Job:


hadoop jar DiscountCalculator.jar DiscountCalculator /DA2/input.csv /DA2/output
Check Output in HDFS:


hdfs dfs -ls /DA2/output
hdfs dfs -cat /DA2/output/part-r-00000
Sample Output
The data in /DA2/output/part-r-00000 file:

![image](https://github.com/user-attachments/assets/10c7929e-8605-4df2-84a3-179ff6fd00c7)

Expected Output
The output file will contain lines with customer IDs, names, original prices, and discounted prices.
Each line represents a customer's purchase details, showing the applied discount (if any) based on the spending criteria.
This project automates the discount process for customers based on their spending patterns, making it useful for businesses looking to manage discounts efficiently.
