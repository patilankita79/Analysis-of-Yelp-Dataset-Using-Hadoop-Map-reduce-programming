package BigData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;




public class TopRatedBusinessData {
	
	/*
	 *  Job 1
	 */
	
	/*
	 * Job 1 Mapper Class : BRatingMapper
	 * Class BRatingMapper parses review.csv file and emits business id and respective rating
	 */
	public static class BRatingMapper extends Mapper<LongWritable, Text, Text, Text> {
		/*
		 * Map function that emits a business ID as a key and rating as a value
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {

			String reviews[] = value.toString().split("::");
			/*
			 * reviews[2] gives business id and reviews[3] gives business rating
			 */
			context.write(new Text(reviews[2]), new Text(reviews[3]));

		}
	} //Closing of Job 1 - Mapper
	
	/*
	 * Job 1 Reducer Class: TopRatedBusinessReducer
	 * Class TopRatedBusinessReducer emits top 10 business id with their average rating
	 */
	
	public static class TopRatedBusinessReducer extends Reducer<Text, Text, Text, Text> {
			
		/*
		* TreeMap is sorted according to the comparator provided at the map creation time
		*/
			
		private TreeMap<String, Float> map = new TreeMap<>();
			
		/*
		* Providing a comparator to sort TreeMp
		*/
			
		private BusinessRatingComparator businessRatingComparator = new BusinessRatingComparator(map);
			
		/*
		* Sorted treeMap
		*/
		private TreeMap<String, Float> sortedMap = new TreeMap<String, Float>(businessRatingComparator);
			
		/*
		* Reduce function that emits business IDs along with their average rating
		*/

			@Override
			protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

				float sumOfRatings = 0;
				float countOfRatings = 0;
			
				for (Text value : values) {
					sumOfRatings += Float.parseFloat(value.toString());
					countOfRatings++;
				}
			
				float avgerageRating = new Float(sumOfRatings / countOfRatings);
				map.put(key.toString(), avgerageRating);
			}

			/*
			 *TreeMap is sorted according to the comparator provided at the map creation time 
			 * Class BusinessRatingComparator provides this comparator
			 * That is following class sorts TreeMap values by average rating.
			 */
			class BusinessRatingComparator implements Comparator<String> {

				TreeMap<String, Float> mMap;

				public BusinessRatingComparator(TreeMap<String, Float> lMap) {
					this.mMap = lMap;
				}

				public int compare(String a, String b) {
					if (mMap.get(a) >= mMap.get(b)) {
						return -1;
					} else {
						return 1;
					}
				}
			}

			@Override
			protected void cleanup(Context context) throws IOException, InterruptedException {
				sortedMap.putAll(map);

				int countOfRating = 10;
				for (Entry<String, Float> entry : sortedMap.entrySet()) {
					if (countOfRating == 0) {
						break;
					}
					context.write(new Text(entry.getKey()),new Text(String.valueOf(entry.getValue())));
					countOfRating--;
				}
			}
	}  // Closing of Job 1 - Reducer
	
	
	/*
	 *  Job 2
	 */
	
	/*
	 * Job 2, Mapper 1
	 * This  mapper class TopTenRatedBusinessRatingMapper parses the input from reducer of job 1 
	 * This class emits all business Id's and average rating appended with T1
	 */
	public static class TopTenRatedBusinessRatingMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		/*
		 * Map function that emits a business ID as a key and average rating as a value
		 */
		 
		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {

			String line = value.toString().trim();
			String[] detail = line.split("\t");
			String businessID = detail[0].trim();
			String businessRating = detail[1].trim();
			context.write(new Text(businessID), new Text("T1|" + businessID + "|" + businessRating));

		}
	} // Closing of Job 2 - Mapper 1

	/* Job 2, Mapper 2
	 * This mapper class parses the business.csv file
	 * This class emits Business ID as key and entire record or tuple as the value appended with T2
	 */
	public static class AllBusinessDetailsMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		/*
		 * This map function emits Business ID as key and entire record or tuple as the value appended with T2
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String businessData[] = value.toString().split("::");
			context.write(new Text(businessData[0].trim()), new Text("T2|"
					+ businessData[0].trim() + "|" + businessData[1].trim()
					+ "|" + businessData[2].trim()));

		}
	} //Closing of Job 2 - Mapper 2

	/*
	 * Job 2-Reducer
	 *  This reducer class joins the output of TopTenRatedBusinessRatingMapper and AllBusinessDetailsMapper mapper classes
	 * This class  emits the entire tuple of business.csv file and average rating.
	 */
	public static class TopTenRatedBusinessDetailReducer extends Reducer<Text, Text, Text, Text> {

		private ArrayList<String> topTenBusiness = new ArrayList<String>();
		private ArrayList<String> businessDetails = new ArrayList<String>();
		private static String splitter = "\\|";

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			for (Text text : values) {
				String value = text.toString();
				if (value.startsWith("T1")) {
					topTenBusiness.add(value.substring(3));
				} else {
					businessDetails.add(value.substring(3));
				}
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			for (String topBusiness : topTenBusiness) {
				for (String detail : businessDetails) {
					String[] t1Split = topBusiness.split(splitter);
					String t1BusinessId = t1Split[0].trim();

					String[] t2Split = detail.split(splitter);
					String t2BusinessId = t2Split[0].trim();

					if (t1BusinessId.equals(t2BusinessId)) {
						context.write(new Text(t1BusinessId), new Text(
								t2Split[1] + "\t" + t2Split[2] + "\t"
										+ t1Split[1]));
						break;
					}
				}
			}
		}
	} // Closing of Job 2- reducer
	
	/*
	 * Driver Program
	 */
	public static void main(String[] args) throws IOException,ClassNotFoundException, InterruptedException {

		Configuration config1 = new Configuration();
		String[] otherArgs = new GenericOptionsParser(config1, args).getRemainingArgs();

		if (otherArgs.length != 4) {
			System.err.println("Argument problem");
			System.err.println("hadoop jar <jarname> <classname> <input1> <input2> <intermediate_output> <final_output>");
			System.exit(0);
		}

		/*
		 * Creating new job for top rated business data
		 */
		Job job1 = Job.getInstance(config1, "JOB1");
		job1.setJarByClass(TopRatedBusinessData.class);
		job1.setMapperClass(BRatingMapper.class);
		job1.setReducerClass(TopRatedBusinessReducer.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job1, new Path(otherArgs[2]));

		boolean isJob1Completed = job1.waitForCompletion(true);

		if (isJob1Completed) {
			Configuration config2 = new Configuration();
			Job job2 = Job.getInstance(config2, "JOB2");
			job2.setJarByClass(TopRatedBusinessData.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			job2.setInputFormatClass(TextInputFormat.class);
			job2.setOutputFormatClass(TextOutputFormat.class);

			/*
			 * Setting multiple mappers
			 * Here we are setting the two mappers of Job 2
			 */
			MultipleInputs.addInputPath(job2, new Path(args[2]),
		    TextInputFormat.class, TopTenRatedBusinessRatingMapper.class);
	        MultipleInputs.addInputPath(job2, new Path(args[1]),
		    TextInputFormat.class, AllBusinessDetailsMapper.class);

	        /*
	         * Setting a single Reduce Class for job 2
	         */
	        job2.setReducerClass(TopTenRatedBusinessDetailReducer.class);
	        FileOutputFormat.setOutputPath(job2, new Path(args[3]));

	        job2.waitForCompletion(true);
		}
	}


}// closing of main class
