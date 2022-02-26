package ssafy;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Wordcount {
	/* 
	Object, Text : input key-value pair type (always same (to get a line of input file))
	Text, IntWritable : output key-value pair type
	*/
	public static class TokenizerMapper
			extends Mapper<Object,Text,Text,IntWritable> {

		// variable declairations
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		// map function (Context -> fixed parameter)
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			// value.toString() : get a line
			StringTokenizer itr = new StringTokenizer(value.toString());
			while ( itr.hasMoreTokens() ) {
				word.set(itr.nextToken());

				// emit a key-value pair
				context.write(word,one);
			}
		}
	}

	/*
	Text, IntWritable : input key type and the value type of input value list
	Text, IntWritable : output key-value pair type
	*/
	public static class IntSumReducer
			extends Reducer<Text,IntWritable,Text,IntWritable> {

		// variables
		private IntWritable result = new IntWritable();

		// key : a disticnt word
		// values :  Iterable type (data list)
		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
				throws IOException, InterruptedException {

			int sum = 0;
			for ( IntWritable val : values ) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key,result);
		}
	}


	/* Main function */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration(); // job 수행하기 위한 설정 초기화
		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		if ( otherArgs.length != 2 ) {
			System.err.println("Usage: <in> <out>");
			System.exit(2);
		}

		Job job = new Job(conf,"word count"); 	// job 작성, 따옴표안은 설명을 쓰면됨(상관없음)
		job.setJarByClass(Wordcount.class);		// job을 수행할 class 선언, 파일명.class, 대소문자주의

		// let hadoop know my map and reduce classes
		job.setMapperClass(TokenizerMapper.class);	// Map class 선언, 위에서 작성한 class명
		job.setReducerClass(IntSumReducer.class);	// Reduce class 선언

		job.setOutputKeyClass(Text.class);			// Output key type 선언
		job.setOutputValueClass(IntWritable.class);	// Output value type 선언

		//job.setMapOutputKeyClass(Text.class);				// Map은 Output key type 선언
		//job.setMapOutputValueClass(IntWritable.class);	// Map은 Output value type 선언

		// set number of reduces
		job.setNumReduceTasks(2);		// 동시에 수행되는 reducer 개수

		// set input and output directories
		FileInputFormat.addInputPath(job,new Path(otherArgs[0]));	// 입력 데이터가 있는 path
		FileOutputFormat.setOutputPath(job,new Path(otherArgs[1]));	// 결과를 출력할 path
		System.exit(job.waitForCompletion(true) ? 0 : 1 );			// 실행
	}
}

