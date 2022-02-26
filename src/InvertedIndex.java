package ssafy;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem;

public class InvertedIndex {
	/* 
	Object, Text : input key-value pair type (always same (to get a line of input file))
	Text, IntWritable : output key-value pair type
	*/
	public static class TokenizerMapper
			extends Mapper<Object,Text,Text,Text> {

		// variable declairations
		private Text word = new Text(); // key로 사용할 변수
		private Text pos = new Text(); // value로 사용할 변수
		private String filename; // 현재 읽는 파일 이름을 가져올 변수
		
		protected void setup(Context context) throws IOException, InterruptedException {
			// 현재 사용하는 파일 이름을 뽑아서 저장
			filename = ((FileSplit)context.getInputSplit()).getPath().getName();
		}

		// map function (Context -> fixed parameter)
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			// value.toString() : get a line
			StringTokenizer itr = new StringTokenizer(value.toString(), " ", true); // 단어 단위로 자르기
			long p = ((LongWritable)key).get(); // 현재 위치가 시작 지점에서 몇 byte인가 표시
			while ( itr.hasMoreTokens() ) {
				String token = itr.nextToken();
				word.set(token.trim());
				if(! token.equals(" ")){
					pos.set(filename +":" + p);
					context.write(word, pos);
				}
				p += token.length() + filename.length() + 2;	// 현재 위치에 단어길이 +delimeiter를 더함
			}
		}
	}

	/*
	Text, Text : input key type and the value type of input value list
	Text, Text : output key-value pair type
	*/
	public static class IntSumReducer
			extends Reducer<Text,Text,Text,Text> {
		private Text list = new Text();
		
		// key : a disticnt word
		// values :  Iterable type (data list)
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {

			String s = new String();
			int comma = 0;

			for ( Text val : values ) { // list의 값들을 하나씩 수행
				if(comma == 0){
					comma = 1;
					s += (":" + val.toString());
				}else{
					s += (",	" + val.toString());
				}
			}
			list.set(s);
			context.write(key,list);
		}
	}


	/* Main function */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		if ( otherArgs.length != 2 ) {
			System.err.println("Usage: <in> <out>");
			System.exit(2);
		}

		// output 디렉토리를 자동으로 삭제
		FileSystem hdfs = FileSystem.get(conf);
		Path output = new Path(otherArgs[1]);
		if(hdfs.exists(output))
			hdfs.delete(output, true);

		Job job = new Job(conf,"inverted index"); // job 작성, 따옴표안은 설명을 쓰면됨(상관없음)
		job.setJarByClass(InvertedIndex.class); // job을 수행할 class 선언, 파일명.class, 대소문자주의

		// let hadoop know my map and reduce classes
		job.setMapperClass(TokenizerMapper.class);	// Map class 선언, 위에서 작성한 class명
		job.setReducerClass(IntSumReducer.class);	// Reduce class 선언

		job.setOutputKeyClass(Text.class);			// Output key type 선언
		job.setOutputValueClass(Text.class);		// Output value type 선언

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

