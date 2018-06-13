import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class CactusPlot {

	static class FirstMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			// SALTO LA PRIMA RIGA
			if (key.get() == 0)
				return;

			// UTILIZZO L'ALGORITMO COME CHIAVE, E COME VALORE CONCATENO TUTTI I VALORI DI
			// CUI HO BISOGNO
			String[] stringArray = value.toString().split("\t");
			context.write(new Text(stringArray[0]),
					new Text(stringArray[11] + ";" + stringArray[14] + ";" + stringArray[9]));

		}

	}

	static class FirstReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			// NEL REDUCER PER OGNI CHIAVE SCORRI I VALORI E PRODUCO IN OUTPUT
			// PER OGNI CHIAVE UNA LISTA DI VALORI CHE SONO I TEMPI "VALIDI"
			/// QUINDI ISTANZA RISOLTA E TIME-LIMIT NON SUPERATO
			String ciao = "";
			for (Text t : values) {
				String[] split = t.toString().split(";");
				if (split[1].equals("solved")) {
					if (Double.valueOf(split[2]) > Double.valueOf(split[0]))
						ciao = ciao + split[0] + ";";

				}
			}

			if (ciao != "")
				context.write(key, new Text(ciao));

		}

	}

	static class SecondMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

		// ORDINO I VALORI CHE ARRIVANO DAL JOB PRECEDENTE
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {

			String[] valueeee = value.toString().split("\t");

			String[] first = valueeee[1].split(";");
			List<Double> orderedList = new ArrayList<>();

			for (String s : first) {
				double l = Double.valueOf(s);
				if (orderedList.isEmpty())
					orderedList.add(l);

				for (int i = 0; i < orderedList.size(); i++) {
					if (orderedList.get(i) > l) {
						orderedList.add(i, l);
						break;
					}

				}
			}

			String s = "";
			for (Double l : orderedList)
				s += String.valueOf(l) + ";";

			// TRASFORMO RIGHE IN COLONNE E VICEVERSA
			long index = 0;
			String[] split = s.split(";");
			for (String s1 : split) {
				context.write(new LongWritable(index), new Text(valueeee[0] + "#" + s1));
				index++;
			}

		}

	}

	static class SecondReducer extends Reducer<LongWritable, Text, Text, Text> {

		private Map<Text, List<Text>> table = new HashMap<>();

		@Override
		protected void cleanup(Reducer<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			
			//MANDO IN OUTPUT LA TABELLA 
			int index = -1;
			String row = "";
			List<Integer> maxSize = new ArrayList<>();

			for (Text t : table.keySet()) {
				row += t + "\t";
				maxSize.add(table.get(t).size());
			}

			int max = -1;
			for (Integer i : maxSize) {
				if (i > max)
					max = i;
			}

			context.write(new Text("# ist"), new Text(row));
			row = "";

			int counter = 0;

			while (counter < max) {
				for (Text t : table.keySet()) {
					if (table.get(t).size() <= counter)
						table.get(t).add(new Text("*"));
					row += table.get(t).get(counter)+"\t";
					
				}
				counter++;
				index++;
				context.write(new Text(String.valueOf(index)), new Text(row));
				row = "";
			}

			
			
		}

		@Override
		protected void reduce(LongWritable arg0, Iterable<Text> arg1,
				Reducer<LongWritable, Text, Text, Text>.Context arg2) throws IOException, InterruptedException {

			// COSTRUISCO UN'HASH MAP CHE SERVIRA' PER STAMPARE LA TABELLA
			//IN MODO ORDINATO
			for (Text t : arg1) {
				String[] split = t.toString().split("#");
				Text text = new Text(split[0]);
				if (!table.containsKey(text))
					table.put(text, new ArrayList<>());

				table.get(text).add(new Text(split[1]));

			}
		}

	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();

		String[] myArgs = new GenericOptionsParser(args).getRemainingArgs();

		Job firstJob = Job.getInstance(conf, "fj");
		firstJob.setOutputKeyClass(Text.class);
		firstJob.setOutputValueClass(Text.class);
		firstJob.setMapOutputKeyClass(Text.class);
		firstJob.setMapOutputValueClass(Text.class);
		firstJob.setMapperClass(FirstMapper.class);
		firstJob.setReducerClass(FirstReducer.class);

		ControlledJob cj1 = new ControlledJob(conf);
		cj1.setJob(firstJob);

		FileInputFormat.setInputPaths(firstJob, new Path(myArgs[0]));
		FileOutputFormat.setOutputPath(firstJob, new Path(myArgs[1]));

		Job job2 = Job.getInstance(conf, "2");
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.setMapOutputKeyClass(LongWritable.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setMapperClass(SecondMapper.class);
		job2.setReducerClass(SecondReducer.class);

		FileInputFormat.setInputPaths(job2, new Path(myArgs[1]));
		FileOutputFormat.setOutputPath(job2, new Path(myArgs[2]));

		ControlledJob cj2 = new ControlledJob(conf);
		cj2.setJob(job2);

		cj2.addDependingJob(cj1);
		JobControl jcl = new JobControl("group");
		jcl.addJob(cj1);
		jcl.addJob(cj2);

		
		//NON E' IL MODO MIGLIORE DI FARLO, GIURO CHE NEL PROGETTO LO FARO' PER BENE
		Thread t = new Thread(jcl);
		t.start();

		while (!jcl.allFinished()) {
			System.out.println("still running");
			Thread.sleep(1000);
		}
		System.exit(0);

	}

}
