import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import de.tudarmstadt.ukp.dkpro.core.api.syntax.type.chunk.Chunk;
import de.tudarmstadt.ukp.dkpro.core.opennlp.OpenNlpChunker;
import de.tudarmstadt.ukp.dkpro.core.opennlp.OpenNlpPosTagger;
import de.tudarmstadt.ukp.dkpro.core.opennlp.OpenNlpSegmenter;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Meter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.uima.UIMAFramework;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.impl.CASImpl;
import org.apache.uima.fit.factory.ResourceCreationSpecifierFactory;
import org.apache.uima.fit.factory.TypeSystemDescriptionFactory;
import org.apache.uima.fit.internal.ResourceManagerFactory;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.ResourceManager;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngineDescription;

public class UimaTestPipeline {

	public static final String INPUT = "/input/eng_news_2015_100k-sentences.txt";

	private static final Logger LOGGER = LoggerFactory.getLogger("UimaTestPipeline");

  public static void main(String[] args) throws Exception {

  	// Configure environment
  	final StreamExecutionEnvironment env =
		StreamExecutionEnvironment.getExecutionEnvironment();

	env.getConfig().registerTypeWithKryoSerializer(CASImpl.class, KryoCasSerializer.class);
	env.getConfig().enableObjectReuse();

	// Init typesystem
	final TypeSystemDescription typeSystemDesc = TypeSystemDescriptionFactory.createTypeSystemDescription();

	// Read the input
	DataStream<String> inputStream = env.readTextFile(
			UimaTestPipeline.class.getResource(INPUT).getFile());
	DataStream<Tuple2<String, String>> leipzigEntries = inputStream.map(new LeipzigParser());

	// Create the CAS
	DataStream<CAS> casStream = leipzigEntries.map(new RichMapFunction<Tuple2<String, String>, CAS>() {

		private CASFactory casFactory;
		private transient Meter meter;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			casFactory = new CASFactory(typeSystemDesc);

			// this is not working:
			this.meter = getRuntimeContext()
					.getMetricGroup()
					.meter("log4j_reporter", new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));
		}

		@Override
	  public CAS map(Tuple2<String, String> value) throws Exception {
	  	CAS cas;
	  	cas = casFactory.createEmptyCAS();

		cas.setDocumentText(value.f1);
		cas.setDocumentLanguage("en");

		this.meter.markEvent();
		return cas;
	  }
	});
	for (AnalysisEngineDescription aed : createAED()) {
  		casStream = casStream.map(new RichAEDMapFunction(aed));
	}

	// count the chunks
	DataStream<Tuple2<String, Integer>> counts =
	  // get the chunks
	  casStream.map((MapFunction<CAS, String[]>) value -> {
		  Collection<Chunk> chunks = JCasUtil.select(value.getJCas(), Chunk.class);
		  List<String> chunksStrList = new ArrayList<>();
		  for (Chunk c: chunks) {
			  chunksStrList.add(c.getCoveredText());
		  }
		  value.reset();
		  //jCasPool.releaseCAS(value);
//                  casPooledObject.returnObject(value);
		  return chunksStrList.toArray(new String[chunksStrList.size()]);
	  })
			  // convert splitted line in pairs (2-tuples) containing: (word,1)
			  .flatMap(new FlatMapFunction<String[], Tuple2<String, Integer>>() {
				  @Override
				  public void flatMap(String[] chunks, Collector<Tuple2<String, Integer>> out) throws Exception {
					  Arrays.stream(chunks)
							  .filter(t -> t.length() > 0)
							  .forEach(t -> out.collect(new Tuple2<String, Integer>(t, 1)));
				  }
			  })
			  // group by the tuple field "0" and sum up tuple field "1"
			  .keyBy(0)
			  .sum(1);

	  counts.writeAsCsv("output.txt", FileSystem.WriteMode.OVERWRITE);


	  System.out.println("Starting execution...");
	  long start = System.nanoTime();

	env.execute();

	System.out.println("FLINK processed " + INPUT + " in " +
			  TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start) + " seconds");
  }

	public static List<AnalysisEngineDescription> createAED() throws ResourceInitializationException {

		List<AnalysisEngineDescription> aedList = new ArrayList<>();

		aedList.add(createEngineDescription(OpenNlpSegmenter.class));
		aedList.add(createEngineDescription(OpenNlpPosTagger.class));
		aedList.add(createEngineDescription(OpenNlpChunker.class));

		return aedList;
	}

	public static AnalysisEngine createEngine(AnalysisEngineDescription desc,
											  ResourceManager rm) throws ResourceInitializationException {
		AnalysisEngineDescription descClone = (AnalysisEngineDescription) desc.clone();
		ResourceCreationSpecifierFactory.setConfigurationParameters(descClone);
		return UIMAFramework.produceAnalysisEngine(descClone,
				rm, null);
	}

	static class RichAEDMapFunction extends RichMapFunction<CAS, CAS> {

  		// tried to create a shared resourceManager, but it is not working.
  		private static final ResourceManager resourceManager;

  		static {
			ResourceManager _resourceManager = null;
			try {
				_resourceManager = ResourceManagerFactory.newResourceManager();
			} catch (ResourceInitializationException e) {
				throw new RuntimeException(e);
			} finally {
				resourceManager = _resourceManager;
			}

		}

  		private AnalysisEngine ae;
		private final AnalysisEngineDescription aed;

		public RichAEDMapFunction(AnalysisEngineDescription aed) throws ResourceInitializationException {
			this.aed = aed;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			LOGGER.info("### creating new AE " + aed.getAnnotatorImplementationName() + " ###");
			this.ae = createEngine(aed, resourceManager);
		}

		@Override
		public CAS map(CAS s) throws Exception {
			ae.process(s);
			return s;
		}

	}

	private static class LeipzigParser implements MapFunction<String, Tuple2<String, String>> {

		private int id = 0;

		@Override
		public Tuple2<String, String> map(String s) throws Exception {
			return new Tuple2<>(Integer.toString(id++), s.substring(s.indexOf('\t') + 1));
		}
	}
}
