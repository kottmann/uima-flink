import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.typeutils.runtime.kryo.JavaSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.uima.cas.CAS;
import org.apache.uima.resource.metadata.TypeSystemDescription;

public class UimaTestPipeline {

  public static void main(String[] args) throws Exception {
	final StreamExecutionEnvironment env =
		StreamExecutionEnvironment.getExecutionEnvironment();

	env.getConfig()
		.registerTypeWithKryoSerializer(CAS.class, JavaSerializer.class);

	DataStream<String> inputStream = env.readTextFile(
			UimaTestPipeline.class.getResource("/input/eng_news_2015_100K-sentences.txt").getFile());

	TypeSystemDescription typeSystemDesc = UimaUtil.createTypeSystemDescription(
		UimaTestPipeline.class.getResourceAsStream("/TypeSystem.xml"));

	DataStream<CAS> casStream = inputStream.map(new MapFunction<String, CAS>() {

	  @Override
	  public CAS map(String value) throws Exception {
		CAS cas = UimaUtil.createEmptyCAS(typeSystemDesc);
		cas.setDocumentText(value);
		return cas;
	  }
	});

	casStream.map(new MapFunction<CAS, String>() {
	  @Override
	  public String map(CAS value) throws Exception {
		return value.getDocumentText();
	  }
	}).print();


	// map detect tokens

	// count tokens and print them

	env.execute();
  }
}
