import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import de.tudarmstadt.ukp.dkpro.core.api.syntax.type.chunk.Chunk;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.CASException;
import org.apache.uima.fit.factory.AggregateBuilder;
import org.apache.uima.fit.factory.TypeSystemDescriptionFactory;
import org.apache.uima.fit.pipeline.SimplePipeline;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.resource.ResourceInitializationException;

import opennlp.tools.cmdline.PerformanceMonitor;

public class StandardPipeline {

  public static final boolean REUSE_CAS = true;


  public static void main(String[] args) throws ResourceInitializationException, AnalysisEngineProcessException, CASException, InterruptedException {

    long start = System.nanoTime();
    AggregateBuilder builder = new AggregateBuilder();
    for (AnalysisEngineDescription aed: UimaTestPipeline.createAED()) {
      builder.add(aed);
    }

    AnalysisEngine aggregatedAE = builder.createAggregate();

    CASFactory casFactory = new CASFactory(TypeSystemDescriptionFactory.createTypeSystemDescription());

    try (BufferedReader br = new BufferedReader(new InputStreamReader(
        StandardPipeline.class.getResourceAsStream(UimaTestPipeline.INPUT), "UTF-8"))) {

      PerformanceMonitor pf = new PerformanceMonitor("docs");
      pf.startAndPrintThroughput();

      String line = br.readLine();
      CAS cas = null;
      while (line != null) {
        line = line.substring(line.indexOf("\t") + 1);
        if (REUSE_CAS) {
          if (cas == null) {
            cas = casFactory.createEmptyCAS();
          }
        } else {
          cas = casFactory.createEmptyCAS();
        }

        cas.setDocumentLanguage("en");
        cas.setDocumentText(line);

        SimplePipeline.runPipeline(cas, aggregatedAE);

        Collection<Chunk> chunks = JCasUtil.select(cas.getJCas(), Chunk.class);
        List<String> chunksStrList = new ArrayList<>();
        for (Chunk c: chunks) {
          chunksStrList.add(c.getCoveredText());
        }
//        System.out.println("Chunks found " + Arrays.toString(chunksStrList.toArray()));


        pf.incrementCounter();
        if (REUSE_CAS) {
          cas.reset();
        }
        line = br.readLine();
      }

      pf.stopAndPrintFinalResult();

      System.out.println("STD processed " + UimaTestPipeline.INPUT + " in " +
          TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start) + " seconds");
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

}
