package reconcile.hbase.mapreduce.annotation;

import static reconcile.hbase.table.DocSchema.annotationsCF;
import static reconcile.hbase.table.DocSchema.metaCF;
import static reconcile.hbase.table.DocSchema.textCF;
import static reconcile.hbase.table.DocSchema.textRaw;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import reconcile.Constructor;
import reconcile.DataConstructor;
import reconcile.SystemConfig;
import reconcile.classifiers.Classifier;
import reconcile.clusterers.Clusterer;
import reconcile.clusterers.ThresholdClusterer;
import reconcile.data.Annotation;
import reconcile.data.AnnotationSet;
import reconcile.data.Document;
import reconcile.featureVector.Feature;
import reconcile.featureVector.FeatureWriter;
import reconcile.featureVector.FeatureWriterARFF;
import reconcile.featureVector.FeatureWriterARFFBinarized;
import reconcile.featureVector.individualFeature.DocNo;
import reconcile.features.properties.SentNum;
import reconcile.filter.PairGenerator;
import reconcile.filter.SmartInstanceGenerator;
import reconcile.general.Constants;
import reconcile.general.Utils;
import reconcile.hbase.ReconcileDocument;
import reconcile.hbase.mapreduce.ChainableAnnotationJob;
import reconcile.hbase.mapreduce.JobConfig;
import reconcile.hbase.table.DocSchema;


public class CorefResolver
    extends ChainableAnnotationJob {

public static final String COREF_FEATURES = "coref_features";

public static final String CONFIG_FILE = "Coreference_Config_File";

/**
 *
 * @param args
 *          :
 *          <ol>
 *          <li>optional start row
 *          </ol>
 */
public static void main(String[] args)
{
  try {
    ToolRunner.run(new Configuration(), new CorefResolver(), args);
  }
  catch (Exception e) {
    e.printStackTrace();
  }

}

@Override
public void init(JobConfig jobConfig, Job job, Scan scan)
{
  String startRow = null;
  if (jobConfig.getArgs().length > 0) {
    startRow = jobConfig.getArgs()[0];
  }

  job.getConfiguration().set("mapred.tasktracker.map.tasks.maximum", "5");

  scan.addColumn(textCF.getBytes(), textRaw.getBytes());
  scan.addFamily(annotationsCF.getBytes());
  scan.addFamily(metaCF.getBytes());

  if (startRow != null) {
    scan.setStartRow(startRow.getBytes());
  }
}

@Override
public Class<? extends AnnotateMapper> getMapperClass()
{
  return FGMapper.class;
}

public static class FGMapper
    extends AnnotateMapper {



private static final String MODEL_NAME = "MODEL_NAME";

private static final int MAX_PAIR_COUNT = 1000;

PairGenerator pairGen;

Context context;

SystemConfig cfg;

List<Feature> featureList;

private Classifier learner;

private String[] options;

private Clusterer clusterer;

private String[] clustOptions;

int docNo = 0;

private int putCount = 0;

StringWriter predictionStringOut = new StringWriter();

StringWriter stringOutput = new StringWriter();

public FGMapper() {

  try {
    pairGen = new SmartInstanceGenerator();
  }
  catch (Exception e) {
    e.printStackTrace();
    LOG.info(e.getMessage());
    throw new RuntimeException(e);
  }

}

@Override
public void setup(Context context)
{
  try {
    super.setup(context);
    this.context = context;
    String configFile = context.getConfiguration().get(CONFIG_FILE, "/config.default");
    URL res = Utils.class.getResource(configFile);
    SystemConfig defaultConfig = new SystemConfig(res.toURI().toString());

    Utils.setConfig(defaultConfig);
    cfg = Utils.getConfig();
    String[] featureNames = cfg.getFeatureNames();
    List<String> fList = Lists.newArrayList(featureNames);
    // for (Iterator<String> i = fList.iterator(); i.hasNext();) {
    // String f = i.next();
    // if (f.equals("instClass")) {
    // i.remove();
    // }
    // }

    featureList = DataConstructor.createFeatures(fList.toArray(new String[fList.size()]));

    String modelFN = cfg.getString(MODEL_NAME);
    String classifier = cfg.getClassifier();
    if (classifier == null) throw new RuntimeException("Classifier not specified");
    options = cfg.getStringArray("TesterOptions." + classifier);

    System.out.println("model work dir:" + cfg.getString("WORK_DIR"));
    String fullModelFN = Utils.getWorkDirectory() + "/" + modelFN;
    System.out.println("full model file:" + fullModelFN);
    String modelName = context.getConfiguration().get(MODEL_NAME, "/reconcile.classifiers.PerceptronM.model");
    System.out.println("modelName:" + modelName);

    learner = Constructor.createClassifier(classifier, modelName);
    System.out.println("classifier:" + learner.getName() + ", class:" + learner.getClass().getName());

    String clustererName = cfg.getClusterer();
    clusterer = Constructor.createClusterer(clustererName);
    if (clusterer instanceof ThresholdClusterer) {
      String thr = cfg.getString("ClustOptions.THRESHOLD");
      if (thr != null && thr.length() > 0) {
        ((ThresholdClusterer) clusterer).setThreshold(Double.parseDouble(thr));
      }
    }
    clustOptions = cfg.getStringArray("ClustOptions." + clustererName);

  }
  catch (IOException e) {
    e.printStackTrace();
  }
  catch (InterruptedException e) {
    e.printStackTrace();
  }
  catch (ConfigurationException e) {
    e.printStackTrace();
  }
  catch (URISyntaxException e) {
    e.printStackTrace();
  }

}


@Override
public void map(ImmutableBytesWritable mapKey, Result row, Context context)
    throws IOException, InterruptedException
{
  context.getCounter("Coref Feature Generator", "row counter").increment(1);

  // keep alive for long parses
  ReportProgressThread progress = null;
  try {
    AnnotationSet coref = DocSchema.getAnnotationSet(row, Constants.RESPONSE_NPS);
    if (coref != null && coref.size() > 0) {
      context.getCounter("Coref Resolver", "skip -- already processed").increment(1);
      return;
    }

    // start keep alive for long parses
    progress = ReportProgressThread.start(context, 1000);
    makeFeatures(row);
  }
  finally {
    if (progress != null) {
      progress.interrupt();
    }
  }
}

private AnnotationSet makeFeatures(Result row)
{
  Document doc = new ReconcileDocument(row);
  stringOutput.getBuffer().setLength(0);

  PrintWriter output = new PrintWriter(stringOutput, true);
  boolean write_binary = Utils.getConfig().getBoolean("WRITE_BINARIZED_FEATURE_FILE", true);
  FeatureWriter writer;
  if (write_binary) {
    writer = new FeatureWriterARFFBinarized(featureList, output);
  }
  else {
    writer = new FeatureWriterARFF(featureList, output);
  }

  // String docId = Bytes.toString(row.getRow());
  docNo = (docNo % Short.MAX_VALUE) + 1;
  addDocNo(doc, String.valueOf(docNo));
  addSentenceNumbers(doc);

  writer.printHeader();
  AnnotationSet basenp = doc.getAnnotationSet(Constants.NP);
  //AnnotationSet basenp = new AnnotationReaderBytespan().read(doc.getAnnotationDir(), Constants.PROPERTIES_FILE_NAME);
  Annotation[] basenpArray = basenp.toArray();
  if (basenpArray.length > 0) {
    // System.out.println("Document " + docId + ": " + " (" + basenpArray.length + " nps)");
    int log10 = ((int) Math.floor(Math.log10(basenpArray.length)));
    if (log10 < 1) {
      log10 = 0;
    }
    context.getCounter("Coref Resolver", log10 + "log(nps)").increment(1);
    if (log10 > 2) {
      context.getCounter("Coref Resolver", "skipping -- too many nps").increment(1);
      return null;
    }
  }
  // Initialize the pair generator with the new document (training == false)
  pairGen.initialize(basenpArray, doc, false);

  int pairCount = 0;
  while (pairGen.hasNext() && pairCount < MAX_PAIR_COUNT) {
    Annotation[] pair = pairGen.nextPair();
    Annotation np1 = pair[0], np2 = pair[1];
    HashMap<Feature, String> values = makeVectorTimed(np1, np2, featureList, doc);
    writer.printInstanceVector(values);
  }
  if (pairCount >= MAX_PAIR_COUNT) {
    context.getCounter("Coref Resolver", "hit max pairs (" + MAX_PAIR_COUNT + ")").increment(1);

  }
  // outputNPProperties(doc, basenp, row);

  String predictionString = stringOutput.toString();
  // System.out.println(predictionString);
  classify(doc, predictionString, row);

  return basenp;
}

private void addSentenceNumbers(Document doc)
{
  AnnotationSet sentences = doc.getAnnotationSet(Constants.SENT);
  int id = 1;
  for (Annotation sent : sentences) {
    String num = sent.getAttribute(SentNum.NAME);
    if (num == null || num.trim().equals("")) {
      sent.setAttribute(SentNum.NAME, String.valueOf(id++));
    }
    else {
      id = Integer.parseInt(num) + 1;
    }
  }

}

/**
 *
 * @param doc
 * @param arff
 *          the feature vector
 * @param row
 */
private void classify(Document doc, String arff, Result row)
{
  predictionStringOut.getBuffer().setLength(0);
  learner.test(new StringReader(arff), predictionStringOut, options);

  Reader predictionIn = new StringReader(predictionStringOut.toString());
  AnnotationSet ces = doc.getAnnotationSet(Constants.NP);
  AnnotationSet clusterResult = clusterer.cluster(ces, predictionIn, clustOptions);

  if (clusterResult != null && clusterResult.size() > 0) {
  try {
    Put put = new Put(doc.getDocumentId().getBytes());
      addAnnotation(row, put, clusterResult, Constants.RESPONSE_NPS);
    docTable.put(put);
      context.getCounter("Coref Resolver", "put " + clusterResult.getName()).increment(1);
      putCount++;
      if (putCount % 100 == 0) {
        docTable.flushCommits();
      }
  }
  catch (IOException e) {
    context.getCounter("FGMapper", "Result put IO error").increment(1);
    e.printStackTrace();
  }
  }
  else {
    context.getCounter("Coref Resolver", "no results").increment(1);
  }
}

private void addDocNo(Document doc, String docId)
{

  AnnotationSet docNo = doc.getAnnotationSet(DocNo.ID);
  if (docNo == null) {
    docNo = new AnnotationSet(DocNo.ID);
  }
  if (docNo.size() == 0) {
    Map<String, String> m = Maps.newHashMap();
    m.put(DocNo.ID, docId);
    Annotation a = new Annotation(0, 0, doc.length(), DocNo.ID, m);
    docNo.add(a);
    doc.writeAnnotationSet(docNo);
  }
}

public HashMap<Feature, String> makeVectorTimed(Annotation np1, Annotation np2, List<Feature> featureList,
    Document doc)
{
  HashMap<Feature, String> result = new HashMap<Feature, String>();
  for (Feature feat : featureList) {
    long stTime = System.currentTimeMillis();
    feat.getValue(np1, np2, doc, result);
    long elapsedTime = System.currentTimeMillis() - stTime;
    context.getCounter("Feature Timer", feat.getName()).increment(elapsedTime);

  }
  return result;
}


}
}
