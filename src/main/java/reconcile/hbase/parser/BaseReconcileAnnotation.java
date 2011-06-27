package reconcile.hbase.parser;

import static reconcile.hbase.mapreduce.annotation.AnnotationUtils.getAnnotationStr;
import static reconcile.hbase.table.DocSchema.annotationsCF;
import static reconcile.hbase.table.DocSchema.textCF;
import static reconcile.hbase.table.DocSchema.textRaw;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.zip.GZIPInputStream;

import opennlp.maxent.MaxentModel;
import opennlp.maxent.io.BinaryGISModelReader;
import opennlp.tools.postag.DefaultPOSContextGenerator;
import opennlp.tools.postag.POSDictionary;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.util.Span;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.mapreduce.Job;

import gov.llnl.text.util.Timer;

import reconcile.data.Annotation;
import reconcile.data.AnnotationSet;
import reconcile.featureExtractor.ParagraphSplitter;
import reconcile.featureExtractor.SentenceSplitterOpenNLP;
import reconcile.general.Constants;
import reconcile.hbase.table.DocSchema;



public class BaseReconcileAnnotation {

static final Log LOG = LogFactory.getLog(BaseReconcileAnnotation.class);

private static final int MAX_RAW_TEXT_LENGTH = 10 * 1024 * 1024;

public static void main(String[] args)
{
  Configuration conf = HBaseConfiguration.create();
  Context context = new Context();

  try {
    String source = args[0];
    String startRow = null;
    if (args.length > 1) {
      startRow = args[1];
    }

    conf.set("mapred.tasktracker.map.tasks.maximum", "5");

    Job job = null;
    LOG.info("Before map/reduce startup");
    job = new Job(conf, "Base Reconcile Annotation");
    job.setJarByClass(BaseReconcileAnnotation.class);
    Scan scan = new Scan();
    scan.addColumn(textCF.getBytes(), textRaw.getBytes());
    scan.addFamily(annotationsCF.getBytes());
    if (startRow != null) {
      scan.setStartRow(startRow.getBytes());
    }
    DocSchema t = new DocSchema(source);
    ResultScanner scanner = t.getScanner(scan);
    BaseReconcileAnnotation bra = new BaseReconcileAnnotation(t);
    Timer timer = new Timer();
    for (Result row : scanner) {
      try {
        bra.map(row, context, timer);
      }
      catch (Exception e) {
        e.printStackTrace();
      }
    }
    bra.cleanup(context);
    timer.end();
  }
  catch (Exception e) {
    e.printStackTrace();
  }
  context.print();
}

private static final String OPEN_NLP_TOKENIZER_MODEL = "OpenNLP/models/EnglishTok.bin.gz";

private DocSchema docTable;

private SentenceSplitterOpenNLP sentenceSplitter;

private ParagraphSplitter paragraphSplitter;

private TokenizerME tknzr;

private POSTaggerME tagr;

/**
*
*/
public BaseReconcileAnnotation(DocSchema table) {
  try {
    paragraphSplitter = new ParagraphSplitter();
    sentenceSplitter = new SentenceSplitterOpenNLP();

    // tokenizer
    InputStream resStream = this.getClass().getClassLoader().getResourceAsStream(OPEN_NLP_TOKENIZER_MODEL);
    DataInputStream dis = new DataInputStream(new GZIPInputStream(resStream));
    tknzr = new TokenizerME(getModel(dis));

    // set up the POS tagger
    resStream = this.getClass().getClassLoader().getResourceAsStream("OpenNLP/models/tag.bin.gz");
    dis = new DataInputStream(new GZIPInputStream(resStream));
    InputStream tagDictRes = this.getClass().getClassLoader().getResourceAsStream("OpenNLP/models/tagdict");
    boolean caseSensitive = true;
    tagr = new POSTaggerME(getModel(dis), new DefaultPOSContextGenerator(null), new POSDictionary(new BufferedReader(
        new InputStreamReader(tagDictRes)), caseSensitive));

    docTable = table;

  }
  catch (Exception e) {
    e.printStackTrace();
    BaseReconcileAnnotation.LOG.info(e.getMessage());
    throw new RuntimeException(e);
  }

}

public void map(Result row, Context context, Timer timer)
    throws IOException, InterruptedException
{
  context.getCounter("Base Reconcile Annotation", "row counter").increment(1);
  timer.increment(".");
  if (timer.getCount() % 10000 < 20) {
    System.out.println(new String(row.getRow()));
  }
  try {

    // Get the raw text
    String rawText = DocSchema.getRawText(row);
    if (rawText == null || rawText.length() == 0) {
      timer.increment("s");
      context.getCounter("Base Reconcile Annotation", "skip -- no raw text").increment(1);
      return;
    }
    if (rawText.length() > MAX_RAW_TEXT_LENGTH) {
      timer.increment("s");
      context.getCounter("Base Reconcile Annotation", "skip -- raw text too long (" + MAX_RAW_TEXT_LENGTH + ")")
          .increment(1);
      return;
    }


    // create put
    Put put = new Put(row.getRow());
    boolean add = false;

    AnnotationSet parSet = DocSchema.getAnnotationSet(row, Constants.PAR);
    if (parSet == null || parSet.size() == 0) {
      timer.increment("p");
      parSet = paragraphSplitter.parse(rawText, Constants.PAR);
      addAnnotation(put, parSet, Constants.PAR);
      context.getCounter("Base Reconcile Annotation", "add paragraph").increment(1);
      add = true;
    }

    AnnotationSet sentences = DocSchema.getAnnotationSet(row, Constants.SENT);
    if (sentences == null || sentences.size() == 0) {
      timer.increment("e");
      sentences = sentenceSplitter.parse(rawText, parSet, Constants.SENT);
      addAnnotation(put, sentences, Constants.SENT);
      context.getCounter("Base Reconcile Annotation", "add sentence").increment(1);
      add = true;
    }

    AnnotationSet toks = DocSchema.getAnnotationSet(row, Constants.TOKEN);
    if (toks == null || toks.size() == 0) {
      timer.increment("t");
      // we create the annotation set here, but we don't populate it until
      // the same time as we populate the POS tags
      toks = new AnnotationSet(Constants.TOKEN);
      add = true;
      context.getCounter("Base Reconcile Annotation", "add token").increment(1);
    }

    AnnotationSet posSet = DocSchema.getAnnotationSet(row, Constants.POS);
    if (posSet == null || posSet.size() == 0 || toks == null || toks.size() == 0) {
      posSet = new AnnotationSet(Constants.POS);
      int sentCount = 0;
      for (Annotation sent : sentences) {
        sentCount++;
        if (sentCount > 200) {
          break;
        }
        String sentText = Annotation.getAnnotText(sent, rawText);
        int sentStart = sent.getStartOffset();
        // tokenize the sentence
        Span[] spans = tknzr.tokenizePos(sentText);

        // add each token to the annotation set
        AnnotationSet sentToks = new AnnotationSet(Constants.TOKEN);
        for (Span token : spans) {
          sentToks.add(sentStart + token.getStart(), sentStart + token.getEnd(), "token");
        }
        toks.addAll(sentToks);
        context.getCounter("Base Reconcile Annotation", "add tokens for sentence").increment(1);
        timer.increment("1");
        addAnnotation(put, toks, Constants.TOKEN);

        addPOS(rawText, sentToks, posSet);
      }
      addAnnotation(put, posSet, Constants.POS);
      timer.increment("o");
      context.getCounter("Base Reconcile Annotation", "add POS").increment(1);
      if (posSet.size() > 0) {
        add = true;
      }
    }

    // write output
    // add annotation text
    if (add) {
      try {
        docTable.put(put);
        timer.increment("+");
        context.getCounter("Base Reconcile Annotation", "put").increment(1);
      }
      catch (IOException e) {
        timer.increment("-");
        context.getCounter("Base Reconcile Annotation error", "io exception in put").increment(1);
        e.printStackTrace();
      }
      catch (IllegalArgumentException e) {
        timer.increment("-");
        context.getCounter("Base Reconcile Annotation error", "illegal arg exception in put").increment(1);
        e.printStackTrace();
      }
    }
  }
  finally {
    timer.increment("_");
    context.getCounter("Base Reconcile Annotation", "finally").increment(1);
  }

}


private void addAnnotation(Put put, AnnotationSet set, String name)
{
  DocSchema.add(put, reconcile.hbase.table.DocSchema.annotationsCF, name, getAnnotationStr(set));
}

/**
 * add part of speech tokens for a sentence given the tokenization and the doc text
 *
 * @param text
 *          document text
 * @param sentToks
 *          in param: tokenization of a sentence
 * @param posSet
 *          out param: part of speech tags for each token
 */
private void addPOS(String text, AnnotationSet sentToks, AnnotationSet posSet)
{
  // list containing text segments that make up each token
  ArrayList<String> tokenList = new ArrayList<String>(sentToks.size());

  // build the list
  Iterator<Annotation> sentToksItr = sentToks.iterator();
  while (sentToksItr.hasNext()) {
    Annotation tok = (sentToksItr.next());
    tokenList.add(text.substring(tok.getStartOffset(), tok.getEndOffset()));
  }

  // Tag the sentence
  @SuppressWarnings({ "rawtypes" })
  List sentTags = tagr.tag(tokenList);

  // reset the iterator
  sentToksItr = sentToks.iterator();

  // add each tag to the annotation set, looping in parallel with sentToksItr
  for (Object tag : sentTags) {
    Annotation tok = (sentToksItr.next());
    posSet.add(tok.getStartOffset(), tok.getEndOffset(), (String) tag);
  }

}

protected void cleanup(Context context1)
    throws IOException
{
  while (true) {
    try {
      docTable.flushCommits();
      docTable.close();
    }
    catch (IOException e) {
      context1.getCounter("Base Reconcile Annotation error", "io exception in flush/close").increment(1);
      e.printStackTrace();
    }
    break;
  }
  System.out.println("done");
}

private static MaxentModel getModel(DataInputStream in)
{
  try {
    return new BinaryGISModelReader(in).getModel();
  }
  catch (IOException e) {
    e.printStackTrace();
    return null;
  }
}
}
