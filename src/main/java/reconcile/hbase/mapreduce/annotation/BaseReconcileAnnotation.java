/*
 * Copyright (c) 2008, Lawrence Livermore National Security, LLC. Produced at the Lawrence Livermore National
 * Laboratory. Written by David Buttler, buttler1@llnl.gov All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public
 * License (as published by the Free Software Foundation) version 2, dated June 1991. This program is distributed in the
 * hope that it will be useful, but WITHOUT ANY WARRANTY; without even the IMPLIED WARRANTY OF MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the terms and conditions of the GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License along with this program; if not, write to the Free
 * Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA For full text see license.txt
 */
package reconcile.hbase.mapreduce.annotation;

import static reconcile.hbase.table.DocSchema.annotationsCF;
import static reconcile.hbase.table.DocSchema.textCF;
import static reconcile.hbase.table.DocSchema.textRaw;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileNotFoundException;
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;

import reconcile.data.Annotation;
import reconcile.data.AnnotationSet;
import reconcile.featureExtractor.ParagraphSplitter;
import reconcile.featureExtractor.SentenceSplitterOpenNLP;
import reconcile.general.Constants;
import reconcile.hbase.mapreduce.ChainableAnnotationJob;
import reconcile.hbase.mapreduce.JobConfig;
import reconcile.hbase.table.DocSchema;

/**
 * Run the base annotators on a document
 *
 * @author David Buttler
 *
 */
public class BaseReconcileAnnotation extends ChainableAnnotationJob {

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
    ToolRunner.run(new Configuration(), new BaseReconcileAnnotation(), args);
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

    if (startRow != null) {
      scan.setStartRow(startRow.getBytes());
    }
}

@Override
public Class<? extends AnnotateMapper> getMapperClass()
{
	return BaseReconcileMapper.class;
}


public static class BaseReconcileMapper extends AnnotateMapper {

private static final String OPEN_NLP_TOKENIZER_MODEL = "OpenNLP/models/EnglishTok.bin.gz";

private static final int MAX_ROW_LENGTH = 1024 * 1024 * 10;

private static final int MAX_RAW_TEXT_LENGTH = 1024 * 1024; // * 10;


private SentenceSplitterOpenNLP sentenceSplitter;

private ParagraphSplitter paragraphSplitter;

private TokenizerME tknzr;

private POSTaggerME tagr;

// private JobConf mJobConf;

// private static final Pattern pSpace = Pattern.compile("\\s");
/**
*
*/
public BaseReconcileMapper() {

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
  }
  catch (Exception e) {
    e.printStackTrace();
    LOG.info(e.getMessage());
    throw new RuntimeException(e);
  }

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

@Override
public void setup(Context context)
{
  System.out.println("start setup");
  try {
    super.setup(context);
  }
  catch (FileNotFoundException e) {
    e.printStackTrace();
  }
  catch (IOException e) {
    e.printStackTrace();
  }
  catch (InterruptedException e) {
    e.printStackTrace();
  }

}

@Override
public void map(ImmutableBytesWritable mapKey, Result row, Context context)
    throws IOException, InterruptedException
{
  context.getCounter("Base Reconcile Annotation", "row counter").increment(1);

  // keep alive for long parses
  ReportProgressThread progress = null;
  try {
    // start keep alive for long parses
    progress = ReportProgressThread.start(context, 1000);

    if (isRowSizeTooLarge(row, context, MAX_ROW_LENGTH)) return;

    // Get the raw text
    String rawText = DocSchema.getRawText(row);
    if (isRawTextInvalidSize(rawText, context, MAX_RAW_TEXT_LENGTH)) return;

    // create put
    Put put = new Put(mapKey.get());
    boolean add = false;

    AnnotationSet parSet = DocSchema.getAnnotationSet(row, Constants.PAR);
    if (parSet == null || parSet.size() == 0) {
      parSet = paragraphSplitter.parse(rawText, Constants.PAR);
      addAnnotation(row, put, parSet, Constants.PAR);
      context.getCounter("Base Reconcile Annotation", "add paragraph").increment(1);
      add = true;
    }

    AnnotationSet sentences = DocSchema.getAnnotationSet(row, Constants.SENT);
    if (sentences == null || sentences.size() == 0) {
      sentences = sentenceSplitter.parse(rawText, parSet, Constants.SENT);
      addAnnotation(row, put, sentences, Constants.SENT);
      context.getCounter("Base Reconcile Annotation", "add sentence").increment(1);
      add = true;
    }

    AnnotationSet toks = DocSchema.getAnnotationSet(row, Constants.TOKEN);
    if (toks == null || toks.size() == 0) {
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
        if (sentCount > 1000) {
          break;
        }
        AnnotationSet sentToks = addSentToks(sent, rawText, toks);
        addPOS(rawText, sentToks, posSet);
      }
      addAnnotation(row, put, toks, Constants.TOKEN);
      context.getCounter("Base Reconcile Annotation", "add tokens for sentence").increment(1);

      addAnnotation(row, put, posSet, Constants.POS);
      context.getCounter("Base Reconcile Annotation", "add POS").increment(1);
      if (posSet.size() > 0) {
        add = true;
      }
    }

    if (add) {
      addRowToHBase(mapKey, context, put);
    }
    else {
      context.getCounter("Base Reconcile Annotation", "skip -- no columns to add").increment(1);
    }
  }
  finally {
    if (progress != null) {
      progress.interrupt();
    }
  }

}


private AnnotationSet addSentToks(Annotation sent, String rawText, AnnotationSet toks)
{
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

  return sentToks;
}

private void addRowToHBase(ImmutableBytesWritable mapKey, Context context, Put put)
{
  try {
    docTable.put(put);
    docTable.flushCommits();
    context.getCounter("Base Reconcile Annotation", "put").increment(1);
  }
  catch (IOException e) {
    context.getCounter("Base Reconcile Annotation error", "io exception in put").increment(1);
    e.printStackTrace();
    LOG.error("row for key("+Bytes.toString(mapKey.get())+") failed. reason: "+e.toString());
  }
  catch (IllegalArgumentException e) {
    context.getCounter("Base Reconcile Annotation error", "illegal arg exception in put").increment(1);
    e.printStackTrace();
    LOG.error("row for key("+Bytes.toString(mapKey.get())+") failed. reason: "+e.toString());
  }
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
@SuppressWarnings("rawtypes")
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
  List sentTags = tagr.tag(tokenList);

  // reset the iterator
  sentToksItr = sentToks.iterator();

  // add each tag to the annotation set, looping in parallel with sentToksItr
  for (Object tag : sentTags) {
    Annotation tok = (sentToksItr.next());
    posSet.add(tok.getStartOffset(), tok.getEndOffset(), (String) tag);
  }

}


}
}
