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

import static reconcile.hbase.mapreduce.annotation.AnnotationUtils.getAnnotationStr;
import static reconcile.hbase.table.DocSchema.add;
import static reconcile.hbase.table.DocSchema.annotationsCF;
import static reconcile.hbase.table.DocSchema.textCF;
import static reconcile.hbase.table.DocSchema.textRaw;

import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.GZIPInputStream;

import opennlp.maxent.io.BinaryGISModelReader;
import opennlp.tools.sentdetect.SentenceDetector;
import opennlp.tools.sentdetect.SentenceDetectorME;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;

import reconcile.data.Annotation;
import reconcile.data.AnnotationSet;
import reconcile.features.properties.SentNum;
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
public class SentenceAnnotation
    extends ChainableAnnotationJob {

static final Log LOG = LogFactory.getLog(SentenceAnnotation.class);

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
    ToolRunner.run(new Configuration(), new SentenceAnnotation(), args);
  }
  catch (Exception e) {
    e.printStackTrace();
  }

}

@Override
public void init(JobConfig jobConfig, Job job, Scan scan)
{
    scan.addColumn(textCF.getBytes(), textRaw.getBytes());
    scan.addFamily(annotationsCF.getBytes());
}

@Override
public Class<? extends AnnotateMapper> getMapperClass()
{
  return SentenceMapper.class;
}

public static class SentenceMapper
    extends AnnotateMapper {


private static final int MAX_RAW_TEXT_LENGTH = 1024 * 1024 * 10;

private SentenceDetector sdetector;

/**
*
*/
public SentenceMapper() {

  try {
    // InputStream resStream =
    // this.getClass().getClassLoader().getResourceAsStream(Utils.lowercaseIfNec("OpenNLP")+"/models/EnglishSD.bin.gz");
    InputStream resStream = this.getClass().getClassLoader().getResourceAsStream("OpenNLP/models/EnglishSD.bin.gz");
    DataInputStream dis = new DataInputStream(new GZIPInputStream(resStream));
    sdetector = new SentenceDetectorME((new BinaryGISModelReader(dis)).getModel());

  }
  catch (Exception e) {
    e.printStackTrace();
    SentenceAnnotation.LOG.info(e.getMessage());
    throw new RuntimeException(e);
  }

}


@Override
public void setup(Context context)
{
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
  context.getCounter("Sentence Annotation", "row counter").increment(1);

  // keep alive for long parses
  ReportProgressThread progress = null;
  try {
    // start keep alive for long parses
    progress = ReportProgressThread.start(context, 1000);

    // Get the raw text
    String rawText = DocSchema.getRawText(row);
    if (rawText == null || rawText.length() == 0) {
      context.getCounter("Sentence Annotation", "skip -- no raw text").increment(1);
      return;
    }
    if (rawText.length() > MAX_RAW_TEXT_LENGTH) {
      context.getCounter("Sentence Annotation", "skip -- raw text too long (" + MAX_RAW_TEXT_LENGTH + ")")
          .increment(1);
      return;
    }


    // create put
    Put put = new Put(mapKey.get());
    boolean add = false;

    AnnotationSet parSet = DocSchema.getAnnotationSet(row, Constants.PAR);
    if (parSet == null || parSet.size() == 0) {
      context.getCounter("Sentence Annotation", "skip -- no paragraphs").increment(1);
      return;
    }

    AnnotationSet sentences = DocSchema.getAnnotationSet(row, Constants.SENT);
    if (sentences == null || sentences.size() == 0) {
      sentences = parse(rawText, parSet, Constants.SENT);
      addAnnotation(put, sentences, Constants.SENT);
      context.getCounter("Sentence Annotation", "add sentence").increment(1);
      add = true;
    }

    // write output
    // add annotation text
    if (add) {
      try {
        docTable.put(put);
        docTable.flushCommits();
        context.getCounter("Sentence Annotation", "put").increment(1);
      }
      catch (IOException e) {
        context.getCounter("Sentence Annotation error", "io exception in put").increment(1);
        e.printStackTrace();
      }
      catch (IllegalArgumentException e) {
        context.getCounter("Sentence Annotation error", "illegal arg exception in put").increment(1);
        e.printStackTrace();
      }
    }
    else {
      context.getCounter("Sentence Annotation", "skip -- no columns to add").increment(1);
    }
  }
  finally {
    if (progress != null) {
      progress.interrupt();
    }
  }

}

private void addAnnotation(Put put, AnnotationSet set, String name)
{
  add(put, DocSchema.annotationsCF, name, getAnnotationStr(set));
}




public AnnotationSet parse(String text, AnnotationSet parSet, String annotationSetName)
{
  AnnotationSet sent = new AnnotationSet(annotationSetName);

  if (parSet == null) {
    parSet = new AnnotationSet(Constants.PAR);
  }
  if (parSet.size() == 0) {
    System.out.println("no paragraphs detected.  Adding a covering paragraph");
    parSet.add(0, text.length() - 1, "paragraph");
  }
  int counter = 0;

  for (Annotation par : parSet) {
    if (par.getEndOffset() >= text.length()) {
      par.setEndOffset(text.length());
    }
    if (par.getStartOffset() >= par.getEndOffset()) {
      continue;
    }
    String parText = Annotation.getAnnotText(par, text);
    int start = par.getStartOffset();
    int prevEndPos = start;
    boolean sentenceAdded = false;
    for (int e : sdetector.sentPosDetect(parText)) {
      sentenceAdded = true;
      int endPos = start + e;
      Map<String, String> feat = new TreeMap<String, String>();
      feat.put(SentNum.NAME, String.valueOf(counter++));
      // add each sentence to the annotation set
      sent.add(prevEndPos, endPos, "sentence_split", feat);

      prevEndPos = endPos;
    }
    if (!sentenceAdded) {
      Map<String, String> feat = new TreeMap<String, String>();
      feat.put(SentNum.NAME, String.valueOf(counter++));
      // add each sentence to the annotation set
      sent.add(start, par.getEndOffset(), "sentence_split", feat);
    }
    else if (prevEndPos < par.getEndOffset() - 1) {
      Map<String, String> feat = new TreeMap<String, String>();
      feat.put(SentNum.NAME, String.valueOf(counter++));
      // add each sentence to the annotation set
      sent.add(prevEndPos, par.getEndOffset(), "sentence_split", feat);
    }

  }
  return sent;
}

}
}
