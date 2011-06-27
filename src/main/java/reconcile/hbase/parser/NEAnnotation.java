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
 *
 */
package reconcile.hbase.parser;

import static reconcile.hbase.mapreduce.annotation.AnnotationUtils.getAnnotationStr;
import static reconcile.hbase.table.DocSchema.annotationsCF;
import static reconcile.hbase.table.DocSchema.textCF;
import static reconcile.hbase.table.DocSchema.textRaw;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.mapreduce.Job;

import gov.llnl.text.util.MapUtil;
import gov.llnl.text.util.Timer;

import reconcile.data.Annotation;
import reconcile.data.AnnotationSet;
import reconcile.featureExtractor.NamedEntityStanford;
import reconcile.featureExtractor.ParagraphSplitter;
import reconcile.featureExtractor.SentenceSplitterOpenNLP;
import reconcile.general.Constants;
import reconcile.hbase.mapreduce.annotation.AnnotationUtils;
import reconcile.hbase.table.DocSchema;

/**
 * Map/Reduce task for annotating Named Entities
 *
 * @author David Buttler
 *
 */
public class NEAnnotation
 {

static final Log LOG = LogFactory.getLog(NEAnnotation.class);

private static final int MAX_RAW_TEXT_LENGTH = 1024 * 1024 * 10;

/**
 *
 * @param args
 *          :
 *          <ol>
 *          <li>source
 *          <li>optional source argument to limit the rows to tag
 *          </ol>
 */
public static void main(String[] args)
{
  Configuration conf = HBaseConfiguration.create();
  Context context = new Context();

  String source = args[0];
  try {
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
    NEAnnotation ne = new NEAnnotation(t);
    Timer timer = new Timer();
    for (Result row : scanner) {
      try {
        ne.map(row, context, timer);
      }
      catch (Exception e) {
        e.printStackTrace();
      }
    }
    ne.cleanup(context);
    timer.end();
  }
  catch (Exception e) {
    e.printStackTrace();
  }
  context.print();

}


private NamedEntityStanford parser;

private SentenceSplitterOpenNLP sentenceSplitter;

private ParagraphSplitter paragraphSplitter;

private DocSchema docTable;

private int nFiles;

// private JobConf mJobConf;

// private static final Pattern pSpace = Pattern.compile("\\s");
/**
*
*/
public NEAnnotation(DocSchema table) {

  try {
    paragraphSplitter = new ParagraphSplitter();
    sentenceSplitter = new SentenceSplitterOpenNLP();
    parser = new NamedEntityStanford();
    docTable = table;
  }
  catch (Exception e) {
    e.printStackTrace();
    NEAnnotation.LOG.info(e.getMessage());
    throw new RuntimeException(e);
  }

}

@edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "WMI_WRONG_MAP_ITERATOR")
public void map(Result row, Context context, Timer timer)
    throws IOException, InterruptedException
{

    nFiles++;
    context.getCounter("named entities", "row counter").increment(1);

  AnnotationSet namedEntities = DocSchema.getAnnotationSet(row, Constants.NE);
    // our job is done here
    if (namedEntities != null && namedEntities.size() != 0) {
      context.getCounter("named entities", "skip -- already annotated").increment(1);
      return;
    }

    // Get the raw text
  String rawText = DocSchema.getRawText(row);
    // nothing to do
    if (rawText == null || rawText.trim().equals("")) {
      context.getCounter("named entities", "skip -- raw text empty").increment(1);
      return;
    }

    if (rawText.length() > MAX_RAW_TEXT_LENGTH) {
      context.getCounter("named entities", "skip -- raw text too long (" + MAX_RAW_TEXT_LENGTH + ")").increment(1);
      return;
    }

    boolean add = false;
    Put put = new Put(row.getRow());

  AnnotationSet parSet = DocSchema.getAnnotationSet(row, Constants.PAR);
    if (parSet == null || parSet.size() == 0) {
      parSet = paragraphSplitter.parse(rawText, Constants.PAR);
    DocSchema.add(put, reconcile.hbase.table.DocSchema.annotationsCF, Constants.PAR, getAnnotationStr(parSet));
      context.getCounter("named entities", "add paragraphs").increment(1);
      add = true;
    }

    // get the sentences from precomputed annotation set on disk
  AnnotationSet sentences = DocSchema.getAnnotationSet(row, Constants.SENT);
    if (sentences == null || sentences.size() == 0) {
      sentences = sentenceSplitter.parse(rawText, parSet, Constants.SENT);
    DocSchema.add(put, reconcile.hbase.table.DocSchema.annotationsCF, Constants.SENT, getAnnotationStr(sentences));
      context.getCounter("named entities", "add sentences").increment(1);
      add = true;
    }

    // now parse out the named entities
    if (namedEntities == null || namedEntities.size() == 0) {
      namedEntities = new AnnotationSet(Constants.NE);

      for (Annotation sent : sentences) {
      String sentText = Annotation.getAnnotText(sent, rawText);
        int sentStart = sent.getStartOffset();
        parser.parseSentence(sentText, sentStart, namedEntities);
      }

      Map<String, Set<String>> map = classify(namedEntities, rawText);

      // write output
      // write named entity sets to meta data
      for (String key : map.keySet()) {
        Set<String> val = map.get(key);
      DocSchema.add(put, reconcile.hbase.table.DocSchema.metaCF, key, AnnotationUtils.toString(val));
        add = true;
      }
      String neStr = getAnnotationStr(namedEntities);
      if (neStr != null && neStr.trim().length() > 0) {
      DocSchema.add(put, reconcile.hbase.table.DocSchema.annotationsCF, Constants.NE, neStr);
        context.getCounter("named entities", "add ne").increment(1);
        add = true;
      }
    }

    if (add) {
      try {
      docTable.put(put);
        context.getCounter("named entities", "update row").increment(1);
      }
      catch (IOException e) {
        context.getCounter("stanford ner error", "io exception in put").increment(1);
        e.printStackTrace();
      }
      catch (IllegalArgumentException e) {
        context.getCounter("stanford ner error", "illegal arg exception in put").increment(1);
        e.printStackTrace();
      }
    }

}

/**
 * put the named entities in a map keyed by the NE type
 *
 * @param doc
 */
private Map<String, Set<String>> classify(AnnotationSet ne, String text)
{
  // Track matches
  HashMap<String, Set<String>> map = new HashMap<String, Set<String>>();

  for (Annotation a : ne) {
    String type = a.getType();
    String neText = Annotation.getAnnotText(a, text);
    MapUtil.addToMapSet(map, type, neText);
  }
  return map;
}

protected void cleanup(Context context1)
    throws IOException, InterruptedException
{
  System.out.println("processed " + nFiles + " files");
  while (true) {
    try {
      docTable.flushCommits();
      docTable.close();
    }
    catch (IOException e) {
      context1.getCounter("stanford ner error", "io exception in flush/close").increment(1);
      e.printStackTrace();
    }
    break;
  }
  System.out.println("done");
}


}
