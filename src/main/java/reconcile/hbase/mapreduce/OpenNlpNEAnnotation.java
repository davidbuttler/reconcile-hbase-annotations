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
package reconcile.hbase.mapreduce;

import static reconcile.hbase.table.DocSchema.annotationsCF;
import static reconcile.hbase.table.DocSchema.textCF;
import static reconcile.hbase.table.DocSchema.textRaw;

import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import opennlp.maxent.MaxentModel;
import opennlp.maxent.io.BinaryGISModelReader;
import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.util.Span;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.collect.Maps;
import com.google.gson.Gson;

import gov.llnl.text.util.MapUtil;

import reconcile.data.Annotation;
import reconcile.data.AnnotationSet;
import reconcile.general.Constants;
import reconcile.hbase.mapreduce.annotation.ReportProgressThread;
import reconcile.hbase.table.DocSchema;

/**
 * Map/Reduce task for annotating Named Entities
 *
 * @author David Buttler
 *
 */
public class OpenNlpNEAnnotation
    extends ChainableAnnotationJob {

public static class ByteOffsetMatch {

public String value;

public int start;

public int end;

public ByteOffsetMatch(String val, int startOffset, int endOffset) {
  value = val;
  start = startOffset;
  end = endOffset;
}

}

static final Log LOG = LogFactory.getLog(OpenNlpNEAnnotation.class);

private static final int MAX_RAW_TEXT_LENGTH = 1024 * 1024 * 10;

/**
 *
 * @param args
 *          :
 *          <ol>
 *          <li>optional source argument to limit the rows to tag
 *          </ol>
 */
public static void main(String[] args)
{
  try {
    ToolRunner.run(new Configuration(), new OpenNlpNEAnnotation(), args);
  }
  catch (Exception e) {
    e.printStackTrace();
  }

}

@Override
public int run(String[] args)
    throws Exception
{
  if (args.length < 3) {
    System.out.println("usage: KeywordAnnotation <keyword file> <column family> <column qualifier> "
        + JobConfig.usage());
    return 1;
  }

  return super.run(args);
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
  return NEMapper.class;
}

public static class NEMapper
    extends AnnotateMapper {


public static final String OPEN_NLP_PERSON_QUAL = "openNLP_person";

public static final String OPEN_NLP_LOCATION_QUAL = "openNLP_location";

public static final String OPEN_NLP_ORGANIZATION_QUAL = "openNLP_organization";

private DocSchema docTable;

private int nFiles;

private static final String OPEN_NLP_TOKENIZER_MODEL = "OpenNLP/models/EnglishTok.bin.gz";

private TokenizerME tknzr;

NameFinderME[] finders;

String[] tagTypes = { "date", "location", "money", "organization", "percentage", "person", "time" };

private static MaxentModel getModel(InputStream resStream)
{
  try {
    DataInputStream dis = new DataInputStream(new GZIPInputStream(resStream));
    return (new BinaryGISModelReader(dis)).getModel();
  }
  catch (IOException e) {
    e.printStackTrace();
    return null;
  }
}

/**
*
*/
public NEMapper() {

  try {
    // set up the tokenizer
    InputStream tokenResStream = this.getClass().getClassLoader().getResourceAsStream(OPEN_NLP_TOKENIZER_MODEL);
    DataInputStream dis = new DataInputStream(new GZIPInputStream(tokenResStream));
    tknzr = new TokenizerME((new BinaryGISModelReader(dis)).getModel());

    /* Establish a name finder for each tag type */
    finders = new NameFinderME[tagTypes.length];

    for (int i = 0; i < tagTypes.length; ++i) {


        InputStream resStream = this.getClass().getClassLoader().getResourceAsStream(
            "OpenNLP/models/" + tagTypes[i] + ".bin.gz");
        // load the model
        MaxentModel model = getModel(resStream);

        // create the finder
        finders[i] = new NameFinderME(model);
    }

  }
  catch (Exception e) {
    e.printStackTrace();
    OpenNlpNEAnnotation.LOG.info(e.getMessage());
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
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "WMI_WRONG_MAP_ITERATOR")
public void map(ImmutableBytesWritable mapKey, Result mapValue, Context context)
    throws IOException, InterruptedException
{
  // keep alive for long parses
  ReportProgressThread progress = null;
  try {

    nFiles++;
    context.getCounter("openNLP NEs", "row counter").increment(1);

    // String personStr = DocSchema.getColumn(mapValue, DocSchema.elecentCF, OPEN_NLP_PERSON_QUAL);
    // String locationStr = DocSchema.getColumn(mapValue, DocSchema.elecentCF, OPEN_NLP_LOCATION_QUAL);
    // String organizationStr = DocSchema.getColumn(mapValue, DocSchema.elecentCF, OPEN_NLP_ORGANIZATION_QUAL);
    // our job is done here
    // if (personStr != null || locationStr != null || organizationStr != null) {
    // context.getCounter("openNLP NEs", "skip -- already annotated").increment(1);
    // return;
    // }

    // Get the raw text
    String rawText = DocSchema.getRawText(mapValue);
    // nothing to do
    if (rawText == null || rawText.trim().equals("")) {
      context.getCounter("openNLP NEs", "skip -- raw text empty").increment(1);
      return;
    }

    if (rawText.length() > MAX_RAW_TEXT_LENGTH) {
      context.getCounter("openNLP NEs", "skip -- raw text too long (" + MAX_RAW_TEXT_LENGTH + ")")
          .increment(1);
      return;
    }

    // start keep alive for long parses
    progress = ReportProgressThread.start(context, 10000);

    boolean add = false;
    Put put = new Put(mapKey.get());

    AnnotationSet parSet = DocSchema.getAnnotationSet(mapValue, Constants.PAR);
    if (parSet == null || parSet.size() == 0) {
      context.getCounter("openNLP NEs", "skip -- paragraphs empty").increment(1);
      return;
    }
    else {
      context.getCounter("openNLP NEs paragraph count", String.valueOf(parSet.size())).increment(1);
    }

    // get the sentences from precomputed annotation set on disk
    AnnotationSet sentences = DocSchema.getAnnotationSet(mapValue, Constants.SENT);
    if (sentences == null || sentences.size() == 0) {
      context.getCounter("openNLP NEs", "skip -- sentences empty").increment(1);
      return;
    }
    else {
      context.getCounter("openNLP NEs sentence count", String.valueOf(sentences.size())).increment(1);
    }

    // get the sentences from precomputed annotation set on disk
    AnnotationSet tokens = DocSchema.getAnnotationSet(mapValue, Constants.TOKEN);
    if (tokens == null || tokens.size() == 0) {
      context.getCounter("openNLP NEs", "skip -- tokens empty").increment(1);
      return;
    }
    else {
      context.getCounter("openNLP NEs token count", String.valueOf(tokens.size())).increment(1);
    }


    // now parse out the named entities
    AnnotationSet namedEntities = getNEs(rawText, sentences, tokens);

    Map<String, String> neMap = getAnnotationStr(rawText, namedEntities);
    if (neMap != null && neMap.size() > 0) {
      for (String key : neMap.keySet()) {
        String val = neMap.get(key);
        DocSchema.add(put, "elecent", key, val);
        add = true;
      }
      context.getCounter("openNLP NEs", "add ne").increment(1);
    }

    if (add) {
      try {
        docTable.put(put);
        context.getCounter("openNLP NEs", "update row").increment(1);
      }
      catch (IOException e) {
        context.getCounter("openNLP ner error", "io exception in put").increment(1);
        e.printStackTrace();
      }
      catch (IllegalArgumentException e) {
        context.getCounter("openNLP ner error", "illegal arg exception in put").increment(1);
        e.printStackTrace();
      }
    }
    else {
      context.getCounter("openNLP NEs", "nothing to add").increment(1);
    }

  }
  catch (Exception e) {
    context.getCounter("openNLP ner error", e.getMessage()).increment(1);
  }
  finally {
    if (progress != null) {
      progress.interrupt();
    }
  }
}

private AnnotationSet getNEs(String text, AnnotationSet sentSet, AnnotationSet tokSet)
{
  AnnotationSet namedEntities = new AnnotationSet(Constants.NE);

  Iterator<Annotation> sents = sentSet.iterator();
  while (sents.hasNext()) {
    Annotation sent = (sents.next());

    int sentStart = sent.getStartOffset();
    int sentEnd = sent.getEndOffset();

    String sentence = text.substring(sentStart, sentEnd);

    // tokenize the sentence
    Span[] spans = tknzr.tokenizePos(sentence);

    // add each token to the annotation set
    String[] tokenList = new String[spans.length];
    for (int i = 0; i < spans.length; i++) {
      tokenList[i] = spans[i].getCoveredText(sentence);
    }

    for (int fndIndx = 0; fndIndx < finders.length; ++fndIndx) {
      NameFinderME findr = finders[fndIndx];

      // Tag the sentence
      Span[] sentTags = findr.find(tokenList); // findr.find(tokenList, new HashMap());

      for (Span span : sentTags) {
        namedEntities.add(span.getStart(), span.getEnd(), tagTypes[fndIndx]);
      }
    }
  }

  return namedEntities;
}

@edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "WMI_WRONG_MAP_ITERATOR")
public static Map<String, String> getAnnotationStr(String text, AnnotationSet annSet)
{
  Map<String, String> map = Maps.newHashMap();
  Map<String, List<ByteOffsetMatch>> mapList = Maps.newHashMap();
  for (Annotation a : annSet) {
    String val = Annotation.getAnnotText(a, text);
    ByteOffsetMatch b = new ByteOffsetMatch(val, a.getStartOffset(), a
        .getEndOffset());
    MapUtil.addToMapList(mapList, a.getType(), b);
  }
  Gson g = new Gson();
  for (String key : mapList.keySet()) {
    List<ByteOffsetMatch> list = mapList.get(key);
    String newKey = "openNLP_" + key.toLowerCase().trim();
    map.put(newKey, g.toJson(list));
  }

  return map;
}

@Override
public void cleanup(Context context1)
    throws IOException, InterruptedException
{
  super.cleanup(context1);
  System.out.println("processed " + nFiles + " files");
}

}
}
