/*
 * Copyright (c) 2008, Lawrence Livermore National Security, LLC. Produced at the Lawrence Livermore National
 * Laboratory. Written by David Buttler, buttler1@llnl.gov rights reserved.
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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;

import edu.berkeley.nlp.PCFGLA.CoarseToFineMaxRuleParser;
import edu.berkeley.nlp.PCFGLA.Grammar;
import edu.berkeley.nlp.PCFGLA.Lexicon;
import edu.berkeley.nlp.PCFGLA.ParserData;
import edu.berkeley.nlp.PCFGLA.TreeAnnotations;
import edu.berkeley.nlp.ling.Tree;
import edu.berkeley.nlp.util.Numberer;
import edu.stanford.nlp.trees.GrammaticalStructure;
import edu.stanford.nlp.trees.GrammaticalStructureFactory;
import edu.stanford.nlp.trees.PennTreebankLanguagePack;
import edu.stanford.nlp.trees.TreebankLanguagePack;
import edu.stanford.nlp.trees.TypedDependency;

import reconcile.data.Annotation;
import reconcile.data.AnnotationSet;
import reconcile.featureExtractor.BerkeleyToStanfordTreeConverter;
import reconcile.featureExtractor.ParserStanfordParser;
import reconcile.general.Constants;
import reconcile.hbase.mapreduce.ChainableAnnotationJob;
import reconcile.hbase.mapreduce.JobConfig;
import reconcile.hbase.table.DocSchema;

/**
 * @author David Buttler
 *
 */
public class ParserAnnotation
    extends ChainableAnnotationJob {

static final Log LOG = LogFactory.getLog(ParserAnnotation.class);



/**
 *
 * @param args
 *          :
 *          <ol>
 *          <li>optional source to filter doc table for
 *          </ol>
 */
public static void main(String[] args)
{
  try {
    ToolRunner.run(new Configuration(), new ParserAnnotation(), args);
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
  return ParserMapper.class;
}

public static class ParserMapper
    extends AnnotateMapper {

/**
 *
 */
private static final String BERKELEY_PARSER_MODEL = "BerkeleyParser/models/eng_sm5.gr";

private static final int TOKEN_LIMIT = 100;

private static final String[] BREAKS = { ";", ":", ",", "." };

final DateFormat formatId = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss.SSS");
private CoarseToFineMaxRuleParser parser = null;

TreebankLanguagePack tlp;

GrammaticalStructureFactory gsf;

private int nFiles;

// private JobConf mJobConf;

// private static final Pattern pSpace = Pattern.compile("\\s");
/**
*
*/
public ParserMapper() {

  try {
    InputStream res = this.getClass().getClassLoader().getResourceAsStream(BERKELEY_PARSER_MODEL);

    System.out.println("Reading Berkeley Parser grammar... " + BERKELEY_PARSER_MODEL);

    ParserData pData = ParserData.Load(res);

    if (pData == null) {
      System.out.println("Failed to load grammar from file: " + BERKELEY_PARSER_MODEL + ".");
      throw new RuntimeException("Failed to load grammar from file: " + BERKELEY_PARSER_MODEL + ".");
    }

    System.out.println("Done reading grammar...");

    // set features
    Grammar grammar = pData.getGrammar();
    Lexicon lexicon = pData.getLexicon();
    Numberer.setNumberers(pData.getNumbs());

    double threshold = 1.0;

    boolean viterbiInsteadOfMaxRule = false;
    boolean outputSubCategories = false;
    boolean outputInsideScoresOnly = false;
    boolean accuracyOverEfficiency = false;

    // create a parser
    parser = new CoarseToFineMaxRuleParser(grammar, lexicon, threshold, -1, viterbiInsteadOfMaxRule,
        outputSubCategories, outputInsideScoresOnly, accuracyOverEfficiency, false, false);

    // Some additional components needed for the dependency parse conversions
    tlp = new PennTreebankLanguagePack();
    gsf = tlp.grammaticalStructureFactory();
  }
  catch (Exception e) {
    e.printStackTrace();
    ParserAnnotation.LOG.info(e.getMessage());
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
public void map(ImmutableBytesWritable mapKey, Result mapValue, Context context)
    throws IOException, InterruptedException
{
  context.progress();
  System.out.println("time: " + formatId.format(new Date()));
  // keep alive for long parses
  ReportProgressThread progress = null;
  boolean add = false;
  try {
    System.out.println("processing: " + new String(mapKey.get()));
    context.getCounter("stanford parser", "seen row").increment(1);
    nFiles++;

    // Get the raw text
    String rawText = DocSchema.getColumn(mapValue, textCF, textRaw);
    if (rawText == null || rawText.length() == 0) {
      context.getCounter("stanford parser", "skipped -- no raw text").increment(1);
      System.out.println("skip, no raw text");
      return;
    }

    // get the sentences from precomputed annotation set on disk
    AnnotationSet sentSet = DocSchema.getAnnotationSet(mapValue, Constants.SENT);
    if (sentSet == null || sentSet.size() == 0) {
      context.getCounter("stanford parser", "skipped -- no sentence annotations").increment(1);
      System.out.println("skip, no sentence annotation");
      return;
    }

    // get the tokens from precomputed annotation set on disk
    AnnotationSet tokSet = DocSchema.getAnnotationSet(mapValue, Constants.TOKEN);
    if (tokSet == null || tokSet.size() == 0) {
      context.getCounter("stanford parser", "skipped -- no token annotations").increment(1);
      System.out.println("skip, no token annotations");
      return;
    }

    // ignore rows with data
    AnnotationSet parses = DocSchema.getAnnotationSet(mapValue, Constants.PARSE);
    if (parses != null && parses.size() > 0) {
      context.getCounter("stanford parser", "skipped -- already parsed").increment(1);
      System.out.println("skip, already parsed");
      return;
    }
    parses = new AnnotationSet(Constants.PARSE);
    AnnotationSet depAnnots = new AnnotationSet(Constants.DEP);

    // start keep alive
    progress = ReportProgressThread.start(context, 1000);

    System.out.println("parse");
    parse(rawText, sentSet, tokSet, parses, depAnnots);

    // write output
    // write named entity sets to meta data
    // add annotation text
    Put put = new Put(mapKey.get());
    add(put, DocSchema.annotationsCF, Constants.PARSE, getAnnotationStr(parses));
    add(put, DocSchema.annotationsCF, Constants.DEP, getAnnotationStr(depAnnots));
    if (parses.size() > 0 || depAnnots.size() > 0) {
      add = true;
    }

    if (add) {
      try {
        docTable.put(put);
        context.getCounter("stanford parser", "tagged row").increment(1);
        System.out.println("tagged");
      }
      catch (IOException e) {
        context.getCounter("stanford parser error", "io exception in put").increment(1);
        e.printStackTrace();
      }
      catch (IllegalArgumentException e) {
        context.getCounter("stanford parser error", "illegal arg exception in put").increment(1);
        e.printStackTrace();
      }
    }
  }
  finally {
    if (progress != null) {
      progress.interrupt();
    }
  }
}

@Override
public void cleanup(Context context1)
    throws IOException, InterruptedException
{
  super.cleanup(context1);
  System.out.println("processed " + nFiles + " files");
}

private void addChild(Annotation parent, Annotation child)
{
  if (!parent.equals(Annotation.getNullAnnot())) {
    String childIds = parent.getAttribute("CHILD_IDS");
    childIds = childIds == null ? Integer.toString(child.getId()) : childIds + "," + child.getId();
    parent.setAttribute("CHILD_IDS", childIds);
  }
}

// recursive method to traverse a tree while adding spans of nodes to the annotset
private int addSpans(Tree<String> parseTree, int startTokenIndx, Object[] sentToks, AnnotationSet parsesSet,
    Annotation parent)
{
  int yieldLength = parseTree.getYield().size();
  Map<String, String> attrs = new TreeMap<String, String>();
  attrs.put("parent", Integer.toString(parent.getId()));
  int curId = parsesSet.add(((Annotation) sentToks[startTokenIndx]).getStartOffset(),
      ((Annotation) sentToks[yieldLength + startTokenIndx - 1]).getEndOffset(), parseTree.getLabel(), attrs);
  Annotation cur = parsesSet.get(curId);
  addChild(parent, cur);
  int offset = startTokenIndx;
  for (Tree<String> tr : parseTree.getChildren()) {
    if (!tr.isLeaf()) {
      offset += addSpans(tr, offset, sentToks, parsesSet, cur);
    }
  }
  return yieldLength;
}

private ArrayList<Annotation> splitSentence(AnnotationSet toks, Annotation sentence, String text)
{
  ArrayList<Annotation> result = new ArrayList<Annotation>();
  if (toks.size() <= TOKEN_LIMIT) {
    result.add(sentence);
    return result;
  }
  System.out.println("Long sentence: " + Annotation.getAnnotText(sentence, text));
  List<Annotation> tokens = toks.getOrderedAnnots();
  int mid = tokens.size() / 2;

  boolean split = false;
  for (String br : BREAKS) {
    for (int indexLow = mid, indexHigh = mid; indexLow >= 0; indexLow--) {
      indexHigh++;
      Annotation tokLow = tokens.get(indexLow);
      if (br.equals(Annotation.getAnnotText(tokLow, text))) {
        Annotation newSen = new Annotation(sentence.getId(), sentence.getStartOffset(), tokLow.getEndOffset(),
            "sentence");
        result = splitSentence(toks.getContained(newSen), newSen, text);
        Annotation newSen1 = new Annotation(sentence.getId(), tokLow.getEndOffset() + 1, sentence.getEndOffset(),
            "sentence");
        result.addAll(splitSentence(toks.getContained(newSen1), newSen1, text));
        System.out.println("Split into:" + Annotation.getAnnotText(newSen, text) + "\nand\n"
            + Annotation.getAnnotText(newSen1, text));
        split = true;
        break;
      }
      else {
        Annotation tokHigh = tokens.get(indexLow);
        if (br.equals(Annotation.getAnnotText(tokHigh, text))) {
          Annotation newSen = new Annotation(sentence.getId(), sentence.getStartOffset(), tokHigh.getEndOffset(),
              "sentence");
          result = splitSentence(toks.getContained(newSen), newSen, text);
          Annotation newSen1 = new Annotation(sentence.getId(), tokHigh.getEndOffset() + 1, sentence.getEndOffset(),
              "sentence");
          result.addAll(splitSentence(toks.getContained(newSen1), newSen1, text));
          System.out.println("Split into:" + Annotation.getAnnotText(newSen, text) + "\nand\n"
              + Annotation.getAnnotText(newSen1, text));
          split = true;
          break;
        }
      }
    }
    if (split) {
      break;
    }
  }

  return result;
}

public void parse(String text, AnnotationSet sentSet, AnnotationSet tokSet, AnnotationSet parses,
    AnnotationSet depAnnots)
{

  // loop through sentences
  for (Annotation sentence : sentSet) {
    // The parser doesn't handle well sentences that are all caps.
    // Need to find those cases
    String sentText = Annotation.getAnnotText(sentence, text);
    boolean allCaps = false;
    // if the sentence contains no lower case characters and more than one word
    if (sentText.matches("[^a-z]+\\s[^a-z]+")) {
      allCaps = true;
    }

    // int sNum = 1;
    // if (allCaps) {
    // System.out.println("On sentence (allCaps) " + (sNum++) + "...");
    // }
    // else {
    // System.out.println("On sentence " + (sNum++) + "...");
    // }
    // get the tokens in this sentence
    AnnotationSet sentenceTok = tokSet.getContained(sentence);
    ArrayList<Annotation> splitSent = splitSentence(sentenceTok, sentence, text);
    for (Annotation sent : splitSent) {
      try {
        AnnotationSet sentTok = tokSet.getContained(sent);
        // add all these tokens to a list
        ArrayList<String> tokList = new ArrayList<String>(sentTok.size());
        Iterator<Annotation> tokenItr = sentTok.iterator();

        while (tokenItr.hasNext()) {
          Annotation tok = (tokenItr.next());
          if (allCaps) {
            tokList.add(Annotation.getAnnotText(tok, text).toLowerCase());
          }
          else {
            tokList.add(Annotation.getAnnotText(tok, text));
          }
        }

        // System.out.println("Begin parsing sentence " + (sNum) +"...");

        // parse this sentence
        Tree<String> parsedTree = parser.getBestConstrainedParse(tokList, null);
        parsedTree = TreeAnnotations.unAnnotateTree(parsedTree);
        // System.out.println("Done parsing sentence " + (sNum++) +"...");
        // System.out.println(parsedTree);
        edu.stanford.nlp.trees.Tree stTree = BerkeleyToStanfordTreeConverter.convert(parsedTree);
        // add each node in tree to the annotation set of parses
        // System.out.println(parsedTree);
        // System.out.println(stTree);
        addSpans(parsedTree, 0, sentTok.toArray(), parses, Annotation.getNullAnnot());

        GrammaticalStructure gs = gsf.newGrammaticalStructure(stTree);
        Collection<TypedDependency> dep = gs.typedDependencies();
        ParserStanfordParser.addDepSpans(dep, sentTok.toArray(), depAnnots);
        // ParserStanfordParser.removeConjunctions(depAnnots);
      }
      catch (ArrayIndexOutOfBoundsException e) {
        e.printStackTrace();
      }
    }
  }

}

}

}
