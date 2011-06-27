package reconcile.hbase.parser;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.google.common.collect.Maps;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreAnnotations.CharacterOffsetBeginAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.CharacterOffsetEndAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.CollapsedCCProcessedDependenciesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.CorefGraphAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.PartOfSpeechAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TreeAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.trees.EnglishGrammaticalRelations;
import edu.stanford.nlp.trees.GrammaticalRelation;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.trees.semgraph.SemanticGraph;
import edu.stanford.nlp.trees.semgraph.SemanticGraphEdge;
import edu.stanford.nlp.util.CoreMap;
import edu.stanford.nlp.util.IntTuple;
import edu.stanford.nlp.util.Pair;

import gov.llnl.text.util.FileUtils;

import reconcile.data.AnnotationSet;
import reconcile.general.Constants;

// @SuppressWarnings("rawtypes")
public class StanfordCoref {

// private static Pattern pWord = Pattern.compile("\\w");
// private Map<String, String> mTypeMap = Maps.newHashMap();

public static final String COREF_DEST = "corefDest";

private static final int M = 1000000;

public static final String STANFORD_PARSE = "stanford_parse";

public static final String STANFORD_DEP = "stanford_dependencies";

public static final String STANFORD_COREF = "stanford_coref";

/**
 *
 */
public StanfordCoref() {

  init();
}

private StanfordCoreNLP pipeline;

public void init()
{
  // creates a StanfordCoreNLP object, with tokenization and sentence splitting
  Properties props = new Properties();
  props.put("annotators", "tokenize, ssplit, pos, lemma, ner, parse, dcoref");
  pipeline = new StanfordCoreNLP(props);

}

public Map<String, AnnotationSet> parse(String text)
{

  Map<String, AnnotationSet> setMap = Maps.newHashMap();
  AnnotationSet tokenSet = new AnnotationSet(Constants.TOKEN);
  setMap.put(Constants.TOKEN, tokenSet);
  AnnotationSet sentenceSet = new AnnotationSet(Constants.SENT);
  setMap.put(Constants.SENT, sentenceSet);
  AnnotationSet posSet = new AnnotationSet(Constants.POS);
  setMap.put(Constants.POS, posSet);
  AnnotationSet nerSet = new AnnotationSet(Constants.NE);
  setMap.put(Constants.NE, nerSet);
  AnnotationSet parseSet = new AnnotationSet(STANFORD_PARSE);
  setMap.put(STANFORD_PARSE, parseSet);
  AnnotationSet depSet = new AnnotationSet(STANFORD_DEP);
  setMap.put(STANFORD_DEP, depSet);
  AnnotationSet dcoref = new AnnotationSet(STANFORD_COREF);
  setMap.put(STANFORD_COREF, dcoref);

  // create an empty Annotation just with the given text
  Annotation document = new Annotation(text);

  // run all Annotators on this text
  pipeline.annotate(document);

  // these are all the sentences in this document
  // a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
  List<CoreMap> sentences = document.get(SentencesAnnotation.class);

  Map<Integer, CoreMap> sentMap = Maps.newHashMap();
  int sentId = 1;
  for (CoreMap sentence : sentences) {
    sentMap.put(sentId++, sentence);

    // traversing the words in the current sentence
    // a CoreLabel is a CoreMap with additional token-specific methods
    int sentBegin = sentence.get(CharacterOffsetBeginAnnotation.class);
    int sentEnd = sentence.get(CharacterOffsetEndAnnotation.class);
    sentenceSet.add(sentBegin, sentEnd, Constants.SENT);

    // traversing the words in the current sentence
    // a CoreLabel is a CoreMap with additional token-specific methods
    String lastType = null;
    int begin = 0;
    int lastEnd = 0;

    for (CoreLabel token : sentence.get(TokensAnnotation.class)) {
      int currentBegin = token.get(CharacterOffsetBeginAnnotation.class);
      int currentEnd = token.get(CharacterOffsetEndAnnotation.class);
      tokenSet.add(currentBegin, currentEnd, Constants.TOKEN);
      // // this is the text of the token
      // String word = token.get(TextAnnotation.class);
      // this is the POS tag of the token
      String pos = token.get(PartOfSpeechAnnotation.class);
      posSet.add(currentBegin, currentEnd, pos);
      // this is the NER label of the token
      String ne = token.get(NamedEntityTagAnnotation.class);
      if (ne.equals(lastType)) {
        lastEnd = currentEnd;
      }
      else {
        if (lastType != null && !lastType.equalsIgnoreCase("O")) {
          nerSet.add(begin, lastEnd, lastType);
        }
        lastType = ne;
        begin = currentBegin;
        lastEnd = currentEnd;
      }
    }

    // this is the parse tree of the current sentence
    Tree tree = sentence.get(TreeAnnotation.class);

    AnnotationSet toks = tokenSet.getOverlapping(sentBegin, sentEnd);
    addSpans(tree, 0, toks.toArray(), parseSet, reconcile.data.Annotation.getNullAnnot());

    // Add the dependencies

    // this is the Stanford dependency graph of the current sentence
    SemanticGraph dependencies = sentence.get(CollapsedCCProcessedDependenciesAnnotation.class);
    addDepSpans(dependencies, toks.toArray(), depSet);
  }

  // this is the coreference link graph
  // each link stores an arc in the graph; the first element in the Pair is the source, the second is the target
  // each node is stored as <sentence id, token id>. Both offsets start at 1!
  List<Pair<IntTuple, IntTuple>> graph = document.get(CorefGraphAnnotation.class);

  reconcile.data.Annotation[] toks = tokenSet.toArray();

  List<List<CoreLabel>> sents = new ArrayList<List<CoreLabel>>();
  int i = 1;
  for (CoreMap sentence : document.get(CoreAnnotations.SentencesAnnotation.class)) {
    List<CoreLabel> tokens = sentence.get(CoreAnnotations.TokensAnnotation.class);
    sents.add(tokens);
    for (int j = 0; j < tokens.size(); j++) {
      System.out.println(i + "," + (j + 1) + ": " + tokens.get(j).get(TextAnnotation.class));
    }
    i++;
  }

  printCoref(graph, sentMap, sents, toks, dcoref);

  return setMap;
}

private void printCoref(List<Pair<IntTuple, IntTuple>> graph, Map<Integer, CoreMap> sentMap,
    List<List<CoreLabel>> sents, reconcile.data.Annotation[] toks, AnnotationSet dcoref)
{
  // note: stanford uses 1 based offsets
  for (Pair<IntTuple, IntTuple> link : graph) {
    System.out.println("pair: " + link.toString());

    int firstSentId = link.first.get(0);
    int firstTokenId = link.first.get(1);
    System.out.println("first id: (" + firstSentId + "," + firstTokenId + ")");
    CoreLabel token = sents.get(firstSentId - 1).get(firstTokenId - 1);
    System.out.println("source token (first): " + token.get(TextAnnotation.class));

    int secSentId = link.second.get(0);
    int secTokenId = link.second.get(1);
    System.out.println("second id: (" + secSentId + "," + secTokenId + ")");
    CoreLabel destToken = sents.get(secSentId - 1).get(secTokenId - 1);
    System.out.println("destination token (second): " + destToken.get(TextAnnotation.class));

    reconcile.data.Annotation sourceA = new reconcile.data.Annotation(M * firstSentId + firstTokenId,
        token.beginPosition(), token.endPosition(), Constants.TOKEN);
    reconcile.data.Annotation destA = new reconcile.data.Annotation(M * secSentId + secTokenId,
        destToken.beginPosition(), destToken.endPosition(), Constants.TOKEN);

    if (!dcoref.contains(sourceA)) {
      dcoref.add(sourceA);
    }
    if (!dcoref.contains(destA)) {
      dcoref.add(destA);
    }
    String dest = sourceA.getAttribute(COREF_DEST);
    if (dest == null) {
      sourceA.setAttribute(COREF_DEST, Integer.toString(destA.getId()));
    }
    else {
      sourceA.setAttribute(COREF_DEST, dest + "," + Integer.toString(destA.getId()));
    }

  }
}

public static void main(String[] args)
{
  try {
    String s = FileUtils.readFile(new File(args[0])); // "David said he would try the new coref";
    StanfordCoref c = new StanfordCoref();
    Map<String, AnnotationSet> map = c.parse(s);
    for (String setName : map.keySet()) {
      System.out.println("returned set: " + setName);
    }
  }
  catch (IOException e) {
    e.printStackTrace();
  }

}

// recursive method to traverse a tree while adding spans of nodes to the annotset
@SuppressWarnings("rawtypes")
private int addSpans(Tree parseTree, int startTokenIndx, Object[] sentToks, AnnotationSet parsesSet,
    reconcile.data.Annotation parent)
{
  // System.err.println("Adding "+parseTree+" - "+startTokenIndx);
  List yield = parseTree.yield();
  int len = yield.size();// sentToks.length;
  Map<String, String> attrs = Maps.newTreeMap();
  attrs.put("parent", Integer.toString(parent.getId()));
  int cur = parsesSet.add(((reconcile.data.Annotation) sentToks[startTokenIndx]).getStartOffset(),
      ((reconcile.data.Annotation) sentToks[len + startTokenIndx - 1]).getEndOffset(), parseTree.value(), attrs);
  reconcile.data.Annotation curAn = parsesSet.get(cur);
  addChild(parent, curAn);
  int offset = startTokenIndx;
  for (Tree tr : parseTree.children()) {
    offset += addSpans(tr, offset, sentToks, parsesSet, curAn);
  }
  return len;
}

private void addDepSpans(SemanticGraph dependencies, reconcile.data.Annotation[] sentToks, AnnotationSet parsesSet)
{
  reconcile.data.Annotation[] sentDeps = new reconcile.data.Annotation[sentToks.length];
  // First create all annotations
  int index = 0;
  for (reconcile.data.Annotation tok : sentToks) {
    // System.err.println(d +" gov "+ d.gov().index());
    int id = parsesSet.add(tok.getStartOffset(), tok.getEndOffset(), "ROOT");
    reconcile.data.Annotation added = parsesSet.get(id);
    sentDeps[index] = added;
    index++;
  }

  for (SemanticGraphEdge edge : dependencies.edgeList()) {
    int dependent = edge.getDependent().index() - 1;
    int gov = edge.getGovernor().index() - 1;

    reconcile.data.Annotation depAn = sentDeps[dependent];
    reconcile.data.Annotation govAn = sentDeps[gov];

    String offset = govAn.getStartOffset() + "," + govAn.getEndOffset();
    depAn.setAttribute("GOV", offset);
    depAn.setAttribute("GOV_ID", Integer.toString(govAn.getId()));
    addChild(govAn, depAn);

    GrammaticalRelation rel = edge.getRelation();
    String relName = rel.toString();
    if (EnglishGrammaticalRelations.SUBJECT.isAncestor(rel)) {
      relName = "SUBJECT";
    }
    else if (EnglishGrammaticalRelations.OBJECT.isAncestor(rel)) {
      relName = "OBJECT";
    }
    else if (EnglishGrammaticalRelations.APPOSITIONAL_MODIFIER.isAncestor(rel)) {
      relName = "APPOS";
    }
    depAn.setType(relName);
  }
}

public static void addChild(reconcile.data.Annotation parent, reconcile.data.Annotation child)
{
  if (!parent.equals(reconcile.data.Annotation.getNullAnnot())) {
    String childIds = parent.getAttribute("CHILD_IDS");
    childIds = childIds == null ? Integer.toString(child.getId()) : childIds + "," + child.getId();
    parent.setAttribute("CHILD_IDS", childIds);
  }
}

public static void removeConjunctions(AnnotationSet dep)
{
  // Conjunctions are uninteresting. Assign the dependency of the parent
  for (reconcile.data.Annotation d : dep) {
    String type = d.getType();
    if (type.equalsIgnoreCase("conj")) {
      reconcile.data.Annotation a = d;
      while (type.equalsIgnoreCase("conj")) {
        String[] span = (a.getAttribute("GOV")).split("\\,");
        int stSpan = Integer.parseInt(span[0]);
        int endSpan = Integer.parseInt(span[1]);
        a = dep.getContained(stSpan, endSpan).getFirst();
        if (a == null) {
          break;
        }
        type = a.getType();
      }
      if (a != null) {
        d.setType(type);
        d.setAttribute("GOV", a.getAttribute("GOV"));
      }
    }
  }
}

}
