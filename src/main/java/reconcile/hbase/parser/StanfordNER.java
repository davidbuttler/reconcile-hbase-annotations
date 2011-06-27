package reconcile.hbase.parser;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.google.common.collect.Maps;

import edu.stanford.nlp.ling.CoreAnnotations.CharacterOffsetBeginAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.CharacterOffsetEndAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.PartOfSpeechAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

import gov.llnl.text.util.FileUtils;

import reconcile.data.AnnotationSet;
import reconcile.general.Constants;

// @SuppressWarnings("rawtypes")
public class StanfordNER {

// private static Pattern pWord = Pattern.compile("\\w");
// private Map<String, String> mTypeMap = Maps.newHashMap();

/**
 *
 */
public StanfordNER() {

  init();
}

private StanfordCoreNLP pipeline;

private StanfordCoreNLP sentencePipeline;

public static final String STANFORD_ORGANIZATION_QUAL = "Stanford_organization";

public static final String STANFORD_LOCATION_QUAL = "Stanford_location";

public static final String STANFORD_PERSON_QUAL = "Stanford_person";


public void init()
{
  // creates a StanfordCoreNLP object, with tokenization and sentence splitting
  Properties props = new Properties();
  // props.put("annotators", "tokenize, ssplit, pos, lemma, ner, parse, dcoref");
  props.put("annotators", "tokenize, ssplit, pos, lemma, ner");
  pipeline = new StanfordCoreNLP(props);

  Properties sentenceProps = new Properties();
  // props.put("annotators", "tokenize, ssplit, pos, lemma, ner, parse, dcoref");
  sentenceProps.put("annotators", "tokenize, ssplit, pos, lemma");
  sentencePipeline = new StanfordCoreNLP(sentenceProps);


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

  // create an empty Annotation just with the given text
  Annotation document = new Annotation(text);

  // run all Annotators on this text
  sentencePipeline.annotate(document);

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

    String sentText = text.substring(sentBegin, sentEnd);
    Map<String, AnnotationSet> neSubMap = parseAll(sentText);
    AnnotationSet neSubSet = neSubMap.get(Constants.NE);
    for (reconcile.data.Annotation a : neSubSet) {
      nerSet.add(sentBegin + a.getStartOffset(), sentBegin + a.getEndOffset(), a.getType());
    }

    // traversing the words in the current sentence
    // a CoreLabel is a CoreMap with additional token-specific methods
    // String lastType = null;
    // int begin = 0;
    // int lastEnd = 0;

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
      // String ne = token.get(NamedEntityTagAnnotation.class);
      // if (ne.equals(lastType)) {
      // lastEnd = currentEnd;
      // }
      // else {
      // if (lastType != null && !lastType.equalsIgnoreCase("O")) {
      // nerSet.add(begin, lastEnd, lastType);
      // }
      // lastType = ne;
      // begin = currentBegin;
      // lastEnd = currentEnd;
      // }
    }
  }

  return setMap;

}

public Map<String, AnnotationSet> parseAll(String text)
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
  }

  return setMap;
}


public static void main(String[] args)
{
  try {
    String s = FileUtils.readFile(new File(args[0])); // "David said he would try the new coref";
    StanfordNER c = new StanfordNER();
    Map<String, AnnotationSet> map = c.parseAll(s);
    for (String setName : map.keySet()) {
      System.out.println("returned set: " + setName);
    }
  }
  catch (IOException e) {
    e.printStackTrace();
  }

}




}
