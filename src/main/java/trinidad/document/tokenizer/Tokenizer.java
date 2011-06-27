package trinidad.document.tokenizer;

import java.util.List;
import java.util.Map;

public abstract class Tokenizer {

public abstract Map<String, Integer> getDocumentTerms(String text);

public abstract List<String> getDocumentTermList(String text);

}
