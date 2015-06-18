package com.ipass.firefly.hotel.regex;

import com.ipass.firefly.hotel.exception.IPassException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by rjain on 7/22/2014.
 */
public class GrokMatcher {
    private final Logger LOG = LoggerFactory.getLogger(GrokMatcher.class);
    Map<String, Matcher> regexes=new HashMap<String, Matcher>();
    public GrokDictionaries dict;

    public boolean isFindSubstrings() {
        return findSubstrings;
    }

    public void setFindSubstrings(boolean findSubstrings) {
        this.findSubstrings = findSubstrings;
    }

    public boolean findSubstrings ;

    public Expression createExpression(String s, int flag) {
    return new  Expression(s,flag);
    }

    public Expression createExpression(String s) {
        return new  Expression(s);
    }

    public static enum NumRequiredMatches {
        AT_LEAST_ONCE,
        ONCE,
        ALL,
        ANY
    }

    NumRequiredMatches numRequiredMatches= NumRequiredMatches.ANY;

    public boolean addEmptyStrings;

    public Map<String, String> getOutputRecord() {
        return outputRecord;
    }
    public NumRequiredMatches getNumRequiredMatches() {
        return numRequiredMatches;
    }

    public void setNumRequiredMatches(NumRequiredMatches numRequiredMatches) {
        this.numRequiredMatches = numRequiredMatches;
    }

    public void setNumRequiredMatches(String numRequiredMatches) {
        this.numRequiredMatches = NumRequiredMatches.valueOf(numRequiredMatches.toUpperCase());
    }
    private Map<String,String> outputRecord=new HashMap<String, String>();

    public GrokMatcher(Map<String, String> dictionaries)
    {
        this.dict=new GrokDictionaries(dictionaries) ;
    }
    public GrokMatcher(GrokDictionaries dict)
    {
        this.dict=dict ;
    }

    public Matcher getMatcher(String expression,int flag)     {
       return dict.compileExpression(expression,flag).matcher("");
    }

    public Matcher getMatcher(String expression)     {
        return dict.compileExpression(expression).matcher("");
    }
    public void prepareExpressionMap(Map<String,Expression> expression)     {
        for (Map.Entry<String, Expression> regexEntry : expression.entrySet()) {
            Expression expr = (Expression) regexEntry.getValue();
            if (regexEntry.getValue().getFlag()>-1)
                 regexes.put(regexEntry.getKey(), dict.compileExpression(regexEntry.getValue().getExpr(),regexEntry.getValue().getFlag()).matcher(""));
            else
                regexes.put(regexEntry.getKey(), dict.compileExpression(regexEntry.getValue().getExpr()).matcher(""));
        }
    }

    public boolean doMatch(String fieldName,String fieldValue, boolean doExtract) {
       return doMatch(fieldName,fieldValue, doExtract,findSubstrings) ;
    }

    public boolean doMatch(String fieldName,String fieldValue, boolean doExtract,boolean doFindSubstrings) {
        outputRecord.clear();
        Matcher matcher = regexes.get(fieldName);
        String value = fieldValue;
        if (value == null)
            return false;
        int todo = 1;//values.size();
        int minMatches = 1;
        int maxMatches = Integer.MAX_VALUE;
        switch (numRequiredMatches) {
            case ONCE:  {
                maxMatches = 1;
                break;
            }
            case ALL : {
                minMatches = todo;
                break;
            }
            case ANY : {
                minMatches = 0;
                break;
            }
            default: {
                break;
            }
        }
        int numMatches = 0;
        //for (Object value : values) {
        matcher.reset(value.toString());
        if (!doFindSubstrings) {
            if (matcher.matches()) {
                numMatches++;
                if (numMatches > maxMatches) {
                    LOG.debug("grok failed because it found too many matches for values: {} for grok command: {}",
                            value,matcher.toString());
                    return false;
                }
                extract(outputRecord, matcher, doExtract);
            }
        } else {
            int previousNumMatches = numMatches;
            while (matcher.find()) {
                numMatches++;
                if (!doExtract && numMatches >= minMatches && maxMatches == Integer.MAX_VALUE) {
                    break; // fast path
                }
                extract(outputRecord, matcher, doExtract);
            }
        }
//                if (!doExtract && numMatches >= minMatches && maxMatches == Integer.MAX_VALUE) {
//            break; // fast path
//        }
        // }
        if (numMatches==0&&!numRequiredMatches.equals(NumRequiredMatches.ANY))
        {
            LOG.debug("grok failed because it didn't found matches for values: {} for grok command: {}",
                    value, matcher.toString());
            return false;
        }
        if (numMatches < minMatches) {
            LOG.debug("grok failed because it found too few matches for values: {} for grok command: {}",
                    value, matcher.toString());
            return false;
        }
        return true;
    }

    private void extract(Map<String,String> outputRecord, Matcher matcher, boolean doExtract) {
        if (doExtract) {
            extractFast(outputRecord, matcher);
        }
    }

    private void extractFast(Map<String,String> outputRecord, Matcher matcher) {
        for (Map.Entry<String, List<GroupInfo>> entry : matcher.namedPattern().groupInfo().entrySet()) {
            String groupName = entry.getKey();
            List<GroupInfo> list = entry.getValue();
            int idx = list.get(0).groupIndex();
            int group = idx > -1 ? idx + 1 : -1; // TODO cache that number (perf)?
            String value = matcher.group(group);
            if (value != null && (value.length() > 0 || addEmptyStrings)) {
                outputRecord.put(groupName, value);
            }
        }
    }

    public class  Expression {
        public static final int CASE_INSENSITIVE= java.util.regex.Pattern.CASE_INSENSITIVE;
        public static final int MULTILINE= java.util.regex.Pattern.MULTILINE;
        public static final int DOTALL= java.util.regex.Pattern.DOTALL;
        public static final int UNICODE_CASE= java.util.regex.Pattern.UNICODE_CASE;
        public static final int CANON_EQ= java.util.regex.Pattern.CANON_EQ;
        public static final int UNIX_LINES= java.util.regex.Pattern.UNIX_LINES;
        public static final int LITERAL= java.util.regex.Pattern.LITERAL;
        public static final int COMMENTS= java.util.regex.Pattern.COMMENTS;
        private String expr;

        public String getExpr() {
            return expr;
        }

        public int getFlag() {
            return flag;
        }

        private int flag;
        public Expression(String expr,int flag)
        {
            this.expr=expr;
            this.flag=flag;
        }

        public Expression(String expr,String flagStr)
        {
            this.expr=expr;
            this.flag=getFlagValueString(flagStr);
        }

        public Expression(String expr)
        {
            this.expr=expr;
            this.flag=-1;
        }

        public  int getFlagValueString(String flagStr)
        {      int flag;
            String s = flagStr.toUpperCase();
            if (s.equals("CASE_INSENSITIVE")) {
                flag = java.util.regex.Pattern.CASE_INSENSITIVE;
            } else if (s.equals("MULTILINE")) {
                flag = java.util.regex.Pattern.MULTILINE;
            } else if (s.equals("DOTALL")) {
                flag = java.util.regex.Pattern.DOTALL;
            } else if (s.equals("UNICODE_CASE")) {
                flag = java.util.regex.Pattern.UNICODE_CASE;
            } else if (s.equals("CANON_EQ")) {
                flag = java.util.regex.Pattern.CANON_EQ;
            } else if (s.equals("UNIX_LINES")) {
                flag = java.util.regex.Pattern.UNIX_LINES;
            } else if (s.equals("LITERAL")) {
                flag = java.util.regex.Pattern.LITERAL;
            } else if (s.equals("COMMENTS")) {
                flag = java.util.regex.Pattern.COMMENTS;
            } else {
                throw new IPassException("No flag type found this name: " + flagStr.toUpperCase());
            }
            return flag ;
        }

    }
}
