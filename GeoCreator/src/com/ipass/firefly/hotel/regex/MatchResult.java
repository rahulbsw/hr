package com.ipass.firefly.hotel.regex;

import java.util.List;
import java.util.Map;

/**
 * The result of a match operation.
 *
 * <p>This interface contains query methods used to determine the results of
 * a match against a regular expression. The match boundaries, groups and
 * group boundaries can be seen but not modified through a MatchResult.</p>
 *
 * @since 0.1.9
 */
public interface MatchResult extends java.util.regex.MatchResult {

    /**
     * Returns the named capture groups in order
     *
     * @return the named capture groups
     */
    public List<String> orderedGroups();

    /**
     * Returns the named capture groups
     *
     * @return the named capture groups
     */
    public Map<String, String> namedGroups();

    /**
     * Returns the input subsequence captured by the given group during the
     * previous match operation.
     *
     * @param groupName name of capture group
     * @return the subsequence
     */
    public String group(String groupName);

    /**
     * Returns the start index of the subsequence captured by the given group
     * during this match.
     *
     * @param groupName name of capture group
     * @return the index
     */
    public int start(String groupName);

    /**
     * Returns the offset after the last character of the subsequence captured
     * by the given group during this match.
     *
     * @param groupName name of capture group
     * @return the offset
     */
    public int end(String groupName);

}