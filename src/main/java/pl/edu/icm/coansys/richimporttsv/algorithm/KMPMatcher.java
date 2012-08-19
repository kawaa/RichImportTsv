/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package pl.edu.icm.coansys.richimporttsv.algorithm;

/**
 *
 * @author akawa
 */
public class KMPMatcher {
    
    public static final int FAILURE = -1;

    /**
     * Finds the first occurrence of the pattern in the text starting form index.
     */
    public static int indexOf(byte[] text, int index, byte[] pattern) {
        int[] failure = getFailures(pattern);

        int j = 0;
        if (text.length == 0) {
            return FAILURE;
        }

        for (int i = index; i < text.length; i++) {
            while (j > 0 && pattern[j] != text[i]) {
                j = failure[j - 1];
            }
            if (pattern[j] == text[i]) {
                j++;
            }
            if (j == pattern.length) {
                return i - pattern.length + 1;
            }
        }
        return FAILURE;
    }

    /**
     * Computes the failure function using a boot-strapping process, where the
     * pattern is matched against itself.
     */
    private static int[] getFailures(byte[] pattern) {
        int[] failure = new int[pattern.length];

        int j = 0;
        for (int i = 1; i < pattern.length; i++) {
            while (j > 0 && pattern[j] != pattern[i]) {
                j = failure[j - 1];
            }
            if (pattern[j] == pattern[i]) {
                j++;
            }
            failure[i] = j;
        }

        return failure;
    }
}
