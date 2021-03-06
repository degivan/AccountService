package ru.ifmo.degtiarenko.splat.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.regex.Pattern;

/**
 * The <code>Identifiers</code> class encapsulates an information about acceptable <code>id</code> values and provides method
 * to get random value with equal probability for all values.
 */
public class Identifiers {
    private static final String BAD_ARGUMENT_MESSAGE = "Wrong string format.";
    private static final Pattern PATTERN = Pattern.compile("\\d+(-\\d+)?");
    private final Random random;
    private final List<Range> ranges;
    private int size;

    /**
     * <B>Note:</B> accepts values in format like that: "1,2,4-5,6-10"
     *
     * @param args acceptable values or ranges of values separated by commas
     * @throws BadArgumentException if <code>args</code> format is unacceptable
     */
    public Identifiers(String args) throws BadArgumentException {
        random = new Random();
        ranges = new ArrayList<>();

        String[] stringRanges = args.split(",");

        for (String range : stringRanges) {
            if (!PATTERN.matcher(range).matches()) {
                throw new BadArgumentException(BAD_ARGUMENT_MESSAGE);
            } else {
                String[] fromTo = range.split("-");
                int from = Integer.parseInt(fromTo[0]);
                int to = fromTo.length == 2 ? Integer.parseInt(fromTo[1]) : from;

                if (from > to)
                    throw new BadArgumentException(BAD_ARGUMENT_MESSAGE);

                ranges.add(new Range(from, size, size + (to - from + 1)));
                size += (to - from + 1);
            }
        }
    }

    /**
     * Gets random acceptable identifier value.
     *
     * @return identifier
     */
    public int getRandomIdentifier() {
        int randomIndex = Math.abs(random.nextInt() % size);
        int result = 0;
        for (Range range : ranges) {
            if (randomIndex < range.getRandomTo() && randomIndex >= range.getRandomFrom())
                result = range.getElement(randomIndex - range.getRandomFrom());
        }
        return result;
    }

    private static class Range {
        private final int from;
        private final int randomFrom;
        private final int randomTo;

        public Range(int from, int randomFrom, int randomTo) {
            this.from = from;
            this.randomFrom = randomFrom;
            this.randomTo = randomTo;
        }


        public int getRandomFrom() {
            return randomFrom;
        }

        public int getRandomTo() {
            return randomTo;
        }

        public int getElement(int index) {
            return (from + index);
        }
    }
}
