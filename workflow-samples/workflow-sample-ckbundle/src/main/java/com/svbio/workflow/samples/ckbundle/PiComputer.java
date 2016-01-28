package com.svbio.workflow.samples.ckbundle;

import java.math.BigDecimal;
import java.math.RoundingMode;

import static java.math.BigDecimal.ONE;
import static java.math.BigDecimal.TEN;
import static java.math.BigDecimal.ZERO;

final class PiComputer {
    private static final BigDecimal TWO = BigDecimal.valueOf(2);
    private static final BigDecimal THREE = BigDecimal.valueOf(3);
    private static final BigDecimal FOUR = BigDecimal.valueOf(4);

    private PiComputer() {
        throw new AssertionError(String.format("No %s instances for you!", getClass().getName()));
    }

    /**
     * Linear fractional transformation that maps a {@link BigDecimal} number {@code x} to {@code (a * x + b) / c}.
     *
     * <p>A fractional transformation (also called a Möbius transformation) of the above form can be represented as a
     * upper-triangular 2 x 2 matrix where the left column contains {@code a} and 0, and the right column contains
     * {@code b} and {@code c}. Using this representation, the composition of linear fractional transformations is
     * equivalent to Matrix multiplication. Formally, let {@code H} be the mapping from linear functional transformation
     * to the matrices as defined before. That is, if {@code f(x) = (a * x + b) / c}, then
     * {@code H(f) = ((a, 0) (b, c))}. It is easy to verify that {@code H(f o g) = H(f) H(g)}. Here, {@code f o g}
     * denotes the function that maps {@code x} to {@code f(g(x))}.
     */
    private static final class MoebiusTransformation {
        private final BigDecimal topLeft;
        private final BigDecimal topRight;
        private final BigDecimal bottomRight;

        private MoebiusTransformation(BigDecimal topLeft, BigDecimal topRight, BigDecimal bottomRight) {
            this.topLeft = topLeft;
            this.topRight = topRight;
            this.bottomRight = bottomRight;
        }

        @Override
        public String toString() {
            return String.format("x -> (%s * x + %s) / %s", topLeft, topRight, bottomRight);
        }

        private static MoebiusTransformation unit() {
            return new MoebiusTransformation(ONE, ZERO, ONE);
        }

        private BigDecimal evaluate(BigDecimal x, RoundingMode roundingMode) {
            return topLeft.multiply(x).add(topRight).divide(bottomRight, roundingMode);
        }

        /**
         * Returns a new Möbius transformation that is the composition of the current transformation <em>after</em> the
         * given transformation.
         *
         * @param other other Möbius transformation
         * @return the new Möbius transformation
         */
        private MoebiusTransformation concatenate(MoebiusTransformation other) {
            return new MoebiusTransformation(
                topLeft.multiply(other.topLeft),
                topLeft.multiply(other.topRight).add(topRight.multiply(other.bottomRight)),
                bottomRight.multiply(other.bottomRight)
            );
        }
    }

    private static MoebiusTransformation sequence(BigDecimal index) {
        return new MoebiusTransformation(
            index,
            FOUR.multiply(index).add(TWO),
            TWO.multiply(index).add(ONE)
        );
    }

    private static MoebiusTransformation digitExtractionTransformation(int digit) {
        return new MoebiusTransformation(
            TEN,
            TEN.negate().multiply(BigDecimal.valueOf(digit)),
            ONE
        );
    }

    /**
     * Returns a {@link String} containing the given number of digits of the decimal representation of π.
     *
     * <p>This method implements a streaming algorithm (also called <em>spigot algorithm</em> in the literature) based
     * on the paper <a href="http://doi.org/10.2307/27641917">Unbounded Spigot Algorithms for the Digits of Pi</a> by
     * Jeremy Gibbons (2006).
     *
     * @param numDigits number of digits in the decimal representation to include
     */
    static String computePi(int numDigits) {
        StringBuilder stringBuilder = new StringBuilder(numDigits + 1);
        MoebiusTransformation state = MoebiusTransformation.unit();
        int digitCount = 0;
        BigDecimal index = ONE;
        while (digitCount < numDigits) {
            state = state.concatenate(sequence(index));
            index = index.add(ONE);
            int digit = state.evaluate(THREE, RoundingMode.FLOOR).intValue();
            if (digit == state.evaluate(FOUR, RoundingMode.FLOOR).intValue()) {
                ++digitCount;
                if (digitCount == 2) {
                    stringBuilder.append('.');
                }
                stringBuilder.append(digit);
                state = digitExtractionTransformation(digit).concatenate(state);
            }
        }
        return stringBuilder.toString();
    }
}
