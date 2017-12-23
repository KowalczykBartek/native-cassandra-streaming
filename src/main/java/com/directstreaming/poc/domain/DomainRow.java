package com.directstreaming.poc.domain;

import java.util.UUID;

public class DomainRow {
    private final String value1;
    private final String value2;
    private final long value3;
    private final UUID value4;
    private final long value5;
    private final long value6;

    public DomainRow(final String value1, final String value2, final long value3, final UUID value4, final long value5, final long value6) {
        this.value1 = value1;
        this.value2 = value2;
        this.value3 = value3;
        this.value4 = value4;
        this.value5 = value5;
        this.value6 = value6;
    }

    public String getValue1() {
        return value1;
    }

    public String getValue2() {
        return value2;
    }

    public long getValue3() {
        return value3;
    }

    public UUID getValue4() {
        return value4;
    }

    public long getValue5() {
        return value5;
    }

    public long getValue6() {
        return value6;
    }

    /**
     * Not thread-safe - but this is Netty ! it doesn't have to be thread safe.
     */
    public static class DomainRowBuilder {
        private String value1;
        private String value2;
        private long value3;
        private UUID value4;
        private long value5;
        private long value6;

        public DomainRowBuilder withValue1(final String value1) {
            this.value1 = value1;
            return this;
        }

        public DomainRowBuilder withValue2(final String value2) {
            this.value2 = value2;
            return this;
        }

        public DomainRowBuilder withValue3(final long value3) {
            this.value3 = value3;
            return this;
        }

        public DomainRowBuilder withValue4(final UUID value4) {
            this.value4 = value4;
            return this;
        }

        public DomainRowBuilder withValue5(final long value5) {
            this.value5 = value5;
            return this;
        }

        public DomainRowBuilder withValue6(final long value6) {
            this.value6 = value6;
            return this;
        }

        public DomainRow build() {
            return new DomainRow(value1, value2, value3, value4, value5, value6);
        }
    }
}
