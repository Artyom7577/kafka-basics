package com.artyom.kafkapart1.sec05;

public class KafkaConsumerGroup {
    /*
        RangeAssignor

        0, 1, 2, 3

        2, 3

        Consumer1 -> 0
        Consumer2 -> 1
        Consumer3 -> 2

     */

    private static class Consumer1 {
        public static void main(String[] args) {
            KafkaConsumer.strat("1");
            // 0
        }
    }

    private static class Consumer2 {
        public static void main(String[] args) {
            KafkaConsumer.strat("2");
            // 1
        }
    }

    private static class Consumer3 {
        public static void main(String[] args) {
            KafkaConsumer.strat("3");
            // 2
        }
    }
}
