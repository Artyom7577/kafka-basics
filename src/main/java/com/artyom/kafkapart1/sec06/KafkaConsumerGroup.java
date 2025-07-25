package com.artyom.kafkapart1.sec06;


public class KafkaConsumerGroup {

    private static class Consumer1 {
        public static void main(String[] args) {
            KafkaConsumer.strat("1");
            // 0
        }
    }

    private static class Consumer2 {
        public static void main(String[] args) {
            KafkaConsumer.strat("2");
            // 2
        }
    }

    private static class Consumer3 {
        public static void main(String[] args) {
            KafkaConsumer.strat("3");
            // 1
        }
    }
}
