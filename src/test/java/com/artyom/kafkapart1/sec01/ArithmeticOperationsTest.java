package com.artyom.kafkapart1.sec01;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ArithmeticOperationsTest {
    private final ArithmeticOperations ops = new ArithmeticOperations();

    @Test
    void testAdd() {
        assertEquals(5, ops.add(2, 3));
        assertEquals(-1, ops.add(2, -3));
        assertEquals(0, ops.add(0, 0));
    }

    @Test
    void testSubtract() {
        assertEquals(-1, ops.subtract(2, 3));
        assertEquals(5, ops.subtract(2, -3));
        assertEquals(0, ops.subtract(0, 0));
    }

    @Test
    void testMultiply() {
        assertEquals(6, ops.multiply(2, 3));
        assertEquals(-6, ops.multiply(2, -3));
        assertEquals(0, ops.multiply(0, 5));
    }

    @Test
    void testDivide() {
        assertEquals(2, ops.divide(6, 3));
        assertEquals(-2, ops.divide(6, -3));
        assertEquals(0, ops.divide(0, 5));
    }

    @Test
    void testDivideByZero() {
        assertThrows(ArithmeticException.class, () -> ops.divide(5, 0));
    }
} 