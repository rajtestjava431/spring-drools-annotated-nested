package com.example.drools.model;

import lombok.Data;

import java.io.Serializable;

@Data
public class Payload implements Serializable {
    @Group("user")
    private String name;

    @Group("user")
    private int age;

    @Group("order")
    private double amount;

    private String type;

    // Getters and Setters
}