package com.nio.reader.model;

public class Product {

    int id;
    String name;
    String condition;
    String state;
    double price;

    public Product(int id, String name, String condition, String state, double price) {
        this.id = id;
        this.name = name;
        this.condition = condition;
        this.state = state;
        this.price = price;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCondition() {
        return condition;
    }

    public void setCondition(String condition) {
        this.condition = condition;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }
}
