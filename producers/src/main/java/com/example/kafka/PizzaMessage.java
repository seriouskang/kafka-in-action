package com.example.kafka;

import com.github.javafaker.Faker;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class PizzaMessage {
    private static final List<String> pizzaNames = List.of("Potato Pizza", "Cheese Pizza",
            "Cheese Garlic Pizza", "Super Supreme", "Peperoni");
    private static final List<String> pizzaShop = List.of("A001", "B001", "C001",
            "D001", "E001", "F001", "G001", "H001", "I001", "J001", "K001", "L001", "M001", "N001",
            "O001", "P001", "Q001");

    public String getRandomValueFromList(List<String> list, Random random) {
        int index = random.nextInt(list.size());
        return list.get(index);
    }

    public Map<String, String> produceMsg(Faker faker, Random random, int id) {
        String shopId = getRandomValueFromList(pizzaShop, random);
        String pizzaName = getRandomValueFromList(pizzaNames, random);

        String message = String.format("%s/%s/%s/%s/%s/%s/%s",
                id,
                shopId,
                pizzaName,
                faker.name().fullName(),
                faker.phoneNumber().phoneNumber(),
                faker.address().streetAddress(),
                LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.KOREAN))
        );

        return Map.of(
                "key", shopId,
                "message", message
        );
    }
    public static void main(String[] args) {
        PizzaMessage pizzaMessage = new PizzaMessage();
        Random random = new Random(2025);
        Faker faker = Faker.instance(random);

        for(int i=0; i<60; i++) {
            Map<String, String> message = pizzaMessage.produceMsg(faker, random, i);
            System.out.println("key:" + message.get("key") + ", message:" + message.get("message"));
        }
    }
}
