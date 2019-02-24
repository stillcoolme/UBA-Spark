package com.stillcoolme.web;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author stillcoolme
 * @date 2019/2/24 21:24
 */
@RestController
@SpringBootApplication
public class UbaApplication {

    @RequestMapping("/")
    String index() {
        return "hello spring boot";
    }

    public static void main(String[] args) {
        SpringApplication.run(UbaApplication.class, args);
    }
}