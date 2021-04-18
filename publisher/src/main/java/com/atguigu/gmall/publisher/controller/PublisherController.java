package com.atguigu.gmall.publisher.controller;


import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
//@Controller  返回渲染好的页面
@RestController //返回数据的结果
public class PublisherController {

    @RequestMapping("/hello/{name}")
    public String helloWorld(@RequestParam("date") String dt){
        System.out.println(dt);
        return "Hello World";
    }
}