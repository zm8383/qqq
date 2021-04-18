package com.atguigu;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.zookeeper.data.Id;

import java.util.ArrayList;
import java.util.List;

public class JsonTest {

    public static void main(String[] args) {

//        对象转字符串
        Movie movie = new Movie();
        movie.id="1";
        movie.name="色戒";
        movie.doubanScore=6.5D;


        Director director = new Director();
        director.id="0";
        director.name="李安";
        movie.director=director;

        ArrayList<Actor> actors = new ArrayList<>();

        actors.add(new Actor("111","梁朝伟"));
        actors.add(new Actor("222","汤唯"));

        movie.actorList=actors;

        //System.out.println(movie.toString());
//     todo  对象转JSON字符串
        String jsonString = JSON.toJSONString(movie);
        System.out.println("JSON字符串:"+jsonString);

//      todo  JSON字符串转对象

//        1.转成JsonObject类型
        JSONObject jsonObject = JSON.parseObject(jsonString);
        JSONArray actorList = jsonObject.getJSONArray("actorList");
        JSONObject jsonObject1 = actorList.getJSONObject(1);
        String name = jsonObject1.getString("name");
        System.out.println(name);

        System.out.println("JsonObject类型:"+jsonObject);
//        2.转成Movie类型
        Movie movie1 = JSON.parseObject(jsonString, Movie.class);
        System.out.println("Movie类型:"+movie1);





    }



    @Data
    public static class Movie{
        String id;
        String name;
        Double doubanScore;
        Director director;
        List<Actor> actorList;

    }

    @Data
    @AllArgsConstructor
    public static class Actor{
        String id;
        String name;
    }

    @Data
    private static class Director{
        String id;
        String name;
    }
}
