package org.llf.bigdata.study.one.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

/**
 * Description 类描述
 *
 * @author luolifeng
 * @version 1.0.0
 * Date 2019-10-22
 * Time 15:46
 */
@SpringBootApplication
public class OneProducerApp {
    @Autowired
    ProducerMain producerMain;
    public static void main(String[] args) {
        new SpringApplicationBuilder(OneProducerApp.class).web(WebApplicationType.NONE).run(args);
        OneProducerApp app=new OneProducerApp();

        for(int i=0;i<10;i++){
            try {
                app.sendMsg(String.valueOf(i),"这是第"+i+"条消息");
            } catch (Exception e) {
                System.out.println(i+"异常:"+e.getMessage());
            }
        }
    }

    public  void sendMsg(String key,String msg) throws Exception {
        producerMain.producer(key,msg);
    }
}
