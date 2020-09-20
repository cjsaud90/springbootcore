package com.mwcheon.springbootcore.scope;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ScopeApplication {
    public static void main(String[] args) {
        SpringApplication.run(ScopeApplication.class, args);

    }
}

//TODO 싱글톤 객체 사용시 주의할 점 - 프로퍼티가 공유, ApplicationContext 초기 구동시 인스턴스 생성
