package com.mwcheon.springbootcore;

import com.mwcheon.springbootcore.ioc.ApplicationConfig;
import com.mwcheon.springbootcore.ioc.BookService;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.context.support.GenericApplicationContext;

import java.awt.print.Book;
import java.util.Arrays;
//@SpringBootApplication ->  빈을 자동으로 등록 해준다. 스캔 위치 설정 ( 현재 패키지와 하위 패키지 )
@SpringBootApplication
public class SpringbootcoreApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringbootcoreApplication.class, args);

//       TODO 1. 고전적인 방법으로 스프링 빈을 등록 해보자 @SpringbootApplication 을 빼고 run을 주석 처리 한다
//        그리고 xml을 통해 빈을 등록 한다 -> 일일이 xml에 등록 하는게 번거롭

//        ApplicationContext context = new ClassPathXmlApplicationContext("application.xml");
//        String[] beanDefinitionNames = context.getBeanDefinitionNames();
//        System.out.println(Arrays.toString(beanDefinitionNames));
//        BookService bookService = (BookService) context.getBean("bookService");
//        System.out.println(bookService != null); // 가져온 빈을 확인 null이 아니니까 의존성 주입이 되어 있는


//        TODO 2. 다음 나온 방식이 xml에 컴포넌트 스캔이다
//            <context:component-scan base-package="com.mwcheon.springbootcore"/> << 이런식으로 컴포넌트를 스캔해 빈으로 등록한다
//          빈으로 등록할 class에 @Service @Compponent @Repository 등 설정 하고 @Autowired 의존성 주입을 한다

//        TODO 3. 다음 나온게 자바 설청 파일을 만드는 방법 --> configuration에서 의존성 주입을 하지 않아도 autowired로 넣어줄수 있따
//        ApplicationContext context1 = new AnnotationConfigApplicationContext(ApplicationConfig.class);


//        TODO 4. 위에 것도 귀찮으니까 @ComponentScan으로 빈을 등록한다 하지만 여전히 applicationContext를 만들어야해

//        TODO 5. 그래서 나온게 SpringBootApplication을 쓴다 모든게 다 들어 있음

        //@ Component와 컴포넌트 스캔
        //TODO 빈을 등록하는 또 다란 방법 펑션을 사용한 빈 등록 ( 비추 )
//        var app = new SpringBootApplication(SpringbootcoreApplication.class);
//        app.addInitializers((new ApplicationContextInitializer<GenericApplicationContext>) application ->  {
//          application.registerBean(BookService.class);
//          application.registerBean(ApplicationRunner.class, () -> args1 -> System.out.println("FUNCTAIONAL REGISTER BEAN"));
//        })
//        app.run(args);
//

   }

}
