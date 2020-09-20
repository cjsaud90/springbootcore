package com.mwcheon.springbootcore.autowire;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.List;

@Service
public class BookService  {


//    1. 생성자로 의존성 주입
// BookRepository bookRepository;

// @Autowired(required = false)
// public BookService(BookRepository bookRepository){
//     this.bookRepository = bookRepository;
// }
//
    // primary 에 해당하는 레파지토리 빈을 가져온다
    @Autowired
    BookRepository bookRepository;

    // 모두 다 가져오려면
    @Autowired
    List<BookRepository> bookRepositories;

    public void printBookRepostory(){
        this.bookRepositories.forEach(System.out::println);
        System.out.println(bookRepository.getClass());
    }

//    @PostConstruct

}
