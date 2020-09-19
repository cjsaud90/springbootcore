package com.mwcheon.springbootcore.autowire;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class BookService  {


//    1. 생성자로 의존성 주입
// BookRepository bookRepository;

// @Autowired(required = false)
// public BookService(BookRepository bookRepository){
//     this.bookRepository = bookRepository;
// }
//
    @Autowired
    BookRepository bookRepository;

    public void printBookRepostory(){
        System.out.println(bookRepository.getClass());
    }

}
