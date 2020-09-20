package com.mwcheon.springbootcore.scope;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;

public class AppRunner implements ApplicationRunner  {


    @Autowired
    Single single;

    @Autowired
    Proto proto;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        System.out.println(single);
        System.out.println(single.getProto());
        System.out.println(proto );
        // 여기서 선언된 빈들은 싱글톤이라 같은 인스턴스를 불러온다

    }
}
