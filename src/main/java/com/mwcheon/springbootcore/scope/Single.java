package com.mwcheon.springbootcore.scope;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class Single {

    @Autowired
    private Proto proto;

    // 근데 이건 스콥을 프로토로 바꿔서 호출할때마다 바뀌어야 하지만 이미 single이 싱글톤이기 때문에 프로토빈이 바뀌지 않는다..
    // 그게 문제임.. 프로토로 인스턴스를 매번 새롭게 생성해서 불러 와야 하지만 바뀌지 않는다
    // 그럴때는 proxyMode = ScopedProxyMode.TARGET 으로 바꿔야한다.

    public Proto getProto() {
        return proto;
    }
}
