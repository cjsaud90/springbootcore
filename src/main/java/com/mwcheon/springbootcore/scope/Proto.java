package com.mwcheon.springbootcore.scope;


import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.stereotype.Component;

@Component @Scope(value = "0prototype", proxyMode = ScopedProxyMode.TARGET_CLASS)
public class Proto {
    // 여기서 선언된 싱글톤 빈을 scope를 prototype로 변경하게 되면 이 빈을 주입해서 불러올 떄마다 새로운 인스턴스를 불러 오게 된다..
}
