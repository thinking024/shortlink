package com.thinking024.shortlink.admin.config;

import com.thinking024.shortlink.admin.common.biz.user.UserContext;
import feign.RequestInterceptor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * openFeign 微服务调用传递用户信息配置
 * admin管理后台调用project业务中台的时候，顺带把user info传过去
 * 用于project端执行链接管理的相关功能时，验证用户身份，防止横向越权
 */
@Configuration
public class OpenFeignConfiguration {

    @Bean
    public RequestInterceptor requestInterceptor() {
        return template -> {
            template.header("username", UserContext.getUsername());
            template.header("userId", UserContext.getUserId());
            template.header("realName", UserContext.getRealName());
        };
    }
}