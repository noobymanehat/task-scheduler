package dev.scheduler.admin;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * Serves the admin UI: static files under {@code classpath:/admin-ui/}, at {@code /ui/}, with
 * {@code /} forwarding to its page. Only API processes register it. The files live outside
 * Spring Boot's default static locations so a worker-only process serves nothing.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "scheduler.api", name = "enabled", matchIfMissing = true)
public class AdminUiConfiguration implements WebMvcConfigurer {

    @Override
    public void addResourceHandlers(ResourceHandlerRegistry registry) {
        registry.addResourceHandler("/ui/**").addResourceLocations("classpath:/admin-ui/");
    }

    @Override
    public void addViewControllers(ViewControllerRegistry registry) {
        registry.addViewController("/").setViewName("forward:/ui/index.html");
    }
}
