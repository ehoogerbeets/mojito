package com.box.l10n.mojito.quartz;

import org.quartz.JobDetail;
import org.quartz.SimpleTrigger;
import org.quartz.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.quartz.JobDetailFactoryBean;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;
import org.springframework.scheduling.quartz.SimpleTriggerFactoryBean;
import org.springframework.scheduling.quartz.SpringBeanJobFactory;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Configuration
@ConfigurationProperties("l10n.org")
public class QuartzPropertiesConfig {

    Logger logger = LoggerFactory.getLogger(getClass());

    Map<String, String> quartz = new HashMap<>();

    public Map<String, String> getQuartz() {
        return quartz;
    }

    @Bean
    public Properties getQuartzProperties() {

        Properties properties = new Properties();

        for (Map.Entry<String, String> entry : quartz.entrySet()) {
            properties.put("org.quartz." + entry.getKey(), entry.getValue());
        }

        return properties;
    }
}