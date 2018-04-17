module com.dbinterrogator.cli {
    requires com.dbinterrogator.service;
    requires ch.qos.logback.core;
    requires ch.qos.logback.classic;
    requires org.slf4j;
    requires info.picocli;
    opens com.dbinterrogator.cli to info.picocli;
    uses com.dbinterrogator.service.InterrogatorService;
}