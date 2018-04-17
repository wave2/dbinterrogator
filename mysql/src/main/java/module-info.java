module com.dbinterrogator.mysql {
    requires transitive com.dbinterrogator.service;
    requires java.sql;
    provides com.dbinterrogator.service.InterrogatorService with com.dbinterrogator.mysql.MySQLInterrogator;
}