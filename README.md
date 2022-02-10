# Demo Application using JPA with Large Data Sets

## To Create project using Spring Initializr
```
curl https://start.spring.io/starter.tgz ^
  -d dependencies=web,actuator,jpa,h2,flyway ^
  -d packageName=com.github.andylke.demo ^
  -d groupId=com.github.andylke.demo ^
  -d artifactId=demo-jpa-large-datasets ^
  -d bootVersion=2.3.8.RELEASE ^
  -d baseDir=demo-jpa-large-datasets ^
  | tar -xzvf -
```
