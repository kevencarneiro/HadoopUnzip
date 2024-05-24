plugins {
    id("java")
}

group = "com.kevencarneiro.hadoop"
version = "1.0"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.hadoop:hadoop-common:3.3.0")
    implementation("org.apache.hadoop:hadoop-hdfs:3.3.0")
    implementation("org.apache.hadoop:hadoop-mapreduce-client-core:3.3.0")
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
    useJUnitPlatform()
}

tasks.jar {
    manifest {
        attributes["Main-Class"] = "com.kevencarneiro.hadoop.UnzipDriver"
    }
}