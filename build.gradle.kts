plugins {
    id("java")
    id("maven-publish")
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

publishing {
    repositories {
        maven {
            name = "GitHubPackages"
            url = uri("https://maven.pkg.github.com/OWNER/REPOSITORY")
            credentials {
                username = project.findProperty("gpr.user") as String? ?: System.getenv("USERNAME")
                password = project.findProperty("gpr.key") as String? ?: System.getenv("TOKEN")
            }
        }
    }
    publications {
        register<MavenPublication>("gpr") {
            from(components["java"])
        }
    }
}

tasks.test {
    useJUnitPlatform()
}

tasks.jar {
    manifest {
        attributes["Main-Class"] = "com.kevencarneiro.hadoop.UnzipDriver"
    }
}