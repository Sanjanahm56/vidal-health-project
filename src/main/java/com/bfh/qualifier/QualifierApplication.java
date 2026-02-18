package com.bfh.qualifier;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.Map;

@SpringBootApplication
public class QualifierApplication {

    public static void main(String[] args) {
        SpringApplication.run(QualifierApplication.class, args);
    }

    @Bean
    CommandLineRunner run() {
        return args -> {

            WebClient webClient = WebClient.builder().build();

            try {

               
                WebhookResponse response = webClient.post()
                        .uri("https://bfhldevapigw.healthrx.co.in/hiring/generateWebhook/JAVA")
                        .header("Content-Type", "application/json")
                        .bodyValue(Map.of(
                                "name", "Sanjana H M",
                                "regNo", "4VM21EE046",
                                "email", "sanjanahm.56@gmail.com"
                        ))
                        .retrieve()
                        .bodyToMono(WebhookResponse.class)
                        .block();

                if (response == null) {
                    System.out.println("Webhook generation failed.");
                    return;
                }

                System.out.println("Access Token Received");

               
                String finalQuery = """
                        SELECT 
                            d.department_name AS DEPARTMENT_NAME,
                            ROUND(AVG(EXTRACT(YEAR FROM AGE(CURRENT_DATE, e.dob))), 2) AS AVERAGE_AGE,
                            STRING_AGG(e.first_name || ' ' || e.last_name, ', ' 
                                       ORDER BY e.first_name || ' ' || e.last_name) 
                                FILTER (WHERE rn <= 10) AS EMPLOYEE_LIST
                        FROM (
                            SELECT DISTINCT e.emp_id, e.first_name, e.last_name, e.dob, e.department,
                                   ROW_NUMBER() OVER (
                                       PARTITION BY e.department
                                       ORDER BY e.first_name || ' ' || e.last_name
                                   ) AS rn
                            FROM employee e
                            JOIN payments p ON e.emp_id = p.emp_id
                            WHERE p.amount > 70000
                        ) e
                        JOIN department d ON e.department = d.department_id
                        GROUP BY d.department_id, d.department_name
                        ORDER BY d.department_id DESC
                        """;

               
                String submissionResponse = webClient.post()
                        .uri("https://bfhldevapigw.healthrx.co.in/hiring/testWebhook/JAVA")
                        .header("Authorization", response.getAccessToken())
                        .header("Content-Type", "application/json")
                        .bodyValue(Map.of("finalQuery", finalQuery))
                        .retrieve()
                        .bodyToMono(String.class)
                        .block();

                System.out.println("Submission Response:");
                System.out.println(submissionResponse);

            } catch (Exception e) {
                e.printStackTrace();
            }
        };
    }
}
