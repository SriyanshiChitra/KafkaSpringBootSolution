package com.example.demo.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.web.SecurityFilterChain;

@Configuration
public class SecurityConfig {

	@Bean
	SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
		http
			.httpBasic(Customizer.withDefaults())
			.csrf(csrf -> csrf.ignoringRequestMatchers("/api/messages"))
			.authorizeHttpRequests(authorize -> authorize
				.anyRequest().authenticated());
		return http.build();
	}
}
