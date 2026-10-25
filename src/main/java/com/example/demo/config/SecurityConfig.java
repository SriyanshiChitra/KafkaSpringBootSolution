package com.example.demo.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.SecurityFilterChain;

@Configuration
public class SecurityConfig {

	@Bean 
	PasswordEncoder passwordEncoder() {
		return new BCryptPasswordEncoder();
	}

	@Bean 
	UserDetailsService userDetailsService(PasswordEncoder passwordEncoder) {
		var user = org.springframework.security.core.userdetails.User
			.withUsername("admin")
			.password(passwordEncoder.encode("admin"))
			.roles("TESTER")
			.build();
		return new org.springframework.security.provisioning.InMemoryUserDetailsManager(user);
	}
	
	@Bean
	SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
		return http
			.csrf(csrf -> csrf.disable())
			.sessionManagement(s->s.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
			.authorizeHttpRequests(auth -> auth.requestMatchers(HttpMethod.GET, "/actuator/health","/actuator/health/liveliness","/actuator/health/readiness").permitAll().requestMatchers("/api/**").hasRole("TESTER").anyRequest().denyAll())
			.httpBasic(Customizer.withDefaults())
			.build();
	}
}
