package com.freelance.app.batch;

import java.util.List;

import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.freelance.app.model.User;
import com.freelance.app.repositories.UserRepository;

@Component
public class DBWriter implements ItemWriter<User> {

	@Autowired
	private UserRepository userRepository;

	@Override
	public void write(List<? extends User> users) throws Exception {
		userRepository.saveAll(users);
	}

}
