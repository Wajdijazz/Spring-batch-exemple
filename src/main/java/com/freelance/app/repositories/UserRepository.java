package com.freelance.app.repositories;

import org.springframework.data.jpa.repository.JpaRepository;

import com.freelance.app.model.User;

public interface UserRepository  extends JpaRepository<User, Integer>  {

}
