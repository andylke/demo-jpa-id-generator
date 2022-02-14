package com.github.andylke.demo.baz;

import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface BazRepository extends JpaRepository<Baz, UUID> {}
