create table users(
	id int not null auto_increment,
    username varchar(45) not null unique,
    pass varchar(32) not null,
    email varchar(105) not null,
    primary key(id)
);