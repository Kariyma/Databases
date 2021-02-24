CREATE TABLE `python`.`st_tasks` ( `key` INT NOT NULL AUTO_INCREMENT , `assignee` INT NOT NULL , `status` VARCHAR(60) NOT NULL ,
                                    `updated` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP , `created` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ,
                                    PRIMARY KEY (`key`)) ENGINE = InnoDB;

INSERT INTO `st_tasks` (`key`, `assignee`, `status`, `updated`, `created`)
            VALUES (NULL, '1+RAND()*100', 'Open', current_timestamp(), current_timestamp())

 SUBDATE(NOW(), INTERVAL RAND()*92 DAY)