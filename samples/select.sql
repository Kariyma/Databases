CREATE TABLE `python`.`st_tasks` ( `key` INT NOT NULL AUTO_INCREMENT , `assignee` INT NOT NULL , `status` VARCHAR(60) NOT NULL ,
                                    `updated` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP , `created` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ,
                                    PRIMARY KEY (`key`)) ENGINE = InnoDB;

INSERT INTO `st_tasks` (`key`, `assignee`, `status`, `updated`, `created`)
            VALUES (NULL, '1+RAND()*100', 'Open', current_timestamp(), current_timestamp())

 SUBDATE(NOW(), INTERVAL RAND()*92 DAY)

 INSERT INTO `test_st_tasks` (`key`, `assignee`, `status`, `updated`, `created`)
            VALUES (NULL, CONVERT(CEILING(1+RAND()*20), INTEGER), 'Open',
            ADDDATE(test_st_tasks.created, INTERVAL RAND()*(DATEDIFF(now(), SUBDATE(NOW() , INTERVAL RAND()*181 DAY))) DAY),
            SUBDATE(NOW(), INTERVAL RAND()*181 DAY));

 INSERT INTO `test_st_tasks` (`key`, `assignee`, `status`, `updated`, `created`)
            VALUES (NULL, test_st_tasks.key + 1, 'Open', current_timestamp(), current_timestamp())

 INSERT INTO 'test_st_tasks'
 SET key = NULL,
     assignee = test_st_tasks.key + 1,
     status = 'Open',
     updated = current_timestamp(),
     created = current_timestamp();

select * from test_st_tasks10 where DATEDIFF(NOW(), updated) > 30 and status in ('Open', 'On support side', 'Verifying');

INSERT INTO `test` ('id', 'name', 'pet') VALUES (NULL, 'маша', 'хомяк'), (NULL, 'юля', 'собака'), (NULL, 'света', 'собака'), (NULL, 'маша', 'рыбки');
INSERT INTO `test` (`id`, `name`, `pet`) VALUES (NULL, 'маша', 'попугай'), (NULL, 'света', 'кошка')

INSERT INTO `test` (`id`, `name`, `pet`) VALUES (NULL, 'маша', 'хомяк'), (NULL, '.юля', 'собака');

Задачи в работе у исполнителей
select * from test_st_tasks10 where status in ('Open', 'On support side', 'Verifying') and assignee in (2,3,4,6);

Просроченные задачи в работе у исполнителей !!!
select * from test_st_tasks100 where DATEDIFF(NOW(), updated) > 30 and status in ('Open', 'On support side', 'Verifying') and assignee in (2,3,4,6);


Нагрузка исполнителей
 select GROUP_CONCAT(`key`) as `keys`, assignee, count(*) as total from test_st_tasks10
 where status in ('Open', 'On support side', 'Verifying') and assignee in (2,3,4,6) group by assignee;

Нагрузка на исполнителей по просроченным задачам
select GROUP_CONCAT(`key`) as `keys`, assignee, count(*) as total, 7 as mini from test_st_tasks100
where DATEDIFF(NOW(), updated) > 30 and status in ('Open', 'On support side', 'Verifying') and assignee in (2,3,4,6) group by assignee order by total DESC;

select tt.* from (
select GROUP_CONCAT(`key`) as `keys`, assignee, count(*) as total, 7 as mini from test_st_tasks100
where DATEDIFF(NOW(), updated) > 30 and status in ('Open', 'On support side', 'Verifying') and assignee in (2,3,4,6) group by assignee order by total DESC) as tt;


Таблица оптимизации нагрузки исполнителей!
select tt.*,  ((tt.total - tt.mini) in (0, 1)) as opim, IF(tt.total <= tt.mini, tt.mini - tt.total, tt.mini + 1 - tt.total) as imper, ((tt.total <= tt.mini)*2 - 1) as facul
from (
select GROUP_CONCAT(`key`) as `keys`, assignee, count(*) as total, 7 as mini from test_st_tasks100
where DATEDIFF(NOW(), updated) > 30 and status in ('Open', 'On support side', 'Verifying') and assignee in (2,3,4,6) group by assignee order by total DESC) as tt order by opim, total DESC;


 Общее число задач в работе
 select count(*) as total from test_st_tasks10 where status in ('Open', 'On support side', 'Verifying') and assignee in (2,3,4,6);



select GROUP_CONCAT(`key`) as `keys`, assignee, count(*) as total from test_st_tasks10
where status in ('Open', 'On support side', 'Verifying') and assignee in (2,3,4,6) group by assignee;

 select a.assignee, a.total, (a.total >= 1 and a.total <= 2) as optim, (1-a.total) as imper,
 (2-a.total) facul from (select GROUP_CONCAT(`key`) as `keys`, assignee, count(*) as total from test_st_tasks10
 where status in ('Open', 'On support side', 'Verifying') and assignee in (2,3,4,6) group by assignee) as a;